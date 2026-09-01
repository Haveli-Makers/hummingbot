import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_api_user_stream_data_source import ZebpayAPIUserStreamDataSource
from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder


def _make_connector() -> ZebpayExchange:
    ex = ZebpayExchange(
        zebpay_api_key="key", zebpay_api_secret="secret",
        trading_pairs=["BTC-INR"], trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC-INR": "BTC-INR"}))
    return ex


class ZebpayUserStreamDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = ZebpayAPIUserStreamDataSource(
            auth=MagicMock(), trading_pairs=["BTC-INR"], connector=self.connector, api_factory=MagicMock(),
        )

    def test_extract_orders_paginated(self):
        resp = {"data": {"items": [{"orderId": "1"}, {"orderId": "2"}], "totalNum": 2}}
        self.assertEqual(2, len(ZebpayAPIUserStreamDataSource._extract_orders(resp)))

    async def test_poll_balance_emits_event(self):
        output = asyncio.Queue()
        self.connector._api_get = AsyncMock(
            return_value={"data": [{"currency": "BTC", "total": "1", "free": "1", "used": "0"}]})
        await self.source._poll_balance(output)
        self.assertEqual("balance_update", output.get_nowait()["event"])

    async def test_poll_account_trades_emits_fills(self):
        # Realtime account fills: an in-flight order's fills are fetched and emitted.
        order = InFlightOrder(
            client_order_id="ZEB-1", exchange_order_id="ord-9", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1"),
            price=Decimal("100"), creation_timestamp=1_700_000_000.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        self.connector._api_get = AsyncMock(
            return_value={"data": {"fills": [{"id": "f1", "price": "100", "amount": "0.5"}]}})
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        event = output.get_nowait()
        self.assertEqual("trade_update", event["event"])
        self.assertEqual("ord-9", event["data"][0]["orderId"])
        self.assertEqual(1, len(event["data"][0]["fills"]))

    async def test_poll_account_trades_no_inflight_no_event(self):
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        self.assertTrue(output.empty())

    def test_extract_orders_plain_list(self):
        self.assertEqual(1, len(ZebpayAPIUserStreamDataSource._extract_orders({"data": [{"orderId": "1"}]})))

    def test_extract_orders_empty(self):
        self.assertEqual([], ZebpayAPIUserStreamDataSource._extract_orders({"data": {"items": []}}))

    def test_last_recv_time_initial(self):
        self.assertEqual(0.0, self.source.last_recv_time)

    async def test_listen_emits_balance_event(self):
        output = asyncio.Queue()
        count = 0

        async def fake_api_get(path_url, **kwargs):
            nonlocal count
            count += 1
            if count == 1:
                return {"data": [{"currency": "BTC", "total": "1", "free": "1", "used": "0"}]}
            raise asyncio.CancelledError()

        self.connector._api_get = fake_api_get
        try:
            await asyncio.wait_for(self.source.listen_for_user_stream(output), timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        found = False
        while not output.empty():
            if output.get_nowait().get("event") == "balance_update":
                found = True
        self.assertTrue(found)

    async def test_subscribe_noops(self):
        await self.source._subscribe_to_user_stream()
        await self.source._unsubscribe_from_user_stream()

    def _track(self, oid="ord-9"):
        order = InFlightOrder(
            client_order_id=f"ZEB-{oid}", exchange_order_id=oid, trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        return order

    # ── Finding 6: terminal state fetched when order leaves the ACTIVE set ────
    async def test_settled_order_fetched_when_leaves_active(self):
        self._track("ord-9")
        self.source._active_order_ids = {"ord-9"}  # was active last poll
        self.connector._api_get = AsyncMock(
            return_value={"data": {"orderId": "ord-9", "status": "FILLED", "updatedAt": 2}})

        # This poll's ACTIVE list no longer contains ord-9.
        settled = await self.source._fetch_settled_order_updates(active_ids=set())
        self.assertEqual(1, len(settled))
        self.assertEqual("FILLED", settled[0]["status"])
        self.assertNotIn("ord-9", self.source._active_order_ids)

    async def test_no_settled_fetch_when_still_active(self):
        self._track("ord-9")
        self.source._active_order_ids = {"ord-9"}
        self.connector._api_get = AsyncMock(side_effect=AssertionError("should not fetch"))

        settled = await self.source._fetch_settled_order_updates(active_ids={"ord-9"})
        self.assertEqual([], settled)
        self.assertIn("ord-9", self.source._active_order_ids)

    # ── Efficiency: per-order fill requests are issued concurrently ───────────
    async def test_poll_account_trades_issues_requests_concurrently(self):
        # The per-order /fills requests must overlap. Sequentially N orders cost
        # N x latency per cycle and the 2s loop falls behind itself as orders grow.
        for oid in ("ord-1", "ord-2", "ord-3"):
            self._track(oid)

        in_flight = 0
        peak = 0

        async def slow_get(path_url, **kwargs):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.05)
            in_flight -= 1
            return {"data": {"fills": [{"id": "f1", "price": "100", "amount": "0.5"}]}}

        self.connector._api_get = slow_get
        await self.source._poll_account_trades(asyncio.Queue())
        self.assertEqual(3, peak, "per-order fill requests were issued sequentially")

    async def test_poll_account_trades_isolates_per_order_failure(self):
        # One order's failure must not lose the other order's fills.
        self._track("ord-1")
        self._track("ord-2")

        async def flaky_get(path_url, params=None, **kwargs):
            if params and params.get("orderId") == "ord-1":
                raise IOError("boom")
            return {"data": {"fills": [{"id": "f2", "price": "100", "amount": "0.5"}]}}

        self.connector._api_get = flaky_get
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        event = output.get_nowait()
        self.assertEqual(["ord-2"], [e["orderId"] for e in event["data"]])

    # ── Outage backoff: independent loops must not all retry at full cadence ──
    async def test_poll_forever_backs_off_exponentially_on_repeated_failure(self):
        delays = []

        async def always_fails(_output):
            raise IOError("exchange down")

        async def fake_sleep(d):
            delays.append(d)
            if len(delays) >= 4:
                raise asyncio.CancelledError()

        with patch.object(asyncio, "sleep", side_effect=fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.source._poll_forever(always_fails, asyncio.Queue(), 2.0, "test")

        # 2s base doubling per consecutive failure: 4, 8, 16, 32 …
        self.assertEqual([4.0, 8.0, 16.0, 32.0], delays)

    async def test_poll_forever_resets_backoff_after_success(self):
        delays = []
        calls = {"n": 0}

        async def fails_then_succeeds(_output):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise IOError("blip")

        async def fake_sleep(d):
            delays.append(d)
            if len(delays) >= 3:
                raise asyncio.CancelledError()

        with patch.object(asyncio, "sleep", side_effect=fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.source._poll_forever(fails_then_succeeds, asyncio.Queue(), 2.0, "test")

        # Two failures back off, then the first success returns to the base cadence.
        self.assertEqual([4.0, 8.0, 2.0], delays)

    async def test_poll_forever_backoff_is_capped(self):
        delays = []

        async def always_fails(_output):
            raise IOError("exchange down")

        async def fake_sleep(d):
            delays.append(d)
            if len(delays) >= 10:
                raise asyncio.CancelledError()

        with patch.object(asyncio, "sleep", side_effect=fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.source._poll_forever(always_fails, asyncio.Queue(), 2.0, "test")

        self.assertEqual(CONSTANTS.MAX_POLL_BACKOFF_INTERVAL, max(delays))


if __name__ == "__main__":
    unittest.main()
