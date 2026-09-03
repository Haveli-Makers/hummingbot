import asyncio
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS
from hummingbot.connector.exchange.csx.csx_api_user_stream_data_source import CsxAPIUserStreamDataSource
from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder

_VALID_SECRET = "aa" * 32


def _make_connector() -> CsxExchange:
    ex = CsxExchange(
        csx_api_key="key",
        csx_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC/INR": "BTC-INR"}))
    return ex


class CsxUserStreamDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = CsxAPIUserStreamDataSource(
            auth=MagicMock(),
            trading_pairs=["BTC-INR"],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    async def test_listen_for_user_stream_emits_balance_event(self):
        output = asyncio.Queue()
        balance_resp = {"Available": {"BTC": "1.0"}, "Locked": {}}

        call_count = 0

        async def fake_api_get(path_url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return balance_resp
            if call_count == 2:
                return {"orders": []}
            raise asyncio.CancelledError()

        self.connector._api_get = fake_api_get

        try:
            await asyncio.wait_for(self.source.listen_for_user_stream(output), timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        # At least the balance event should have been put
        found_balance = False
        while not output.empty():
            event = output.get_nowait()
            if event.get("event") == "balance_update":
                found_balance = True
        self.assertTrue(found_balance)

    async def test_subscribe_and_unsubscribe_are_noops(self):
        # Should complete without error
        await self.source._subscribe_to_user_stream()
        await self.source._unsubscribe_from_user_stream()

    def test_last_recv_time_initial_value(self):
        self.assertEqual(0.0, self.source.last_recv_time)

    async def test_terminal_state_fetched_when_order_leaves_open_set(self):
        # Finding #6: an order that was OPEN last poll but is gone from the
        # onlyOpen=true response has settled; its terminal status must be fetched
        # and surfaced, since the stream would otherwise never emit it.
        order = InFlightOrder(
            client_order_id="x-CSX-6",
            exchange_order_id="oid-6",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        self.source._open_order_ids = {"oid-6"}  # it was open in the prior poll

        self.connector._api_get = AsyncMock(
            return_value={"orderId": "oid-6", "status": "FULFILLED", "updatedAt": 2})

        # This poll's open-orders list no longer contains oid-6.
        settled = await self.source._fetch_settled_order_updates(open_orders=[])
        self.assertEqual(1, len(settled))
        self.assertEqual("FULFILLED", settled[0]["status"])
        self.assertNotIn("oid-6", self.source._open_order_ids)

    async def test_no_terminal_fetch_when_order_still_open(self):
        # An order that is still present in the open list must NOT trigger an
        # extra per-id status fetch.
        order = InFlightOrder(
            client_order_id="x-CSX-7",
            exchange_order_id="oid-7",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        self.source._open_order_ids = {"oid-7"}
        self.connector._api_get = AsyncMock(side_effect=AssertionError("should not fetch"))

        settled = await self.source._fetch_settled_order_updates(
            open_orders=[{"orderId": "oid-7", "status": "OPEN"}])
        self.assertEqual([], settled)
        self.assertIn("oid-7", self.source._open_order_ids)

    async def test_poll_balance_emits_event(self):
        output = asyncio.Queue()
        self.connector._api_get = AsyncMock(return_value={"Available": {"BTC": "1.0"}, "Locked": {}})
        await self.source._poll_balance(output)
        self.assertEqual("balance_update", output.get_nowait()["event"])

    async def test_poll_account_trades_emits_fills(self):
        # Realtime account fills: an in-flight order's status (cumulative
        # filledQuantity) is fetched and emitted as a trade_update event.
        order = InFlightOrder(
            client_order_id="x-CSX-9", exchange_order_id="oid-9", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1"),
            price=Decimal("100"), creation_timestamp=1.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        self.connector._api_get = AsyncMock(return_value={
            "orderId": "oid-9", "status": "PARTIALLY_FULFILLED",
            "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1})
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        event = output.get_nowait()
        self.assertEqual("trade_update", event["event"])
        self.assertEqual("oid-9", event["data"][0]["orderId"])

    async def test_poll_account_trades_no_inflight_no_event(self):
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        self.assertTrue(output.empty())

    def _track(self, oid: str) -> InFlightOrder:
        order = InFlightOrder(
            client_order_id=f"x-CSX-{oid}", exchange_order_id=oid, trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("1"),
            price=Decimal("100"), creation_timestamp=1.0,
        )
        self.connector._order_tracker.start_tracking_order(order)
        return order

    # ── Efficiency: per-order requests are issued concurrently ────────────────
    async def test_poll_account_trades_issues_requests_concurrently(self):
        for oid in ("oid-1", "oid-2", "oid-3"):
            self._track(oid)

        in_flight = 0
        peak = 0

        async def slow_get(path_url, **kwargs):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.05)
            in_flight -= 1
            return {"orderId": path_url.rsplit("/", 1)[-1], "status": "OPEN",
                    "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1}

        self.connector._api_get = slow_get
        await self.source._poll_account_trades(asyncio.Queue())
        self.assertEqual(3, peak, "per-order status requests were issued sequentially")

    async def test_poll_account_trades_isolates_per_order_failure(self):
        self._track("oid-1")
        self._track("oid-2")

        async def flaky_get(path_url, **kwargs):
            if path_url.endswith("oid-1"):
                raise IOError("boom")
            return {"orderId": "oid-2", "status": "OPEN",
                    "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1}

        self.connector._api_get = flaky_get
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        self.assertEqual(["oid-2"], [d["orderId"] for d in output.get_nowait()["data"]])

    # ── Duplication: the batch already carries the per-order fill data ────────
    async def test_active_orders_batch_emits_trade_update(self):
        # GET /me/orders?onlyOpen=true already returns filledQuantity for every open
        # order, so the fills are emitted straight from that one response.
        self._track("oid-1")
        self.connector._api_get = AsyncMock(return_value={"data": [{
            "orderId": "oid-1", "status": "OPEN",
            "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1,
        }]})
        output = asyncio.Queue()
        await self.source._poll_active_orders(output)
        events = [output.get_nowait() for _ in range(output.qsize())]
        kinds = [e["event"] for e in events]
        self.assertIn("trade_update", kinds)
        self.assertIn("order_update", kinds)
        # Fills must be recorded before the (possibly terminal) state transition.
        self.assertLess(kinds.index("trade_update"), kinds.index("order_update"))

    async def test_account_trades_skips_orders_the_batch_already_covered(self):
        # No second per-order request for an order whose filledQuantity the batch
        # already reported — that was double the API volume every 2s.
        self._track("oid-1")
        self.source._batch_filled_ids = {"oid-1"}
        self.source._batch_filled_ts = time.time()
        # Assert on the call count, NOT on a raising side_effect: the requests go
        # through safe_gather(return_exceptions=True), which captures an exception
        # as a result instead of propagating it — a tripwire that raises would be
        # swallowed and the test would pass even with the skip removed.
        mock_get = AsyncMock(return_value={
            "orderId": "oid-1", "status": "OPEN",
            "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1})
        self.connector._api_get = mock_get
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        mock_get.assert_not_called()
        self.assertTrue(output.empty())

    async def test_account_trades_polls_orders_the_batch_did_not_cover(self):
        # A brand-new order not yet seen in a batch still gets polled individually.
        self._track("oid-2")
        self.source._batch_filled_ids = {"oid-1"}
        self.source._batch_filled_ts = time.time()
        self.connector._api_get = AsyncMock(return_value={
            "orderId": "oid-2", "status": "OPEN",
            "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1})
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        self.assertEqual("oid-2", output.get_nowait()["data"][0]["orderId"])

    async def test_account_trades_ignores_stale_batch_coverage(self):
        # If the active-orders loop stalls (or is backing off), its coverage set goes
        # stale and must be ignored, or these orders would silently stop being polled.
        self._track("oid-1")
        self.source._batch_filled_ids = {"oid-1"}
        self.source._batch_filled_ts = time.time() - (CONSTANTS.ACTIVE_ORDERS_POLL_INTERVAL * 10)
        self.connector._api_get = AsyncMock(return_value={
            "orderId": "oid-1", "status": "OPEN",
            "filledQuantity": "0.5", "filledQuoteQuantity": "50", "updatedAt": 1})
        output = asyncio.Queue()
        await self.source._poll_account_trades(output)
        self.assertEqual("oid-1", output.get_nowait()["data"][0]["orderId"])

    async def test_batch_coverage_requires_filled_quantity_field(self):
        # If CSX ever drops filledQuantity from the batch endpoint, nothing may be
        # skipped — the per-order path has to silently take over again.
        self._track("oid-1")
        self.connector._api_get = AsyncMock(return_value={"data": [
            {"orderId": "oid-1", "status": "OPEN", "updatedAt": 1},  # no filledQuantity
        ]})
        await self.source._poll_active_orders(asyncio.Queue())
        self.assertEqual(set(), self.source._batch_filled_ids)

    # ── Outage backoff ────────────────────────────────────────────────────────
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

        self.assertEqual([4.0, 8.0, 2.0], delays)


if __name__ == "__main__":
    unittest.main()
