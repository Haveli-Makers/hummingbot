import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from bidict import bidict

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


if __name__ == "__main__":
    unittest.main()
