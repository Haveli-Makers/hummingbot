import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.coinswitch_perpetual import coinswitch_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_api_user_stream_data_source import (
    CoinswitchPerpetualAPIUserStreamDataSource,
)


class CoinswitchPerpetualUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector._request_wallet_balance = AsyncMock(return_value={"data": {"base_asset_balances": []}})
        self.connector._request_positions = AsyncMock(return_value={"data": []})
        self.connector._fetch_order_snapshots = AsyncMock(return_value=[({"order_id": "o1"}, True)])
        self.connector.in_flight_orders = {}
        self.data_source = CoinswitchPerpetualAPIUserStreamDataSource(
            auth=MagicMock(), trading_pairs=["BTC-USDT", "ETH-USDT"], connector=self.connector,
            api_factory=MagicMock())
        self.data_source._time = lambda: 1234.0
        self.data_source._sleep = AsyncMock()
        self.queue = asyncio.Queue()

    async def test_balance_poll(self):
        self.assertEqual(0, self.data_source.last_recv_time)
        await self.data_source._poll_balance(self.queue)
        self.assertEqual({"channel": "balance", "data": {"data": {"base_asset_balances": []}}}, self.queue.get_nowait())
        self.assertEqual(1234.0, self.data_source.last_recv_time)

    async def test_positions_poll_is_one_call_per_pair(self):
        await self.data_source._poll_positions(self.queue)
        self.assertEqual(["BTC-USDT", "ETH-USDT"],
                         [call.args[0] for call in self.connector._request_positions.call_args_list])
        self.assertEqual("BTC-USDT", self.queue.get_nowait()["trading_pair"])
        self.assertEqual({"channel": "positions", "trading_pair": "ETH-USDT", "data": {"data": []}},
                         self.queue.get_nowait())

    def test_positions_interval_scales_with_the_pair_count(self):
        self.assertEqual(10.0, self.data_source._positions_poll_interval())
        self.data_source._trading_pairs = ["BTC-USDT"]
        self.assertEqual(CONSTANTS.MIN_POSITIONS_POLL_INTERVAL, self.data_source._positions_poll_interval())
        self.data_source._trading_pairs = [f"P{i}-USDT" for i in range(6)]
        self.assertEqual(30.0, self.data_source._positions_poll_interval())

    async def test_orders_poll_is_skipped_while_nothing_is_in_flight(self):
        pending = MagicMock(exchange_order_id=None)
        self.connector.in_flight_orders = {"a": pending}
        await self.data_source._poll_orders(self.queue)
        self.connector._fetch_order_snapshots.assert_not_called()
        self.assertTrue(self.queue.empty())
        self.assertEqual(0, self.data_source.last_recv_time)

    async def test_orders_poll_emits_snapshots(self):
        live = MagicMock(exchange_order_id="o1")
        self.connector.in_flight_orders = {"a": live, "b": MagicMock(exchange_order_id=None)}
        await self.data_source._poll_orders(self.queue)
        self.connector._fetch_order_snapshots.assert_awaited_once_with([live])
        self.assertEqual({"channel": "orders", "data": [{"order": {"order_id": "o1"}, "working": True}]},
                         self.queue.get_nowait())

        self.connector._fetch_order_snapshots = AsyncMock(return_value=[])
        await self.data_source._poll_orders(self.queue)
        self.assertTrue(self.queue.empty())

    async def test_poll_loop_backs_off_and_resets(self):
        poll = AsyncMock(side_effect=[IOError("429"), IOError("429"), None, asyncio.CancelledError])
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._poll_forever(poll, self.queue, lambda: 4.0, "orders")
        self.assertEqual([8.0, 16.0, 4.0], [call.args[0] for call in self.data_source._sleep.call_args_list])

    async def test_backoff_is_capped(self):
        poll = AsyncMock(side_effect=[IOError("down")] * 6 + [asyncio.CancelledError])
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._poll_forever(poll, self.queue, lambda: 5.0, "wallet")
        self.assertEqual(CONSTANTS.MAX_POLL_BACKOFF_INTERVAL, self.data_source._sleep.call_args_list[-1].args[0])

    async def test_listen_runs_all_three_loops(self):
        self.data_source._sleep = AsyncMock(side_effect=asyncio.CancelledError)
        self.connector.in_flight_orders = {"a": MagicMock(exchange_order_id="o1")}
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_user_stream(self.queue)
        self.connector._request_wallet_balance.assert_awaited()
        self.connector._request_positions.assert_awaited()
        self.connector._fetch_order_snapshots.assert_awaited()

    async def test_websocket_hooks_are_not_used(self):
        with self.assertRaises(NotImplementedError):
            await self.data_source._connected_websocket_assistant()
        with self.assertRaises(NotImplementedError):
            await self.data_source._subscribe_channels(MagicMock())
        await self.data_source._process_event_message({"x": 1}, self.queue)
        self.assertTrue(self.queue.empty())
