import asyncio
from decimal import Decimal
from test.hummingbot.connector.derivative.wazirx_perpetual.test_wazirx_perpetual_derivative import EXCHANGE_INFO
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.wazirx_perpetual import wazirx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_api_order_book_data_source import (
    WazirxPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_order_book import WazirxPerpetualOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.web_assistant.connections.data_types import WSResponse

# Frames as captured from wss://fstreamx.wazirx.com/stream (levels trimmed).
DEPTH_FRAME = {
    "data": {"E": 1791362785184, "T": 1791362785182, "e": "depthUpdate", "s": "btcinr",
             "a": [["8031463", "4.558"], ["8031473", "0.003"]],
             "b": [["8031358", "2.528"], ["8031348", "0.004"]]},
    "stream": "btcinr@depth",
}
TRADE_FRAME = {
    "data": {"E": 1791362785814, "T": 1791362785664, "e": "aggTrade", "m": True, "p": "8031358", "q": "2.381",
             "s": "btcinr"},
    "stream": "btcinr@aggTrade",
}
MARK_PRICE_FRAME = {
    "data": [
        {"E": 1791362784000, "T": 1791388800000, "e": "markPriceUpdate", "i": "8035595", "p": "8031148",
         "r": "-0.000042003", "s": "btcinr"},
        {"E": 1791362784000, "T": 1791388800000, "e": "markPriceUpdate", "i": "126114", "p": "126072",
         "r": "0.00002534", "s": "zecinr"},
    ],
    "stream": "!markPrice@arr",
}


class WazirxPerpetualOrderBookTests(IsolatedAsyncioWrapperTestCase):
    def test_rest_snapshot(self):
        msg = WazirxPerpetualOrderBook.snapshot_message_from_exchange(
            {"E": 1791362757953, "T": 1791362757953, "bids": [["8030600", "0.002"]], "asks": [["8030650", "3.347"]]},
            1791362758.0, metadata={"trading_pair": "BTC-INR"})
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(1791362757953, msg.update_id)
        self.assertEqual(8030600.0, msg.bids[0].price)
        self.assertEqual(3.347, msg.asks[0].amount)

    def test_stream_depth_frame_is_a_snapshot(self):
        msg = WazirxPerpetualOrderBook.snapshot_message_from_exchange(
            DEPTH_FRAME["data"], 1791362785.2, metadata={"trading_pair": "BTC-INR"})
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(2, len(msg.bids))
        self.assertEqual(8031358.0, msg.bids[0].price)
        self.assertEqual(8031463.0, msg.asks[0].price)

    def test_trade_side_from_maker_flag(self):
        sell = WazirxPerpetualOrderBook.trade_message_from_exchange(TRADE_FRAME["data"], {"trading_pair": "BTC-INR"})
        self.assertEqual(float(TradeType.SELL.value), sell.content["trade_type"])
        self.assertEqual(8031358.0, sell.content["price"])
        self.assertEqual(2.381, sell.content["amount"])
        self.assertAlmostEqual(1791362785.664, sell.timestamp, places=3)

        buy = WazirxPerpetualOrderBook.trade_message_from_exchange(
            {**TRADE_FRAME["data"], "m": False}, {"trading_pair": "BTC-INR"})
        self.assertEqual(float(TradeType.BUY.value), buy.content["trade_type"])


class WazirxPerpetualAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = WazirxPerpetualDerivative(
            wazirx_perpetual_api_key="", wazirx_perpetual_api_secret="",
            trading_pairs=["BTC-INR"], trading_required=False)
        self.connector._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.data_source = WazirxPerpetualAPIOrderBookDataSource(
            trading_pairs=["BTC-INR"], connector=self.connector, api_factory=MagicMock())

    async def test_subscribes_depth_trades_and_mark_price(self):
        ws = AsyncMock()
        await self.data_source._subscribe_channels(ws)
        payload = ws.send.call_args.args[0].payload
        self.assertEqual("subscribe", payload["event"])
        self.assertEqual(["btcinr@depth", "btcinr@aggTrade", "!markPrice@arr"], payload["streams"])

    def test_channel_routing(self):
        ds = self.data_source
        self.assertEqual(ds._snapshot_messages_queue_key, ds._channel_originating_message(DEPTH_FRAME))
        self.assertEqual(ds._trade_messages_queue_key, ds._channel_originating_message(TRADE_FRAME))
        self.assertEqual(ds._funding_info_messages_queue_key, ds._channel_originating_message(MARK_PRICE_FRAME))
        self.assertEqual("", ds._channel_originating_message({"event": "pong", "data": {}}))

    async def test_parsers(self):
        await self.data_source._subscribe_channels(AsyncMock())
        out = asyncio.Queue()

        await self.data_source._parse_order_book_snapshot_message(DEPTH_FRAME, out)
        snapshot = out.get_nowait()
        self.assertEqual("BTC-INR", snapshot.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)

        await self.data_source._parse_trade_message(TRADE_FRAME, out)
        self.assertEqual("BTC-INR", out.get_nowait().trading_pair)

        await self.data_source._parse_funding_info_message(MARK_PRICE_FRAME, out)
        update = out.get_nowait()
        self.assertIsInstance(update, FundingInfoUpdate)
        self.assertEqual("BTC-INR", update.trading_pair)
        self.assertEqual(Decimal("8031148"), update.mark_price)
        self.assertEqual(Decimal("8035595"), update.index_price)
        self.assertEqual(Decimal("-0.000042003"), update.rate)
        self.assertEqual(1791388800, update.next_funding_utc_timestamp)
        # zecinr is not subscribed.
        self.assertTrue(out.empty())

    async def test_unknown_symbol_is_dropped(self):
        out = asyncio.Queue()
        await self.data_source._parse_order_book_snapshot_message(
            {"data": {**DEPTH_FRAME["data"], "s": "nosuchinr"}, "stream": "nosuchinr@depth"}, out)
        self.assertTrue(out.empty())

    async def test_frames_flow_from_socket_to_parsed_output(self):
        """The path a running strategy uses: socket -> raw queue -> listen_for_* -> parsed message."""
        await self.data_source._subscribe_channels(AsyncMock())
        ws = MagicMock()
        ws.send = AsyncMock()

        async def _messages():
            await asyncio.sleep(0)  # let the ping task start, as it would on a live socket
            for frame in ({"data": {"timeout_duration": 1800}, "event": "connected"},
                          {"data": {"streams": ["btcinr@depth"]}, "event": "subscribed", "id": 0},
                          DEPTH_FRAME, TRADE_FRAME, MARK_PRICE_FRAME):
                yield WSResponse(data=frame)

        ws.iter_messages = _messages
        await self.data_source._process_websocket_messages(ws)

        snapshots, trades, funding = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        tasks = [
            asyncio.ensure_future(self.data_source.listen_for_order_book_snapshots(asyncio.get_event_loop(), snapshots)),
            asyncio.ensure_future(self.data_source.listen_for_trades(asyncio.get_event_loop(), trades)),
            asyncio.ensure_future(self.data_source.listen_for_funding_info(funding)),
        ]
        try:
            self.assertEqual("BTC-INR", (await asyncio.wait_for(snapshots.get(), 1)).trading_pair)
            self.assertEqual("BTC-INR", (await asyncio.wait_for(trades.get(), 1)).trading_pair)
            self.assertEqual("BTC-INR", (await asyncio.wait_for(funding.get(), 1)).trading_pair)
        finally:
            for task in tasks:
                task.cancel()

    async def test_unacknowledged_streams_are_reported(self):
        await self.data_source._subscribe_channels(AsyncMock())
        logger = MagicMock()
        self.data_source.logger = lambda: logger
        await self.data_source._process_message_for_unknown_channel(
            {"data": {"streams": None}, "event": "subscribed", "id": 0}, AsyncMock())
        self.assertIn("did not acknowledge", logger.warning.call_args.args[0])

    async def test_rest_snapshot_request(self):
        rest = AsyncMock()
        rest.execute_request = AsyncMock(return_value={
            "E": 1791362757953, "T": 1791362757953, "bids": [["8030600", "0.002"]], "asks": [["8030650", "3.347"]]})
        self.data_source._api_factory.get_rest_assistant = AsyncMock(return_value=rest)

        msg = await self.data_source._order_book_snapshot("BTC-INR")

        kwargs = rest.execute_request.call_args.kwargs
        self.assertEqual({"symbol": "BTCINR", "limit": CONSTANTS.ORDER_BOOK_DEPTH}, kwargs["params"])
        self.assertEqual(CONSTANTS.DEPTH_PATH_URL, kwargs["throttler_limit_id"])
        self.assertEqual("BTC-INR", msg.trading_pair)

    async def test_ping_loop_sends_application_ping(self):
        ws = AsyncMock()
        calls = []

        async def _sleep(delay):
            calls.append(delay)
            if len(calls) > 1:
                raise asyncio.CancelledError

        self.data_source._sleep = _sleep
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._ping_loop(ws)
        self.assertEqual({"event": "ping"}, ws.send.call_args.args[0].payload)
        self.assertEqual(CONSTANTS.WS_PING_INTERVAL, calls[0])
