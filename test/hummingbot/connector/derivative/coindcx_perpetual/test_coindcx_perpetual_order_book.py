import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_order_book_data_source import (
    CoinDCXPerpetualAPIOrderBookDataSource,
    unwrap_frame,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_order_book import CoinDCXPerpetualOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

# Real payloads captured from wss://stream.coindcx.com.
DEPTH_PAYLOAD = {
    "ts": 1785133949849,
    "vs": 210599929,
    "asks": {"65480": "0.07", "65479.6": "0.414"},
    "bids": {"65479": "0.997", "65479.5": "19.792"},
    "type": "depth-snapshot",
    "pts": 1785133949849,
    "pr": "futures",
    "s": "BTCUSDT",
}
TRADE_PAYLOAD = {
    "T": 1785133948141,
    "RT": 1785133999480.4836,
    "p": "65478.2",
    "q": "0.004",
    "m": 0,
    "s": "B-BTC_USDT",
    "pr": "f",
}


class CoinDCXPerpetualOrderBookTests(TestCase):
    def test_snapshot_parses_dict_sides(self):
        # CoinDCX sends each side as a {price: qty} mapping rather than arrays.
        msg = CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            DEPTH_PAYLOAD, 1785133949.85, metadata={"trading_pair": "BTC-USDT"})

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-USDT", msg.content["trading_pair"])
        self.assertEqual(210599929, msg.content["update_id"])
        self.assertEqual(sorted([[65479.0, 0.997], [65479.5, 19.792]]), sorted(msg.content["bids"]))
        self.assertEqual(sorted([[65480.0, 0.07], [65479.6, 0.414]]), sorted(msg.content["asks"]))

    def test_snapshot_falls_back_to_timestamp_when_version_missing(self):
        msg = CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            {"bids": {}, "asks": {}}, 1700000000.0, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual(1700000000000, msg.content["update_id"])

    def test_snapshot_accepts_array_sides(self):
        msg = CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            {"bids": [["1", "2"]], "asks": [["3", "4"]], "vs": 7}, 1.0, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual([[1.0, 2.0]], msg.content["bids"])
        self.assertEqual([[3.0, 4.0]], msg.content["asks"])

    def test_trade_taker_buy_prints_as_buy(self):
        msg = CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            TRADE_PAYLOAD, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])
        self.assertEqual(65478.2, msg.content["price"])
        self.assertEqual(0.004, msg.content["amount"])
        self.assertEqual(1785133948.141, msg.timestamp)

    def test_trade_maker_flag_prints_as_sell(self):
        msg = CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            {**TRADE_PAYLOAD, "m": 1}, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual(float(TradeType.SELL.value), msg.content["trade_type"])


class UnwrapFrameTests(TestCase):
    def test_unwraps_json_string_data(self):
        # Frames arrive as {"event": ..., "data": "<json string>"}.
        frame = {"event": "depth-snapshot", "data": json.dumps(DEPTH_PAYLOAD)}
        self.assertEqual(DEPTH_PAYLOAD, unwrap_frame(frame))

    def test_accepts_plain_dict_data(self):
        self.assertEqual(DEPTH_PAYLOAD, unwrap_frame({"event": "x", "data": DEPTH_PAYLOAD}))

    def test_accepts_bare_dict(self):
        self.assertEqual(DEPTH_PAYLOAD, unwrap_frame(DEPTH_PAYLOAD))

    def test_returns_none_on_garbage(self):
        self.assertIsNone(unwrap_frame("not json"))
        self.assertIsNone(unwrap_frame(None))
        self.assertIsNone(unwrap_frame({"event": "x", "data": "[1,2]"}))


class CoinDCXPerpetualOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value="BTC-USDT")
        self.ds = CoinDCXPerpetualAPIOrderBookDataSource(
            trading_pairs=["BTC-USDT", "ETH-USDT"],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    async def test_depth_routed_by_market_symbol(self):
        queue = asyncio.Queue()
        await self.ds._parse_order_book_snapshot_message(DEPTH_PAYLOAD, queue)
        msg = queue.get_nowait()
        self.assertEqual("BTC-USDT", msg.content["trading_pair"])

    async def test_depth_for_unknown_symbol_is_dropped(self):
        queue = asyncio.Queue()
        await self.ds._parse_order_book_snapshot_message({**DEPTH_PAYLOAD, "s": "SOLUSDT"}, queue)
        self.assertTrue(queue.empty())

    async def test_unlabelled_depth_dropped_when_multiple_pairs(self):
        queue = asyncio.Queue()
        payload = {k: v for k, v in DEPTH_PAYLOAD.items() if k != "s"}
        await self.ds._parse_order_book_snapshot_message(payload, queue)
        self.assertTrue(queue.empty())

    async def test_unlabelled_depth_accepted_for_single_pair(self):
        ds = CoinDCXPerpetualAPIOrderBookDataSource(
            trading_pairs=["BTC-USDT"], connector=self.connector, api_factory=MagicMock())
        queue = asyncio.Queue()
        payload = {k: v for k, v in DEPTH_PAYLOAD.items() if k != "s"}
        await ds._parse_order_book_snapshot_message(payload, queue)
        self.assertEqual("BTC-USDT", queue.get_nowait().content["trading_pair"])

    async def test_trade_routed_by_coindcx_pair(self):
        queue = asyncio.Queue()
        await self.ds._parse_trade_message(TRADE_PAYLOAD, queue)
        msg = queue.get_nowait()
        self.assertEqual("BTC-USDT", msg.content["trading_pair"])
        self.connector.trading_pair_associated_to_exchange_symbol.assert_awaited_with(symbol="B-BTC_USDT")

    async def test_trade_without_symbol_is_dropped(self):
        queue = asyncio.Queue()
        await self.ds._parse_trade_message({k: v for k, v in TRADE_PAYLOAD.items() if k != "s"}, queue)
        self.assertTrue(queue.empty())

    async def _drain_via_base_consumer(self, consumer, queue_key, raw_payload, timeout=1.0):
        """
        Push a RAW payload into the data source's internal queue exactly as the
        socket handler does, then let the base class's consumer loop process it
        and return whatever lands on the output queue.
        """
        import inspect

        output = asyncio.Queue()
        self.ds._message_queue[queue_key].put_nowait(raw_payload)
        # listen_for_funding_info takes only (output); the others take (ev_loop, output).
        takes_loop = len(inspect.signature(consumer).parameters) > 1
        task = asyncio.create_task(
            consumer(asyncio.get_event_loop(), output) if takes_loop else consumer(output))
        try:
            return await asyncio.wait_for(output.get(), timeout=timeout)
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def test_depth_frames_survive_the_base_class_consumer(self):
        """
        The socket handler must enqueue the RAW dict: the base class's
        listen_for_order_book_snapshots pulls from _message_queue and calls
        _parse_order_book_snapshot_message itself. Enqueueing an already-parsed
        OrderBookMessage makes it parse twice and raise
        "'OrderBookMessage' object has no attribute 'get'", which silently kills
        the order book feed under a real strategy.
        """
        message = await self._drain_via_base_consumer(
            self.ds.listen_for_order_book_snapshots,
            self.ds._snapshot_messages_queue_key,
            DEPTH_PAYLOAD)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual("BTC-USDT", message.content["trading_pair"])
        self.assertEqual(2, len(message.content["bids"]))

    async def test_trade_frames_survive_the_base_class_consumer(self):
        message = await self._drain_via_base_consumer(
            self.ds.listen_for_trades,
            self.ds._trade_messages_queue_key,
            TRADE_PAYLOAD)
        self.assertEqual(OrderBookMessageType.TRADE, message.type)
        self.assertEqual("BTC-USDT", message.content["trading_pair"])
        self.assertEqual(65478.2, message.content["price"])

    async def test_funding_frames_survive_the_base_class_consumer(self):
        self.connector.next_funding_timestamp = MagicMock(return_value=1785139200)
        update = await self._drain_via_base_consumer(
            self.ds.listen_for_funding_info,
            self.ds._funding_info_messages_queue_key,
            {"prices": {"B-BTC_USDT": {"mp": 65500.5, "fr": 0.0001}}})
        self.assertEqual("BTC-USDT", update.trading_pair)
        self.assertEqual(float(update.mark_price), 65500.5)

    async def test_socket_handlers_enqueue_raw_payloads(self):
        """The handlers built for the live socket must not pre-parse."""
        client = self.ds._build_client()
        handlers = client.handlers["/"]
        import json as _json

        await handlers["depth-snapshot"]({"event": "depth-snapshot",
                                          "data": _json.dumps(DEPTH_PAYLOAD)})
        await handlers["new-trade"]({"event": "new-trade", "data": _json.dumps(TRADE_PAYLOAD)})

        queued_depth = self.ds._message_queue[self.ds._snapshot_messages_queue_key].get_nowait()
        queued_trade = self.ds._message_queue[self.ds._trade_messages_queue_key].get_nowait()
        self.assertIsInstance(queued_depth, dict, "depth must be queued raw, not pre-parsed")
        self.assertIsInstance(queued_trade, dict, "trades must be queued raw, not pre-parsed")
        self.assertEqual("BTCUSDT", queued_depth["s"])

    async def test_funding_info_update_parsed(self):
        self.connector.next_funding_timestamp = MagicMock(return_value=1785139200)
        queue = asyncio.Queue()
        await self.ds._parse_funding_info_message(
            {"prices": {"B-BTC_USDT": {"mp": 65500.5, "fr": 0.0001}, "B-SOL_USDT": {"mp": 1.0}}}, queue)

        update = queue.get_nowait()
        self.assertEqual("BTC-USDT", update.trading_pair)
        self.assertEqual(float(update.mark_price), 65500.5)
        self.assertEqual(float(update.rate), 0.0001)
        # Pairs outside the subscribed set are ignored.
        self.assertTrue(queue.empty())
