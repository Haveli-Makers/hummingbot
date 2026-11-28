import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aioresponses.core import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import (
    CoinDCXAPIOrderBookDataSource,
    unwrap_frame,
)
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker

# Recorded from CoinDCX for SOL-INR on 2026-11-24, with the levels trimmed to three per side.
# The REST order book has no "vs"; its "timestamp" is CoinDCX's clock in ms.
REST_BOOK = {
    "asks": {"7433.7000000000": "46.406000", "7445.4000000000": "31.704228", "7447.1000000000": "3.373000"},
    "bids": {"7405.1000000000": "1.902000", "7400.3000000000": "0.014000", "7400.0000000000": "10.358000"},
    "timestamp": 1795517777466,
}
# Stream message bodies. On the wire each one arrives as {"event": ..., "data": "<the body as JSON text>"}.
DEPTH_SNAPSHOT = {
    "vs": 216476, "ts": 1795517780511,
    "asks": {"7445.3000000000": "31.704228", "7458.3000000000": "3.373000", "7465.7000000000": "28.195000"},
    "bids": {"7405.1000000000": "1.902000", "7400.3000000000": "0.014000", "7400.0000000000": "10.358000"},
    "type": "depth-snapshot", "pr": "spot", "s": "SOLINR", "E": 1795517780511, "pts": 1795517780511,
}
DEPTH_UPDATE = {
    "vs": 216476, "ts": 1795517780511,
    "asks": {"7445.3000000000": "31.704228", "7433.7000000000": "0"},
    "bids": {"7398.2000000000": "3.373000"},
    "type": "depth-update", "pr": "spot", "s": "SOLINR", "E": 1795517780511, "pts": 1795517780511,
}
TRADE = {"T": 1795517796000, "p": "7435.4000000000", "q": "0.026739", "m": 0, "s": "I-SOL_INR", "pr": "spot"}


def wire(event: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """A message as CoinDCX's Socket.IO stream delivers it."""
    return {"event": event, "data": json.dumps(body)}


class CoinDCXAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "SOL"
        cls.quote_asset = "INR"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair_symbol = cls.base_asset + cls.quote_asset  # "SOLINR": markets_details and depth messages
        cls.ex_trading_pair_channel = f"I-{cls.base_asset}_{cls.quote_asset}"  # "I-SOL_INR": channels and trades
        cls.domain = ""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None

        self.connector = CoindcxExchange(
            coindcx_api_key="",
            coindcx_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain)
        self.data_source = CoinDCXAPIOrderBookDataSource(trading_pairs=[self.trading_pair],
                                                         connector=self.connector,
                                                         api_factory=self.connector._web_assistants_factory,
                                                         domain=self.domain)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        # Like the map the exchange builds from markets_details: keyed by the market symbol only.
        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair_symbol: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _is_logged_starting_with(self, log_level: str, prefix: str) -> bool:
        return any(record.levelname == log_level and record.getMessage().startswith(prefix)
                   for record in self.log_records)

    def _waits(self):
        return [call.args[0] for call in self.data_source._sleep.await_args_list]

    def _rest_url_regex(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self.domain)
        return re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

    async def _drain_via_base_loop(self, loop_method, queue_key: str, body: Dict[str, Any]) -> OrderBookMessage:
        """Queue a body the way the socket handlers do, and return what the base class's loop makes of it."""
        output: asyncio.Queue = asyncio.Queue()
        self.data_source._message_queue[queue_key].put_nowait(body)
        self.listening_task = self.local_event_loop.create_task(loop_method(self.local_event_loop, output))
        return await asyncio.wait_for(output.get(), timeout=1)

    @aioresponses()
    async def test_get_new_order_book_successful(self, mock_api):
        mock_api.get(self._rest_url_regex(), body=json.dumps(REST_BOOK))

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        # The REST book's id is CoinDCX's clock, the same scale as the stream's "ts".
        self.assertEqual(REST_BOOK["timestamp"], order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(3, len(bids))
        self.assertEqual(7405.1, bids[0].price)
        self.assertEqual(1.902, bids[0].amount)
        self.assertEqual(REST_BOOK["timestamp"], bids[0].update_id)
        self.assertEqual(3, len(asks))
        self.assertEqual(7433.7, asks[0].price)
        self.assertEqual(46.406, asks[0].amount)

    @aioresponses()
    async def test_get_new_order_book_raises_exception(self, mock_api):
        # Every attempt fails, so after the last one the error reaches the caller.
        mock_api.get(self._rest_url_regex(), status=400, repeat=True)
        self.data_source._sleep = AsyncMock()

        with self.assertRaises(IOError):
            await self.data_source.get_new_order_book(self.trading_pair)

        self.assertEqual(5, sum(len(calls) for calls in mock_api.requests.values()))
        self.assertEqual([1.0, 2.0, 4.0, 8.0], self._waits())
        self.assertEqual(4, sum(1 for record in self.log_records if record.levelname == "WARNING"))
        self.assertTrue(self._is_logged_starting_with("ERROR", "Could not get the SOL-INR order book after 5 attempts: "))

    async def test_get_new_order_book_retries_a_failed_read(self):
        # The errors a flaky proxy gave on 2026-11-27, then CoinDCX's answer.
        self.data_source._request_order_book_snapshot = AsyncMock(side_effect=[
            aiohttp.ClientOSError(104, "Connection reset by peer"),
            asyncio.TimeoutError(),
            json.loads(json.dumps(REST_BOOK)),
        ])
        self.data_source._sleep = AsyncMock()

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        self.assertEqual(REST_BOOK["timestamp"], order_book.snapshot_uid)
        self.assertEqual(7405.1, order_book.get_price(False))
        self.assertEqual(3, self.data_source._request_order_book_snapshot.await_count)
        self.assertEqual([1.0, 2.0], self._waits())
        self.assertTrue(self._is_logged(
            "WARNING",
            "Could not get the SOL-INR order book (attempt 1 of 5): [Errno 104] Connection reset by peer. "
            "Retrying in 1 s."))
        self.assertTrue(self._is_logged(
            "WARNING", "Could not get the SOL-INR order book (attempt 2 of 5): TimeoutError. Retrying in 2 s."))
        self.assertFalse(any(record.levelname == "ERROR" for record in self.log_records))

    async def test_get_new_order_book_does_not_retry_a_cancel(self):
        self.data_source._request_order_book_snapshot = AsyncMock(side_effect=asyncio.CancelledError)
        self.data_source._sleep = AsyncMock()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.get_new_order_book(self.trading_pair)

        self.assertEqual(1, self.data_source._request_order_book_snapshot.await_count)
        self.data_source._sleep.assert_not_awaited()

    async def test_order_book_tracker_gets_ready_when_the_first_read_fails(self):
        """
        What happened on 2026-11-27: the first read hit a reset connection. OrderBookTracker reads the first book only
        once, so before the retry its init task died and the connector stayed not ready.
        """
        self.data_source._request_order_book_snapshot = AsyncMock(side_effect=[
            aiohttp.ClientOSError(104, "Connection reset by peer"),
            json.loads(json.dumps(REST_BOOK)),
        ])
        self.data_source._sleep = AsyncMock()
        tracker = OrderBookTracker(data_source=self.data_source, trading_pairs=[self.trading_pair])
        tracker._sleep = AsyncMock()

        try:
            await asyncio.wait_for(tracker._init_order_books(), timeout=1)

            self.assertTrue(tracker.ready)
            self.assertEqual(REST_BOOK["timestamp"], tracker.order_books[self.trading_pair].snapshot_uid)
        finally:
            await asyncio.sleep(0)  # lets the book's tracking task start, so that stopping it ends it cleanly
            tracking_tasks = list(tracker._tracking_tasks.values())
            tracker.stop()
            await asyncio.gather(*tracking_tasks, return_exceptions=True)

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.socketio.AsyncClient")
    async def test_listen_for_subscriptions_subscribes_to_order_book_and_trades(self, mock_client_class):
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.emit = AsyncMock()
        mock_client.wait = AsyncMock(side_effect=asyncio.CancelledError)
        mock_client.disconnect = AsyncMock()
        mock_client.event = MagicMock(side_effect=lambda func: func)
        mock_client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
        mock_client_class.return_value = mock_client

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())
        await asyncio.sleep(0.2)

        mock_client.connect.assert_called_once()
        channels = [call.args[1]["channelName"] for call in mock_client.emit.call_args_list if call.args[0] == "join"]
        self.assertEqual([f"{self.ex_trading_pair_channel}@orderbook@50", f"{self.ex_trading_pair_channel}@trades"],
                         channels)

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_client = MagicMock()
        mock_client.emit = AsyncMock(side_effect=asyncio.CancelledError)

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_client)

    @patch("hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.socketio.AsyncClient")
    async def test_listen_for_subscriptions_logs_exception_and_retries(self, mock_client_class):
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.emit = AsyncMock(side_effect=Exception("Test Error"))
        mock_client.disconnect = AsyncMock()
        mock_client.event = MagicMock(side_effect=lambda func: func)
        mock_client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
        mock_client_class.return_value = mock_client

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())
        await asyncio.sleep(0.2)

        self.assertTrue(self._is_logged(
            "ERROR", "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."))

    async def test_socket_handlers_queue_the_decoded_body(self):
        handlers = self.data_source._build_client().handlers["/"]

        await handlers[CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE](wire("depth-snapshot", DEPTH_SNAPSHOT))
        await handlers[CONSTANTS.TRADE_EVENT_TYPE](wire("new-trade", TRADE))

        # The body itself is queued, not a parsed message: the base class's loops do the parsing.
        snapshot_queue = self.data_source._message_queue[CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE]
        trade_queue = self.data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE]
        self.assertEqual(DEPTH_SNAPSHOT, snapshot_queue.get_nowait())
        self.assertEqual(TRADE, trade_queue.get_nowait())

    async def test_depth_updates_are_not_used(self):
        handlers = self.data_source._build_client().handlers["/"]

        self.assertNotIn(CONSTANTS.DIFF_EVENT_TYPE, handlers)

    async def test_message_without_a_json_body_is_ignored(self):
        handlers = self.data_source._build_client().handlers["/"]

        await handlers[CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE]({"event": "depth-snapshot", "data": "not json"})

        self.assertTrue(self.data_source._message_queue[CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE].empty())

    def test_unwrap_frame(self):
        self.assertEqual(TRADE, unwrap_frame(wire("new-trade", TRADE)))
        self.assertEqual(TRADE, unwrap_frame(json.dumps(wire("new-trade", TRADE))))
        self.assertEqual(TRADE, unwrap_frame({"event": "new-trade", "data": TRADE}))
        self.assertEqual(TRADE, unwrap_frame(TRADE))
        self.assertIsNone(unwrap_frame("not json"))
        self.assertIsNone(unwrap_frame(["not", "a", "dict"]))
        self.assertIsNone(unwrap_frame({"event": "new-trade", "data": "[1, 2]"}))

    async def test_listen_for_order_book_snapshots_parses_stream_snapshots(self):
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60

        msg = await self._drain_via_base_loop(
            self.data_source.listen_for_order_book_snapshots, CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE, DEPTH_SNAPSHOT)

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)  # mapped from the market symbol "SOLINR"
        self.assertEqual(DEPTH_SNAPSHOT["ts"], msg.update_id)
        self.assertEqual([7405.1, 7400.3, 7400.0], [bid.price for bid in msg.bids])
        self.assertEqual([7445.3, 7458.3, 7465.7], [ask.price for ask in msg.asks])

    @aioresponses()
    async def test_listen_for_order_book_snapshots_falls_back_to_rest(self, mock_api):
        mock_api.get(self._rest_url_regex(), body=json.dumps(REST_BOOK))
        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue))
        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(REST_BOOK["timestamp"], msg.update_id)

    def test_books_refresh_from_rest_after_ten_seconds_without_snapshots(self):
        # CoinDCX sends a snapshot about every 3 s per pair, so 10 s without one means the stream is stuck.
        self.assertEqual(10, CoinDCXAPIOrderBookDataSource.FULL_ORDER_BOOK_RESET_DELTA_SECONDS)

    async def test_listen_for_trades_parses_stream_trades(self):
        msg = await self._drain_via_base_loop(self.data_source.listen_for_trades, CONSTANTS.TRADE_EVENT_TYPE, TRADE)

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)  # mapped from the pair code "I-SOL_INR"
        self.assertEqual(TRADE["T"], msg.trade_id)
        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])  # m=0: the buyer took liquidity
        self.assertEqual(7435.4, msg.content["price"])
        self.assertEqual(0.026739, msg.content["amount"])

    async def test_messages_for_unknown_markets_are_dropped_with_one_warning(self):
        queue: asyncio.Queue = asyncio.Queue()

        await self.data_source._parse_order_book_snapshot_message(dict(DEPTH_SNAPSHOT, s="XYZINR"), queue)
        await self.data_source._parse_order_book_snapshot_message(dict(DEPTH_SNAPSHOT, s="XYZINR"), queue)
        await self.data_source._parse_trade_message(dict(TRADE, s="I-XYZ_INR"), queue)
        await self.data_source._parse_trade_message(dict(TRADE, s=""), queue)

        self.assertTrue(queue.empty())
        self.assertTrue(self._is_logged("WARNING", "Ignoring CoinDCX market data for unknown symbol XYZINR."))
        self.assertTrue(self._is_logged("WARNING", "Ignoring CoinDCX market data for unknown symbol I-XYZ_INR."))
        self.assertEqual(2, sum(1 for record in self.log_records if record.levelname == "WARNING"))

    async def test_order_book_tracker_follows_the_stream(self):
        """
        End to end through OrderBookTracker with messages recorded from CoinDCX: the book starts from REST, then takes
        each newer snapshot from the stream; depth updates are ignored; trades set the last trade price.
        """
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60
        self.data_source._request_order_book_snapshot = AsyncMock(return_value=json.loads(json.dumps(REST_BOOK)))
        self.data_source.get_last_traded_prices = AsyncMock(return_value={self.trading_pair: 7400.0})
        tracker = OrderBookTracker(data_source=self.data_source, trading_pairs=[self.trading_pair])
        tracker._sleep = AsyncMock()
        ids_at_start = []

        async def recorded_stream():  # stands in for the Socket.IO connection
            handlers = self.data_source._build_client().handlers["/"]
            await tracker._order_books_initialized.wait()
            ids_at_start.append(tracker.order_books[self.trading_pair].snapshot_uid)
            for event, body in (("depth-update", DEPTH_UPDATE), ("depth-snapshot", DEPTH_SNAPSHOT),
                                ("new-trade", TRADE)):
                handler = handlers.get(event)
                if handler is not None:
                    await handler(wire(event, body))
            await asyncio.Event().wait()

        self.data_source.listen_for_subscriptions = recorded_stream
        tracker.start()
        try:
            book = None
            for _ in range(100):
                await asyncio.sleep(0.02)
                book = tracker.order_books.get(self.trading_pair)
                if book is not None and book.snapshot_uid == DEPTH_SNAPSHOT["ts"] and book.last_trade_price == 7435.4:
                    break

            self.assertEqual([REST_BOOK["timestamp"]], ids_at_start)
            self.assertEqual(DEPTH_SNAPSHOT["ts"], book.snapshot_uid)
            self.assertEqual(7405.1, book.get_price(False))  # best bid
            self.assertEqual(7445.3, book.get_price(True))  # best ask: 7433.7 is gone from CoinDCX's snapshot
            self.assertEqual(7435.4, book.last_trade_price)
        finally:
            tracker.stop()
