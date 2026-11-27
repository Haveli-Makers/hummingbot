
import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Dict
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class WazirxAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = ""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.connector = WazirxExchange(
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain)
        self.data_source = WazirxAPIOrderBookDataSource(trading_pairs=[self.trading_pair],
                                                        connector=self.connector,
                                                        api_factory=self.connector._web_assistants_factory,
                                                        domain=self.domain)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.msg_queue = asyncio.Queue()
        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.data_source._data_source_obsolete = True

    def get_order_book_data_mock(self) -> Dict:
        order_book_data = {
            "timestamp": 1630644717528,
            "lastUpdateId": 12345,
            "bids": [["100.0", "1.0"], ["99.0", "2.0"]],
            "asks": [["101.0", "1.5"], ["102.0", "0.5"]]
        }
        return order_book_data

    def get_trade_data_mock(self) -> Dict:
        trade_data = {
            "event": "trade",
            "symbol": self.ex_trading_pair,
            "price": "100.5",
            "quantity": "0.1",
            "timestamp": 1630644717528
        }
        return trade_data

    def get_order_book_diff_mock(self) -> Dict:
        diff_data = {
            "event": "depthUpdate",
            "symbol": self.ex_trading_pair,
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.5"]],
            "timestamp": 1630644717528
        }
        return diff_data

    def get_invalid_trading_pair_mock(self) -> Dict:
        diff_data = {
            "event": "depthUpdate",
            "symbol": "INVALIDPAIR",
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.5"]],
            "timestamp": 1630644717528
        }
        return diff_data

    def get_invalid_message_mock(self) -> Dict:
        return {"invalid": "message"}

    async def test_get_new_order_book_successful(self):
        mock_message = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": self.trading_pair,
                "update_id": 12345,
                "bids": [["100.0", "1.0"], ["99.0", "2.0"]],
                "asks": [["101.0", "1.5"], ["102.0", "0.5"]],
            },
            timestamp=1234567890.0
        )

        with patch.object(self.data_source, '_order_book_snapshot', new_callable=AsyncMock) as mock_snapshot:
            mock_snapshot.return_value = mock_message

            order_book = await self.data_source.get_new_order_book(self.trading_pair)

            self.assertIsNotNone(order_book)
            self.assertEqual(2, len(list(order_book.bid_entries())))
            self.assertEqual(2, len(list(order_book.ask_entries())))
            mock_snapshot.assert_called_once_with(trading_pair=self.trading_pair)

    async def test_get_new_order_book_failure(self):
        with patch.object(self.data_source, '_order_book_snapshot', new_callable=AsyncMock) as mock_snapshot:
            mock_snapshot.side_effect = Exception("Snapshot failed")

            with self.assertRaises(Exception):
                await self.data_source.get_new_order_book(self.trading_pair)

    async def test_listen_for_order_book_diffs_successful(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_order_book_diff_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_diff_message") as mock_parse:
                mock_parse.return_value = OrderBookMessage(
                    message_type=OrderBookMessageType.DIFF,
                    content=self.get_order_book_diff_mock(),
                    timestamp=1630644717528
                )
                task = asyncio.create_task(self.data_source.listen_for_order_book_diffs(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_diffs_full_order_book_received(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_order_book_data_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_snapshot_message") as mock_parse:
                mock_parse.return_value = OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content=self.get_order_book_data_mock(),
                    timestamp=1630644717528
                )
                task = asyncio.create_task(self.data_source.listen_for_order_book_diffs(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_diffs_ignore_invalid_trading_pair(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_invalid_trading_pair_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_diff_message") as mock_parse:
                task = asyncio.create_task(self.data_source.listen_for_order_book_diffs(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_diffs_invalid_message(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_invalid_message_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_diff_message") as mock_parse:
                task = asyncio.create_task(self.data_source.listen_for_order_book_diffs(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_diffs_connection_failed(self):
        task = asyncio.create_task(self.data_source.listen_for_order_book_diffs(self.local_event_loop, self.msg_queue))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_listen_for_order_book_snapshots_successful(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_order_book_data_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_snapshot_message") as mock_parse:
                mock_parse.return_value = OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content=self.get_order_book_data_mock(),
                    timestamp=1630644717528
                )
                task = asyncio.create_task(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_snapshots_ignore_invalid_trading_pair(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_invalid_trading_pair_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_snapshot_message") as mock_parse:
                task = asyncio.create_task(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_snapshots_invalid_message(self):
        mock_ws = AsyncMock()
        mock_ws.recv.side_effect = [json.dumps(self.get_invalid_message_mock()), Exception("Test end")]
        with patch("websockets.connect", return_value=mock_ws):
            with patch.object(self.data_source, "_parse_order_book_snapshot_message") as mock_parse:
                task = asyncio.create_task(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, self.msg_queue))
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                self.assertEqual(0, mock_parse.call_count)

    async def test_listen_for_order_book_snapshots_connection_failed(self):
        task = asyncio.create_task(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, self.msg_queue))
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ---- websocket streams ----------------------------------------------------------------------------------------

    def _set_symbol_map(self):
        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair.lower(): self.trading_pair}))

    def _ws_depth_event(self, symbol=None) -> Dict:
        symbol = symbol or self.ex_trading_pair.lower()
        return {"data": {"E": 1631682370, "s": symbol,
                         "b": [["100.0", "1.0"], ["99.0", "2.0"]],
                         "a": [["101.0", "1.5"], ["102.0", "0.5"]]},
                "stream": f"{symbol}@depth20@100ms"}

    def _ws_trades_event(self) -> Dict:
        symbol = self.ex_trading_pair.lower()
        return {"data": {"trades": [
            {"E": 1631681323000, "S": "buy", "a": 1, "b": 2, "m": True, "p": "100.5", "q": "0.1", "s": symbol, "t": 17},
            {"E": 1631681323500, "S": "buy", "a": 3, "b": 4, "m": False, "p": "100.6", "q": "0.2", "s": symbol, "t": 18},
        ]}, "stream": f"{symbol}@trades"}

    async def test_subscribe_channels_sends_one_message_with_depth_and_trade_streams(self):
        self._set_symbol_map()
        ws = AsyncMock()
        await self.data_source._subscribe_channels(ws)
        ws.send.assert_awaited_once()
        payload = ws.send.call_args.args[0].payload
        symbol = self.ex_trading_pair.lower()
        self.assertEqual({"event": "subscribe", "streams": [f"{symbol}@depth20@100ms", f"{symbol}@trades"]}, payload)

    def test_channel_routing(self):
        self.assertEqual(self.data_source._snapshot_messages_queue_key,
                         self.data_source._channel_originating_message(self._ws_depth_event()))
        self.assertEqual(self.data_source._trade_messages_queue_key,
                         self.data_source._channel_originating_message(self._ws_trades_event()))
        for control in ({"event": "pong", "data": {"timeout_duration": 1800}}, {"event": "subscribed"}, {}):
            self.assertEqual("", self.data_source._channel_originating_message(control))

    async def test_depth_event_becomes_full_snapshot(self):
        self._set_symbol_map()
        with patch.object(self.data_source, "_time", return_value=1700000000.5):
            await self.data_source._parse_order_book_snapshot_message(self._ws_depth_event(), self.msg_queue)
        message: OrderBookMessage = self.msg_queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(self.trading_pair, message.trading_pair)
        self.assertEqual([100.0, 99.0], [row.price for row in message.bids])
        self.assertEqual([101.0, 102.0], [row.price for row in message.asks])
        self.assertEqual(1700000000.5, message.timestamp)
        self.assertEqual(1700000000500, message.update_id)

    async def test_depth_event_for_unknown_symbol_is_ignored(self):
        self._set_symbol_map()
        await self.data_source._parse_order_book_snapshot_message(self._ws_depth_event("unknownpair"), self.msg_queue)
        self.assertTrue(self.msg_queue.empty())

    async def test_trades_event_yields_one_message_per_trade(self):
        self._set_symbol_map()
        await self.data_source._parse_trade_message(self._ws_trades_event(), self.msg_queue)
        first, second = self.msg_queue.get_nowait(), self.msg_queue.get_nowait()
        self.assertTrue(self.msg_queue.empty())
        self.assertEqual(OrderBookMessageType.TRADE, first.type)
        self.assertEqual(float(TradeType.SELL.value), first.content["trade_type"])    # buyer was maker -> taker sold
        self.assertEqual(float(TradeType.BUY.value), second.content["trade_type"])
        self.assertEqual([17, 18], [first.trade_id, second.trade_id])
        self.assertEqual(1631681323.0, first.timestamp)                               # trade "E" is in milliseconds

    async def test_failed_rest_snapshot_raises_instead_of_returning_empty_book(self):
        self._set_symbol_map()
        for bad in ({}, {"message": "Too many api request", "code": 2136}):
            with patch.object(self.data_source, "_request_order_book_snapshot", new_callable=AsyncMock, return_value=bad):
                with self.assertRaises(IOError):
                    await self.data_source._order_book_snapshot(self.trading_pair)

    async def test_rest_snapshot_parsed(self):
        self._set_symbol_map()
        with patch.object(self.data_source, "_request_order_book_snapshot", new_callable=AsyncMock,
                          return_value=self.get_order_book_data_mock()):
            message = await self.data_source._order_book_snapshot(self.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(2, len(message.bids))
        self.assertEqual(2, len(message.asks))

    async def test_open_but_silent_market_streams_trigger_reconnect(self):
        ws = AsyncMock()
        disconnected = asyncio.Event()

        async def only_pongs():
            while not disconnected.is_set():
                await asyncio.sleep(0.01)
                yield WSResponse(data={"event": "pong", "data": {"timeout_duration": 1800}})

        async def disconnect():
            disconnected.set()

        ws.iter_messages = only_pongs
        ws.disconnect.side_effect = disconnect
        with patch.object(CONSTANTS, "WS_DATA_STALL_TIMEOUT", 0.1), patch.object(CONSTANTS, "WS_WATCHDOG_INTERVAL", 0.02):
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(self.data_source._process_websocket_messages(ws), 2)
        ws.disconnect.assert_awaited()

    async def test_websocket_messages_are_queued_by_channel(self):
        ws = AsyncMock()
        events = [self._ws_depth_event(), self._ws_trades_event(), {"event": "pong", "data": {"timeout_duration": 1800}}]

        async def messages():
            for event in events:
                yield WSResponse(data=event)

        ws.iter_messages = messages
        await self.data_source._process_websocket_messages(ws)
        self.assertEqual(1, self.data_source._message_queue[self.data_source._snapshot_messages_queue_key].qsize())
        self.assertEqual(1, self.data_source._message_queue[self.data_source._trade_messages_queue_key].qsize())

    async def test_get_last_traded_prices(self):
        mock_prices = {
            self.trading_pair: "100.5"
        }

        with patch.object(self.connector, 'get_last_traded_prices', new_callable=AsyncMock) as mock_get_prices:
            mock_get_prices.return_value = mock_prices

            prices = await self.data_source.get_last_traded_prices([self.trading_pair])
            self.assertIn(self.trading_pair, prices)
            self.assertEqual("100.5", prices[self.trading_pair])

    async def test_get_last_traded_prices_empty_response(self):
        with patch.object(self.connector, 'get_last_traded_prices', new_callable=AsyncMock) as mock_get_prices:
            mock_get_prices.return_value = {}
            prices = await self.data_source.get_last_traded_prices([self.trading_pair])
            self.assertEqual({}, prices)

    async def test_get_last_traded_prices_exception(self):
        with patch.object(self.connector, 'get_last_traded_prices', new_callable=AsyncMock) as mock_get_prices:
            mock_get_prices.side_effect = Exception("API Error")
            prices = await self.data_source.get_last_traded_prices([self.trading_pair])
            self.assertEqual({}, prices)
