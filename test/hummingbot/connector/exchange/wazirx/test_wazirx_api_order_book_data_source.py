
import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Dict
from unittest.mock import AsyncMock, patch

from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


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
