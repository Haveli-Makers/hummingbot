import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.coinswitch import (
    coinswitch_constants as CONSTANTS,
    coinswitch_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source import (
    CoinswitchAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

_VALID_SECRET = "aa" * 32


class CoinswitchAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "INR"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.exchange_symbol = f"{cls.base_asset}/{cls.quote_asset}"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None

        self.connector = CoinswitchExchange(
            coinswitch_api_key="test_api_key",
            coinswitch_api_secret=_VALID_SECRET,
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.connector._time_synchronizer.add_time_offset_ms_sample(0.0)
        self.connector._set_trading_pair_symbol_map(bidict({self.exchange_symbol: self.trading_pair}))

        self.data_source = CoinswitchAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            exchange=CONSTANTS.DEFAULT_EXCHANGE,
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self):
        if self.listening_task:
            self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and message in record.getMessage()
            for record in self.log_records
        )

    def test_namespace_for_default_exchange(self):
        self.assertEqual("/coinswitchx", self.data_source._namespace)

    def test_namespace_for_custom_exchange(self):
        ds = CoinswitchAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            exchange="wazirx",
        )
        self.assertEqual("/wazirx", ds._namespace)

    @aioresponses()
    async def test_request_order_book_snapshot_returns_data(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL)
        regex_url = re.compile(rf"^{re.escape(url)}")

        resp = {
            "data": {
                "bids": [["5000000", "0.001"], ["4999000", "0.002"]],
                "asks": [["5001000", "0.001"], ["5002000", "0.003"]],
            }
        }
        mock_api.get(regex_url, body=json.dumps(resp))

        result = await self.data_source._request_order_book_snapshot(self.trading_pair)

        self.assertIn("data", result)
        self.assertIn("bids", result["data"])
        self.assertIn("asks", result["data"])

    @aioresponses()
    async def test_order_book_snapshot_message_type(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL)
        regex_url = re.compile(rf"^{re.escape(url)}")

        resp = {
            "data": {
                "bids": [["5000000", "0.001"]],
                "asks": [["5001000", "0.001"]],
            }
        }
        mock_api.get(regex_url, body=json.dumps(resp))

        msg = await self.data_source._order_book_snapshot(self.trading_pair)

        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])

    async def test_parse_order_book_snapshot_message_puts_snapshot_in_queue(self):
        queue = asyncio.Queue()
        raw = {
            "s": "BTC,INR",
            "bids": [["5000000", "0.001"]],
            "asks": [["5001000", "0.001"]],
            "timestamp": 1640000000000,
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)

        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])

    async def test_parse_order_book_snapshot_message_unknown_symbol_skips(self):
        queue = asyncio.Queue()
        raw = {
            "s": "UNKNOWN,PAIR",
            "bids": [],
            "asks": [],
            "timestamp": 1640000000000,
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)

        self.assertTrue(queue.empty())

    async def test_parse_trade_message_puts_trade_in_queue(self):
        queue = asyncio.Queue()
        raw = {
            "s": "BTC,INR",
            "E": 1640000000000,
            "p": "5000000",
            "q": "0.001",
            "t": "trade_001",
            "m": False,
        }
        await self.data_source._parse_trade_message(raw, queue)

        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])

    async def test_parse_trade_message_missing_symbol_skips(self):
        queue = asyncio.Queue()
        raw = {"E": 1640000000000, "p": "5000000", "q": "0.001"}
        await self.data_source._parse_trade_message(raw, queue)

        self.assertTrue(queue.empty())

    async def test_parse_order_book_diff_message_delegates_to_snapshot(self):
        queue = asyncio.Queue()
        raw = {
            "s": "BTC,INR",
            "bids": [["5000000", "0.001"]],
            "asks": [],
            "timestamp": 1640000000000,
        }
        await self.data_source._parse_order_book_diff_message(raw, queue)

        self.assertFalse(queue.empty())

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source.socketio.AsyncClient")
    def test_build_client_creates_socketio_client(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda **kwargs: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, **kwargs: (lambda f: f))
        mock_client_cls.return_value = mock_client

        client = self.data_source._build_client()

        mock_client_cls.assert_called_once_with(logger=False, reconnection=False)
        self.assertIs(client, mock_client)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source.socketio.AsyncClient")
    async def test_subscribe_channels_emits_for_each_pair(self, _mock_cls):
        mock_client = MagicMock()
        mock_client.emit = AsyncMock()

        await self.data_source._subscribe_channels(mock_client)

        # Expect 2 emits per pair (ORDER_BOOK + TRADE)
        self.assertEqual(2, mock_client.emit.call_count)

        call_args = [c[0][0] for c in mock_client.emit.call_args_list]
        self.assertIn(CONSTANTS.ORDER_BOOK_EVENT_TYPE, call_args)
        self.assertIn(CONSTANTS.TRADE_EVENT_TYPE, call_args)

    async def test_disconnect_calls_client_disconnect(self):
        mock_client = AsyncMock()
        mock_client.disconnect = AsyncMock()
        self.data_source._client = mock_client

        await self.data_source._disconnect()

        mock_client.disconnect.assert_called_once()
        self.assertIsNone(self.data_source._client)

    async def test_disconnect_handles_exception_gracefully(self):
        mock_client = AsyncMock()
        mock_client.disconnect = AsyncMock(side_effect=Exception("connection error"))
        self.data_source._client = mock_client

        # Should not raise
        await self.data_source._disconnect()

        self.assertIsNone(self.data_source._client)

    async def test_disconnect_when_client_is_none_does_nothing(self):
        self.data_source._client = None
        await self.data_source._disconnect()

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source.socketio.AsyncClient")
    async def test_listen_for_subscriptions_connects_and_subscribes(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.emit = AsyncMock()
        mock_client.wait = AsyncMock(side_effect=asyncio.CancelledError)
        mock_client.disconnect = AsyncMock()
        mock_client.event = MagicMock(side_effect=lambda **kwargs: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, **kwargs: (lambda f: f))
        mock_client_cls.return_value = mock_client

        with patch.object(
            self.data_source,
            "_request_order_book_snapshot",
            new=AsyncMock(return_value={"data": {"bids": [], "asks": []}}),
        ):
            try:
                await self.data_source.listen_for_subscriptions()
            except asyncio.CancelledError:
                pass

        mock_client.connect.assert_called_once()
        self.assertEqual(
            CONSTANTS.WSS_URL,
            mock_client.connect.call_args[0][0],
        )


if __name__ == "__main__":
    import unittest
    unittest.main()
