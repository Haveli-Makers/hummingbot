import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage


class CoinDCXAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair_symbol = cls.base_asset + cls.quote_asset  # e.g., COINALPHAHBOT
        cls.ex_trading_pair_channel = f"B-{cls.base_asset}_{cls.quote_asset}"
        cls.domain = ""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

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

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map({self.ex_trading_pair_symbol: self.trading_pair, self.ex_trading_pair_channel: self.trading_pair})

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _snapshot_response(self):
        return {
            "vs": 1027024,
            "bids": {"4.000000": "431.0"},
            "asks": {"4.000002": "12.0"}
        }

    def _trade_update_event(self):
        return {"T": 123456789, "s": self.ex_trading_pair_symbol, "p": "0.001", "q": "100", "m": 1}

    def _order_diff_event(self):
        return {"ts": 123456789, "vs": 160, "bids": {"0.0024": "10"}, "asks": {"0.0026": "100"},
                "channel": f"{self.ex_trading_pair_channel}@orderbook@20"}

    @aioresponses()
    async def test_get_new_order_book_successful(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = self._snapshot_response()

        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBook = await self.data_source.get_new_order_book(self.trading_pair)

        expected_update_id = resp["vs"]

        self.assertEqual(expected_update_id, order_book.snapshot_uid)
        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(4, bids[0].price)
        self.assertEqual(431, bids[0].amount)
        self.assertEqual(expected_update_id, bids[0].update_id)
        self.assertEqual(1, len(asks))
        self.assertEqual(4.000002, asks[0].price)
        self.assertEqual(12, asks[0].amount)
        self.assertEqual(expected_update_id, asks[0].update_id)

    @aioresponses()
    async def test_get_new_order_book_raises_exception(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            await self.data_source.get_new_order_book(self.trading_pair)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())

        await asyncio.sleep(0.1)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(ws_connect_mock.return_value)

        self.assertEqual(2, len(sent_messages))
        first_payload = sent_messages[0]
        second_payload = sent_messages[1]

        expected_orderbook_channel = f"{self.ex_trading_pair_channel}@orderbook@20"
        expected_trades_channel = f"{self.ex_trading_pair_channel}@trades"

        self.assertEqual("join", first_payload.get("type"))
        self.assertIn(expected_orderbook_channel, first_payload.get("channelName"))
        self.assertEqual("join", second_payload.get("type"))
        self.assertIn(expected_trades_channel, second_payload.get("channelName"))

        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to public order book and trade channels..."
        ))

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book trading and delta streams...")
        )

    async def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [self._trade_update_event(), asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.TRADE_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue))

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(123456789, msg.trade_id)

    async def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        diff_event = self._order_diff_event()
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.DIFF_EVENT_TYPE] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue))

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(diff_event["vs"], msg.update_id)

    @aioresponses()
    async def test_listen_for_order_book_snapshots_successful(self, mock_api, ):
        msg_queue: asyncio.Queue = asyncio.Queue()
        url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response()))

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(1027024, msg.update_id)


def test_channel_originating_message_trade_and_diff():
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=[], connector=None, api_factory=None)

    trade_event = {"p": 1, "q": 2, "T": 123}
    diff_event = {"bids": [], "asks": []}

    assert ds._channel_originating_message(trade_event) == ds._trade_messages_queue_key
    assert ds._channel_originating_message(diff_event) == ds._diff_messages_queue_key


def test_parse_trade_and_diff_message_async():
    async def run_test():
        class ConnectorStub:
            async def trading_pair_associated_to_exchange_symbol(self, symbol: str):
                return "BTC-USDT"

        connector = ConnectorStub()
        ds = CoinDCXAPIOrderBookDataSource(trading_pairs=[], connector=connector, api_factory=None)

        q = asyncio.Queue()

        raw_trade = {"s": "BTCUSDT", "T": 123456, "p": 1, "q": 2}
        await ds._parse_trade_message(raw_trade, q)
        item = q.get_nowait()
        assert item is not None

        q2 = asyncio.Queue()
        raw_diff = {"bids": {"1": "2"}, "channel": "B-BTC_USDT@orderbook@20"}
        await ds._parse_order_book_diff_message(raw_diff, q2)
        item2 = q2.get_nowait()
        assert item2 is not None

        q3 = asyncio.Queue()
        await ds._parse_trade_message({}, q3)
        assert q3.empty()

        q4 = asyncio.Queue()
        raw_diff2 = {"bids": {"1": "2"}, "channel": "UNKNOWN"}
        await ds._parse_order_book_diff_message(raw_diff2, q4)
        assert q4.empty()

    asyncio.run(run_test())
