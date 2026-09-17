import asyncio
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import WazirxAPIUserStreamDataSource
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class WazirxUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = ""

        cls.listen_key = "TEST_LISTEN_KEY"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000
        self.auth = WazirxAuth(api_key="TEST_API_KEY", secret_key="TEST_SECRET", time_provider=self.mock_time_provider)
        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(0)

        self.connector = WazirxExchange(
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain)
        self.data_source = WazirxAPIUserStreamDataSource(
            auth=self.auth,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

    async def asyncTearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _create_return_value_and_unlock_test_with_event(self, value):
        self.resume_test_event.set()
        return value

    @aioresponses()
    async def test_listen_for_user_stream_successful(self, mock_api):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.OPEN_ORDERS_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = [
            {
                "symbol": "btcusdt",
                "orderId": "12345",
                "clientOrderId": "test_order_1",
                "price": "50000.0",
                "origQty": "0.001",
                "status": "NEW"
            }
        ]
        mock_api.get(regex_url, payload=mock_response)

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await asyncio.sleep(0.1)

        self.listening_task.cancel()
        try:
            await self.listening_task
        except asyncio.CancelledError:
            pass

        self.assertEqual(1, msg_queue.qsize())
        msg = msg_queue.get_nowait()
        self.assertIn("open_orders", msg)
        self.assertEqual(mock_response, msg["open_orders"])

    @aioresponses()
    @patch("asyncio.sleep")
    async def test_listen_for_user_stream_iter_message_throws_exception(self, mock_api, mock_sleep):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.OPEN_ORDERS_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, exception=Exception("TEST ERROR"))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        mock_sleep.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.listening_task

        self.assertTrue(self._is_logged("ERROR", "Error fetching user stream data: TEST ERROR"))

    @aioresponses()
    async def test_listen_for_user_stream_connection_failed(self, mock_api):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.OPEN_ORDERS_PATH_URL}"
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_api.get(regex_url, status=500)

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await asyncio.sleep(0.1)

        self.listening_task.cancel()
        try:
            await self.listening_task
        except asyncio.CancelledError:
            pass

        self.assertEqual(0, msg_queue.qsize())

    async def test_listen_for_user_stream_order_update(self):
        order_update_event = {
            "event": "orderUpdate",
            "timestamp": 1640995200000,
            "order": {
                "clientOrderId": "test_order_1",
                "orderId": "12345",
                "status": "FILLED",
                "symbol": "btcusdt"
            }
        }

        msg_queue = asyncio.Queue()
        await self.data_source._process_event_message(order_update_event, msg_queue)

        self.assertEqual(1, msg_queue.qsize())
        msg = msg_queue.get_nowait()
        self.assertEqual(order_update_event, msg)

    async def test_listen_for_user_stream_balance_update(self):
        balance_update_event = {
            "event": "balanceUpdate",
            "timestamp": 1640995200000,
            "balance": {
                "asset": "BTC",
                "free": "0.5",
                "total": "0.5"
            }
        }

        msg_queue = asyncio.Queue()
        await self.data_source._process_event_message(balance_update_event, msg_queue)

        self.assertEqual(1, msg_queue.qsize())
        msg = msg_queue.get_nowait()
        self.assertEqual(balance_update_event, msg)

    async def test_listen_for_user_stream_user_update(self):
        user_update_event = {
            "event": "userUpdate",
            "timestamp": 1640995200000,
            "user": {
                "accountType": "SPOT",
                "balances": [
                    {"asset": "BTC", "free": "0.5", "locked": "0.1"}
                ]
            }
        }

        msg_queue = asyncio.Queue()
        await self.data_source._process_event_message(user_update_event, msg_queue)

        self.assertEqual(1, msg_queue.qsize())
        msg = msg_queue.get_nowait()
        self.assertEqual(user_update_event, msg)

    async def test_listen_for_user_stream_unknown_event(self):
        unknown_event = {
            "event": "unknownEvent",
            "timestamp": 1640995200000,
            "data": "some data"
        }

        msg_queue = asyncio.Queue()
        await self.data_source._process_event_message(unknown_event, msg_queue)

        self.assertEqual(1, msg_queue.qsize())
        msg = msg_queue.get_nowait()
        self.assertEqual(unknown_event, msg)


class WazirxUserStreamReviewFixTests(IsolatedAsyncioWrapperTestCase):
    """
    Regression cover for the PR #34 review findings on the WazirX account websocket.
    """

    def setUp(self):
        super().setUp()
        self.source = WazirxAPIUserStreamDataSource(auth=MagicMock())

    # ── Finding 4: only real account data may look "healthy" ─────────────────
    async def test_last_recv_time_only_advances_for_stream_frames(self):
        # Stamping every TEXT frame made a FAILED SUBSCRIBE look healthy: the 30s
        # app-level ping keeps producing pongs, so _is_user_stream_initialized()
        # reports ready and _get_poll_interval() picks LONG_POLL_INTERVAL — REST
        # polling slows down while zero account data is arriving.
        q = asyncio.Queue()
        self.assertEqual(0.0, self.source.last_recv_time)

        for noise in ({"event": "pong"},
                      {"event": "subscribed", "streams": ["orderUpdate"]}):
            self.source._handle_frame(noise, q)
        self.assertEqual(0.0, self.source.last_recv_time,
                         "a pong/ack must not make a dead stream look alive")
        self.assertTrue(q.empty())

        self.source._handle_frame({"stream": "ownTrade", "data": {"t": "1"}}, q)
        self.assertGreater(self.source.last_recv_time, 0.0)
        self.assertEqual(1, q.qsize())

    async def test_error_frame_is_logged_and_forces_reconnect(self):
        # A rejected auth_key arrives as an error frame. Previously it was silently
        # discarded and the socket sat open forever delivering nothing.
        q = asyncio.Queue()
        with self.assertLogs(level="ERROR") as cm:
            should_reconnect = self.source._handle_frame(
                {"event": "error", "message": "invalid auth_key"}, q)
        self.assertTrue(should_reconnect)
        self.assertTrue(any("NOT flowing" in line for line in cm.output))
        self.assertEqual(0.0, self.source.last_recv_time)

    async def test_unrecognised_frame_is_warned_not_silent(self):
        q = asyncio.Queue()
        with self.assertLogs(level="WARNING") as cm:
            self.source._handle_frame({"something": "unexpected"}, q)
        self.assertTrue(any("Unrecognised" in line for line in cm.output))

    # ── Finding 8: shutdown must not be delayed by the backoff sleep ─────────
    async def test_cancellation_does_not_pay_the_reconnect_backoff(self):
        # The 5s backoff used to sit in the `finally`, so it ran on the
        # CancelledError path too — UserStreamTracker.stop() cancels this task and
        # awaits it, adding ~5s to every connector stop/restart.
        slept = []

        async def record_sleep(d):
            slept.append(d)
            await asyncio.sleep(0)

        self.source._auth.get_ws_auth_key = AsyncMock(side_effect=asyncio.CancelledError())
        with patch.object(self.source, "_sleep", side_effect=record_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.source.listen_for_user_stream(asyncio.Queue())
        self.assertEqual([], slept, "backoff slept while shutting down")

    async def test_reconnect_requests_a_fresh_auth_key(self):
        # A cached key can be nearly expired by the time a reconnect re-subscribes.
        self.source._auth.get_ws_auth_key = AsyncMock(side_effect=asyncio.CancelledError())
        with patch.object(self.source, "_sleep", new=AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):
                await self.source.listen_for_user_stream(asyncio.Queue())
        self.source._auth.get_ws_auth_key.assert_awaited_with(force_refresh=True)
