import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.wazirx_perpetual import wazirx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_api_user_stream_data_source import (
    WazirxPerpetualAPIUserStreamDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import RESTMethod


class WazirxPerpetualUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.rest = AsyncMock()
        self.rest.execute_request = AsyncMock(return_value={"auth_key": "Xx-key", "timeout_duration": 1800})
        api_factory = MagicMock()
        api_factory.get_rest_assistant = AsyncMock(return_value=self.rest)
        self.now = 1_000.0
        self.data_source = WazirxPerpetualAPIUserStreamDataSource(
            auth=MagicMock(), trading_pairs=["BTC-INR"], connector=MagicMock(), api_factory=api_factory)
        self.data_source._time = lambda: self.now
        self.data_source._sleep = AsyncMock()

    async def test_auth_key_is_signed_post_and_cached(self):
        self.assertEqual("Xx-key", await self.data_source._get_auth_key())
        kwargs = self.rest.execute_request.call_args.kwargs
        self.assertEqual(RESTMethod.POST, kwargs["method"])
        self.assertTrue(kwargs["is_auth_required"])
        self.assertTrue(kwargs["url"].endswith(CONSTANTS.CREATE_AUTH_TOKEN_PATH_URL))

        self.now += 60
        await self.data_source._get_auth_key()
        self.assertEqual(1, self.rest.execute_request.call_count)

        self.now += CONSTANTS.AUTH_KEY_REFRESH_INTERVAL
        await self.data_source._get_auth_key()
        self.assertEqual(2, self.rest.execute_request.call_count)

    async def test_missing_auth_key_raises(self):
        self.rest.execute_request = AsyncMock(return_value={"code": 2112, "message": "API key is missing"})
        with self.assertRaises(IOError):
            await self.data_source._get_auth_key()

    async def test_subscribe_carries_the_auth_key(self):
        ws = AsyncMock()
        await self.data_source._subscribe_channels(ws)
        payload = ws.send.call_args.args[0].payload
        self.assertEqual({
            "event": "subscribe",
            "streams": ["orderUpdate", "ownTrade", "outboundAccountPosition", "positionUpdate"],
            "auth_key": "Xx-key",
        }, payload)

    async def test_private_frames_are_forwarded_and_control_frames_are_not(self):
        queue = asyncio.Queue()
        for frame in ({"data": {"timeout_duration": 1800}, "event": "connected"},
                      {"data": {"streams": CONSTANTS.PRIVATE_STREAMS}, "event": "subscribed", "id": 0},
                      {"data": {"timeout_duration": 1800}, "event": "pong", "id": 0},
                      {"data": {"e": "aggTrade"}, "stream": "btcinr@aggTrade"},
                      {"data": {"e": "orderUpdate", "X": "wait"}, "stream": "orderUpdate"},
                      {"data": {"e": "ownTrade"}, "stream": "ownTrade"}):
            await self.data_source._process_event_message(frame, queue)

        forwarded = [queue.get_nowait()["stream"] for _ in range(queue.qsize())]
        self.assertEqual(["orderUpdate", "ownTrade"], forwarded)

    async def test_rejected_subscription_drops_the_key_and_reconnects(self):
        await self.data_source._get_auth_key()
        with self.assertRaises(ConnectionError):
            await self.data_source._process_event_message(
                {"data": {"code": 401, "message": "Invalid request: unautorized access"}, "event": "error", "id": 0},
                asyncio.Queue())
        self.assertIsNone(self.data_source._auth_key)
        self.data_source._sleep.assert_awaited_with(self.data_source.AUTH_ERROR_RETRY_DELAY)

    async def test_keepalive_pings_and_refreshes_the_key(self):
        await self.data_source._get_auth_key()
        ws = AsyncMock()
        rounds = []

        async def _sleep(delay):
            rounds.append(delay)
            self.now += CONSTANTS.AUTH_KEY_REFRESH_INTERVAL
            if len(rounds) > 1:
                raise asyncio.CancelledError

        self.data_source._sleep = _sleep
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._keepalive_loop(ws)

        self.assertEqual({"event": "ping"}, ws.send.call_args.args[0].payload)
        self.assertEqual(2, self.rest.execute_request.call_count)
