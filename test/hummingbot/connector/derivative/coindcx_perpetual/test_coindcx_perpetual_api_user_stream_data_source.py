import asyncio
import os
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_user_stream_data_source import (
    CoinDCXPerpetualAPIUserStreamDataSource,
    private_stream_payload,
)

ORDER_EVENT = {"e": "ORDER_TRADE_UPDATE", "E": 1714003200000,
               "o": {"s": "BTCUSDT", "c": "haveli-1", "X": "NEW", "i": "ex-1", "z": "0"}}


class CoinDCXPerpetualAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    def setUp(self):
        super().setUp()
        self.log_records = []
        self.connector = MagicMock()
        self.connector._api_post = AsyncMock(return_value={"listenKey": "key-1"})
        self.connector._api_put = AsyncMock(return_value={})
        self.api_factory = MagicMock()
        self.ds = CoinDCXPerpetualAPIUserStreamDataSource(
            auth=MagicMock(), connector=self.connector, api_factory=self.api_factory)
        self.ds._sleep = AsyncMock()
        self.ds.logger().setLevel(1)
        self.ds.logger().addHandler(self)

    def handle(self, record):
        self.log_records.append(record)

    def _logged(self, level, fragment):
        return any(record.levelname == level and fragment in record.getMessage() for record in self.log_records)

    # ---- listenKey ----------------------------------------------------------------------

    async def test_a_listen_key_is_created_with_a_signed_post(self):
        self.assertEqual("key-1", await self.ds._get_listen_key())
        self.connector._api_post.assert_awaited_once_with(
            path_url=CONSTANTS.LISTEN_KEY_PATH_URL, data={}, is_auth_required=True)

    async def test_a_response_without_a_listen_key_is_an_error(self):
        self.connector._api_post.return_value = {"code": -1000}
        with self.assertRaises(IOError):
            await self.ds._get_listen_key()

    async def test_keep_alive(self):
        self.ds._current_listen_key = "key-1"
        self.assertTrue(await self.ds._ping_listen_key())
        self.connector._api_put.assert_awaited_once_with(
            path_url=CONSTANTS.LISTEN_KEY_PATH_URL, data={"listenKey": "key-1"}, is_auth_required=True,
            return_err=True)

        self.connector._api_put.return_value = {"code": -1125, "msg": "This listenKey does not exist."}
        self.assertFalse(await self.ds._ping_listen_key())
        self.connector._api_put.side_effect = IOError("down")
        self.assertFalse(await self.ds._ping_listen_key())

    async def test_the_key_is_obtained_then_kept_alive(self):
        clock = iter([1000.0, 1000.0 + CONSTANTS.LISTEN_KEY_KEEP_ALIVE_INTERVAL])
        sleeps = 0

        async def sleep(_):
            nonlocal sleeps
            sleeps += 1
            if sleeps == 2:
                raise asyncio.CancelledError

        self.ds._sleep = sleep
        with patch("hummingbot.connector.derivative.coindcx_perpetual."
                   "coindcx_perpetual_api_user_stream_data_source.time.time", side_effect=lambda: next(clock)):
            with self.assertRaises(asyncio.CancelledError):
                await self.ds._manage_listen_key_task_loop()

        self.assertEqual("key-1", self.ds._current_listen_key)
        self.assertTrue(self.ds._listen_key_initialized_event.is_set())
        self.connector._api_put.assert_awaited_once()

    async def test_a_failed_keep_alive_drops_the_key_and_the_connection(self):
        self.ds._current_listen_key = "key-1"
        self.ds._listen_key_initialized_event.set()
        self.ds._last_listen_key_ping_ts = 0
        self.ds._ws_assistant = AsyncMock()
        self.connector._api_put.return_value = {"code": -1125}

        await self.ds._manage_listen_key_task_loop()

        self.assertIsNone(self.ds._current_listen_key)
        self.assertFalse(self.ds._listen_key_initialized_event.is_set())
        self.ds._ws_assistant.disconnect.assert_awaited_once()

    async def test_a_key_that_cannot_be_obtained_is_reported_clearly(self):
        self.connector._api_post.side_effect = IOError("HTTP status is 404")
        await self.ds._manage_listen_key_task_loop()
        self.assertIsNone(self.ds._current_listen_key)
        self.assertTrue(self._logged("ERROR", "the guide documents no listenKey endpoint"))

    # ---- connection ---------------------------------------------------------------------------

    async def test_connects_with_the_listen_key_and_the_three_events(self):
        async def yielding_sleep(_):
            await asyncio.sleep(0)

        self.ds._sleep = yielding_sleep  # the key task runs for real here
        ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=ws)
        with patch.dict(os.environ, {CONSTANTS.WSS_URL_ENV_VAR: "wss://stream.example"}):
            self.assertIs(ws, await self.ds._connected_websocket_assistant())
            await asyncio.sleep(0)
        ws.connect.assert_awaited_once_with(
            ws_url="wss://stream.example/private/ws?listenKey=key-1"
                   "&events=ORDER_TRADE_UPDATE,ACCOUNT_UPDATE,POSITION_UPDATE",
            ping_timeout=CONSTANTS.PRIVATE_WS_HEARTBEAT_INTERVAL)
        self.ds._manage_listen_key_task.cancel()

    async def test_no_listen_key_means_no_connection_yet(self):
        self.ds._ensure_listen_key_task_running = AsyncMock()  # the key never arrives
        with patch.object(CONSTANTS, "PRIVATE_WS_HEARTBEAT_INTERVAL", 0.01):
            with self.assertRaises(IOError):
                await self.ds._connected_websocket_assistant()

    async def test_an_interruption_drops_the_key_and_its_task(self):
        task = MagicMock()
        task.done.return_value = False
        self.ds._manage_listen_key_task = task
        self.ds._current_listen_key = "key-1"
        self.ds._listen_key_initialized_event.set()

        await self.ds._on_user_stream_interruption(websocket_assistant=None)

        task.cancel.assert_called_once()
        self.assertIsNone(self.ds._manage_listen_key_task)
        self.assertIsNone(self.ds._current_listen_key)
        self.assertFalse(self.ds._listen_key_initialized_event.is_set())

    # ---- events -----------------------------------------------------------------------------------

    async def test_private_events_are_forwarded(self):
        queue = asyncio.Queue()
        for event in ({"e": "ORDER_TRADE_UPDATE"}, {"e": "ACCOUNT_UPDATE"}, {"e": "POSITION_UPDATE"},
                      {"stream": "key-1", "data": ORDER_EVENT}):
            await self.ds._process_event_message(event, queue)
        await self.ds._process_event_message({"e": "somethingElse"}, queue)
        await self.ds._process_event_message({"result": None, "id": 1}, queue)
        self.assertEqual(4, queue.qsize())

    async def test_an_expired_listen_key_ends_the_connection(self):
        self.ds._current_listen_key = "key-1"
        with self.assertRaises(ConnectionError):
            await self.ds._process_event_message({"e": "listenKeyExpired", "E": 1}, asyncio.Queue())
        self.assertIsNone(self.ds._current_listen_key)

    def test_payload_unwrapping(self):
        self.assertEqual(ORDER_EVENT, private_stream_payload(ORDER_EVENT))
        self.assertEqual(ORDER_EVENT, private_stream_payload({"stream": "x", "data": ORDER_EVENT}))
        self.assertIsNone(private_stream_payload("not a dict"))
