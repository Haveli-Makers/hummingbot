import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source import (
    CoinswitchAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

_VALID_SECRET = "aa" * 32


def _make_data_source(trading_pairs=None) -> CoinswitchAPIUserStreamDataSource:
    connector = CoinswitchExchange(
        coinswitch_api_key="test_api_key",
        coinswitch_api_secret=_VALID_SECRET,
        trading_pairs=trading_pairs or ["BTC-INR"],
        trading_required=False,
    )
    return CoinswitchAPIUserStreamDataSource(
        auth=connector._auth,
        trading_pairs=trading_pairs or ["BTC-INR"],
        connector=connector,
        api_factory=connector._web_assistants_factory,
    )


class CoinswitchUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.data_source = _make_data_source()

    def test_last_recv_time_initial_value(self):
        self.assertEqual(0.0, self.data_source.last_recv_time)

    def test_last_recv_time_can_be_updated(self):
        self.data_source._last_recv_time = 12345.67
        self.assertEqual(12345.67, self.data_source.last_recv_time)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    def test_build_order_client_returns_socketio_client(self, mock_cls):
        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, namespace=None: (lambda f: f))
        mock_cls.return_value = mock_client

        q = asyncio.Queue()
        client = self.data_source._build_order_client(q)

        mock_cls.assert_called_once_with(logger=False, reconnection=False)
        self.assertIs(client, mock_client)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    def test_build_order_client_registers_handlers(self, mock_cls):
        mock_client = MagicMock()
        event_calls = []
        on_calls = []
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: (event_calls.append(f.__name__), f)[1]))
        mock_client.on = MagicMock(side_effect=lambda event, namespace=None: (lambda f: (on_calls.append(event), f)[1]))
        mock_cls.return_value = mock_client

        q = asyncio.Queue()
        self.data_source._build_order_client(q)

        # Should register connect, disconnect events and order update + error handlers
        self.assertGreaterEqual(mock_client.event.call_count, 2)
        self.assertGreaterEqual(mock_client.on.call_count, 1)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    def test_build_balance_client_returns_socketio_client(self, mock_cls):
        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, namespace=None: (lambda f: f))
        mock_cls.return_value = mock_client

        q = asyncio.Queue()
        client = self.data_source._build_balance_client(q)

        self.assertIs(client, mock_client)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    async def test_on_order_update_adds_event_key_and_puts_in_queue(self, mock_cls):
        """The on_order_update handler should tag the message with event key and put it in the queue."""
        q = asyncio.Queue()
        captured_handler = {}

        def capture_on(event, namespace=None):
            def decorator(func):
                captured_handler[event] = func
                return func
            return decorator

        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=capture_on)
        mock_cls.return_value = mock_client

        self.data_source._build_order_client(q)

        # Simulate receiving an order update
        handler = captured_handler.get(CONSTANTS.ORDER_UPDATE_EVENT_TYPE)
        self.assertIsNotNone(handler)
        msg = {"order_id": "ex_001", "status": "EXECUTED"}
        await handler(msg)

        self.assertFalse(q.empty())
        item = q.get_nowait()
        self.assertEqual(CONSTANTS.ORDER_UPDATE_EVENT_TYPE, item.get("event"))

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    async def test_on_balance_update_adds_event_key_and_puts_in_queue(self, mock_cls):
        q = asyncio.Queue()
        captured_handler = {}

        def capture_on(event, namespace=None):
            def decorator(func):
                captured_handler[event] = func
                return func
            return decorator

        mock_client = MagicMock()
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=capture_on)
        mock_cls.return_value = mock_client

        self.data_source._build_balance_client(q)

        handler = captured_handler.get(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE)
        self.assertIsNotNone(handler)
        msg = {"currency": "BTC", "main_balance": "1.0"}
        await handler(msg)

        self.assertFalse(q.empty())
        item = q.get_nowait()
        self.assertEqual(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE, item.get("event"))

    async def test_disconnect_all_disconnects_both_clients(self):
        order_mock = AsyncMock()
        order_mock.disconnect = AsyncMock()
        balance_mock = AsyncMock()
        balance_mock.disconnect = AsyncMock()

        self.data_source._order_client = order_mock
        self.data_source._balance_client = balance_mock

        await self.data_source._disconnect_all()

        order_mock.disconnect.assert_called_once()
        balance_mock.disconnect.assert_called_once()
        self.assertIsNone(self.data_source._order_client)
        self.assertIsNone(self.data_source._balance_client)

    async def test_disconnect_all_handles_order_client_exception(self):
        order_mock = AsyncMock()
        order_mock.disconnect = AsyncMock(side_effect=Exception("order disconnect failed"))
        balance_mock = AsyncMock()
        balance_mock.disconnect = AsyncMock()

        self.data_source._order_client = order_mock
        self.data_source._balance_client = balance_mock

        await self.data_source._disconnect_all()  # Should not raise

        self.assertIsNone(self.data_source._order_client)
        self.assertIsNone(self.data_source._balance_client)

    async def test_disconnect_all_handles_balance_client_exception(self):
        order_mock = AsyncMock()
        order_mock.disconnect = AsyncMock()
        balance_mock = AsyncMock()
        balance_mock.disconnect = AsyncMock(side_effect=Exception("balance disconnect failed"))

        self.data_source._order_client = order_mock
        self.data_source._balance_client = balance_mock

        await self.data_source._disconnect_all()  # Should not raise

        self.assertIsNone(self.data_source._order_client)

    async def test_disconnect_all_handles_none_clients(self):
        self.data_source._order_client = None
        self.data_source._balance_client = None
        await self.data_source._disconnect_all()

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    async def test_listen_for_user_stream_connects_both_clients(self, mock_cls):
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.wait = AsyncMock(side_effect=asyncio.CancelledError)
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, namespace=None: (lambda f: f))
        mock_cls.return_value = mock_client

        output_queue = asyncio.Queue()

        task = asyncio.create_task(self.data_source.listen_for_user_stream(output_queue))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        self.assertGreaterEqual(mock_client.connect.call_count, 1)

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    async def test_listen_for_user_stream_uses_ws_url_for_order_client(self, mock_cls):
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.wait = AsyncMock(side_effect=asyncio.CancelledError)
        mock_client.event = MagicMock(side_effect=lambda namespace=None: (lambda f: f))
        mock_client.on = MagicMock(side_effect=lambda event, namespace=None: (lambda f: f))
        mock_cls.return_value = mock_client

        output_queue = asyncio.Queue()

        task = asyncio.create_task(self.data_source.listen_for_user_stream(output_queue))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        connect_urls = [call[0][0] for call in mock_client.connect.call_args_list]
        for url in connect_urls:
            self.assertEqual(CONSTANTS.WS_URL, url, "User stream must connect via https:// (WS_URL), not wss://")

    async def test_subscribe_to_user_stream_is_noop(self):
        await self.data_source._subscribe_to_user_stream()  # Should not raise

    async def test_unsubscribe_from_user_stream_is_noop(self):
        await self.data_source._unsubscribe_from_user_stream()  # Should not raise

    @patch("hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source.socketio.AsyncClient")
    async def test_ping_task_sends_ping_to_connected_clients(self, mock_cls):
        order_mock = MagicMock()
        order_mock.connected = True
        order_mock.emit = AsyncMock()
        balance_mock = MagicMock()
        balance_mock.connected = True
        balance_mock.emit = AsyncMock()

        self.data_source._order_client = order_mock
        self.data_source._balance_client = balance_mock

        with patch.object(CONSTANTS, "WS_HEARTBEAT_TIME_INTERVAL", 0):
            task = asyncio.create_task(self.data_source._ping_task())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        order_mock.emit.assert_called()
        balance_mock.emit.assert_called()


if __name__ == "__main__":
    import unittest
    unittest.main()
