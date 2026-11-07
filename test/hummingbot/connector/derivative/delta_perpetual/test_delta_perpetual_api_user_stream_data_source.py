import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_api_user_stream_data_source import (
    DeltaPerpetualAPIUserStreamDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class DeltaPerpetualAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.ws = AsyncMock()
        api_factory = MagicMock()
        api_factory.get_ws_assistant = AsyncMock(return_value=self.ws)
        auth = MagicMock()
        auth.ws_auth_payload.return_value = {"type": "key-auth", "payload": {"api-key": "key"}}
        self.data_source = DeltaPerpetualAPIUserStreamDataSource(auth=auth, connector=MagicMock(), api_factory=api_factory)

    async def test_connects_after_delta_confirms_authentication(self):
        self.ws.receive.side_effect = [
            WSResponse(data={"type": "subscriptions", "channels": []}),
            WSResponse(data={"type": "key-auth", "success": True, "status_code": 200, "status": "authenticated"}),
        ]

        ws = await self.data_source._connected_websocket_assistant()

        self.assertIs(self.ws, ws)
        self.assertEqual("wss://socket.india.delta.exchange", self.ws.connect.call_args.kwargs["ws_url"])
        self.assertEqual("key-auth", self.ws.send.call_args.args[0].payload["type"])
        self.ws.disconnect.assert_not_awaited()

    async def test_failed_authentication_raises_and_disconnects(self):
        self.ws.receive.return_value = WSResponse(data={
            "type": "key-auth", "success": False, "status_code": 401, "status": "ip_not_whitelisted",
            "message": "IP address not whitelisted"})

        with self.assertRaisesRegex(IOError, "ip_not_whitelisted"):
            await self.data_source._connected_websocket_assistant()

        self.ws.disconnect.assert_awaited_once()

    async def test_socket_closing_during_authentication_raises(self):
        self.ws.receive.return_value = None

        with self.assertRaises(ConnectionError):
            await self.data_source._connected_websocket_assistant()

        self.ws.disconnect.assert_awaited_once()

    async def test_subscribes_to_perpetual_categories(self):
        await self.data_source._subscribe_channels(self.ws)

        channels = self.ws.send.call_args.args[0].payload["payload"]["channels"]
        self.assertEqual([
            {"name": "orders", "symbols": ["perpetual_futures"]},
            {"name": "positions", "symbols": ["perpetual_futures"]},
            {"name": "v2/user_trades", "symbols": ["perpetual_futures"]},
            {"name": "margins"},
        ], channels)

    async def test_account_events_are_forwarded_and_acks_are_not(self):
        queue = asyncio.Queue()
        for message in ({"type": "key-auth", "success": True}, {"type": "heartbeat"},
                        {"type": "orders", "order_id": 1}):
            await self.data_source._process_event_message(message, queue)

        self.assertEqual({"type": "orders", "order_id": 1}, queue.get_nowait())
        self.assertTrue(queue.empty())

    async def test_rejected_subscription_is_logged(self):
        with self.assertLogs(self.data_source.logger(), level="ERROR") as logs:
            await self.data_source._process_event_message(
                {"type": "subscriptions", "channels": [{"name": "orders", "error": "Unauthorized user"}]},
                asyncio.Queue())

        self.assertIn("Unauthorized user", logs.output[0])
