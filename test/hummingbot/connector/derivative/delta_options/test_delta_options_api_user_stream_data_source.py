from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_api_user_stream_data_source import (
    DeltaOptionsAPIUserStreamDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class DeltaOptionsAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.ws = AsyncMock()
        api_factory = MagicMock()
        api_factory.get_ws_assistant = AsyncMock(return_value=self.ws)
        auth = MagicMock()
        auth.ws_auth_payload.return_value = {"type": "key-auth", "payload": {"api-key": "key"}}
        self.data_source = DeltaOptionsAPIUserStreamDataSource(auth=auth, connector=MagicMock(), api_factory=api_factory)

    async def test_connects_after_delta_confirms_authentication(self):
        self.ws.receive.side_effect = [
            WSResponse(data={"type": "subscriptions", "channels": []}),
            WSResponse(data={"type": "key-auth", "success": True, "status_code": 200, "status": "authenticated"}),
        ]

        ws = await self.data_source._connected_websocket_assistant()

        self.assertIs(self.ws, ws)
        self.assertEqual(CONSTANTS.WSS_PRIVATE_URL, self.ws.connect.call_args.kwargs["ws_url"])
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

    async def test_subscribes_to_option_positions_and_margins(self):
        await self.data_source._subscribe_channels(self.ws)

        channels = self.ws.send.call_args.args[0].payload["payload"]["channels"]
        self.assertEqual([
            {"name": "positions", "symbols": ["call_options", "put_options"]},
            {"name": "margins"},
        ], channels)
