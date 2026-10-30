import asyncio
from typing import TYPE_CHECKING

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_auth import DeltaOptionsAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative


class DeltaOptionsAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private WebSocket: authenticates with `key-auth`, waits for Delta to confirm, then subscribes to option position
    updates and wallet (margins) updates.
    """

    def __init__(
        self,
        auth: DeltaOptionsAuth,
        connector: "DeltaOptionsDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_PRIVATE_URL, ping_timeout=CONSTANTS.WS_PING_INTERVAL)
        try:
            await ws.send(WSJSONRequest(payload=self._auth.ws_auth_payload()))
            await asyncio.wait_for(self._wait_for_authentication(ws), timeout=CONSTANTS.WS_AUTH_TIMEOUT)
        except Exception:
            await ws.disconnect()
            raise
        return ws

    async def _wait_for_authentication(self, ws: WSAssistant):
        while True:
            response = await ws.receive()
            if response is None:
                raise ConnectionError("The Delta private WebSocket closed during authentication.")
            message = response.data
            if isinstance(message, dict) and message.get("type") == CONSTANTS.WS_AUTH_TYPE:
                if message.get("success"):
                    return
                raise IOError(f"Delta WebSocket authentication failed: {message.get('status')} {message.get('message', '')}")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        channels = [
            {"name": CONSTANTS.WS_POSITIONS_CHANNEL, "symbols": [CONSTANTS.CALL_OPTIONS, CONSTANTS.PUT_OPTIONS]},
            {"name": CONSTANTS.WS_MARGINS_CHANNEL},
        ]
        await websocket_assistant.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))
        self.logger().info("Subscribed to Delta option positions and wallet updates.")
