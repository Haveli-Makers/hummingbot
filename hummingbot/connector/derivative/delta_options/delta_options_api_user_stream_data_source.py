import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.derivative.delta_options import (
    delta_options_constants as CONSTANTS,
    delta_options_web_utils as web_utils,
)
from hummingbot.connector.derivative.delta_options.delta_options_auth import DeltaOptionsAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative


class DeltaOptionsAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private WebSocket: authenticates with `key-auth`, waits for Delta to confirm, then subscribes to option orders,
    fills and positions, and to wallet (margins) updates.
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
        self._missed_updates = False

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=web_utils.private_wss_url(self._domain), ping_timeout=CONSTANTS.WS_PING_INTERVAL)
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
        options = [CONSTANTS.CALL_OPTIONS, CONSTANTS.PUT_OPTIONS]
        channels = [
            {"name": CONSTANTS.WS_ORDERS_CHANNEL, "symbols": options},
            {"name": CONSTANTS.WS_USER_TRADES_CHANNEL, "symbols": options},
            {"name": CONSTANTS.WS_POSITIONS_CHANNEL, "symbols": options},
            {"name": CONSTANTS.WS_MARGINS_CHANNEL},
        ]
        await websocket_assistant.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))
        self.logger().info("Subscribed to Delta option orders, fills, positions and wallet updates.")
        if self._missed_updates:
            # Back after a drop: what was sent meanwhile is gone, so the connector re-reads it over REST.
            self._missed_updates = False
            self._connector.resync_after_stream_gap()

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        self._missed_updates = True
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        message_type = event_message.get("type", "")
        if message_type == "subscriptions":
            for channel in event_message.get("channels") or []:
                if channel.get("error"):
                    self.logger().error(f"Delta rejected the {channel.get('name')} subscription: {channel['error']}")
        elif message_type == "error":
            self.logger().error(f"Delta private WebSocket error: {event_message}")
        elif message_type not in ("", CONSTANTS.WS_AUTH_TYPE, "success", "heartbeat", "pong"):
            queue.put_nowait(event_message)
