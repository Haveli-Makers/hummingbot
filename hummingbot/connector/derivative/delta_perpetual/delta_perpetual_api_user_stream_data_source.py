import asyncio
from typing import TYPE_CHECKING, Optional

from hummingbot.connector.derivative.delta_perpetual import (
    delta_perpetual_constants as CONSTANTS,
    delta_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_auth import DeltaPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative


class DeltaPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private stream: logs in with `key-auth`, waits for Delta to confirm, then subscribes to
    orders, positions and user trades (all perpetuals) and margins (wallet).
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: DeltaPerpetualAuth,
        connector: "DeltaPerpetualDerivative",
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
        await ws.connect(ws_url=web_utils.private_wss_url(self._domain), ping_timeout=CONSTANTS.PING_TIMEOUT)
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
        try:
            # By category, so a delisted pair can't break the subscription.
            perpetuals = [CONSTANTS.PERPETUAL_CONTRACT_TYPE]
            channels = [
                {"name": CONSTANTS.WS_ORDERS_CHANNEL, "symbols": perpetuals},
                {"name": CONSTANTS.WS_POSITIONS_CHANNEL, "symbols": perpetuals},
                {"name": CONSTANTS.WS_USER_TRADES_CHANNEL, "symbols": perpetuals},
                {"name": CONSTANTS.WS_WALLET_CHANNEL},
            ]
            await websocket_assistant.send(
                WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}})
            )
            self.logger().info("Subscribed to Delta private user channels (orders/positions/trades/margins).")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Delta private streams.")
            raise

    async def _process_event_message(self, event_message: dict, queue: asyncio.Queue):
        msg_type = event_message.get("type", "")
        if msg_type == "subscriptions":
            for channel in event_message.get("channels") or []:
                if channel.get("error"):
                    self.logger().error(f"Delta rejected the {channel.get('name')} subscription: {channel['error']}")
            return
        # Skip acks / heartbeats; forward real account events.
        if msg_type in ("success", CONSTANTS.WS_AUTH_TYPE, "heartbeat", "pong", ""):
            return
        if msg_type == "error":
            self.logger().error(f"Delta private WS error: {event_message}")
            return
        queue.put_nowait(event_message)
