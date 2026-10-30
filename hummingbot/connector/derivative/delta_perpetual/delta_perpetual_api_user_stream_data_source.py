import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.derivative.delta_perpetual import delta_perpetual_constants as CONSTANTS
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
    Private WebSocket stream for Delta Exchange perpetuals.

    After connecting, an `auth` message (signed "GET" + timestamp + "/live") is
    sent, then we subscribe to the private channels: orders, positions,
    user trades and margins (wallet). Messages are forwarded verbatim to the
    connector's `_user_stream_event_listener`.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: DeltaPerpetualAuth,
        trading_pairs: List[str],
        connector: "DeltaPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
        # Authenticate the socket with a single signed `auth` message.
        await ws.send(WSJSONRequest(payload=self._auth.ws_auth_payload()))
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            symbols = [
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=tp)
                for tp in self._trading_pairs
            ]
            channels = [
                {"name": CONSTANTS.WS_ORDERS_CHANNEL, "symbols": symbols},
                {"name": CONSTANTS.WS_POSITIONS_CHANNEL, "symbols": symbols},
                {"name": CONSTANTS.WS_USER_TRADES_CHANNEL, "symbols": symbols},
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
        # Skip subscription acks / heartbeats; forward real account events.
        if msg_type in ("subscriptions", "success", "auth", "pong", ""):
            return
        if msg_type == "error":
            self.logger().error(f"Delta private WS error: {event_message}")
            return
        queue.put_nowait(event_message)
