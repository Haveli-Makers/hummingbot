import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_auth import ValrAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private WebSocket account stream for VALR (wss://api.valr.com/ws/account).

    VALR authenticates this socket with the HMAC headers on the connection
    UPGRADE (signed over "/ws/account"); once connected it streams account events
    (ORDER_STATUS_UPDATE, BALANCE_UPDATE, NEW_ACCOUNT_TRADE, …) without an explicit
    subscribe. Events are forwarded to the connector's _user_stream_event_listener.
    """

    _logger: Optional[HummingbotLogger] = None
    _ACCOUNT_PATH = "/ws/account"

    def __init__(
        self,
        auth: ValrAuth,
        trading_pairs: List[str],
        connector: "ValrExchange",
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
        await ws.connect(
            ws_url=CONSTANTS.WSS_ACCOUNT_URL,
            ws_headers=self._auth.ws_auth_headers(self._ACCOUNT_PATH),
            ping_timeout=CONSTANTS.PING_TIMEOUT,
        )
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        # VALR streams account events automatically once the socket is authenticated
        # (via the upgrade headers) — no explicit subscribe message is required.
        self.logger().info("VALR account stream authenticated; receiving account events.")

    async def _process_event_message(self, event_message: dict, queue: asyncio.Queue):
        if not isinstance(event_message, dict):
            return
        event_type = event_message.get("type", "")
        if event_type in (CONSTANTS.WS_ORDER_STATUS_UPDATE, CONSTANTS.WS_BALANCE_UPDATE,
                          CONSTANTS.WS_NEW_ACCOUNT_TRADE, CONSTANTS.WS_OPEN_ORDERS_UPDATE):
            queue.put_nowait(event_message)
        elif event_type == CONSTANTS.WS_UNAUTHORIZED:
            self.logger().error(f"VALR account WebSocket auth rejected: {event_message}")
