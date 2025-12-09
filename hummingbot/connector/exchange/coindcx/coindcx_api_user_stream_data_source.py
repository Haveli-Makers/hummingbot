import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoinDCXExchange


class CoinDCXAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for CoinDCX.
    Handles WebSocket connections for receiving user account updates including:
    - Balance updates
    - Order updates
    - Trade updates
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    PING_INTERVAL = 20.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinDCXAuth,
                 trading_pairs: List[str],
                 connector: 'CoinDCXExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: CoinDCXAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_ws_message_sent_timestamp = 0

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WSAssistant instance.
        """
        return await self._api_factory.get_ws_assistant()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an authenticated WebSocket connection for user stream data.
        CoinDCX uses a 'join' message with authentication to subscribe to private channels.
        """
        ws: WSAssistant = await self._get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
            message_timeout=None
        )
        
        auth_payload = self._auth.generate_ws_auth_payload()
        join_message = {
            "type": "join",
            **auth_payload
        }
        
        await ws.send(WSJSONRequest(payload=join_message))
        self.logger().info("Authenticated and joined CoinDCX private channel")
        
        self._ws_assistant = ws
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribe to user-specific channels.
        CoinDCX's private channel 'coindcx' automatically provides:
        - balance-update events
        - order-update events
        - trade-update events
        
        No additional subscription is needed after joining the authenticated channel.
        """
        try:
            self.logger().info("Subscribed to CoinDCX user stream channels (balance, order, trade updates)")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to user stream channels...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Processes incoming WebSocket messages and adds them to the queue.
        Also handles periodic ping to keep the connection alive.
        """
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            
            if isinstance(data, dict):
                event_type = data.get("event", data.get("e", ""))
                
                if event_type in ["ping", "pong"]:
                    continue
                
                if event_type in [
                    CONSTANTS.BALANCE_UPDATE_EVENT_TYPE,
                    CONSTANTS.ORDER_UPDATE_EVENT_TYPE,
                    CONSTANTS.TRADE_UPDATE_EVENT_TYPE,
                    "balance-update",
                    "order-update",
                    "trade-update"
                ]:
                    queue.put_nowait(data)
                elif "data" in data:
                    queue.put_nowait(data["data"])
                else:
                    queue.put_nowait(data)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        """
        Called when the user stream is interrupted.
        Performs cleanup and resets state.
        """
        self._ws_assistant = None
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Main method to listen for user stream data.
        Establishes connection and processes messages.
        """
        while True:
            try:
                ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws_assistant)
                await self._process_websocket_messages(ws_assistant, output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.",
                    exc_info=True
                )
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                await self._sleep(5.0)
