import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS
from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private WebSocket stream for CoinEx spot.

    After connecting, a `server.sign` message authenticates the socket, then we
    subscribe to the private `order` and `balance` channels. Account events are
    forwarded to the connector's `_user_stream_event_listener`. WS frames are
    gzip-binary; the factory's CoinexWSPostProcessor decompresses them first.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: CoinexAuth,
        trading_pairs: List[str],
        connector: "CoinexExchange",
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
        # Authenticate the socket with a single signed `server.sign` message.
        await ws.send(WSJSONRequest(payload=self._auth.ws_auth_payload(request_id=1)))
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            symbols = [
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=tp)
                for tp in self._trading_pairs
            ]
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_ORDER_SUBSCRIBE,
                "params": {"market_list": symbols},
                "id": 2,
            }))
            await websocket_assistant.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_BALANCE_SUBSCRIBE,
                "params": {"ccy_list": []},  # empty = all currencies
                "id": 3,
            }))
            self.logger().info("Subscribed to CoinEx private order / balance channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinEx private streams.")
            raise

    async def _process_event_message(self, event_message: dict, queue: asyncio.Queue):
        if not isinstance(event_message, dict):
            return
        method = event_message.get("method", "")
        # Forward only account push events; skip auth/subscribe acks and pongs.
        if method in (CONSTANTS.WS_ORDER_UPDATE, CONSTANTS.WS_BALANCE_UPDATE):
            queue.put_nowait(event_message)
        elif event_message.get("code") not in (0, None):
            self.logger().error(f"CoinEx private WS error: {event_message}")
