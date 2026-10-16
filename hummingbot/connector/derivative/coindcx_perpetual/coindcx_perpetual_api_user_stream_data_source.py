import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_auth import CoinDCXPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


def private_stream_payload(message: Any) -> Optional[Dict[str, Any]]:
    """
    The event itself. The guide shows bare events; a combined-stream wrapper
    ``{"stream": ..., "data": {...}}`` is accepted too.
    """
    if not isinstance(message, dict):
        return None
    data = message.get("data") if "stream" in message else None
    return data if isinstance(data, dict) else message


class CoinDCXPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private account stream from the CoinDCX FAPI (guide §8).

    Connects to ``{WS_URL}/private/ws?listenKey=…&events=…`` and forwards
    ``ORDER_TRADE_UPDATE``, ``ACCOUNT_UPDATE`` and ``POSITION_UPDATE`` to the
    connector. The guide does not say how a listenKey is obtained, so this follows
    Binance: ``POST /fapi/v1/listenKey`` creates one, ``PUT`` keeps it alive, and a
    failed keep-alive or a ``listenKeyExpired`` frame means a new key and reconnect.
    """

    LISTEN_KEY_RETRY_INTERVAL = 5.0
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinDCXPerpetualAuth,
                 connector: 'CoindcxPerpetualDerivative',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._current_listen_key: Optional[str] = None
        self._last_listen_key_ping_ts: float = 0
        self._listen_key_initialized_event = asyncio.Event()
        self._manage_listen_key_task: Optional[asyncio.Task] = None

    # ---- listenKey ----------------------------------------------------------------

    async def _get_listen_key(self) -> str:
        response = await self._connector._api_post(
            path_url=CONSTANTS.LISTEN_KEY_PATH_URL, data={}, is_auth_required=True)
        listen_key = response.get("listenKey") if isinstance(response, dict) else None
        if not listen_key:
            raise IOError(f"No listenKey in the {CONSTANTS.LISTEN_KEY_PATH_URL} response: {response}")
        return str(listen_key)

    async def _ping_listen_key(self) -> bool:
        try:
            response = await self._connector._api_put(
                path_url=CONSTANTS.LISTEN_KEY_PATH_URL, data={"listenKey": self._current_listen_key},
                is_auth_required=True, return_err=True)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to keep the listenKey alive: {exception}")
            return False
        if isinstance(response, dict) and response.get("code") not in (None, 200):
            self.logger().warning(f"Failed to keep the listenKey alive: {response}")
            return False
        return True

    async def _manage_listen_key_task_loop(self):
        """Obtain a listenKey, then keep it alive until a keep-alive fails."""
        try:
            while True:
                now = time.time()
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now
                    self._listen_key_initialized_event.set()
                    self.logger().info("Obtained a CoinDCX private stream listenKey.")
                elif now - self._last_listen_key_ping_ts >= CONSTANTS.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    if not await self._ping_listen_key():
                        # A dead key means a dead stream: end it so the stream reconnects
                        # with a fresh key.
                        self._expire_listen_key()
                        if self._ws_assistant is not None:
                            await self._ws_assistant.disconnect()
                        return
                    self._last_listen_key_ping_ts = now
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().error(
                f"Could not obtain a CoinDCX private stream listenKey from {CONSTANTS.LISTEN_KEY_PATH_URL} "
                f"(the guide documents no listenKey endpoint; Binance's is assumed): {exception}")
            self._expire_listen_key()

    def _expire_listen_key(self):
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()

    async def _ensure_listen_key_task_running(self):
        if self._manage_listen_key_task is None or self._manage_listen_key_task.done():
            self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    # ---- WebSocket ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ensure_listen_key_task_running()
        try:
            await asyncio.wait_for(self._listen_key_initialized_event.wait(),
                                   timeout=CONSTANTS.PRIVATE_WS_HEARTBEAT_INTERVAL)
        except asyncio.TimeoutError:
            raise IOError("No listenKey for the CoinDCX private stream; retrying.")
        url = (f"{web_utils.wss_url(CONSTANTS.PRIVATE_WS_PATH, self._domain)}"
               f"?listenKey={self._current_listen_key}&events={','.join(CONSTANTS.PRIVATE_EVENTS)}")
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.PRIVATE_WS_HEARTBEAT_INTERVAL)
        self.logger().info("Connected to the CoinDCX private stream.")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """The events are chosen in the connection URL; nothing to send."""
        pass

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        event = private_stream_payload(event_message)
        if not event:
            return
        event_type = event.get("e")
        if event_type == CONSTANTS.LISTEN_KEY_EXPIRED_EVENT:
            self.logger().warning("The CoinDCX private stream listenKey expired; reconnecting with a new one.")
            self._expire_listen_key()
            raise ConnectionError("listenKey expired")
        if event_type in CONSTANTS.PRIVATE_EVENTS:
            queue.put_nowait(event)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
        self._manage_listen_key_task = None
        self._expire_listen_key()
        await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
