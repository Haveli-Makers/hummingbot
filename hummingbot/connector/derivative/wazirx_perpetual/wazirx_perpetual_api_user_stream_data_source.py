import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.wazirx_perpetual import (
    wazirx_perpetual_constants as CONSTANTS,
    wazirx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_auth import WazirxPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative


class WazirxPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private account stream for WazirX futures.

    A signed ``POST /sapi/v1/create_auth_token`` yields an ``auth_key`` (valid 30
    minutes, one per member, shared with spot). The key goes inside the
    subscribe message for the private streams — ``orderUpdate``, ``ownTrade``,
    ``outboundAccountPosition`` and ``positionUpdate`` — on the same socket host
    as the public data. Requesting the key again while it is live returns the
    same key and extends it, so it is refreshed on a timer while connected.

    A rejected subscription comes back as ``{"event": "error", "data":
    {"code": 401, ...}}`` and the socket stays open but silent. That is treated
    as a broken stream: the cached key is dropped and the connection is torn
    down so the base class reconnects with a fresh key.
    """

    _logger: Optional[HummingbotLogger] = None

    AUTH_ERROR_RETRY_DELAY = 5.0

    def __init__(self,
                 auth: WazirxPerpetualAuth,
                 trading_pairs: List[str],
                 connector: "WazirxPerpetualDerivative",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._auth_key: Optional[str] = None
        self._auth_key_ts: float = 0.0

    async def _get_auth_key(self, force_refresh: bool = False) -> str:
        now = self._time()
        if (not force_refresh and self._auth_key is not None
                and now - self._auth_key_ts < CONSTANTS.AUTH_KEY_REFRESH_INTERVAL):
            return self._auth_key

        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.private_rest_url(CONSTANTS.CREATE_AUTH_TOKEN_PATH_URL, domain=self._domain),
            method=RESTMethod.POST,
            throttler_limit_id=CONSTANTS.CREATE_AUTH_TOKEN_PATH_URL,
            is_auth_required=True,
        )
        auth_key = response.get("auth_key") if isinstance(response, dict) else None
        if not auth_key:
            raise IOError(f"WazirX did not return a websocket auth_key: {response}")
        self._auth_key = auth_key
        self._auth_key_ts = now
        return auth_key

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=web_utils.wss_url(self._domain), ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            auth_key = await self._get_auth_key()
            await websocket_assistant.send(WSJSONRequest(payload={
                "event": CONSTANTS.SUBSCRIBE_EVENT,
                "streams": CONSTANTS.PRIVATE_STREAMS,
                "auth_key": auth_key,
            }))
            self.logger().info(f"Subscribed to WazirX futures private streams {CONSTANTS.PRIVATE_STREAMS}.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to WazirX futures private streams.")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        keepalive_task = safe_ensure_future(self._keepalive_loop(websocket_assistant))
        try:
            await super()._process_websocket_messages(websocket_assistant=websocket_assistant, queue=queue)
        finally:
            keepalive_task.cancel()

    async def _keepalive_loop(self, websocket_assistant: WSAssistant):
        """
        Sends the application ping (keeps the 30-minute connection alive) and
        re-requests the auth_key before it lapses, which extends the same key.
        """
        while True:
            await self._sleep(CONSTANTS.WS_PING_INTERVAL)
            try:
                await websocket_assistant.send(WSJSONRequest(payload={"event": CONSTANTS.PING_EVENT}))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().debug("Failed to send the WazirX futures private stream ping.", exc_info=True)
                return
            if self._time() - self._auth_key_ts >= CONSTANTS.AUTH_KEY_REFRESH_INTERVAL:
                try:
                    await self._get_auth_key(force_refresh=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exception:
                    self.logger().warning(f"Could not refresh the WazirX websocket auth_key: {exception}")

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if not isinstance(event_message, dict) or not event_message:
            return
        event = event_message.get("event")
        if event == CONSTANTS.ERROR_EVENT:
            self._auth_key = None
            self.logger().error(
                f"WazirX futures private stream rejected the subscription: {event_message.get('data')}. "
                f"Reconnecting with a fresh auth_key in {self.AUTH_ERROR_RETRY_DELAY:.0f}s.")
            await self._sleep(self.AUTH_ERROR_RETRY_DELAY)
            raise ConnectionError(f"WazirX futures private stream error: {event_message.get('data')}")
        if event == CONSTANTS.SUBSCRIBED_EVENT:
            accepted = set((event_message.get("data") or {}).get("streams") or [])
            missing = [stream for stream in CONSTANTS.PRIVATE_STREAMS if stream not in accepted]
            if missing:
                self.logger().warning(f"WazirX futures did not acknowledge private streams {missing}.")
            return
        if event is not None:
            # connected / pong / unsubscribed
            return
        if event_message.get("stream") in CONSTANTS.PRIVATE_STREAMS:
            queue.put_nowait(event_message)
