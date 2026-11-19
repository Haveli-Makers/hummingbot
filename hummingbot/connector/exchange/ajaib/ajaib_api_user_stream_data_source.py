import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_web_utils as web_utils
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Ajaib user-data stream.

    A listenKey is obtained from ``POST /auth/v1/listen-key`` (API-key header
    only, no signing), the websocket connects to ``/ws/<listenKey>`` and Ajaib
    pushes ``executionReport`` events without an explicit subscription. The key
    is valid for 60 minutes and refreshed every 30 minutes via ``PUT``.
    """

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # 30 minutes
    HEARTBEAT_TIME_INTERVAL = 30.0
    LISTEN_KEY_RETRY_INTERVAL = 5.0
    MAX_RETRIES = 3

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: AjaibAuth,
                 trading_pairs: List[str],
                 connector: 'AjaibExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: AjaibAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._trading_pairs = trading_pairs
        self._current_listen_key = None
        self._last_listen_key_ping_ts = None
        self._manage_listen_key_task = None
        self._listen_key_initialized_event = asyncio.Event()

    async def _get_listen_key(self, max_retries: int = MAX_RETRIES) -> str:
        retry_count = 0
        backoff_time = 1.0
        rest_assistant = await self._api_factory.get_rest_assistant()
        while True:
            try:
                # Must be SIGNED, not just API-key-headered. Verified live:
                # an unsigned POST (with or without Content-Type) is rejected
                # 400 -1102 "Bad Request", while a signed form body returns the
                # key. is_auth_required routes it through AjaibAuth, which puts
                # timestamp/recvWindow/signature in the form body for POSTs.
                data = await rest_assistant.execute_request(
                    url=web_utils.public_rest_url(path_url=CONSTANTS.LISTEN_KEY_PATH_URL, domain=self._domain),
                    method=RESTMethod.POST,
                    data={},
                    throttler_limit_id=CONSTANTS.LISTEN_KEY_PATH_URL,
                    is_auth_required=True,
                )
                return data["listenKey"]
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                retry_count += 1
                if retry_count > max_retries:
                    raise IOError(f"Error fetching user stream listen key after {max_retries} retries. "
                                  f"Error: {exception}")
                self.logger().warning(f"Retry {retry_count}/{max_retries} fetching user stream listen key. "
                                      f"Error: {repr(exception)}")
                await self._sleep(backoff_time)
                backoff_time *= 2

    async def _ping_listen_key(self) -> bool:
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.LISTEN_KEY_PATH_URL, domain=self._domain),
                data={"listenKey": self._current_listen_key},
                method=RESTMethod.PUT,
                return_err=True,
                throttler_limit_id=CONSTANTS.LISTEN_KEY_PATH_URL,
                is_auth_required=True,
            )
            if isinstance(data, dict) and "code" in data:
                self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {data}")
                return False
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {exception}")
            return False
        return True

    async def _manage_listen_key_task_loop(self):
        self.logger().info("Starting listen key management task...")
        while True:
            try:
                now = int(time.time())
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now
                    self._listen_key_initialized_event.set()
                    self.logger().info(f"Successfully obtained listen key {self._current_listen_key}")

                if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success = await self._ping_listen_key()
                    if success:
                        self.logger().info(f"Successfully refreshed listen key {self._current_listen_key}")
                        self._last_listen_key_ping_ts = now
                    else:
                        self.logger().error(f"Failed to refresh listen key {self._current_listen_key}. "
                                            f"Getting a new key...")
                        raise IOError("Error refreshing listen key.")
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
            except asyncio.CancelledError:
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                raise
            except Exception as e:
                self.logger().error(f"Error occurred renewing listen key ... {e}")
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)

    async def _ensure_listen_key_task_running(self):
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            return
        if self._manage_listen_key_task is not None:
            self._manage_listen_key_task.cancel()
        self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ensure_listen_key_task_running()
        await self._listen_key_initialized_event.wait()

        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = (f"{web_utils.wss_url(self._domain)}{CONSTANTS.WS_USER_PATH}"
               f"/{self._current_listen_key}")
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        self.logger().info("Successfully connected to user stream...")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        # Ajaib streams user data automatically once connected with the listenKey.
        pass

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        if self._manage_listen_key_task and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._manage_listen_key_task = None

        websocket_assistant and await websocket_assistant.disconnect()
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
