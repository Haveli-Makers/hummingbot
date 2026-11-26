from typing import Callable, Optional

import aiohttp

import hummingbot.connector.exchange.coindcx.coindcx_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Construct a full public REST API URL for CoinDCX.
    """
    if path_url.startswith("/market_data"):
        return CONSTANTS.BASE_URL.format(CONSTANTS.PUBLIC_DOMAIN) + path_url
    return CONSTANTS.BASE_URL.format(domain) + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Construct a full private REST API URL for CoinDCX.
    """
    return CONSTANTS.BASE_URL.format(domain) + path_url


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
        proxy_url: Optional[str] = None,
) -> WebAssistantsFactory:
    """
    Build a WebAssistantsFactory with required components for CoinDCX API.
    With a proxy URL, every REST request goes through that proxy (see docs/PROXY_SERVER_GUIDE.md).
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time())

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory


def _build_connections_factory(proxy_url: Optional[str]):
    """
    A ProxyConnectionsFactory when a proxy URL is set, otherwise the shared default factory.
    The proxy import is lazy so connectors without a proxy never load aiohttp_socks.
    """
    if proxy_url:
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        return ProxyConnectionsFactory(proxy_url=proxy_url)

    from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
    return ConnectionsFactory()


def build_proxy_session(proxy_url: Optional[str]) -> Optional[aiohttp.ClientSession]:
    """
    An aiohttp session that connects through the proxy, for one Socket.IO connection, or None without a proxy.

    CoinDCX's streams use python-socketio, which opens its own connections instead of going through the
    WebAssistantsFactory, so the proxy has to reach it as the client's http_session. The caller closes the session
    after the client disconnects, because engine.io never closes a session it was given. Make a new one for every
    connection: engine.io replaces a closed session with a plain, direct one without saying so.
    """
    if not proxy_url:
        return None
    from aiohttp_socks import ProxyConnector

    # The same settings as ProxyConnectionsFactory: SOCKS or HTTP, with host names resolved by the proxy.
    return aiohttp.ClientSession(connector=ProxyConnector.from_url(proxy_url, rdns=True))


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Build a WebAssistantsFactory without time synchronizer pre-processor.
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Create a rate limiter with CoinDCX rate limits.
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Get the current server time in milliseconds.
    """
    import time
    return time.time() * 1000
