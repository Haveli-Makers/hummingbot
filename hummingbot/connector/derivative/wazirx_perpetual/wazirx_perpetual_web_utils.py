from typing import Callable, Optional

from hummingbot.connector.derivative.wazirx_perpetual import wazirx_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Paths carry their own /fapi or /sapi prefix; both live on api.wazirx.com."""
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return public_rest_url(path_url=path_url, domain=domain)


def wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.WSS_URL


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
        proxy_url: Optional[str] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler, domain=domain))
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def _build_connections_factory(proxy_url: Optional[str]):
    """
    Use a proxied session when the connector is configured with a proxy URL
    (WazirX keys can be restricted to whitelisted IPs, error 2128; see
    ``docs/PROXY_SERVER_GUIDE.md``), otherwise the shared default factory. The
    proxy import is lazy so non-proxy users never load ``aiohttp_socks``.
    """
    if proxy_url:
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        return ProxyConnectionsFactory(proxy_url=proxy_url)

    from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
    return ConnectionsFactory()


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """Server time in milliseconds from the public, unsigned /fapi/v1/time."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    return float(response["serverTime"])
