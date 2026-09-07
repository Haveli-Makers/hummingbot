import time
from typing import Callable, Optional

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Private + instrument endpoints live on api.coindcx.com."""
    return CONSTANTS.REST_URL + path_url


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return private_rest_url(path_url=path_url, domain=domain)


def public_market_data_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Public market data (order book, current prices) lives on public.coindcx.com."""
    return CONSTANTS.PUBLIC_REST_URL + path_url


def order_book_url(coindcx_pair: str, depth: int = CONSTANTS.ORDER_BOOK_DEPTH,
                   domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Build the futures order-book URL.

    The ``-futures`` suffix is mandatory: the same path without it returns the
    SPOT book for the pair, which would silently feed wrong prices.
    """
    return (f"{CONSTANTS.PUBLIC_REST_URL}{CONSTANTS.ORDER_BOOK_PATH_URL}/"
            f"{coindcx_pair}{CONSTANTS.ORDER_BOOK_SUFFIX}/{depth}")


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
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def _build_connections_factory(proxy_url: Optional[str]):
    """
    Use a proxied session when the connector is configured with a proxy URL
    (see ``docs/PROXY_SERVER_GUIDE.md``), otherwise the shared default factory.
    The proxy import is lazy so non-proxy users never load ``aiohttp_socks``.
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
    """
    CoinDCX exposes no futures server-time endpoint; the API only requires the
    request timestamp to be within ~10s of its clock, so local time is used.
    Returned in milliseconds to match the TimeSynchronizer convention.
    """
    return time.time() * 1e3
