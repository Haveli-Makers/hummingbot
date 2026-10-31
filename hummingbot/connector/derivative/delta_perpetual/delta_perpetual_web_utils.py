import time as _time
from typing import Callable, Optional

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URLS[domain]}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URLS[domain]}{path_url}"


def public_wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.WSS_PUBLIC_URLS[domain]


def private_wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.WSS_PRIVATE_URLS[domain]


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


def _build_connections_factory(proxy_url: Optional[str]):
    """Proxy factory (imported lazily) when a proxy URL is set, else the shared default factory."""
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
    """Delta has no server-time endpoint; returns the local time in ms, as TimeSynchronizer expects."""
    return _time.time() * 1000
