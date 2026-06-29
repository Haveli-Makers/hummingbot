import time as _time
from typing import Callable, Optional

import hummingbot.connector.exchange.csx.csx_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


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

    # Use get_current_server_time as the default time provider.
    # CSX has no server-time endpoint, so it returns local time.
    # This gives the TimeSynchronizer an ~0 offset so CsxAuth
    # signs every request with the correct current epoch seconds.
    effective_time_provider = time_provider or (
        lambda: get_current_server_time(throttler=throttler, domain=domain)
    )

    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=effective_time_provider,
            ),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    CSX does not expose a server-time endpoint.

    TimeSynchronizer.update_server_time_offset_with_time_provider() stores
    the value returned here in a variable called `server_time_ms` and
    computes the offset against `perf_counter() * 1000` (also in ms).
    Returning seconds here would make the synchronizer produce a timestamp
    ~1000x too small (≈ Jan 1970) and the exchange would reject with
    "Stale request. Timestamp is old".

    Returning time.time() * 1000 keeps the offset ≈ 0 so
    synchronizer.time() ≈ time.time(), and CsxAuth signs every request
    with int(time.time()) — the correct current epoch second.

    Called by:
      - TimeSynchronizerRESTPreProcessor on the first authenticated request
      - ExchangePyBase._update_time_synchronizer() on a "Stale request" error
    """
    return _time.time() * 1000  # milliseconds — required by TimeSynchronizer


def _build_connections_factory(proxy_url: Optional[str]):
    """
    Returns a ProxyConnectionsFactory when a proxy URL is provided, otherwise
    falls back to the default singleton ConnectionsFactory.
    """
    if proxy_url:
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        return ProxyConnectionsFactory(proxy_url=proxy_url)
    from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
    return ConnectionsFactory()
