import time as _time
from typing import Callable, Optional

import hummingbot.connector.exchange.zebpay.zebpay_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class ZebpayUserAgentRESTPreProcessor(RESTPreProcessorBase):
    """Injects a browser-like User-Agent so Zebpay's WAF does not reject requests with HTTP 403."""

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers or {})
        headers.setdefault("User-Agent", USER_AGENT)
        request.headers = headers
        return request


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
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()

    # Zebpay signs with the client clock (no server-time endpoint), so the
    # time provider returns local time in milliseconds, keeping the
    # TimeSynchronizer offset ~0.
    effective_time_provider = time_provider or (
        lambda: get_current_server_time(throttler=throttler, domain=domain)
    )

    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            ZebpayUserAgentRESTPreProcessor(),
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=effective_time_provider,
            ),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[ZebpayUserAgentRESTPreProcessor()],
    )


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Zebpay does not require a server-time call — it signs with the client clock.

    Returns local time in MILLISECONDS. TimeSynchronizer stores this against
    perf_counter()*1000 (also ms), so the computed offset stays ~0 and
    synchronizer.time() ≈ time.time(); ZebpayAuth then multiplies by 1000 to
    produce the epoch-ms timestamp the API expects.
    """
    return _time.time() * 1000
