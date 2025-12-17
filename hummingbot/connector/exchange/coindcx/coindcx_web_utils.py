from typing import Callable, Optional

import hummingbot.connector.exchange.coindcx.coindcx_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint.
    Uses the main API URL for most endpoints.

    :param path_url: a public REST endpoint
    :param domain: not used for CoinDCX but kept for compatibility
    :return: the full URL to the endpoint
    """
    # Some endpoints use public.coindcx.com
    if path_url.startswith("/market_data"):
        return CONSTANTS.PUBLIC_REST_URL + path_url
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint.

    :param path_url: a private REST endpoint
    :param domain: not used for CoinDCX but kept for compatibility
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + path_url


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    Builds and returns a WebAssistantsFactory configured for CoinDCX.
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()

    # CoinDCX doesn't require server time synchronization as we use local time
    time_provider = time_provider or (lambda: get_current_server_time())

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Builds a WebAssistantsFactory without time synchronization pre-processor.
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates and returns an AsyncThrottler configured with CoinDCX rate limits.
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Returns the current time in milliseconds.
    CoinDCX doesn't have a dedicated server time endpoint, so we use local time.
    """
    import time
    return time.time() * 1000
