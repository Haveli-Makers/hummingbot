from typing import Any, Callable, Optional

import hummingbot.connector.exchange.coinswitch.coinswitch_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


def build_api_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
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
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=build_api_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    server_time = response.get("serverTime", response.get("data", {}).get("serverTime", 0))
    return server_time


class CoinswitchWebUtils:
    @staticmethod
    def build_ws_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
        return f"{CONSTANTS.WSS_URL}{path_url}"

    @staticmethod
    def async_run_with_timeout(coroutine, timeout_seconds: float = 30):
        import asyncio
        return asyncio.wait_for(coroutine, timeout=timeout_seconds)

    @staticmethod
    def get_rest_assistant(client_config_map) -> Any:
        return WebAssistantsFactory(client_config_map=client_config_map)

    @staticmethod
    def get_ws_path_for_client(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
        return "/pro/realtime-rates-socket/spot"

    @staticmethod
    def parse_trading_pair(trading_pair: str) -> tuple:
        parts = trading_pair.split("-")
        if len(parts) != 2:
            parts = trading_pair.split("/")
        return parts[0], parts[1]

    @staticmethod
    def format_trading_pair(base: str, quote: str) -> str:
        return f"{base}/{quote}"

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        return symbol.upper()
