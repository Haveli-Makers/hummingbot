import gzip
import json
import time as _time
from typing import Callable, Optional

import hummingbot.connector.exchange.coinex.coinex_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_post_processors import WSPostProcessorBase


class CoinexWSPostProcessor(WSPostProcessorBase):
    """
    CoinEx pushes WebSocket frames as gzip-compressed BINARY messages. The base
    WSConnection passes binary frames through as raw bytes, so this post-processor
    decompresses + JSON-decodes them, leaving the data sources to work with plain
    dicts (text frames / already-decoded payloads pass through untouched).
    """

    async def post_process(self, response: WSResponse) -> WSResponse:
        data = response.data
        if isinstance(data, (bytes, bytearray)):
            try:
                data = json.loads(gzip.decompress(data).decode("utf-8"))
            except Exception:
                try:
                    data = json.loads(bytes(data).decode("utf-8"))
                except Exception:
                    return response
            return WSResponse(data)
        return response


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
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler, domain=domain))
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ],
        ws_post_processors=[CoinexWSPostProcessor()],
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
    CoinEx signs with the request timestamp in milliseconds. We sign with the
    client clock (returned here in MILLISECONDS; TimeSynchronizer stores ms and
    its .time() yields seconds, which CoinexAuth multiplies back to ms).
    """
    return _time.time() * 1000
