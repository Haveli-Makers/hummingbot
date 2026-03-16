import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: AjaibAuth,
                 trading_pairs: List[str],
                 connector: 'AjaibExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._domain = domain
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs = trading_pairs
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Ajaib user data stream uses WebSocket for execution reports.
        For now, this is a placeholder - the user stream will be implemented
        when full trading support is added.
        """
        while True:
            try:
                await self._sleep(30.0)
                self._last_recv_time = self._time()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in user stream listener")
                await self._sleep(5.0)
