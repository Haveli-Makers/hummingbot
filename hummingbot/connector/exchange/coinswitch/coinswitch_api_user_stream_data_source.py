import asyncio
import logging
import time
from typing import Any, List, Optional

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

_logger = logging.getLogger(__name__)


class CoinswitchAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for CoinSwitch API.
    """

    def __init__(self,
                 auth: Optional[AuthBase] = None,
                 trading_pairs: Optional[List[str]] = None,
                 connector=None,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize the CoinSwitch user stream data source.

        Args:
            auth: Authentication handler
            trading_pairs: List of trading pairs
            connector: The exchange connector
            api_factory: WebAssistantsFactory for API calls
            domain: The domain (default: com)
        """
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message.
        """
        return self._last_recv_time

    async def _connected_websocket_assistant(self) -> Any:
        return None

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        """
        Listen for user stream updates.

        Args:
            output: Queue to output user stream messages
        """
        while True:
            try:
                self._last_recv_time = time.time()

                try:
                    portfolio_response = await self._connector._api_get(
                        path_url=CONSTANTS.GET_PORTFOLIO_PATH_URL,
                        is_auth_required=True,
                    )

                    if portfolio_response and "data" in portfolio_response:
                        message = {
                            "type": "balance_update",
                            "data": portfolio_response.get("data", [])
                        }
                        output.put_nowait(message)
                        self._last_recv_time = time.time()
                except Exception as e:
                    _logger.debug(f"Error fetching portfolio: {e}")

                try:
                    orders_response = await self._connector._api_get(
                        path_url=CONSTANTS.OPEN_ORDERS_PATH_URL,
                        params={"open": True},
                        is_auth_required=True,
                    )

                    if orders_response and "data" in orders_response:
                        message = {
                            "type": "order_update",
                            "data": orders_response.get("data", {}).get("orders", [])
                        }
                        output.put_nowait(message)
                        self._last_recv_time = time.time()
                except Exception as e:
                    _logger.debug(f"Error fetching orders: {e}")

                await asyncio.sleep(5)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                _logger.warning(f"Error in listen_for_user_stream: {e}")
                await asyncio.sleep(5)

    async def _subscribe_to_user_stream(self) -> None:
        pass

    async def _unsubscribe_from_user_stream(self) -> None:
        pass
