import asyncio
from typing import Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class WazirxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for WazirX exchange using REST API polling.
    """

    POLL_INTERVAL = 5.0

    def __init__(self, auth: Any, trading_pairs: Optional[List[str]] = None, connector: Optional[ExchangePyBase] = None,
                 api_factory=None, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize the user stream data source.
        """
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._stopping = False

    async def start(self):
        """
        Start the user stream polling task.
        """
        self._stopping = False
        self._task = asyncio.ensure_future(self._poll_user_updates())

    async def stop(self):
        """
        Stop the user stream polling task.
        """
        self._stopping = True
        if hasattr(self, "_task"):
            self._task.cancel()

    async def _poll_user_updates(self):
        while not self._stopping:
            try:
                await self._fetch_and_enqueue()
            except Exception:
                pass
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _fetch_and_enqueue(self):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.OPEN_ORDERS_PATH_URL}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    await self._process_event_message({"open_orders": data}, self._user_stream_queue)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        self._user_stream_queue = output
        self._stopping = False

        while not self._stopping:
            try:
                await self._fetch_and_enqueue()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error fetching user stream data: {e}")
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if len(event_message) > 0:
            queue.put_nowait(event_message)
