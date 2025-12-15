from typing import Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class WazirxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """REST order book data source for WazirX. Fetches depth snapshots."""

    def __init__(self, trading_pairs: Optional[List[str]] = None, connector: Optional[ExchangePyBase] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN, api_factory=None):
        super().__init__(trading_pairs or [])
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory

    async def get_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = trading_pair.replace("-", "").lower()
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.DEPTH_PATH_URL}?symbol={symbol}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return {}
                return await resp.json()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        try:
            return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)
        except Exception:
            return {}
