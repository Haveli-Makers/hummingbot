import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class WazirxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """REST-only order book data source for WazirX. Polls depth snapshots."""

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None,
                 connector: Optional[ExchangePyBase] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 snapshot_poll_interval: float = 5.0):
        super().__init__(trading_pairs or [])
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._snapshot_poll_interval = snapshot_poll_interval

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

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Fetches a single depth snapshot through the shared REST assistant.
        Avoids dependency on symbol map by deriving the exchange symbol directly.
        """
        exchange_symbol = trading_pair.replace("-", "").lower()
        params = {"symbol": exchange_symbol, "limit": "100"}

        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Builds an OrderBookMessage snapshot from the REST depth response."""
        snapshot = await self._request_order_book_snapshot(trading_pair)
        if not snapshot:
            update_id = int(self._time() * 1e3)
            return OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                content={
                    "trading_pair": trading_pair,
                    "update_id": update_id,
                    "bids": [],
                    "asks": [],
                },
                timestamp=self._time(),
            )

        timestamp = snapshot.get("timestamp") or snapshot.get("T") or self._time()
        if timestamp > 1e12:
            timestamp = timestamp / 1e3

        update_id = int(snapshot.get("lastUpdateId", 0)) or int(self._time() * 1e3)
        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot.get("bids", []),
            "asks": snapshot.get("asks", []),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content=content, timestamp=timestamp)

    async def listen_for_subscriptions(self):
        """Polls REST snapshots and pushes them to the snapshot queue."""
        snapshot_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                await self._request_order_book_snapshots(output=snapshot_queue)
                await self._sleep(self._snapshot_poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error fetching WazirX order book snapshots; retrying soon.")
                await self._sleep(5.0)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        return

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        return

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        return

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("WazirX order book streaming not implemented; using REST polling instead.")

    async def _subscribe_channels(self, ws: WSAssistant):
        return

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger
