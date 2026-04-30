import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class WazirxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order book data source for WazirX exchange.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None,
                 connector: Optional[ExchangePyBase] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 snapshot_poll_interval: float = 5.0):
        """
        Initialize the order book data source.
        """
        super().__init__(trading_pairs or [])
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._snapshot_poll_interval = snapshot_poll_interval

    async def get_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Get an order book snapshot for a trading pair.
        """
        symbol = trading_pair.replace("-", "").lower()
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.DEPTH_PATH_URL}?symbol={symbol}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return {}
                return await resp.json()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """
        Get last traded prices for multiple trading pairs.
        """
        try:
            return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)
        except Exception:
            return {}

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        exchange_symbol = await self._exchange_symbol_for_pair(trading_pair)
        params = {"symbol": exchange_symbol, "limit": "100"}

        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
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

        timestamp = snapshot.get("timestamp") or snapshot.get("T") or snapshot.get("lastUpdateAt") or self._time()
        if timestamp > 1e12:
            timestamp = timestamp / 1e3

        update_id = int(snapshot.get("lastUpdateId", 0)) or int(timestamp * 1e3)
        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot.get("bids", []),
            "asks": snapshot.get("asks", []),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content=content, timestamp=timestamp)

    async def listen_for_subscriptions(self):
        await super().listen_for_subscriptions()

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        return

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data", {})
        symbol = data.get("s")
        if symbol is None:
            return

        trading_pair = await self._trading_pair_for_symbol(symbol)
        event_timestamp = data.get("E") or int(time.time() * 1e3)
        timestamp = event_timestamp / 1e3 if event_timestamp > 1e12 else event_timestamp
        order_book_message = OrderBookMessage(
            OrderBookMessageType.DIFF,
            content={
                "trading_pair": trading_pair,
                "update_id": int(event_timestamp),
                "bids": data.get("b", []),
                "asks": data.get("a", []),
            },
            timestamp=timestamp,
        )
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            streams = []
            for trading_pair in self._trading_pairs:
                symbol = await self._exchange_symbol_for_pair(trading_pair)
                streams.append(f"{symbol}@depth")

            payload = {
                "event": "subscribe",
                "streams": streams,
                "id": 1,
            }
            await ws.send(WSJSONRequest(payload=payload))
            self.logger().info("Subscribed to WazirX public order book depth streams...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to WazirX depth streams...", exc_info=True)
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        stream = event_message.get("stream", "")
        data = event_message.get("data", {})
        if stream.endswith("@depth") and data.get("s") is not None:
            return self._diff_messages_queue_key
        return ""

    async def _exchange_symbol_for_pair(self, trading_pair: str) -> str:
        if self._connector is not None:
            try:
                return await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except Exception:
                pass
        return trading_pair.replace("-", "").lower()

    async def _trading_pair_for_symbol(self, symbol: str) -> str:
        if self._connector is not None:
            try:
                return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except Exception:
                pass
        for quote in ("usdt", "inr", "wrx", "btc"):
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return f"{symbol[:-len(quote)].upper()}-{quote.upper()}"
        return symbol.upper()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger
