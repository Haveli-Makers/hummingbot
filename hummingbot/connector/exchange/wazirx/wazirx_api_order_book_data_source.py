import asyncio
import logging
from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_order_book import WazirxOrderBook
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class WazirxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for WazirX order book and trade data.
    The depth stream sends the full top-20 book in every message, so each one is handled as a snapshot.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None,
                 connector: Optional[ExchangePyBase] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 api_factory: Optional[WebAssistantsFactory] = None):
        super().__init__(trading_pairs or [])
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_data_time: float = 0
        self._stalled: bool = False

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """
        Returns the last traded price for each trading pair.
        """
        try:
            return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)
        except Exception:
            return {}

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params={"symbol": symbol, "limit": "100"},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Fetch and create an order book snapshot message for the trading pair.
        """
        snapshot = await self._request_order_book_snapshot(trading_pair)
        if not isinstance(snapshot, dict) or ("bids" not in snapshot and "asks" not in snapshot):
            raise IOError(f"Unexpected WazirX order book response for {trading_pair}: {str(snapshot)[:200]}")
        return WazirxOrderBook.snapshot_message_from_exchange(
            snapshot, self._time(), metadata={"trading_pair": trading_pair})

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to order book and trade channels for all trading pairs.
        """
        streams = []
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            streams.append(f"{symbol}@{CONSTANTS.DEPTH_EVENT_TYPE}")
            streams.append(f"{symbol}@{CONSTANTS.TRADE_EVENT_TYPE}")
        await ws.send(WSJSONRequest(payload={"event": "subscribe", "streams": streams}))
        self.logger().info("Subscribed to public order book and trade channels")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        """
        Queue stream messages and reconnect if the streams go silent while the socket stays open.
        """
        self._last_data_time = self._time()
        self._stalled = False
        ping_task = asyncio.ensure_future(self._ping_task(websocket_assistant))
        try:
            async for ws_response in websocket_assistant.iter_messages():
                data = ws_response.data
                if data is None:
                    continue
                channel = self._channel_originating_message(event_message=data)
                if channel in self._get_messages_queue_keys():
                    self._last_data_time = self._time()
                    self._message_queue[channel].put_nowait(data)
                else:
                    await self._process_message_for_unknown_channel(event_message=data,
                                                                    websocket_assistant=websocket_assistant)
        finally:
            ping_task.cancel()
        if self._stalled:
            raise ConnectionError(f"No WazirX stream data for {CONSTANTS.WS_DATA_STALL_TIMEOUT}s; resubscribing")

    async def _ping_task(self, ws: WSAssistant):
        """
        Periodically send ping messages, and drop the connection when the streams stop sending data.
        """
        last_ping = self._time()
        while True:
            await self._sleep(CONSTANTS.WS_WATCHDOG_INTERVAL)
            now = self._time()
            try:
                if now - self._last_data_time >= CONSTANTS.WS_DATA_STALL_TIMEOUT:
                    self._stalled = True
                    await ws.disconnect()
                    return
                if now - last_ping >= CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL:
                    last_ping = now
                    await ws.send(WSJSONRequest(payload={"event": "ping"}))
            except asyncio.CancelledError:
                raise
            except Exception:
                return

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        stream = event_message.get("stream", "") if isinstance(event_message, dict) else ""
        if stream.endswith(f"@{CONSTANTS.DEPTH_EVENT_TYPE}"):
            return self._snapshot_messages_queue_key
        if stream.endswith(f"@{CONSTANTS.TRADE_EVENT_TYPE}"):
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        if isinstance(event_message, dict) and event_message.get("event") not in ("connected", "subscribed", "pong"):
            self.logger().warning(f"Unexpected WazirX websocket message: {str(event_message)[:300]}")

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse and enqueue a depth stream message (a full top-20 book).
        """
        data = raw_message.get("data") or {}
        trading_pair = await self._trading_pair_for_symbol(data.get("s") or raw_message.get("stream", "").split("@")[0])
        if trading_pair:
            message_queue.put_nowait(WazirxOrderBook.snapshot_message_from_exchange(
                data, self._time(), {"trading_pair": trading_pair}))

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse and enqueue each trade of a trade stream message.
        """
        for trade in (raw_message.get("data") or {}).get("trades", []):
            trading_pair = await self._trading_pair_for_symbol(trade.get("s", ""))
            if trading_pair:
                message_queue.put_nowait(WazirxOrderBook.trade_message_from_exchange(
                    trade, {"trading_pair": trading_pair}))

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # WazirX has no incremental depth stream.
        return

    async def _trading_pair_for_symbol(self, symbol: str) -> Optional[str]:
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol.lower())
        except KeyError:
            return None
