import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_order_book import AjaibOrderBook
from hummingbot.connector.exchange.ajaib.ajaib_utils import hb_pair_to_ajaib_symbol
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order book data source for Ajaib.

    Ajaib exposes no public depth REST endpoint; the order book is built entirely
    from the ``<symbol>@depth`` partial-book-depth stream (a full top-20 snapshot
    every 500ms), so each depth message is routed to the snapshot queue.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'AjaibExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        # No REST depth endpoint exists; seed an empty book that the depth stream
        # (full snapshots) refreshes within ~500ms of connecting.
        snapshot_timestamp = time.time()
        return AjaibOrderBook.snapshot_message_from_exchange(
            {"bids": [], "asks": []},
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair},
        )

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=f"{CONSTANTS.WSS_URL}{CONSTANTS.WS_PUBLIC_PATH}",
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            params = []
            for trading_pair in self._trading_pairs:
                symbol = hb_pair_to_ajaib_symbol(trading_pair).lower()
                params.append(f"{symbol}@{CONSTANTS.WS_DEPTH_EVENT_TYPE}")
                params.append(f"{symbol}@{CONSTANTS.WS_TRADE_EVENT_TYPE}")

            subscribe_request = WSJSONRequest(payload={
                "method": "SUBSCRIBE",
                "params": params,
                "id": int(time.time()),
            })
            await ws.send(subscribe_request)
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = event_message.get("e")
        if event_type == CONSTANTS.WS_DEPTH_EVENT_TYPE:
            return self._snapshot_messages_queue_key
        if event_type == CONSTANTS.WS_TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key
        return ""

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        symbol = raw_message.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            trade_message = AjaibOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        symbol = raw_message.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            snapshot_message = AjaibOrderBook.snapshot_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(snapshot_message)
