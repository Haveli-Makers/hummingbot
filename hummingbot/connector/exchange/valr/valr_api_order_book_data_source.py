import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS, valr_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    WebSocket order-book / trades data source for VALR spot.

    The public market-data socket (wss://api.valr.com/ws/trade) needs no auth and
    pushes a FULL aggregated book on `AGGREGATED_ORDERBOOK_UPDATE` (snapshots) and
    public trades on `NEW_TRADE`, both as plain-JSON text frames. A REST
    `/v1/public/{pair}/orderbook` snapshot is used as a fallback.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "ValrExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    def _time(self) -> float:
        return time.time()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for tp in trading_pairs:
            try:
                price = await self._connector._get_last_traded_price(trading_pair=tp)
                if price and price > 0:
                    prices[tp] = price
            except Exception as exc:
                self.logger().warning(f"Error fetching last price for {tp}: {exc}")
        return prices

    # ── WebSocket connection / subscription ────────────────────────────────────

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_TRADE_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = [
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=tp)
                for tp in self._trading_pairs
            ]
            await ws.send(WSJSONRequest(payload={
                "type": CONSTANTS.WS_SUBSCRIBE,
                "subscriptions": [
                    {"event": CONSTANTS.WS_AGGREGATED_ORDERBOOK_UPDATE, "pairs": symbols},
                    {"event": CONSTANTS.WS_NEW_TRADE, "pairs": symbols},
                ],
            }))
            self.logger().info("Subscribed to VALR public order book / trade channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to VALR public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        if not isinstance(event_message, dict):
            return ""
        event_type = event_message.get("type", "")
        if event_type == CONSTANTS.WS_AGGREGATED_ORDERBOOK_UPDATE:
            return self._snapshot_messages_queue_key
        if event_type == CONSTANTS.WS_NEW_TRADE:
            return self._trade_messages_queue_key
        return ""

    # ── Order book ─────────────────────────────────────────────────────────────

    @staticmethod
    def _levels(raw_levels) -> list:
        out = []
        for lvl in raw_levels or []:
            if isinstance(lvl, dict):
                price, qty = lvl.get("price"), lvl.get("quantity")
                if price is not None and qty is not None:
                    out.append([str(price), str(qty)])
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                out.append([str(lvl[0]), str(lvl[1])])
        return out

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("currencyPairSymbol")
        data = raw_message.get("data") or {}
        if not symbol or not data:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        ts = self._time()
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(data.get("SequenceNumber") or ts * 1e3),
                "bids": self._levels(data.get("Bids")),
                "asks": self._levels(data.get("Asks")),
            },
            timestamp=ts,
        ))

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("currencyPairSymbol")
        data = raw_message.get("data") or {}
        if not symbol or not data:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        ts = self._time()
        is_sell = str(data.get("takerSide", "")).lower() == "sell"
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.SELL.value) if is_sell else float(TradeType.BUY.value),
                "trade_id": str(data.get("id") or data.get("sequenceId") or int(ts * 1e3)),
                "update_id": str(data.get("id") or int(ts * 1e3)),
                "price": str(data.get("price", "0")),
                "amount": str(data.get("quantity", "0")),
            },
            timestamp=ts,
        ))

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL.format(pair=symbol)),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )
        return response if isinstance(response, dict) else {}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        data = await self._request_order_book_snapshot(trading_pair)
        ts = self._time()
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(data.get("SequenceNumber") or ts * 1e3),
                "bids": self._levels(data.get("Bids")),
                "asks": self._levels(data.get("Asks")),
            },
            timestamp=ts,
        )
