import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS, coinex_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    WebSocket order-book / trades data source for CoinEx spot.

    CoinEx pushes a FULL order book on the `depth.update` channel (snapshots, via
    if_full=true) and public trades on `deals.update`. WS frames are gzip-binary;
    the factory's CoinexWSPostProcessor decompresses them before they reach here.
    A REST `/spot/depth` snapshot is used as a fallback.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinexExchange",
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
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = [
                await self._connector.exchange_symbol_associated_to_pair(trading_pair=tp)
                for tp in self._trading_pairs
            ]
            depth_list = [[s, CONSTANTS.WS_DEPTH_LIMIT, CONSTANTS.WS_DEPTH_INTERVAL, True] for s in symbols]
            await ws.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_DEPTH_SUBSCRIBE,
                "params": {"market_list": depth_list},
                "id": 1,
            }))
            await ws.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_DEALS_SUBSCRIBE,
                "params": {"market_list": symbols},
                "id": 2,
            }))
            self.logger().info("Subscribed to CoinEx public depth / deals channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinEx public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        if not isinstance(event_message, dict):
            return ""
        method = event_message.get("method", "")
        if method == CONSTANTS.WS_DEPTH_UPDATE:
            return self._snapshot_messages_queue_key
        if method == CONSTANTS.WS_DEALS_UPDATE:
            return self._trade_messages_queue_key
        return ""

    # ── Order book ─────────────────────────────────────────────────────────────

    @staticmethod
    def _levels(raw_levels) -> list:
        out = []
        for lvl in raw_levels or []:
            if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                out.append([str(lvl[0]), str(lvl[1])])
        return out

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        data = raw_message.get("data") or {}
        symbol = data.get("market")
        depth = data.get("depth") or {}
        if not symbol or not depth:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        ts = self._normalize_ts(depth.get("updated_at")) or self._time()
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(depth.get("updated_at") or ts * 1000),
                "bids": self._levels(depth.get("bids")),
                "asks": self._levels(depth.get("asks")),
            },
            timestamp=ts,
        ))

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        data = raw_message.get("data") or {}
        symbol = data.get("market")
        if not symbol:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        deals = data.get("deal_list") or data.get("deals") or []
        for deal in deals if isinstance(deals, list) else []:
            ts = self._normalize_ts(deal.get("created_at")) or self._time()
            is_sell = str(deal.get("side", "")).lower() == "sell"
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(TradeType.SELL.value) if is_sell else float(TradeType.BUY.value),
                    "trade_id": str(deal.get("deal_id") or deal.get("id") or int(ts * 1000)),
                    "update_id": str(deal.get("deal_id") or int(ts * 1000)),
                    "price": str(deal.get("price", "0")),
                    "amount": str(deal.get("amount", "0")),
                },
                timestamp=ts,
            ))

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL),
            params={"market": symbol, "limit": CONSTANTS.WS_DEPTH_LIMIT, "interval": CONSTANTS.WS_DEPTH_INTERVAL},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )
        if isinstance(response, dict):
            return (response.get("data") or {}).get("depth") or {}
        return {}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        depth = await self._request_order_book_snapshot(trading_pair)
        ts = self._time()
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(depth.get("updated_at") or ts * 1000),
                "bids": self._levels(depth.get("bids")),
                "asks": self._levels(depth.get("asks")),
            },
            timestamp=ts,
        )

    @staticmethod
    def _normalize_ts(raw) -> float:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        return value / 1000.0 if value > 1e11 else value
