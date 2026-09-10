import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.delta_perpetual import (
    delta_perpetual_constants as CONSTANTS,
    delta_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative


class DeltaPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    WebSocket order-book / trades / funding data source for Delta Exchange perpetuals.

    Delta pushes a FULL L2 order book on the `l2_orderbook` channel (snapshots,
    not diffs), public trades on `all_trades`, and funding/mark data on
    `funding_rate` / `mark_price`. A REST L2 endpoint is used as a fallback.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "DeltaPerpetualDerivative",
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
            channels = [
                {"name": CONSTANTS.WS_ORDERBOOK_CHANNEL, "symbols": symbols},
                {"name": CONSTANTS.WS_TRADES_CHANNEL, "symbols": symbols},
                {"name": CONSTANTS.WS_FUNDING_CHANNEL, "symbols": symbols},
            ]
            await ws.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))
            self.logger().info("Subscribed to Delta public order book / trades / funding channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Delta public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        msg_type = event_message.get("type", "")
        if msg_type in ("error",):
            raise IOError(f"Delta WS error: {event_message}")
        if msg_type == CONSTANTS.WS_ORDERBOOK_CHANNEL:
            return self._snapshot_messages_queue_key
        if msg_type == CONSTANTS.WS_TRADES_CHANNEL:
            return self._trade_messages_queue_key
        if msg_type in (CONSTANTS.WS_FUNDING_CHANNEL, CONSTANTS.WS_MARK_PRICE_CHANNEL):
            return self._funding_info_messages_queue_key
        return ""

    # ── Order book ─────────────────────────────────────────────────────────────

    def _levels(self, trading_pair: str, raw_levels) -> list:
        out = []
        for lvl in raw_levels or []:
            if isinstance(lvl, dict):
                price = lvl.get("limit_price", lvl.get("price", "0"))
                size = lvl.get("size", "0")
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price, size = lvl[0], lvl[1]
            else:
                continue
            amount = self._connector._format_size_to_amount(trading_pair, Decimal(str(size)))
            out.append([str(price), str(amount)])
        return out

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("symbol")
        if not symbol:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        ts = self._parse_ts(raw_message.get("timestamp")) or self._time()
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(raw_message.get("last_sequence_no", ts * 1000)),
                "bids": self._levels(trading_pair, raw_message.get("buy", raw_message.get("bids", []))),
                "asks": self._levels(trading_pair, raw_message.get("sell", raw_message.get("asks", []))),
            },
            timestamp=ts,
        ))

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("symbol")
        if not symbol:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        trades = raw_message.get("trades")
        entries = trades if isinstance(trades, list) else [raw_message]
        for t in entries:
            ts = self._parse_ts(t.get("timestamp")) or self._time()
            # Delta marks the aggressor; a 'sell' aggressor / buyer-maker is a SELL print.
            is_sell = str(t.get("buyer_role", "")).lower() == "maker" or str(t.get("side", "")).lower() == "sell"
            size = Decimal(str(t.get("size", "0")))
            amount = abs(self._connector._format_size_to_amount(trading_pair, size))
            message_queue.put_nowait(OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": float(TradeType.SELL.value) if is_sell else float(TradeType.BUY.value),
                    "trade_id": str(t.get("trade_id", t.get("id", int(ts * 1000)))),
                    "update_id": str(t.get("trade_id", int(ts * 1000))),
                    "price": str(t.get("price", "0")),
                    "amount": str(amount),
                },
                timestamp=ts,
            ))

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL.format(symbol=symbol)),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )
        return response.get("result", response) if isinstance(response, dict) else response

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raw = await self._request_order_book_snapshot(trading_pair)
        ts = self._time()
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(ts * 1000),
                "bids": self._levels(trading_pair, raw.get("buy", raw.get("bids", []))),
                "asks": self._levels(trading_pair, raw.get("sell", raw.get("asks", []))),
            },
            timestamp=ts,
        )

    # ── Funding info ───────────────────────────────────────────────────────────

    async def _request_complete_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL.format(symbol=symbol)),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.TICKER_PATH_URL,
        )
        return response.get("result", response) if isinstance(response, dict) else response

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        info = await self._request_complete_funding_info(trading_pair)
        mark = Decimal(str(info.get("mark_price", info.get("close", "0")) or "0"))
        index = Decimal(str(info.get("spot_price", info.get("close", "0")) or "0"))
        rate = Decimal(str(info.get("funding_rate", "0") or "0"))
        next_ts = self._parse_ts(info.get("next_funding_realization") or info.get("funding_time")) or int(self._time())
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=index,
            mark_price=mark,
            next_funding_utc_timestamp=int(next_ts),
            rate=rate,
        )

    async def _parse_funding_info_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("symbol", "").replace("MARK:", "")
        if not symbol:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return
        update = FundingInfoUpdate(trading_pair)
        if "mark_price" in raw_message or raw_message.get("type") == CONSTANTS.WS_MARK_PRICE_CHANNEL:
            mp = raw_message.get("mark_price", raw_message.get("price"))
            if mp is not None:
                update.mark_price = Decimal(str(mp))
        if "spot_price" in raw_message:
            update.index_price = Decimal(str(raw_message["spot_price"]))
        if "funding_rate" in raw_message:
            update.rate = Decimal(str(raw_message["funding_rate"]))
        nxt = raw_message.get("next_funding_realization") or raw_message.get("funding_time")
        if nxt is not None:
            update.next_funding_utc_timestamp = int(self._parse_ts(nxt))
        message_queue.put_nowait(update)

    @staticmethod
    def _parse_ts(raw) -> float:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if value > 1e15:
            return value / 1e6
        if value > 1e12:
            return value / 1e3
        return value
