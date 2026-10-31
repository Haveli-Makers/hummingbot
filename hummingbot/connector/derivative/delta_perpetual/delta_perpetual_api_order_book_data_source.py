import asyncio
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
    Public stream: `ob_l2` (top 15 levels, full snapshots), `trades`, `ticker` (mark and index price) and
    `funding_rate` (in percent). Sizes are in contracts.
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
        await ws.connect(ws_url=web_utils.public_wss_url(self._domain), ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = []
            for trading_pair in self._trading_pairs:
                try:
                    symbols.append(await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair))
                except KeyError:
                    # A delisted pair must not stop the others from subscribing.
                    self.logger().warning(f"{trading_pair} is not listed on Delta; skipping its market data.")
            if not symbols:
                return
            if len(symbols) > CONSTANTS.WS_MAX_SYMBOLS:
                self.logger().warning(f"Delta streams order books for at most {CONSTANTS.WS_MAX_SYMBOLS} symbols per "
                                      f"connection; {len(symbols)} were requested.")
            channels = [
                {"name": name, "symbols": symbols}
                for name in (CONSTANTS.WS_ORDERBOOK_CHANNEL, CONSTANTS.WS_TRADES_CHANNEL,
                             CONSTANTS.WS_TICKER_CHANNEL, CONSTANTS.WS_FUNDING_CHANNEL)
            ]
            await ws.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))
            self.logger().info("Subscribed to Delta public order book / trades / ticker / funding channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Delta public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return {
            CONSTANTS.WS_ORDERBOOK_CHANNEL: self._snapshot_messages_queue_key,
            CONSTANTS.WS_TRADES_CHANNEL: self._trade_messages_queue_key,
            CONSTANTS.WS_TICKER_CHANNEL: self._funding_info_messages_queue_key,
            CONSTANTS.WS_FUNDING_CHANNEL: self._funding_info_messages_queue_key,
        }.get(event_message.get("type"), "")

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        if event_message.get("type") == "subscriptions":
            for channel in event_message.get("channels") or []:
                if channel.get("error"):
                    self.logger().error(f"Delta rejected the {channel.get('name')} subscription: {channel['error']}")
        elif event_message.get("type") == "error":
            self.logger().warning(f"Delta public WS error: {event_message}")

    # ── Order book ─────────────────────────────────────────────────────────────

    def _levels(self, trading_pair: str, raw_levels) -> list:
        out = []
        for lvl in raw_levels or []:
            if isinstance(lvl, dict):
                price, size = lvl.get("price", "0"), lvl.get("size", "0")
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price, size = lvl[0], lvl[1]
            else:
                continue
            amount = self._connector._format_size_to_amount(trading_pair, Decimal(str(size)))
            out.append([str(price), str(amount)])
        return out

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair(raw_message.get("sy"))
        if trading_pair is None:
            return
        update_id = int(raw_message.get("lts") or raw_message.get("ts") or self._time() * 1e6)
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": self._levels(trading_pair, raw_message.get("b")),
                "asks": self._levels(trading_pair, raw_message.get("a")),
            },
            timestamp=update_id / 1e6,
        ))

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair(raw_message.get("sy"))
        if trading_pair is None:
            return
        trade_time = int(raw_message.get("t") or raw_message.get("ts") or self._time() * 1e6)
        # "r" is the buyer's role: a maker buyer means the seller took liquidity.
        trade_type = TradeType.SELL if raw_message.get("r") == "m" else TradeType.BUY
        amount = abs(self._connector._format_size_to_amount(trading_pair, Decimal(str(raw_message.get("s", "0")))))
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": str(trade_time),
                "update_id": trade_time,
                "price": str(raw_message.get("p", "0")),
                "amount": str(amount),
            },
            timestamp=trade_time / 1e6,
        ))

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL.format(symbol=symbol), domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )
        return response.get("result", response) if isinstance(response, dict) else response

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raw = await self._request_order_book_snapshot(trading_pair)
        update_id = int(raw.get("last_updated_at") or self._time() * 1e6)
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": self._levels(trading_pair, raw.get("buy")),
                "asks": self._levels(trading_pair, raw.get("sell")),
            },
            timestamp=update_id / 1e6,
        )

    # ── Funding info ───────────────────────────────────────────────────────────

    async def _request_complete_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest = await self._api_factory.get_rest_assistant()
        response = await rest.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL.format(symbol=symbol), domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.TICKER_PATH_URL,
        )
        return response.get("result", response) if isinstance(response, dict) else response

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        info = await self._request_complete_funding_info(trading_pair)
        mark = Decimal(str(info.get("mark_price", info.get("close", "0")) or "0"))
        index = Decimal(str(info.get("spot_price", info.get("close", "0")) or "0"))
        next_ts = self._parse_ts(info.get("next_funding_realization") or info.get("funding_time")) or int(self._time())
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=index,
            mark_price=mark,
            next_funding_utc_timestamp=int(next_ts),
            rate=self._rate_from_percent(info.get("funding_rate")) or Decimal("0"),
        )

    async def _parse_funding_info_message(self, raw_message: Any, message_queue: asyncio.Queue):
        if raw_message.get("type") == CONSTANTS.WS_TICKER_CHANNEL:
            index_price = raw_message.get("sp")
            for entry in raw_message.get("d") or []:
                trading_pair = await self._trading_pair(entry.get("s"))
                if trading_pair is None:
                    continue
                update = FundingInfoUpdate(trading_pair)
                if entry.get("m") is not None:
                    update.mark_price = Decimal(str(entry["m"]))
                if index_price is not None:
                    update.index_price = Decimal(str(index_price))
                message_queue.put_nowait(update)
            return

        trading_pair = await self._trading_pair(raw_message.get("sy"))
        if trading_pair is None:
            return
        update = FundingInfoUpdate(trading_pair)
        update.rate = self._rate_from_percent(raw_message.get("fr"))
        if raw_message.get("nfr") is not None:
            update.next_funding_utc_timestamp = int(self._parse_ts(raw_message["nfr"]))
        message_queue.put_nowait(update)

    async def _trading_pair(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return None

    @staticmethod
    def _rate_from_percent(raw) -> Optional[Decimal]:
        return None if raw is None or raw == "" else Decimal(str(raw)) / Decimal("100")

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
