import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.delta_options import (
    delta_options_constants as CONSTANTS,
    delta_options_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.option_api_order_book_data_source import OptionAPIOrderBookDataSource
from hummingbot.core.data_type.option_info import OptionInfo, OptionInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative


def _decimal(value: Any) -> Optional[Decimal]:
    return None if value is None or value == "" else Decimal(str(value))


def _item(values: Optional[List[Any]], index: int) -> Any:
    return values[index] if values and len(values) > index else None


class DeltaOptionsAPIOrderBookDataSource(OptionAPIOrderBookDataSource):
    """
    Public market data from Delta's public WebSocket endpoint: `ob_l2` (top 15 levels, a full snapshot every
    500 ms), `trades`, and `ticker` (mark price, IV and Greeks every 5 s). Sizes arrive in contracts.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "DeltaOptionsDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices = {}
        for trading_pair, ticker in (await self._request_tickers(trading_pairs)).items():
            price = _decimal(ticker.get("close")) or _decimal(ticker.get("mark_price"))
            if price:
                prices[trading_pair] = float(price)
        return prices

    async def get_option_info(self, trading_pair: str) -> OptionInfo:
        ticker = (await self._request_tickers([trading_pair]))[trading_pair]
        greeks = ticker.get("greeks") or {}
        quotes = ticker.get("quotes") or {}
        price_band = ticker.get("price_band") or {}
        open_interest = _decimal(ticker.get("oi"))
        return OptionInfo(
            trading_pair=trading_pair,
            mark_price=Decimal(str(ticker["mark_price"])),
            underlying_price=Decimal(str(ticker["spot_price"])),
            mark_iv=_decimal(quotes.get("mark_iv")),
            bid_iv=_decimal(quotes.get("bid_iv")),
            ask_iv=_decimal(quotes.get("ask_iv")),
            delta=_decimal(greeks.get("delta")),
            gamma=_decimal(greeks.get("gamma")),
            theta=_decimal(greeks.get("theta")),
            vega=_decimal(greeks.get("vega")),
            rho=_decimal(greeks.get("rho")),
            open_interest=None if open_interest is None else self._connector.size_to_amount(trading_pair, open_interest),
            price_band_lower=_decimal(price_band.get("lower_limit")),
            price_band_upper=_decimal(price_band.get("upper_limit")),
            timestamp=self._seconds(ticker.get("timestamp")) or self._time(),
        )

    async def _request_tickers(self, trading_pairs: List[str]) -> Dict[str, Dict[str, Any]]:
        pair_by_symbol = {}
        for trading_pair in trading_pairs:
            try:
                pair_by_symbol[await self._connector.exchange_symbol_associated_to_pair(trading_pair)] = trading_pair
            except KeyError:
                continue
        symbols = list(pair_by_symbol)
        rest_assistant = await self._api_factory.get_rest_assistant()
        tickers = {}
        for start in range(0, len(symbols), CONSTANTS.MAX_SYMBOLS_PER_REQUEST):
            chunk = ",".join(symbols[start:start + CONSTANTS.MAX_SYMBOLS_PER_REQUEST])
            response = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(CONSTANTS.TICKERS_PATH_URL.format(symbols=chunk), domain=self._domain),
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.TICKERS_PATH_URL,
            )
            result = response.get("result")
            for ticker in [result] if isinstance(result, dict) else result or []:
                trading_pair = pair_by_symbol.get(ticker.get("symbol"))
                if trading_pair is not None:
                    tickers[trading_pair] = ticker
        return tickers

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL.format(symbol=symbol), domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )
        book = response["result"]
        update_id = int(book.get("last_updated_at") or self._time() * 1e6)
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": self._levels(trading_pair, book.get("buy")),
                "asks": self._levels(trading_pair, book.get("sell")),
            },
            timestamp=update_id / 1e6,
        )

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_PUBLIC_URL, ping_timeout=CONSTANTS.WS_PING_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            symbols = await self._connector.exchange_symbols_for_live_pairs(self._trading_pairs)
            if not symbols:
                self.logger().warning("No live Delta options to subscribe to.")
                return
            channels = [
                {"name": channel, "symbols": symbols}
                for channel in (CONSTANTS.WS_ORDER_BOOK_CHANNEL, CONSTANTS.WS_TRADES_CHANNEL, CONSTANTS.WS_TICKER_CHANNEL)
            ]
            await ws.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))
            self.logger().info(f"Subscribed to Delta order books, trades and tickers for {len(symbols)} options.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Delta public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return {
            CONSTANTS.WS_ORDER_BOOK_CHANNEL: self._snapshot_messages_queue_key,
            CONSTANTS.WS_TRADES_CHANNEL: self._trade_messages_queue_key,
            CONSTANTS.WS_TICKER_CHANNEL: self._option_info_messages_queue_key,
        }.get(event_message.get("type"), "")

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        if event_message.get("type") == "subscriptions":
            for channel in event_message.get("channels") or []:
                if channel.get("error"):
                    self.logger().error(f"Delta rejected the {channel.get('name')} subscription: {channel['error']}")
        elif event_message.get("type") == "error":
            self.logger().warning(f"Delta WebSocket error: {event_message}")

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair(raw_message.get("sy"))
        if trading_pair is None:
            return
        update_id = int(raw_message.get("lts") or raw_message.get("ts"))
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

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair(raw_message.get("sy"))
        if trading_pair is None:
            return
        trade_time = int(raw_message.get("t") or raw_message.get("ts"))
        # "r" is the buyer's role: a maker buyer means the seller took liquidity.
        trade_type = TradeType.SELL if raw_message.get("r") == "m" else TradeType.BUY
        amount = self._connector.size_to_amount(trading_pair, Decimal(str(raw_message["s"])))
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(trade_type.value),
                "trade_id": str(trade_time),
                "update_id": trade_time,
                "price": str(raw_message["p"]),
                "amount": str(amount),
            },
            timestamp=trade_time / 1e6,
        ))

    async def _parse_option_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        underlying_price = _decimal(raw_message.get("sp"))
        timestamp = self._seconds(raw_message.get("ts"))
        for entry in raw_message.get("d") or []:
            trading_pair = await self._trading_pair(entry.get("s"))
            if trading_pair is None:
                continue
            greeks = entry.get("g")
            ivs = entry.get("qiv")
            price_band = entry.get("pb")
            open_interest = _decimal(_item(entry.get("oi"), 0))
            message_queue.put_nowait(OptionInfoUpdate(
                trading_pair=trading_pair,
                mark_price=_decimal(entry.get("m")),
                underlying_price=underlying_price,
                ask_iv=_decimal(_item(ivs, 0)),
                bid_iv=_decimal(_item(ivs, 1)),
                mark_iv=_decimal(_item(ivs, 2)),
                delta=_decimal(_item(greeks, 0)),
                gamma=_decimal(_item(greeks, 1)),
                rho=_decimal(_item(greeks, 2)),
                theta=_decimal(_item(greeks, 3)),
                vega=_decimal(_item(greeks, 4)),
                open_interest=None if open_interest is None else self._connector.size_to_amount(trading_pair, open_interest),
                price_band_lower=_decimal(_item(price_band, 0)),
                price_band_upper=_decimal(_item(price_band, 1)),
                timestamp=timestamp or None,
            ))

    async def _trading_pair(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return None

    def _levels(self, trading_pair: str, raw_levels: Optional[List[Any]]) -> List[List[str]]:
        levels = []
        for level in raw_levels or []:
            price, size = (level["price"], level["size"]) if isinstance(level, dict) else (level[0], level[1])
            levels.append([str(price), str(self._connector.size_to_amount(trading_pair, Decimal(str(size))))])
        return levels

    @staticmethod
    def _seconds(timestamp: Any) -> float:
        # Delta documents microseconds, but some REST samples show seconds; accept both.
        try:
            value = float(timestamp)
        except (TypeError, ValueError):
            return 0.0
        return value / 1e6 if value > 1e14 else value / 1e3 if value > 1e11 else value
