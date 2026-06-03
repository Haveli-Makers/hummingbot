import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS, zebpay_web_utils as web_utils
from hummingbot.connector.exchange.zebpay.zebpay_api_order_book_data_source import ZebpayAPIOrderBookDataSource
from hummingbot.connector.exchange.zebpay.zebpay_api_user_stream_data_source import ZebpayAPIUserStreamDataSource
from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.connector.exchange.zebpay.zebpay_utils import raise_for_status, str_to_decimal, unwrap_data
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

_logger = logging.getLogger(__name__)


def _extract_instruments(exchange_info: Any) -> List[Dict[str, Any]]:
    """Normalise the GET /api/v2/ex/exchangeInfo payload into a list of symbol dicts."""
    data = unwrap_data(exchange_info)
    if isinstance(data, list):
        return [i for i in data if isinstance(i, dict)]
    if isinstance(data, dict):
        for key in ("symbols", "pairs", "markets", "instruments"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [i for i in inner if isinstance(i, dict)]
        # dict keyed by symbol → values are the dicts
        values = [v for v in data.values() if isinstance(v, dict)]
        if values:
            return values
    return []


class ZebpayExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(
        self,
        zebpay_api_key: str,
        zebpay_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = zebpay_api_key
        self.secret_key = zebpay_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_timestamp = 1.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Static helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def zebpay_order_type(order_type: OrderType) -> str:
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def to_hb_order_type(zebpay_type: str) -> OrderType:
        return OrderType.LIMIT

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def authenticator(self):
        return ZebpayAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def name(self) -> str:
        return "zebpay"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    # ── Factory methods ────────────────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ZebpayAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ZebpayAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ── Exception classification ───────────────────────────────────────────────

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        msg = str(request_exception).lower()
        return "timestamp" in msg or "signature" in msg and "time" in msg

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        msg = str(status_update_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        msg = str(cancelation_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    # ── Trading pair initialisation ────────────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        """
        Build the symbol bidict from GET /api/v2/ex/exchangeInfo.
        Zebpay symbols are already in BASE-QUOTE form (e.g. 'BTC-INR'),
        which matches hummingbot's native trading-pair format.
        """
        mapping = bidict()
        for item in _extract_instruments(exchange_info):
            try:
                symbol = str(item.get("symbol") or "").strip()
                base = str(item.get("baseAsset") or item.get("base") or "").strip()
                quote = str(item.get("quoteAsset") or item.get("quote") or "").strip()

                if base and quote:
                    hb_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
                elif "-" in symbol:
                    b, q = symbol.split("-", 1)
                    hb_pair = combine_to_hb_trading_pair(base=b.upper(), quote=q.upper())
                else:
                    continue

                if not symbol:
                    symbol = hb_pair
                mapping[symbol] = hb_pair
            except Exception as exc:
                self.logger().debug(f"Error parsing Zebpay instrument '{item}': {exc}")

        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, is_auth_required=False)

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, is_auth_required=False)

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        for item in _extract_instruments(exchange_info):
            try:
                symbol = str(item.get("symbol") or "")
                base = str(item.get("baseAsset") or "")
                quote = str(item.get("quoteAsset") or "")
                if base and quote:
                    trading_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
                elif "-" in symbol:
                    trading_pair = symbol.upper()
                else:
                    continue

                price_prec = item.get("pricePrecision")
                qty_prec = item.get("quantityPrecision")

                tick = item.get("tickSz")
                lot = item.get("lotSz")

                min_price_increment = (
                    str_to_decimal(tick) if tick not in (None, "")
                    else (Decimal(10) ** -int(price_prec) if price_prec is not None else Decimal("0.01"))
                )
                min_base_increment = (
                    str_to_decimal(lot) if lot not in (None, "")
                    else (Decimal(10) ** -int(qty_prec) if qty_prec is not None else Decimal("0.0001"))
                )
                min_order_size = str_to_decimal(item.get("minSize", item.get("minQty", lot))) or min_base_increment
                min_notional = str_to_decimal(item.get("minNotional", item.get("minQuoteSize", "1"))) or Decimal("1")

                trading_rules.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=min_base_increment,
                        min_notional_size=min_notional,
                    )
                )
            except Exception as exc:
                self.logger().debug(f"Error parsing Zebpay trading rule for '{item}': {exc}")
        return trading_rules

    # ── Pricing (rate / volume oracle helpers) ─────────────────────────────────

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        response = await self._api_get(path_url=CONSTANTS.ALL_TICKERS_PATH_URL, is_auth_required=False)
        data = unwrap_data(response)
        if isinstance(data, list):
            return [t for t in data if isinstance(t, dict)]
        if isinstance(data, dict):
            # dict keyed by symbol → fold the symbol into each ticker
            tickers = []
            for sym, t in data.items():
                if isinstance(t, dict):
                    t.setdefault("symbol", sym)
                    tickers.append(t)
            return tickers
        return []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return tickers
        requested = {tp.upper() for tp in trading_pairs}
        return [t for t in tickers if str(t.get("symbol", "")).upper() in requested]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair
        try:
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL,
                params={"symbol": symbol},
                is_auth_required=False,
            )
            data = unwrap_data(response)
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                return float(data.get("last") or data.get("lastPrice") or 0)
        except Exception as exc:
            self.logger().error(f"Error fetching last traded price for {trading_pair}: {exc}")
        return 0.0

    # ── Order placement & cancellation ────────────────────────────────────────

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair

        payload: Dict[str, Any] = {
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            "price": str(price),
            "amount": str(amount),
            "clientOrderId": order_id,
        }

        result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=payload,
            is_auth_required=True,
        )

        # Zebpay returns HTTP 200 even when it rejects an order (e.g. price outside
        # the allowed band → statusCode 77, data=null). Surface it as a failure so
        # the order is not left silently OPEN with an empty exchange order id.
        raise_for_status(result)

        data = unwrap_data(result)
        if isinstance(data, list) and data:
            data = data[0]
        data = data if isinstance(data, dict) else {}

        exchange_order_id = str(data.get("orderId") or data.get("id") or "")
        if not exchange_order_id:
            raise IOError(f"Zebpay accepted the request but returned no orderId: {result}")
        ts_raw = data.get("timestamp") or data.get("createdAt") or 0
        transact_time = float(ts_raw) / 1000.0 if ts_raw else self._time_synchronizer.time()
        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={"orderId": tracked_order.exchange_order_id},
            is_auth_required=True,
        )
        # Raise on a business error (e.g. order already gone) so the cancel flow's
        # not-found classification can handle it instead of reporting a false cancel.
        raise_for_status(result)
        data = unwrap_data(result)
        if isinstance(data, dict):
            status = str(data.get("status") or "").upper()
            if status in ("CANCELLED", "CANCELED") or data.get("cancelled") is True:
                return True
        return bool(result)

    # ── Order & trade status ───────────────────────────────────────────────────

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        result = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={"orderId": tracked_order.exchange_order_id},
            is_auth_required=True,
        )
        raise_for_status(result)
        data = unwrap_data(result)
        if isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected Zebpay order status response: {result}")

        status_str = str(data.get("status") or "").upper()
        new_state = CONSTANTS.ORDER_STATE.get(status_str)
        if new_state is None:
            raise ValueError(f"Unknown Zebpay order status '{status_str}' for {tracked_order.exchange_order_id}")

        # Promote an OPEN order with partial fills to PARTIALLY_FILLED.
        from hummingbot.core.data_type.in_flight_order import OrderState
        filled = str_to_decimal(data.get("filled", "0"))
        if new_state == OrderState.OPEN and filled > 0:
            new_state = OrderState.PARTIALLY_FILLED

        ts_raw = data.get("updatedAt") or data.get("timestamp") or 0
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(ts_raw) / 1000.0 if ts_raw else self._time_synchronizer.time(),
            new_state=new_state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        trade_updates: List[TradeUpdate] = []
        try:
            result = await self._api_get(
                path_url=CONSTANTS.ORDER_FILLS_PATH_URL,
                params={"orderId": order.exchange_order_id},
                is_auth_required=True,
            )
            data = unwrap_data(result)
            fills = data.get("fills", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for fill in fills:
                if not isinstance(fill, dict):
                    continue
                fill_price = str_to_decimal(fill.get("price", "0"))
                fill_base = str_to_decimal(fill.get("amount", fill.get("qty", "0")))
                fill_quote = str_to_decimal(fill.get("cost")) or (fill_price * fill_base)
                fee_token = fill.get("feeCurrency", fill.get("feeCoin", order.quote_asset))
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=fee_token,
                    flat_fees=[TokenAmount(amount=str_to_decimal(fill.get("fees", "0")), token=fee_token)],
                )
                ts_raw = fill.get("createdAt") or fill.get("timestamp") or 0
                trade_updates.append(
                    TradeUpdate(
                        trade_id=str(fill.get("id", fill.get("tradeId", f"{order.exchange_order_id}-{len(trade_updates)}"))),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(order.exchange_order_id),
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_base,
                        fill_quote_amount=fill_quote,
                        fill_price=fill_price,
                        fill_timestamp=float(ts_raw) / 1000.0 if ts_raw else self._time_synchronizer.time(),
                    )
                )
        except Exception as exc:
            self.logger().error(f"Error fetching Zebpay fills for {order.exchange_order_id}: {exc}")
        return trade_updates

    # ── Balance ────────────────────────────────────────────────────────────────

    async def _update_balances(self) -> None:
        local_assets = set(self._account_balances.keys())
        remote_assets: set = set()
        try:
            response = await self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, is_auth_required=True)
            from hummingbot.connector.exchange.zebpay.zebpay_utils import parse_balance_response
            parsed = parse_balance_response(response)
            for asset, balances in parsed.items():
                self._account_balances[asset] = balances["total"]
                self._account_available_balances[asset] = balances["free"]
                remote_assets.add(asset)

            for stale in local_assets - remote_assets:
                del self._account_balances[stale]
                del self._account_available_balances[stale]
        except Exception as exc:
            self.logger().error(f"Error updating Zebpay balances: {exc}", exc_info=True)

    # ── Fees ───────────────────────────────────────────────────────────────────

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self):
        """Zebpay exposes maker/taker fees per symbol via exchangeInfo; fee
        estimation falls back to DEFAULT_FEES, so this is a no-op."""
        pass

    # ── User-stream event listener ─────────────────────────────────────────────

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                event_type = event.get("event")

                if event_type == "balance_update":
                    from hummingbot.connector.exchange.zebpay.zebpay_utils import parse_balance_response
                    parsed = parse_balance_response(event.get("data"))
                    for asset, balances in parsed.items():
                        self._account_balances[asset] = balances["total"]
                        self._account_available_balances[asset] = balances["free"]

                elif event_type == "order_update":
                    for order_data in event.get("data") or []:
                        if not isinstance(order_data, dict):
                            continue
                        client_order_id = order_data.get("clientOrderId") or order_data.get("client_order_id")
                        exchange_order_id = str(order_data.get("orderId") or order_data.get("id") or "")
                        status_str = str(order_data.get("status") or "").upper()

                        tracked = None
                        if client_order_id:
                            tracked = self._order_tracker.all_updatable_orders.get(client_order_id)
                        if tracked is None and exchange_order_id:
                            for o in self._order_tracker.all_updatable_orders.values():
                                if o.exchange_order_id == exchange_order_id:
                                    tracked = o
                                    break
                        if tracked is None:
                            continue

                        new_state = CONSTANTS.ORDER_STATE.get(status_str)
                        if new_state is None:
                            continue

                        from hummingbot.core.data_type.in_flight_order import OrderState
                        if new_state == OrderState.OPEN and str_to_decimal(order_data.get("filled", "0")) > 0:
                            new_state = OrderState.PARTIALLY_FILLED

                        ts_raw = order_data.get("updatedAt") or order_data.get("timestamp") or 0
                        order_update = OrderUpdate(
                            trading_pair=tracked.trading_pair,
                            update_timestamp=float(ts_raw) / 1000.0 if ts_raw else self._time_synchronizer.time(),
                            new_state=new_state,
                            client_order_id=tracked.client_order_id,
                            exchange_order_id=exchange_order_id,
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in Zebpay user-stream listener.", exc_info=True)
                await self._sleep(5.0)
