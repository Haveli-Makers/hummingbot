import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS, valr_web_utils as web_utils
from hummingbot.connector.exchange.valr.valr_api_order_book_data_source import ValrAPIOrderBookDataSource
from hummingbot.connector.exchange.valr.valr_api_user_stream_data_source import ValrAPIUserStreamDataSource
from hummingbot.connector.exchange.valr.valr_auth import ValrAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _ensure_ok(response: Any) -> Any:
    """VALR returns the payload directly; a business error comes back as a
    {"code": …, "message": …} envelope (without the usual success keys). Raise on it."""
    if isinstance(response, dict) and "message" in response and "code" in response \
            and "id" not in response and "orderStatusType" not in response:
        raise IOError(f"VALR API error: {response}")
    return response


class ValrExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(
        self,
        valr_api_key: str,
        valr_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = valr_api_key
        self.secret_key = valr_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def authenticator(self):
        return ValrAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "valr"

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
        return CONSTANTS.PAIRS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.PAIRS_PATH_URL

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
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    # ── Factory methods ────────────────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ValrAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ValrAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ── Exception classification ───────────────────────────────────────────────

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        msg = str(request_exception).lower()
        return "timestamp" in msg or "signature" in msg

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        msg = str(status_update_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        msg = str(cancelation_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    # ── Trading-pair initialisation ────────────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        for pair in exchange_info or []:
            try:
                if not isinstance(pair, dict):
                    continue
                if pair.get("currencyPairType") != CONSTANTS.PAIR_TYPE_SPOT:
                    continue
                if not pair.get("active", True):
                    continue
                symbol = pair.get("symbol")
                base = pair.get("baseCurrency")
                quote = pair.get("quoteCurrency")
                if not symbol or not base or not quote:
                    continue
                mapping[symbol] = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
            except Exception as exc:
                self.logger().debug(f"Error parsing VALR pair '{pair}': {exc}")
        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.PAIRS_PATH_URL, is_auth_required=False)

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.PAIRS_PATH_URL, is_auth_required=False)

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for pair in exchange_info or []:
            try:
                if not isinstance(pair, dict) or pair.get("currencyPairType") != CONSTANTS.PAIR_TYPE_SPOT:
                    continue
                base, quote = pair.get("baseCurrency"), pair.get("quoteCurrency")
                if not base or not quote:
                    continue
                trading_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())

                base_dp = int(pair.get("baseDecimalPlaces", 8))
                min_base_increment = Decimal(10) ** -base_dp
                tick = Decimal(str(pair.get("tickSize", "0.01")))
                min_order_size = Decimal(str(pair.get("minBaseAmount", min_base_increment)))
                min_notional = Decimal(str(pair.get("minQuoteAmount", "0")))

                rules.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=tick,
                        min_base_amount_increment=min_base_increment,
                        min_notional_size=min_notional,
                    )
                )
            except Exception as exc:
                self.logger().debug(f"Error parsing VALR trading rule for '{pair}': {exc}")
        return rules

    # ── Pricing (rate / volume oracle helpers) ─────────────────────────────────

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        response = await self._api_get(path_url=CONSTANTS.MARKET_SUMMARY_PATH_URL, is_auth_required=False)
        return [t for t in response if isinstance(t, dict)] if isinstance(response, list) else []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return tickers
        requested = {tp.replace("-", "").upper() for tp in trading_pairs}
        return [t for t in tickers if str(t.get("currencyPair", "")).upper() in requested]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair.replace("-", "")
        try:
            response = await self._api_get(
                path_url=CONSTANTS.MARKET_SUMMARY_PATH_URL.replace("marketsummary", f"{symbol}/marketsummary"),
                is_auth_required=False,
                limit_id=CONSTANTS.MARKET_SUMMARY_PATH_URL,
            )
            if isinstance(response, dict):
                return float(response.get("lastTradedPrice") or 0)
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
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        side = CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL

        if order_type is OrderType.MARKET:
            payload = {"side": side, "pair": symbol, "baseAmount": str(amount), "customerOrderId": order_id}
            path = CONSTANTS.PLACE_MARKET_ORDER_PATH_URL
        else:
            payload = {
                "side": side,
                "pair": symbol,
                "quantity": str(amount),
                "price": str(price),
                "postOnly": order_type is OrderType.LIMIT_MAKER,
                "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
                "customerOrderId": order_id,
            }
            path = CONSTANTS.PLACE_LIMIT_ORDER_PATH_URL

        response = await self._api_post(path_url=path, data=payload, is_auth_required=True)
        result = _ensure_ok(response)
        exchange_order_id = str(result.get("id", "")) if isinstance(result, dict) else ""
        if not exchange_order_id:
            raise IOError(f"VALR did not return an order id: {response}")
        return exchange_order_id, self._time_synchronizer.time()

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        if tracked_order.exchange_order_id is None:
            self.logger().warning(f"Cannot cancel {order_id} yet: no exchange order id (pending creation).")
            return False
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data={"orderId": tracked_order.exchange_order_id, "pair": symbol},
            is_auth_required=True,
        )
        # VALR returns HTTP 200 (empty/ack body) on an accepted cancel; a rejected
        # cancel returns a non-2xx (raised upstream) or an error envelope.
        _ensure_ok(response)
        return True

    # ── Order & trade status ───────────────────────────────────────────────────

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(pair=symbol, order_id=tracked_order.exchange_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_STATUS_PATH_URL,
        )
        order = _ensure_ok(response)
        if not isinstance(order, dict):
            raise ValueError(f"Unexpected VALR order status response: {response}")
        status_str = str(order.get("orderStatusType", ""))
        new_state = CONSTANTS.ORDER_STATE.get(status_str)
        if new_state is None:
            raise ValueError(f"Unknown VALR order status '{status_str}' for {tracked_order.exchange_order_id}")
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._time_synchronizer.time(),
            new_state=new_state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # VALR delivers per-fill detail over the private WebSocket (NEW_ACCOUNT_TRADE);
        # there is no per-order fills REST endpoint, so nothing to poll here.
        return []

    # ── Balance ────────────────────────────────────────────────────────────────

    async def _update_balances(self) -> None:
        local_assets = set(self._account_balances.keys())
        remote_assets: set = set()
        try:
            response = await self._api_get(path_url=CONSTANTS.BALANCES_PATH_URL, is_auth_required=True)
            for entry in _ensure_ok(response) or []:
                if not isinstance(entry, dict):
                    continue
                currency = entry.get("currency")
                asset = (currency.get("symbol") if isinstance(currency, dict) else currency) or ""
                asset = str(asset).upper()
                if not asset:
                    continue
                available = Decimal(str(entry.get("available", "0")))
                reserved = Decimal(str(entry.get("reserved", "0")))
                total = entry.get("total")
                total = Decimal(str(total)) if total is not None else (available + reserved)
                self._account_balances[asset] = total
                self._account_available_balances[asset] = available
                remote_assets.add(asset)
            for stale in local_assets - remote_assets:
                del self._account_balances[stale]
                del self._account_available_balances[stale]
        except Exception as exc:
            self.logger().error(f"Error updating VALR balances: {exc}", exc_info=True)

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
        pass

    # ── User-stream event listener ─────────────────────────────────────────────

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                event_type = event.get("type")
                data = event.get("data") or {}

                if event_type == CONSTANTS.WS_BALANCE_UPDATE:
                    currency = data.get("currency")
                    asset = str((currency.get("symbol") if isinstance(currency, dict) else currency) or "").upper()
                    if asset:
                        available = Decimal(str(data.get("available", "0")))
                        reserved = Decimal(str(data.get("reserved", "0")))
                        total = data.get("total")
                        self._account_balances[asset] = Decimal(str(total)) if total is not None else available + reserved
                        self._account_available_balances[asset] = available

                elif event_type == CONSTANTS.WS_ORDER_STATUS_UPDATE:
                    self._process_ws_order_update(data)

                elif event_type == CONSTANTS.WS_NEW_ACCOUNT_TRADE:
                    self._process_ws_trade(data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in VALR user-stream listener.", exc_info=True)
                await self._sleep(5.0)

    def _process_ws_order_update(self, data: Dict[str, Any]):
        client_order_id = data.get("customerOrderId")
        exchange_order_id = str(data.get("orderId", ""))
        status_str = str(data.get("orderStatusType", ""))

        tracked = None
        if client_order_id:
            tracked = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked is None and exchange_order_id:
            for o in self._order_tracker.all_updatable_orders.values():
                if o.exchange_order_id == exchange_order_id:
                    tracked = o
                    break
        if tracked is None:
            return
        new_state = CONSTANTS.ORDER_STATE.get(status_str)
        if new_state is None:
            return
        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked.trading_pair,
            update_timestamp=self._time_synchronizer.time(),
            new_state=new_state,
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    def _process_ws_trade(self, data: Dict[str, Any]):
        client_order_id = data.get("customerOrderId")
        exchange_order_id = str(data.get("orderId", ""))
        tracked = None
        if client_order_id:
            tracked = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked is None and exchange_order_id:
            for o in self._order_tracker.all_fillable_orders.values():
                if o.exchange_order_id == exchange_order_id:
                    tracked = o
                    break
        if tracked is None:
            return
        fill_price = Decimal(str(data.get("price", "0")))
        fill_base = Decimal(str(data.get("quantity", "0")))
        fee_token = str(data.get("feeCurrency") or tracked.quote_asset)
        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=tracked.trade_type,
            percent_token=fee_token,
            flat_fees=[TokenAmount(amount=Decimal(str(data.get("fee", "0"))), token=fee_token)],
        )
        self._order_tracker.process_trade_update(TradeUpdate(
            trade_id=str(data.get("id") or data.get("tradeId") or f"{exchange_order_id}-fill"),
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked.trading_pair,
            fee=fee,
            fill_base_amount=fill_base,
            fill_quote_amount=fill_base * fill_price,
            fill_price=fill_price,
            fill_timestamp=self._time_synchronizer.time(),
        ))
