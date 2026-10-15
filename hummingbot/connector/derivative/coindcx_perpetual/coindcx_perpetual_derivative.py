import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_utils as utils,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_order_book_data_source import (
    CoinDCXPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_user_stream_data_source import (
    CoinDCXPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_auth import CoinDCXPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_decimal_0 = Decimal("0")


class CoindcxPerpetualDerivative(PerpetualDerivativePyBase):
    """
    CoinDCX perpetual futures connector.

    The connector is moving to the CoinDCX FAPI (Futures API Market Maker
    Integration Guide v1.2), a Binance-style API on hosts assigned per account.
    Market data already uses it: instruments and trading rules from
    ``exchangeInfo``, the order book from a ``depth`` snapshot plus ``@depth``
    diffs, trades from ``@aggTrade``, funding from ``premiumIndex`` and
    ``@markPrice``, and prices from ``ticker/price`` and ``ticker/bookTicker``.

    Order management, balances, positions and the private stream below still
    speak the legacy API (``/exchange/v1/derivatives/futures/*`` and Socket.IO)
    and are replaced next; until then this connector cannot trade.

    Legacy-API traits the remaining private code handles:

    * Orders have **no client-order-id**; CoinDCX only returns its own UUID, so
      the connector tracks orders by exchange id and reconciles fills by pair.
    * Positions expose no unrealised PnL, so it is derived from the mark price.
    * Fees and every margin figure the API reports are in USDT even when the
      margin currency is INR, so fee tokens are always USDT.

    With ``margin_currency == "INR"`` both sides of the budget check are kept in
    USDT: the collateral token stays USDT (see ``get_buy_collateral_token``) and
    ``_update_balances`` publishes the INR wallet's USDT equivalent rather than
    its native amount, so required collateral and available balance are
    commensurate. The conversion uses the public ``USDTINR`` spot ticker, which
    can sit 1-2% away from CoinDCX's internal
    ``settlement_currency_conversion_price``; the published buying power is
    therefore approximate, and slightly conservative when the spot rate is high.
    """

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter" = None,
        coindcx_perpetual_api_key: str = None,
        coindcx_perpetual_api_secret: str = None,
        coindcx_perpetual_margin_currency: str = CONSTANTS.DEFAULT_MARGIN_CURRENCY,
        coindcx_perpetual_proxy_url: str = "",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        self.api_key = coindcx_perpetual_api_key or ""
        self.secret_key = coindcx_perpetual_api_secret or ""
        self._margin_currency = utils.normalize_margin_currency(coindcx_perpetual_margin_currency)
        self._proxy_url = coindcx_perpetual_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        # Cached margin-currency -> quote-currency rate (see _margin_conversion_rate).
        self._conversion_rate: Optional[Decimal] = None
        self._conversion_rate_ts: float = 0.0

        # Order events that arrived before _place_order recorded the exchange id,
        # held as (exchange_order_id, payload, buffered_at) in arrival order.
        self._pending_order_events: List[Tuple[str, Dict[str, Any], float]] = []
        self._pending_order_events_task: Optional[asyncio.Task] = None

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---- identity / configuration -------------------------------------------

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinDCXPerpetualAuth:
        return CoinDCXPerpetualAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
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

    @property
    def funding_fee_poll_interval(self) -> int:
        return CONSTANTS.FUNDING_FEE_POLL_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        # CoinDCX futures instruments report allow_post_only == false, so
        # LIMIT_MAKER cannot be honoured.
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    @property
    def margin_currency(self) -> str:
        """Currency the account posts as margin (USDT or INR)."""
        return self._margin_currency

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        # Collateral is denominated in the contract's quote currency, NOT in the
        # wallet currency: CoinDCX reports every margin figure (ideal_margin,
        # locked_margin, fees) in USDT even for INR-margined futures, and
        # converts the INR wallet itself at its own settlement rate. Reporting
        # the wallet currency instead would make Hummingbot look for a
        # "USDT-INR" market to convert with, which CoinDCX futures does not list.
        return CONSTANTS.QUOTE_CURRENCY

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return CONSTANTS.QUOTE_CURRENCY

    # ---- factories -----------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> CoinDCXPerpetualAPIOrderBookDataSource:
        return CoinDCXPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinDCXPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def start_network(self):
        await super().start_network()
        if self._margin_currency != CONSTANTS.QUOTE_CURRENCY:
            rate = await self._margin_conversion_rate()
            self.logger().info(
                f"Margining in {self._margin_currency} against {CONSTANTS.QUOTE_CURRENCY}-quoted "
                f"contracts. The wallet's buying power is published in "
                f"{CONSTANTS.QUOTE_CURRENCY} for collateral maths "
                f"(1 {CONSTANTS.QUOTE_CURRENCY} = {rate} {self._margin_currency}); "
                f"orders still debit the {self._margin_currency} wallet."
            )

    async def stop_network(self):
        await super().stop_network()

        if self._pending_order_events_task is not None:
            self._pending_order_events_task.cancel()
            try:
                await self._pending_order_events_task
            except (asyncio.CancelledError, Exception):
                pass
            self._pending_order_events_task = None
        self._pending_order_events.clear()

        # A proxied factory owns a dedicated aiohttp session; the default factory
        # is a shared singleton and must be left alone.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Failed closing the proxy connections factory.", exc_info=True)

    # ---- error classification -------------------------------------------------

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        exc = str(status_update_exception)
        return (str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in exc
                or CONSTANTS.ORDER_NOT_EXIST_MESSAGE.lower() in exc.lower())

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """
        Only treat a cancel failure as "the order is already gone".

        HTTP 422 on CoinDCX is a generic validation status — it is also returned
        for malformed params, a wrong margin currency and permission problems.
        Treating any 422 as "already cancelled" would make the framework drop a
        still-resting order from tracking, leaving it live and unmanaged, so a
        422 must also carry a message that names the condition.
        """
        exc = str(cancelation_exception).lower()
        if str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in exc:
            return True
        has_hint = any(hint in exc for hint in CONSTANTS.ORDER_GONE_MESSAGE_HINTS)
        if str(CONSTANTS.INVALID_REQUEST_ERROR_CODE) in exc:
            return has_hint
        return has_hint

    async def _paginated_records(self, path_url: str, data: Dict[str, Any],
                                 page_size: int = CONSTANTS.PAGE_SIZE,
                                 max_pages: int = CONSTANTS.MAX_PAGES):
        """
        Page through one of the account-wide list endpoints.

        None of them accepts a pair filter, so a record for the pair we care
        about can sit well beyond the first page on a busy account. Stops on the
        first short page (the last one) or when ``max_pages`` is reached.
        """
        for page in range(1, max_pages + 1):
            payload = dict(data)
            payload["page"] = str(page)
            payload["size"] = str(page_size)
            records = await self._api_post(path_url=path_url, data=payload, is_auth_required=True)
            if not isinstance(records, list) or not records:
                return
            yield records
            if len(records) < page_size:
                return
        self.logger().warning(
            f"Stopped paging {path_url} after {max_pages} pages; some records may not have "
            f"been read.")

    # ---- exchange info / trading rules ---------------------------------------

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_info in filter(utils.is_exchange_information_valid, exchange_info.get("symbols") or []):
            trading_pair = combine_to_hb_trading_pair(base=symbol_info["baseAsset"], quote=symbol_info["quoteAsset"])
            if trading_pair in mapping.inverse:
                continue
            mapping[symbol_info["symbol"]] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for symbol_info in filter(utils.is_exchange_information_valid, exchange_info.get("symbols") or []):
            try:
                price_filter = utils.symbol_filter(symbol_info, "PRICE_FILTER")
                lot_size = utils.symbol_filter(symbol_info, "LOT_SIZE")
                min_notional = utils.symbol_filter(symbol_info, "MIN_NOTIONAL")
                rules.append(TradingRule(
                    trading_pair=combine_to_hb_trading_pair(base=symbol_info["baseAsset"],
                                                            quote=symbol_info["quoteAsset"]),
                    min_order_size=Decimal(lot_size["minQty"]),
                    max_order_size=Decimal(lot_size["maxQty"]),
                    min_price_increment=Decimal(price_filter["tickSize"]),
                    min_base_amount_increment=Decimal(lot_size["stepSize"]),
                    min_notional_size=Decimal(str(min_notional.get("notional", 0))),
                    # Must match get_buy/sell_collateral_token: collateral is
                    # denominated in the quote currency, not the wallet currency.
                    buy_order_collateral_token=CONSTANTS.QUOTE_CURRENCY,
                    sell_order_collateral_token=CONSTANTS.QUOTE_CURRENCY,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {symbol_info.get('symbol')}.")
        return rules

    async def _update_trading_fees(self):
        """The FAPI publishes no per-instrument fees; ``_get_fee`` uses the default schema."""
        pass

    # ---- fees ---------------------------------------------------------------

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker if is_maker is not None else (order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER))
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self.estimate_fee_pct(is_maker),
            # Fees are charged in USDT even on INR-margined futures.
            percent_token=CONSTANTS.FEE_CURRENCY,
        )

    # ---- orders --------------------------------------------------------------

    @staticmethod
    def coindcx_order_type(order_type: OrderType) -> str:
        return CONSTANTS.ORDER_TYPE_MARKET if order_type is OrderType.MARKET else CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def coindcx_side(trade_type: TradeType) -> str:
        return CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           position_action: PositionAction = PositionAction.NIL,
                           **kwargs) -> Tuple[str, float]:
        coindcx_pair = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        order: Dict[str, Any] = {
            "side": self.coindcx_side(trade_type),
            "pair": coindcx_pair,
            "order_type": self.coindcx_order_type(order_type),
            "total_quantity": float(amount),
            "leverage": int(self.get_leverage(trading_pair) or 1),
            "notification": CONSTANTS.NO_NOTIFICATION,
            "margin_currency_short_name": self._margin_currency,
        }
        if order_type is not OrderType.MARKET:
            order["price"] = float(price)
            # CoinDCX rejects time_in_force on market orders.
            order["time_in_force"] = CONSTANTS.TIME_IN_FORCE_GTC

        response = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data={"order": order},
            is_auth_required=True,
        )

        created = response[0] if isinstance(response, list) and response else response
        if not isinstance(created, dict) or not created.get("id"):
            raise IOError(f"Error submitting order to CoinDCX: {response}")

        exchange_order_id = str(created["id"])
        transact_time = float(created.get("created_at", self._time_synchronizer.time() * 1e3)) * 1e-3
        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id is None:
            # Nothing to cancel yet; let the framework retry once the id arrives.
            self.logger().warning(
                f"Cannot cancel order {order_id}: it has not been assigned an exchange id yet.")
            return False

        response = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data={"id": exchange_order_id},
            is_auth_required=True,
        )
        # Only an explicit success counts. Falling back to "any non-null payload"
        # would report a list-shaped error body as a successful cancel and drop
        # tracking of an order still resting on the exchange.
        if isinstance(response, dict):
            code = response.get("code", response.get("status"))
            if str(response.get("message", "")).lower() == "success" or code in (200, "200"):
                return True
        raise IOError(f"Unexpected response cancelling order {exchange_order_id}: {response}")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order_data = await self._fetch_order_by_id(tracked_order)
        if order_data is None:
            raise IOError(f"{CONSTANTS.ORDER_NOT_EXIST_MESSAGE}: {tracked_order.exchange_order_id}")
        return self._order_update_from_payload(order_data, tracked_order)

    async def _fetch_order_by_id(self, tracked_order: InFlightOrder) -> Optional[Dict[str, Any]]:
        """
        CoinDCX has no "fetch one order" endpoint, so the order list for the pair
        is scanned for the tracked exchange id.
        """
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id is None:
            return None

        # The list is account-wide (no pair filter) and covers every status, so a
        # still-open order can sit beyond the first page on a busy account. Page
        # through rather than declaring it missing — the caller turns "not found"
        # into an order failure.
        base_payload = {
            "status": CONSTANTS.ALL_ORDER_STATUSES,
            "side": self.coindcx_side(tracked_order.trade_type),
            "margin_currency_short_name": [self._margin_currency],
        }
        async for page in self._paginated_records(CONSTANTS.LIST_ORDERS_PATH_URL, base_payload):
            for order in page:
                if isinstance(order, dict) and str(order.get("id")) == str(exchange_order_id):
                    return order
        return None

    def _order_update_from_payload(self, order: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        status = str(order.get("status", "")).lower()
        new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)
        update_ts = order.get("updated_at") or order.get("created_at")
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(update_ts) * 1e-3 if update_ts else self._time_synchronizer.time(),
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order.get("id", tracked_order.exchange_order_id or "")),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates: List[TradeUpdate] = []
        if order.exchange_order_id is None:
            return trade_updates
        try:
            fills = await self._api_post(
                path_url=CONSTANTS.TRADES_PATH_URL,
                data={"order_id": order.exchange_order_id, "page": "1", "size": "100"},
                is_auth_required=True,
            )
        except Exception as exception:
            self.logger().debug(f"Could not fetch fills for {order.client_order_id}: {exception}")
            return trade_updates

        for fill in fills if isinstance(fills, list) else []:
            if str(fill.get("order_id", order.exchange_order_id)) != str(order.exchange_order_id):
                continue
            trade_updates.append(self._trade_update_from_fill(fill, order))
        return trade_updates

    @staticmethod
    def _fill_id(fill: Dict[str, Any]) -> str:
        """
        Stable, unique id for a fill.

        ``/futures/trades`` identifies a fill with **``fill_id``** (confirmed on a
        live fill; the docs omit the field entirely). The synthesised fallback
        stays for safety because the id is load-bearing:
        ``InFlightOrder.update_with_trade_update`` discards any fill whose
        ``trade_id`` it has already seen, so a constant (e.g. empty) id would
        collapse every partial fill of an order into the first and under-count
        the executed amount.

        The fallback composite must be deterministic — the same fill re-polled
        has to produce the same id so it de-duplicates — and distinct across
        fills, which the sub-millisecond timestamp plus price/quantity provides.
        """
        explicit = fill.get("fill_id") or fill.get("id") or fill.get("trade_id")
        if explicit:
            return str(explicit)
        parts = (fill.get("order_id", ""), fill.get("timestamp", ""),
                 fill.get("price", ""), fill.get("quantity", ""))
        return "-".join(str(part) for part in parts)

    def _trade_update_from_fill(self, fill: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        price = Decimal(str(fill.get("price", 0) or 0))
        amount = Decimal(str(fill.get("quantity", fill.get("total_quantity", 0)) or 0))
        fee_amount = Decimal(str(fill.get("fee_amount", fill.get("fee", 0)) or 0))
        timestamp = fill.get("timestamp") or fill.get("created_at") or self._time_synchronizer.time() * 1e3

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=order.position,
            flat_fees=[TokenAmount(amount=fee_amount, token=CONSTANTS.FEE_CURRENCY)] if fee_amount else [],
        )
        return TradeUpdate(
            trade_id=self._fill_id(fill),
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=amount,
            fill_quote_amount=amount * price,
            fill_price=price,
            fill_timestamp=float(timestamp) * 1e-3,
        )

    # ---- balances ------------------------------------------------------------

    async def _update_balances(self):
        wallets = await self._api_request(
            path_url=CONSTANTS.WALLETS_PATH_URL,
            method=RESTMethod.GET,
            is_auth_required=True,
        )

        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        # Balances are reported in the contract quote currency, because that is
        # the unit Hummingbot sizes collateral in (and the unit CoinDCX itself
        # reports margin in). With a non-quote margin wallet the balance is
        # CONVERTED rather than listed natively: publishing both the INR wallet
        # and its USDT equivalent shows the same money twice, and the native row
        # prices at $0 because Hummingbot has no INR rate.
        rate = await self._margin_conversion_rate()
        if not rate or rate <= s_decimal_0:
            self.logger().warning(
                f"Could not price the {self._margin_currency} margin wallet in "
                f"{CONSTANTS.QUOTE_CURRENCY}; skipping this balance update rather than "
                f"reporting a wrong figure.")
            return

        for wallet in wallets if isinstance(wallets, list) else []:
            asset = wallet.get("currency_short_name")
            # Only the configured margin wallet can back orders: CoinDCX debits
            # whatever `margin_currency_short_name` the order carries.
            if asset != self._margin_currency:
                continue
            available = Decimal(str(wallet.get("balance", 0) or 0))
            locked = Decimal(str(wallet.get("locked_balance", 0) or 0))

            self._account_available_balances[CONSTANTS.QUOTE_CURRENCY] = available / rate
            self._account_balances[CONSTANTS.QUOTE_CURRENCY] = (available + locked) / rate
            remote_asset_names.add(CONSTANTS.QUOTE_CURRENCY)

            if self._margin_currency != CONSTANTS.QUOTE_CURRENCY:
                self.logger().debug(
                    f"Margin wallet {available} {self._margin_currency} "
                    f"= {available / rate} {CONSTANTS.QUOTE_CURRENCY} at {rate}.")

        for asset in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset]
            del self._account_balances[asset]

    async def _margin_conversion_rate(self) -> Optional[Decimal]:
        """
        Units of the margin currency per 1 unit of the quote currency (e.g. INR
        per USDT), from CoinDCX's public spot ticker. Cached briefly since it is
        only used to value the wallet, not to price orders.
        """
        if self._margin_currency == CONSTANTS.QUOTE_CURRENCY:
            return Decimal("1")

        now = time.time()
        if (self._conversion_rate is not None
                and now - self._conversion_rate_ts < CONSTANTS.CONVERSION_RATE_TTL):
            return self._conversion_rate

        market = CONSTANTS.CONVERSION_MARKET.format(
            quote=CONSTANTS.QUOTE_CURRENCY, margin=self._margin_currency)
        try:
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            tickers = await rest_assistant.execute_request(
                url=CONSTANTS.SPOT_REST_URL + CONSTANTS.SPOT_TICKER_PATH_URL,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.SPOT_TICKER_PATH_URL,
            )
            for ticker in tickers if isinstance(tickers, list) else []:
                if ticker.get("market") == market:
                    rate = Decimal(str(ticker.get("last_price", 0) or 0))
                    if rate > s_decimal_0:
                        self._conversion_rate = rate
                        self._conversion_rate_ts = now
                        return rate
            self.logger().warning(f"Spot market {market} not found; cannot value the "
                                  f"{self._margin_currency} margin wallet.")
        except Exception as exception:
            self.logger().warning(f"Failed fetching the {market} conversion rate: {exception}")
        return self._conversion_rate

    # ---- positions -----------------------------------------------------------

    async def _update_positions(self):
        # Account-wide and paged, with no pair filter: an open position can sit
        # beyond the first page once an account has traded many contracts.
        payload = {"margin_currency_short_name": [self._margin_currency]}
        reported_keys = set()
        async for page in self._paginated_records(CONSTANTS.POSITIONS_PATH_URL, payload):
            for position in page:
                if isinstance(position, dict):
                    pos_key = self._process_position_payload(position)
                    if pos_key is not None:
                        reported_keys.add(pos_key)

        # Anything the exchange no longer reports is flat. Relying solely on an
        # active_pos=0 row would strand a "ghost" position forever if CoinDCX
        # simply omits a position closed outside this poll (e.g. from the web
        # UI) instead of returning it with a zero size.
        for pos_key in list(self._perpetual_trading.account_positions.keys()):
            if pos_key not in reported_keys:
                self.logger().debug(
                    f"Position {pos_key} is no longer reported by CoinDCX; treating it as closed.")
                self._perpetual_trading.remove_position(pos_key)

    def _process_position_payload(self, position: Dict[str, Any]) -> Optional[str]:
        """Returns the position key when a live position was recorded, else None."""
        coindcx_pair = position.get("pair")
        if not coindcx_pair:
            return None
        trading_pair = utils.coindcx_pair_to_hb_pair(coindcx_pair)

        amount = Decimal(str(position.get("active_pos", 0) or 0))
        position_side = PositionSide.LONG if amount > 0 else PositionSide.SHORT
        pos_key = self._perpetual_trading.position_key(trading_pair, position_side)

        if amount == s_decimal_0:
            # Flat: drop any position previously tracked for the pair. In ONEWAY
            # mode the key is the trading pair, so this covers both sides.
            self._perpetual_trading.remove_position(pos_key)
            return None

        entry_price = Decimal(str(position.get("avg_price", 0) or 0))
        mark_price = Decimal(str(position.get("mark_price", 0) or 0))
        leverage = Decimal(str(position.get("leverage", 1) or 1))
        # CoinDCX does not report unrealised PnL, so derive it from the mark price.
        unrealized_pnl = (mark_price - entry_price) * amount if mark_price > 0 else s_decimal_0

        self._perpetual_trading.set_position(pos_key, Position(
            trading_pair=trading_pair,
            position_side=position_side,
            unrealized_pnl=unrealized_pnl,
            entry_price=entry_price,
            amount=amount,
            leverage=leverage,
        ))
        return pos_key

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is PositionMode.ONEWAY:
            return True, ""
        return False, "CoinDCX futures only supports ONEWAY position mode."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        coindcx_pair = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        try:
            response = await self._api_post(
                path_url=CONSTANTS.UPDATE_LEVERAGE_PATH_URL,
                data={
                    "leverage": str(leverage),
                    "pair": coindcx_pair,
                    "margin_currency_short_name": self._margin_currency,
                },
                is_auth_required=True,
            )
        except Exception as exception:
            return False, str(exception)

        if isinstance(response, dict):
            code = response.get("code", response.get("status"))
            if str(response.get("message", "")).lower() == "success" or code in (200, "200"):
                return True, ""
            return False, str(response.get("message", response))
        return True, ""

    # ---- funding -------------------------------------------------------------

    async def build_funding_info(self, trading_pair: str) -> FundingInfo:
        """Mark price, index price, funding rate and next funding time from ``premiumIndex``."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        info = await self._api_get(path_url=CONSTANTS.PREMIUM_INDEX_PATH_URL, params={"symbol": symbol})
        if isinstance(info, list):
            info = next((entry for entry in info if isinstance(entry, dict) and entry.get("symbol") == symbol), None)
        mark_price = Decimal(str((info or {}).get("markPrice") or 0))
        if mark_price <= s_decimal_0:
            # A zero mark price would silently corrupt PnL and funding maths.
            raise ValueError(f"CoinDCX reported no usable mark price for {trading_pair}: {info}")
        index_price = Decimal(str(info.get("indexPrice") or 0))
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price if index_price > s_decimal_0 else mark_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=int(float(info["nextFundingTime"]) * 1e-3),
            rate=Decimal(str(info.get("lastFundingRate") or 0)),
        )

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Most recent funding payment for the pair, as (timestamp, rate, amount).

        Funding shows up in the position-transaction ledger under
        ``stage="funding"``; ``amount`` is the PnL of that transaction (negative
        when funding was paid). The record carries no rate, so — as Binance's
        connector does — the current rate is read from ``premiumIndex``.

        Returns ``(0, -1, -1)`` when the account has never paid/received funding
        on this pair, which is what the base class expects.
        """
        coindcx_pair = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # The ledger is account-wide with no pair filter, so on a multi-pair
        # account this pair's entries can fall outside the first page. Paging
        # avoids silently reporting "never paid funding".
        payload = {
            "stage": CONSTANTS.TRANSACTION_STAGE_FUNDING,
            "margin_currency_short_name": [self._margin_currency],
        }
        records: List[Dict[str, Any]] = []
        async for page in self._paginated_records(CONSTANTS.TRANSACTIONS_PATH_URL, payload):
            records.extend(
                record for record in page
                if isinstance(record, dict) and record.get("pair") == coindcx_pair
            )

        if not records:
            return 0, Decimal("-1"), Decimal("-1")

        latest = max(records, key=lambda record: float(record.get("created_at", 0) or 0))
        payment = Decimal(str(latest.get("amount", 0) or 0))
        # created_at is in milliseconds; the rest of this connector works in seconds.
        timestamp = float(latest.get("created_at", 0) or 0) * 1e-3

        rate = Decimal("-1")
        try:
            rate = (await self.build_funding_info(trading_pair)).rate
        except Exception as exception:
            self.logger().debug(f"Could not read the funding rate for {trading_pair}: {exception}")

        return timestamp, rate, payment

    # ---- prices --------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(path_url=CONSTANTS.TICKER_PRICE_PATH_URL, params={"symbol": symbol})
        price = response.get("price") if isinstance(response, dict) else None
        if price is None or Decimal(str(price)) <= s_decimal_0:
            # Returning 0 would feed a zero price straight into strategy maths.
            raise ValueError(f"CoinDCX reported no last price for {trading_pair}: {response}")
        return float(price)

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """Best bid and ask for every symbol, from ``ticker/bookTicker`` (rate oracle)."""
        tickers = await self._api_get(path_url=CONSTANTS.BOOK_TICKER_PATH_URL)
        return tickers if isinstance(tickers, list) else []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        24h statistics for every symbol, from ``ticker/24hr`` (volume oracle). One
        all-symbols call is cheaper than one call per pair once there are more than
        a handful, so callers filter.
        """
        tickers = await self._api_get(path_url=CONSTANTS.TICKER_24HR_PATH_URL)
        return tickers if isinstance(tickers, list) else []

    # ---- user stream ---------------------------------------------------------

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event")
                payload = event_message.get("data")
                records = payload if isinstance(payload, list) else [payload]

                if event_type == CONSTANTS.ORDER_UPDATE_EVENT_TYPE:
                    for record in records:
                        if isinstance(record, dict):
                            self._process_order_event(record)
                elif event_type == CONSTANTS.POSITION_UPDATE_EVENT_TYPE:
                    for record in records:
                        if isinstance(record, dict):
                            self._process_position_payload(record)
                elif event_type == CONSTANTS.BALANCE_UPDATE_EVENT_TYPE:
                    for record in records:
                        if isinstance(record, dict):
                            self._process_balance_event(record)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)

    def _process_order_event(self, order: Dict[str, Any]):
        exchange_order_id = str(order.get("id", ""))
        if not exchange_order_id:
            return
        tracked_order = self._tracked_order_for(exchange_order_id)
        if tracked_order is None:
            self._defer_order_event(exchange_order_id, order)
            return
        self._apply_order_event(order, tracked_order)

    def _tracked_order_for(self, exchange_order_id: str) -> Optional[InFlightOrder]:
        return next(
            (o for o in self._order_tracker.all_updatable_orders.values()
             if o.exchange_order_id == exchange_order_id),
            None,
        )

    def _apply_order_event(self, order: Dict[str, Any], tracked_order: InFlightOrder, replayed: bool = False):
        # A replayed event is by definition older than anything already applied.
        # ``update_with_order_update`` assigns ``current_state`` unconditionally,
        # so applying a stale "open" over a settled order would resurrect it.
        # Fills are always safe: they dedupe on trade id.
        if not (replayed and tracked_order.is_done):
            self._order_tracker.process_order_update(self._order_update_from_payload(order, tracked_order))

        # ``trades`` carries the fills that belong to this order update.
        for fill in order.get("trades") or []:
            if isinstance(fill, dict):
                self._order_tracker.process_trade_update(self._trade_update_from_fill(fill, tracked_order))

    def _defer_order_event(self, exchange_order_id: str, order: Dict[str, Any]):
        """
        Hold an unmatched order event until its order becomes trackable.

        CoinDCX assigns no client-order-id, so an order is only matchable once
        ``_place_order`` returns and the tracker records the exchange id. A market
        order can fill before that REST response lands, and dropping the event
        would leave the fill to the periodic REST poll seconds later. Events are
        only worth holding while some order is still awaiting its id — anything
        else belongs to another session or a manual trade.
        """
        if not any(o.exchange_order_id is None
                   for o in self._order_tracker.all_updatable_orders.values()):
            self.logger().debug(
                f"Ignoring {CONSTANTS.ORDER_UPDATE_EVENT_TYPE} for untracked order "
                f"{exchange_order_id} (status={order.get('status')}); no order is awaiting "
                f"an exchange id, so it is not ours.")
            return

        if len(self._pending_order_events) >= CONSTANTS.MAX_PENDING_ORDER_EVENTS:
            dropped_id = self._pending_order_events.pop(0)[0]
            self.logger().warning(
                f"Pending order-event buffer is full; dropped the oldest event for "
                f"{dropped_id}. The REST poll will reconcile it.")

        self._pending_order_events.append((exchange_order_id, order, time.time()))
        if self._pending_order_events_task is None or self._pending_order_events_task.done():
            self._pending_order_events_task = safe_ensure_future(self._replay_pending_order_events())

    async def _replay_pending_order_events(self):
        """Re-match held events until they land, expire, or the buffer drains."""
        try:
            while self._pending_order_events:
                await self._sleep(CONSTANTS.PENDING_ORDER_EVENT_RETRY_INTERVAL)
                still_pending: List[Tuple[str, Dict[str, Any], float]] = []
                for exchange_order_id, order, buffered_at in self._pending_order_events:
                    tracked_order = self._tracked_order_for(exchange_order_id)
                    if tracked_order is not None:
                        self.logger().debug(
                            f"Replaying deferred {CONSTANTS.ORDER_UPDATE_EVENT_TYPE} for "
                            f"{exchange_order_id} against {tracked_order.client_order_id}.")
                        self._apply_order_event(order, tracked_order, replayed=True)
                    elif time.time() - buffered_at < CONSTANTS.PENDING_ORDER_EVENT_TTL:
                        still_pending.append((exchange_order_id, order, buffered_at))
                    else:
                        self.logger().debug(
                            f"Deferred {CONSTANTS.ORDER_UPDATE_EVENT_TYPE} for {exchange_order_id} "
                            f"expired unmatched; leaving it to the REST poll.")
                self._pending_order_events = still_pending
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error replaying deferred order events.")

    def _process_balance_event(self, balance: Dict[str, Any]):
        """
        Apply a websocket balance frame using the same rules as ``_update_balances``:
        only the configured margin wallet counts, and it is published in the quote
        currency. Writing the raw wallet row instead would add a bogus native-currency
        balance and leave the quote-denominated figure — the one the budget checker
        reads — stale.
        """
        asset = balance.get("currency_short_name")
        if asset != self._margin_currency:
            return

        rate = self._conversion_rate if self._margin_currency != CONSTANTS.QUOTE_CURRENCY else Decimal("1")
        if not rate or rate <= s_decimal_0:
            # No cached rate yet: leave the REST-polled figure rather than
            # publishing an unconverted one.
            self.logger().debug(
                f"Skipping {self._margin_currency} balance frame: no "
                f"{CONSTANTS.QUOTE_CURRENCY} conversion rate cached yet.")
            return

        available = Decimal(str(balance.get("balance", 0) or 0))
        locked = Decimal(str(balance.get("locked_balance", 0) or 0))
        self._account_available_balances[CONSTANTS.QUOTE_CURRENCY] = available / rate
        self._account_balances[CONSTANTS.QUOTE_CURRENCY] = (available + locked) / rate
