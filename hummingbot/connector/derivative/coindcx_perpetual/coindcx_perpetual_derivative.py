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
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_decimal_0 = Decimal("0")


class CoindcxPerpetualDerivative(PerpetualDerivativePyBase):
    """
    CoinDCX perpetual futures connector.

    Contracts are USDT-quoted; the margin posted against them is chosen with
    ``coindcx_perpetual_margin_currency`` (``USDT`` — the default — or ``INR``,
    which CoinDCX converts at its own ``settlement_currency_conversion_price``).

    Notable exchange traits handled here:

    * Orders have **no client-order-id**; CoinDCX only returns its own UUID, so
      the connector tracks orders by exchange id and reconciles fills by pair.
    * Order quantities are plain base units (``unit_contract_value`` is 1), so no
      contract-size conversion is needed.
    * Post-only is not supported (``allow_post_only`` is false on every
      instrument), so only LIMIT and MARKET order types are advertised.
    * Positions expose no unrealised PnL, so it is derived from the mark price.
    * Fees and every margin figure the API reports are in USDT even when the
      margin currency is INR, so fee tokens are always USDT.

    CAVEAT for ``margin_currency == "INR"``: the collateral token becomes INR to
    match the funded wallet, but Hummingbot's budget checker sizes collateral
    from the contract's USDT-quoted notional. Those units differ by the INR/USDT
    rate, so an automated strategy would UNDERSTATE the INR required. Direct
    calls that pass an explicit amount (``_place_order``, the verify script) are
    unaffected — the budget checker is not in that path.
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

        # Instrument metadata keyed by CoinDCX pair, populated from exchange info.
        self._instruments: Dict[str, Dict[str, Any]] = {}
        self._funding_frequency_hours: Dict[str, int] = {}
        # Cached margin-currency -> quote-currency rate (see _margin_conversion_rate).
        self._conversion_rate: Optional[Decimal] = None
        self._conversion_rate_ts: float = 0.0

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
        return CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL

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
        exc = str(cancelation_exception)
        return (str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in exc
                or str(CONSTANTS.INVALID_REQUEST_ERROR_CODE) in exc
                or CONSTANTS.ORDER_NOT_EXIST_MESSAGE.lower() in exc.lower())

    # ---- exchange info / trading rules ---------------------------------------

    async def _make_trading_pairs_request(self) -> Any:
        return await self._fetch_instruments()

    async def _make_trading_rules_request(self) -> Any:
        return await self._fetch_instruments()

    async def _fetch_instruments(self) -> List[Dict[str, Any]]:
        """
        ``active_instruments`` only returns pair NAMES, so the details of each
        pair we care about are fetched individually. When the strategy declares
        trading pairs only those are resolved, keeping start-up to a couple of
        calls instead of ~485.
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        pair_names: List[str] = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL,
        )
        if not isinstance(pair_names, list):
            return []

        if self._trading_pairs:
            wanted = {utils.hb_pair_to_coindcx_pair(tp) for tp in self._trading_pairs}
            pair_names = [p for p in pair_names if p in wanted]

        results = await safe_gather(*[self._fetch_instrument(p) for p in pair_names], return_exceptions=True)
        instruments: List[Dict[str, Any]] = []
        for pair_name, result in zip(pair_names, results):
            if isinstance(result, Exception) or not result:
                self.logger().debug(f"Could not fetch instrument details for {pair_name}: {result}")
                continue
            instruments.append(result)
            self._instruments[result.get("pair", pair_name)] = result
        return instruments

    async def _fetch_instrument(self, coindcx_pair: str) -> Optional[Dict[str, Any]]:
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.INSTRUMENT_PATH_URL, domain=self._domain),
            params={"pair": coindcx_pair},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.INSTRUMENT_PATH_URL,
        )
        if isinstance(response, dict):
            return response.get("instrument", response)
        return None

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        mapping = bidict()
        for instrument in filter(utils.is_exchange_information_valid, exchange_info or []):
            coindcx_pair = instrument.get("pair", "")
            base = instrument.get("underlying_currency_short_name") or instrument.get("position_currency_short_name")
            quote = instrument.get("quote_currency_short_name")
            if not (coindcx_pair and base and quote):
                continue
            trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
            if trading_pair in mapping.inverse:
                continue
            mapping[coindcx_pair] = trading_pair
            self._funding_frequency_hours[trading_pair] = int(
                instrument.get("funding_frequency", CONSTANTS.DEFAULT_FUNDING_FREQUENCY_HOURS) or
                CONSTANTS.DEFAULT_FUNDING_FREQUENCY_HOURS)
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: List[Dict[str, Any]]) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for instrument in filter(utils.is_exchange_information_valid, exchange_info or []):
            try:
                coindcx_pair = instrument["pair"]
                trading_pair = utils.coindcx_pair_to_hb_pair(coindcx_pair)
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(str(instrument.get("min_quantity", instrument.get("min_trade_size", 0)))),
                    max_order_size=Decimal(str(instrument.get("max_quantity", 0))),
                    min_price_increment=Decimal(str(instrument.get("price_increment", 0))),
                    min_base_amount_increment=Decimal(str(instrument.get("quantity_increment", 0))),
                    min_notional_size=Decimal(str(instrument.get("min_notional", 0))),
                    # Must match get_buy/sell_collateral_token: collateral is
                    # denominated in the quote currency, not the wallet currency.
                    buy_order_collateral_token=CONSTANTS.QUOTE_CURRENCY,
                    sell_order_collateral_token=CONSTANTS.QUOTE_CURRENCY,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {instrument}.")
        return rules

    async def _update_trading_fees(self):
        """Per-instrument maker/taker fees are already captured with the trading rules."""
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
        is_maker = is_maker if is_maker is not None else (order_type is OrderType.LIMIT)
        trading_pair = combine_to_hb_trading_pair(base=base_currency, quote=quote_currency)
        instrument = self._instruments.get(utils.hb_pair_to_coindcx_pair(trading_pair))
        if instrument is not None:
            fee_percent = instrument.get("maker_fee" if is_maker else "taker_fee")
            if fee_percent is not None:
                return TradeFeeBase.new_perpetual_fee(
                    fee_schema=self.trade_fee_schema(),
                    position_action=position_action,
                    percent=utils.percent_fee_to_decimal(fee_percent),
                    # Fees are charged in USDT even on INR-margined futures.
                    percent_token=CONSTANTS.FEE_CURRENCY,
                )
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self.estimate_fee_pct(is_maker),
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
        if isinstance(response, dict):
            code = response.get("code", response.get("status"))
            return str(response.get("message", "")).lower() == "success" or code in (200, "200")
        return response is not None

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
        orders = await self._api_post(
            path_url=CONSTANTS.LIST_ORDERS_PATH_URL,
            data={
                "status": CONSTANTS.ALL_ORDER_STATUSES,
                "side": self.coindcx_side(tracked_order.trade_type),
                "page": "1",
                "size": "100",
                "margin_currency_short_name": [self._margin_currency],
            },
            is_auth_required=True,
        )
        for order in orders if isinstance(orders, list) else []:
            if str(order.get("id")) == str(exchange_order_id):
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
                url=web_utils.public_rest_url(CONSTANTS.SPOT_TICKER_PATH_URL, domain=self._domain),
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
        positions = await self._api_post(
            path_url=CONSTANTS.POSITIONS_PATH_URL,
            data={
                "page": "1",
                "size": "100",
                "margin_currency_short_name": [self._margin_currency],
            },
            is_auth_required=True,
        )

        for position in positions if isinstance(positions, list) else []:
            self._process_position_payload(position)

    def _process_position_payload(self, position: Dict[str, Any]):
        coindcx_pair = position.get("pair")
        if not coindcx_pair:
            return
        trading_pair = utils.coindcx_pair_to_hb_pair(coindcx_pair)

        amount = Decimal(str(position.get("active_pos", 0) or 0))
        position_side = PositionSide.LONG if amount > 0 else PositionSide.SHORT
        pos_key = self._perpetual_trading.position_key(trading_pair, position_side)

        if amount == s_decimal_0:
            # Flat: drop any position previously tracked for the pair. In ONEWAY
            # mode the key is the trading pair, so this covers both sides.
            self._perpetual_trading.remove_position(pos_key)
            return

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

    def next_funding_timestamp(self, trading_pair: str) -> int:
        """
        CoinDCX settles funding every ``funding_frequency`` hours on the UTC
        clock, so the next boundary is derived rather than fetched.
        """
        frequency = self._funding_frequency_hours.get(trading_pair, CONSTANTS.DEFAULT_FUNDING_FREQUENCY_HOURS)
        interval = max(int(frequency), 1) * 3600
        now = int(self._time_synchronizer.time() or time.time())
        return ((now // interval) + 1) * interval

    async def _fetch_current_prices(self) -> Dict[str, Dict[str, Any]]:
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_market_data_url(CONSTANTS.CURRENT_PRICES_PATH_URL, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.CURRENT_PRICES_PATH_URL,
        )
        prices = response.get("prices", {}) if isinstance(response, dict) else {}
        return prices if isinstance(prices, dict) else {}

    async def build_funding_info(self, trading_pair: str) -> FundingInfo:
        prices = await self._fetch_current_prices()
        info = prices.get(utils.hb_pair_to_coindcx_pair(trading_pair), {})
        mark_price = Decimal(str(info.get("mp", 0) or 0))
        last_price = Decimal(str(info.get("ls", 0) or 0))
        rate = Decimal(str(info.get("fr", 0) or 0))
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=mark_price if mark_price > 0 else last_price,
            mark_price=mark_price if mark_price > 0 else last_price,
            next_funding_utc_timestamp=self.next_funding_timestamp(trading_pair),
            rate=rate,
        )

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Most recent funding payment for the pair, as (timestamp, rate, amount).

        Funding shows up in the position-transaction ledger under
        ``stage="funding"``; ``amount`` is the PnL of that transaction (negative
        when funding was paid). The record carries no rate, so — as Binance's
        connector does — the current rate is read from the live prices feed.

        Returns ``(0, -1, -1)`` when the account has never paid/received funding
        on this pair, which is what the base class expects.
        """
        coindcx_pair = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        transactions = await self._api_post(
            path_url=CONSTANTS.TRANSACTIONS_PATH_URL,
            data={
                "stage": CONSTANTS.TRANSACTION_STAGE_FUNDING,
                "page": "1",
                "size": "50",
                "margin_currency_short_name": [self._margin_currency],
            },
            is_auth_required=True,
        )

        records = [
            record for record in (transactions if isinstance(transactions, list) else [])
            if isinstance(record, dict) and record.get("pair") == coindcx_pair
        ]
        if not records:
            return 0, Decimal("-1"), Decimal("-1")

        latest = max(records, key=lambda record: float(record.get("created_at", 0) or 0))
        payment = Decimal(str(latest.get("amount", 0) or 0))
        # created_at is in milliseconds; the rest of this connector works in seconds.
        timestamp = float(latest.get("created_at", 0) or 0) * 1e-3

        rate = Decimal("-1")
        try:
            prices = await self._fetch_current_prices()
            info = prices.get(coindcx_pair, {})
            if info.get("fr") is not None:
                rate = Decimal(str(info["fr"]))
        except Exception as exception:
            self.logger().debug(f"Could not read the funding rate for {trading_pair}: {exception}")

        return timestamp, rate, payment

    # ---- prices --------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        prices = await self._fetch_current_prices()
        info = prices.get(utils.hb_pair_to_coindcx_pair(trading_pair), {})
        last = info.get("ls") or info.get("mp")
        return float(last) if last is not None else 0.0

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """Ticker-shaped view over the public current-prices feed (rate oracle)."""
        prices = await self._fetch_current_prices()
        results: List[Dict[str, str]] = []
        for coindcx_pair, info in prices.items():
            last = info.get("ls") or info.get("mp")
            if last is None:
                continue
            results.append({
                "symbol": coindcx_pair,
                "lastPrice": str(last),
                "markPrice": str(info.get("mp", last)),
                "fundingRate": str(info.get("fr", 0)),
                "volume": str(info.get("v", 0)),
            })
        return results

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
        tracked_order = next(
            (o for o in self._order_tracker.all_updatable_orders.values()
             if o.exchange_order_id == exchange_order_id),
            None,
        )
        if tracked_order is None:
            return

        self._order_tracker.process_order_update(self._order_update_from_payload(order, tracked_order))

        # ``trades`` carries the fills that belong to this order update.
        for fill in order.get("trades") or []:
            if isinstance(fill, dict):
                self._order_tracker.process_trade_update(self._trade_update_from_fill(fill, tracked_order))

    def _process_balance_event(self, balance: Dict[str, Any]):
        asset = balance.get("currency_short_name")
        if not asset:
            return
        available = Decimal(str(balance.get("balance", 0) or 0))
        locked = Decimal(str(balance.get("locked_balance", 0) or 0))
        self._account_available_balances[asset] = available
        self._account_balances[asset] = available + locked
