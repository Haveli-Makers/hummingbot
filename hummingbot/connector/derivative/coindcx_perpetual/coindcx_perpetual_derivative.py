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
from hummingbot.core.data_type.common import OpenOrder, OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
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

        # Instrument metadata keyed by CoinDCX pair, populated from exchange info.
        self._instruments: Dict[str, Dict[str, Any]] = {}
        self._funding_frequency_hours: Dict[str, int] = {}
        # Cached margin-currency -> quote-currency rate (see _margin_conversion_rate).
        self._conversion_rate: Optional[Decimal] = None
        self._conversion_rate_ts: float = 0.0

        # Order events that arrived before _place_order recorded the exchange id,
        # held as (exchange_order_id, payload, buffered_at) in arrival order.
        self._pending_order_events: List[Tuple[str, Dict[str, Any], float]] = []
        # Signatures of order frames already applied, so repeats are dropped. See
        # _already_applied.
        self._applied_order_events: List[Tuple[str, str, str, str]] = []
        self._pending_order_events_task: Optional[asyncio.Task] = None
        self._balance_refresh_task: Optional[asyncio.Task] = None
        # Orders whose creation has already been announced. See trigger_event.
        self._announced_creations: List[str] = []
        # Last set of unready status keys reported, so status_dict logs only on change.
        self._last_reported_block: List[str] = []

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

    @property
    def tracking_states(self) -> Dict[str, Any]:
        """
        Persist only the orders CoinDCX actually acknowledged.

        An order the venue rejected at creation never receives an exchange order id, and
        CoinDCX has no client-order-id to look it up by instead — so there is nothing at the
        exchange with that identity to reconcile against, now or ever. The base class keeps
        it anyway (failed orders are held as "lost" and lost orders are saved), so the next
        run restores it, polls it until the not-found counter trips, logs it as lost, and
        warns that it cannot be cancelled. That noise then outlives the incident by however
        many restarts it takes for someone to clear the database by hand.

        Orders that failed WITH an exchange id are kept, because those can still be checked,
        and so are orders still awaiting one — dropping those could hide a position the venue
        accepted while we missed the reply.
        """
        return {
            client_order_id: order.to_json()
            for client_order_id, order in self._order_tracker.all_updatable_orders.items()
            if order.exchange_order_id is not None or not order.is_failure
        }

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
        if self._balance_refresh_task is not None:
            self._balance_refresh_task.cancel()
            try:
                await self._balance_refresh_task
            except (asyncio.CancelledError, Exception):
                pass
            self._balance_refresh_task = None
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

    async def _make_trading_pairs_request(self) -> Any:
        # The symbol map only needs pair/base/quote, all of which are encoded in
        # the instrument name (B-BTC_USDT). Skipping the per-instrument detail
        # calls matters: TradingPairFetcher builds this connector with no trading
        # pairs at every startup, which would otherwise fan out to one request
        # per listed instrument.
        return await self._fetch_instrument_names()

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

        # One request per instrument, so bound the fan-out.
        semaphore = asyncio.Semaphore(CONSTANTS.INSTRUMENT_FETCH_CONCURRENCY)

        async def _fetch(pair_name: str):
            async with semaphore:
                return await self._fetch_instrument(pair_name)

        results = await safe_gather(*[_fetch(p) for p in pair_names], return_exceptions=True)
        instruments: List[Dict[str, Any]] = []
        for pair_name, result in zip(pair_names, results):
            if isinstance(result, Exception) or not result:
                self.logger().debug(f"Could not fetch instrument details for {pair_name}: {result}")
                continue
            instruments.append(result)
            self._instruments[result.get("pair", pair_name)] = result
        return instruments

    async def _fetch_instrument_names(self) -> List[Dict[str, Any]]:
        """
        Minimal instrument records built from the active-instrument names alone,
        for callers that only need the symbol map. One request, no fan-out.
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        pair_names = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ACTIVE_INSTRUMENTS_PATH_URL,
        )
        instruments: List[Dict[str, Any]] = []
        for pair_name in pair_names if isinstance(pair_names, list) else []:
            base, quote = utils.split_coindcx_pair(pair_name)
            if not (base and quote):
                continue
            instruments.append({
                "pair": pair_name,
                "underlying_currency_short_name": base,
                "quote_currency_short_name": quote,
                # Listed by active_instruments, so it is tradable by definition.
                "status": "active",
                "kind": "perpetual",
                "is_inverse": False,
                "exit_only": False,
                "min_quantity": 0,
                "max_quantity": 1,
            })
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
        if position_action is PositionAction.CLOSE:
            # Without this the venue treats a close as a fresh opposite position and demands
            # margin for it, so closing fails with "Insufficient funds" exactly when the
            # position is large relative to the wallet — the moment you most need to get out.
            order[CONSTANTS.REDUCE_ONLY_FIELD] = True
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

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        Readiness, plus a note naming whatever is holding it up.

        An unready connector makes the strategy log "is not ready" every tick without
        saying which check is failing. Logged only when the set changes.
        """
        status = super().status_dict
        blocked = sorted(key for key, ready in status.items() if not ready)
        if blocked and blocked != self._last_reported_block:
            self._last_reported_block = blocked
            self.logger().info(f"Connector not ready; waiting on: {', '.join(blocked)}.")
        elif not blocked and self._last_reported_block:
            self._last_reported_block = []
            self.logger().info("Connector ready.")
        return status

    async def get_open_orders(self) -> List[OpenOrder]:
        """
        Orders resting on the exchange right now.

        Asks the exchange rather than reading ``in_flight_orders``, so manual orders and
        orders left by an earlier session are included.
        """
        orders: List[OpenOrder] = []
        payload = {"status": CONSTANTS.OPEN_ORDER_STATUSES,
                   "margin_currency_short_name": [self._margin_currency]}
        for side in (TradeType.BUY, TradeType.SELL):
            side_payload = dict(payload, side=self.coindcx_side(side))
            async for page in self._paginated_records(CONSTANTS.LIST_ORDERS_PATH_URL, side_payload):
                for order in page:
                    if not isinstance(order, dict):
                        continue
                    try:
                        trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                            symbol=str(order.get("pair", "")))
                    except Exception:
                        # An order on a pair this connector was not configured for.
                        continue
                    amount = Decimal(str(order.get("total_quantity") or 0))
                    orders.append(OpenOrder(
                        client_order_id=str(order.get("client_order_id") or order.get("id", "")),
                        trading_pair=trading_pair,
                        price=Decimal(str(order.get("price") or 0)),
                        amount=amount,
                        executed_amount=Decimal(str(order.get("filled_quantity") or 0)),
                        status=str(order.get("status", "")),
                        order_type=OrderType.LIMIT if "limit" in str(
                            order.get("order_type", "")).lower() else OrderType.MARKET,
                        is_buy=str(order.get("side", "")).lower() == "buy",
                        time=int(order.get("created_at") or 0),
                        exchange_order_id=str(order.get("id", "")),
                    ))
        return orders

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
        info = prices.get(utils.hb_pair_to_coindcx_pair(trading_pair))
        if not info:
            # A zero mark price would silently corrupt PnL and funding maths.
            raise ValueError(
                f"No entry for {trading_pair} in the CoinDCX current-prices feed.")
        mark_price = Decimal(str(info.get("mp", 0) or 0))
        last_price = Decimal(str(info.get("ls", 0) or 0))
        if mark_price <= s_decimal_0 and last_price <= s_decimal_0:
            raise ValueError(f"CoinDCX reported no usable price for {trading_pair}.")
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
        if last is None:
            # Returning 0 would feed a zero price straight into strategy maths.
            raise ValueError(f"No price for {trading_pair} in the CoinDCX current-prices feed.")
        return float(last)

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

    def trigger_event(self, event_tag, message):
        """
        Announce an order's creation once, however many times the state machine says so.

        The REST placement response and the websocket frame both drive an order out of
        PENDING_CREATE, and landing ~400ms apart they each fire the created event.
        MarketsRecorder writes a row per event, so the second fails on a duplicate
        Order.id and aborts whatever was queued behind it. The two paths cannot see each
        other, so the constraint is enforced here, where they converge.
        """
        if event_tag in (MarketEvent.BuyOrderCreated, MarketEvent.SellOrderCreated):
            order_id = getattr(message, "order_id", None)
            if order_id is not None:
                if order_id in self._announced_creations:
                    self.logger().debug(
                        f"Suppressing a repeat {event_tag.name} for {order_id}.")
                    return
                self._announced_creations.append(order_id)
                if len(self._announced_creations) > CONSTANTS.MAX_APPLIED_ORDER_EVENTS:
                    del self._announced_creations[:-CONSTANTS.MAX_APPLIED_ORDER_EVENTS]
        super().trigger_event(event_tag, message)

    def _process_order_event(self, order: Dict[str, Any]):
        exchange_order_id = str(order.get("id", ""))
        if not exchange_order_id:
            return
        if self._already_applied(order, exchange_order_id):
            return
        tracked_order = self._tracked_order_for(exchange_order_id)
        if tracked_order is None:
            self._defer_order_event(exchange_order_id, order)
            return
        self._apply_order_event(order, tracked_order)

    def _already_applied(self, order: Dict[str, Any], exchange_order_id: str) -> bool:
        """
        Drop an order frame we have already handled.

        CoinDCX repeats frames — its Socket.IO payloads are double-wrapped, and a deferred
        event can also be replayed — so the same state transition can be applied twice. The
        second one re-fires BuyOrderCreated/SellOrderCreated, and MarketsRecorder then tries
        to insert a row it already has:

            sqlite3.IntegrityError: UNIQUE constraint failed: Order.id

        That exception escapes into the event listener, so any work queued behind it on the
        same frame is skipped. Keying on the fields that define a transition lets genuine
        updates through while repeats are ignored.
        """
        signature = (exchange_order_id,
                     str(order.get("status", "")).lower(),
                     str(order.get("updated_at") or order.get("created_at") or ""),
                     str(order.get("filled_quantity") or ""))
        if signature in self._applied_order_events:
            self.logger().debug(f"Ignoring repeated order frame for {exchange_order_id} "
                                f"(status={signature[1]}).")
            return True
        self._applied_order_events.append(signature)
        if len(self._applied_order_events) > CONSTANTS.MAX_APPLIED_ORDER_EVENTS:
            del self._applied_order_events[:-CONSTANTS.MAX_APPLIED_ORDER_EVENTS]
        return False

    def _tracked_order_for(self, exchange_order_id: str) -> Optional[InFlightOrder]:
        return next(
            (o for o in self._order_tracker.all_updatable_orders.values()
             if o.exchange_order_id == exchange_order_id),
            None,
        )

    def _apply_order_event(self, order: Dict[str, Any], tracked_order: InFlightOrder, replayed: bool = False):
        # ``trades`` carries the fills that belong to this order update, and they are
        # applied *before* the order update. ``executed_amount_base`` is only ever written
        # by trade updates, so an order that reaches a terminal state first is briefly
        # "filled" with nothing executed — and ClientOrderTracker stops tracking it the
        # moment it settles, so anything arriving afterwards is lost.
        self._apply_fills(order.get("trades"), tracked_order)

        # A terminal status with no fills attached is the case that costs money: the order
        # settles with executed_amount_base at zero and every size derived from it collapses.
        # CoinDCX does not always attach them, so fetch them over REST before letting the
        # order settle rather than leaving it to the periodic poll seconds later.
        if self._is_terminal_fill_payload(order) and not tracked_order.is_done \
                and tracked_order.executed_amount_base <= s_decimal_0:
            safe_ensure_future(self._settle_with_fetched_fills(order, tracked_order, replayed))
            return

        # A replayed event is by definition older than anything already applied.
        # ``update_with_order_update`` assigns ``current_state`` unconditionally,
        # so applying a stale "open" over a settled order would resurrect it.
        if not (replayed and tracked_order.is_done):
            self._push_order_update(order, tracked_order)

    def _push_order_update(self, order: Dict[str, Any], tracked_order: InFlightOrder):
        """
        Send an order update only when it actually changes the order's state.

        The venue repeats frames for an unchanged order, and each one would otherwise
        schedule a tracker update that assigns the same values back.
        """
        update = self._order_update_from_payload(order, tracked_order)
        if update.new_state == tracked_order.current_state:
            return
        self._order_tracker.process_order_update(update)
        if update.new_state in (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED):
            self._refresh_balances_soon()

    def _refresh_balances_soon(self):
        """
        Re-read balances as soon as an order settles.

        Closing a position releases margin, but the connector only learns that on its next
        scheduled poll — until then a budget check sees the pre-close figure and refuses
        an order the wallet can afford. Orders settle rarely enough for this to be cheap.
        """
        if not self._trading_required:
            return
        if self._balance_refresh_task is not None and not self._balance_refresh_task.done():
            return
        self._balance_refresh_task = safe_ensure_future(self._refresh_balances())

    async def _refresh_balances(self):
        try:
            await self._update_balances()
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().debug(f"Post-settlement balance refresh failed: {exception}")

    def _apply_fills(self, fills: Optional[List[Any]], tracked_order: InFlightOrder):
        """Apply any fills on a payload. Safe to repeat: trade updates dedupe on trade id."""
        for fill in fills or []:
            if isinstance(fill, dict):
                self._order_tracker.process_trade_update(self._trade_update_from_fill(fill, tracked_order))

    @staticmethod
    def _is_terminal_fill_payload(order: Dict[str, Any]) -> bool:
        status = str(order.get("status", "")).lower()
        return CONSTANTS.ORDER_STATE.get(status) == OrderState.FILLED and not order.get("trades")

    async def _settle_with_fetched_fills(self, order: Dict[str, Any], tracked_order: InFlightOrder,
                                         replayed: bool):
        """
        Pull an order's fills over REST, then settle it.

        The websocket says the order is done but has not told us what it traded. The trades
        endpoint has the data, so ask for it directly instead of settling blind and waiting
        for the periodic poll to reconcile a position we have already mis-sized.
        """
        try:
            for trade_update in await self._all_trade_updates_for_order(tracked_order):
                self._order_tracker.process_trade_update(trade_update)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(
                f"Could not fetch fills for {tracked_order.client_order_id} before settling it: "
                f"{exception}. Settling on the order status alone; sizes may be incomplete "
                f"until the next poll.")
        if not (replayed and tracked_order.is_done):
            self._push_order_update(order, tracked_order)

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
