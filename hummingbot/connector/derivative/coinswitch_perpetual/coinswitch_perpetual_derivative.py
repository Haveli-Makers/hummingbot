import asyncio
import math
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.coinswitch_perpetual import (
    coinswitch_perpetual_constants as CONSTANTS,
    coinswitch_perpetual_utils as utils,
    coinswitch_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_api_order_book_data_source import (
    CoinswitchPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_api_user_stream_data_source import (
    BALANCE_CHANNEL,
    ORDERS_CHANNEL,
    POSITIONS_CHANNEL,
    CoinswitchPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_auth import CoinswitchPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_decimal_0 = Decimal("0")

# One order as seen in a poll, and whether it was in the open-orders list
# (True), the closed-orders list (False) or fetched on its own (None).
OrderSnapshot = Tuple[Dict[str, Any], Optional[bool]]


class CoinswitchPerpetualDerivative(PerpetualDerivativePyBase):
    """
    CoinSwitch PRO perpetual futures connector (Futures v2 API, ``EXCHANGE_2``).

    Exchange traits handled here:

    * **USDT everywhere.** Every contract is quoted, margined and settled in USDT,
      so USDT is the collateral for both sides and the only wallet asset.
    * **Market data streams, the account is polled.** The Socket.IO sockets carry
      order books, trades and tickers (funding info) only. Wallet, positions and
      orders come from REST loops in the user-stream data source, paced to the
      20-calls-per-minute limits of those endpoints.
    * **Orders are tracked in batches.** One open-orders call covers every live
      order; orders that left it are settled from one closed-orders call, and
      only what is in neither is fetched on its own. The base class's per-order
      status loop would exhaust the 20/min order-status budget with a handful of
      orders.
    * **Fills are cumulative.** There is no own-trades endpoint: each order
      reports ``exec_quantity``, ``avg_execution_price`` and ``execution_fee``
      so far, and the connector books the increment since the last poll.
    * **PARTIALLY_EXECUTED is terminal** (the remainder no longer works), so it
      settles as CANCELED with its fills, unless the order is still listed open.
    * **Cancels are two-phase** (CANCELLATION_RAISED, then CANCELLED). The
      acknowledgement is not treated as final; a fill racing the cancel is kept.
    * **client_order_id must be a UUID**: uuid5 of the Hummingbot id is sent, so
      an order whose create response was lost can still be found by it.
    * ONEWAY positions; a CLOSE order is sent ``reduce_only``. LIMIT and MARKET
      only (no post-only flag exists). Leverage is a per-symbol account setting
      that cannot change while the symbol has an open position or open orders.
    """

    web_utils = web_utils

    def __init__(
        self,
        coinswitch_perpetual_api_key: str = None,
        coinswitch_perpetual_api_secret: str = None,
        coinswitch_perpetual_proxy_url: str = "",
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = coinswitch_perpetual_api_key or ""
        self.secret_key = coinswitch_perpetual_api_secret or ""
        self._proxy_url = coinswitch_perpetual_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        # instrument_info records keyed by Hummingbot trading pair.
        self._instruments: Dict[str, Dict[str, Any]] = {}
        # (maker, taker) fee rates per trading pair, from instrument_info.
        self._fee_rates: Dict[str, Tuple[Decimal, Decimal]] = {}
        # Funding rows carry no timestamp, so new payments are told apart by id.
        self._funding_transactions_seen: Dict[str, Set[str]] = {}
        self._last_funding_payment: Dict[str, Tuple[float, Decimal, Decimal]] = {}
        # Order payload fetched by the fills pass, reused by the state pass that follows.
        self._order_payload_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._last_forced_time_sync: float = 0.0

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---- identity / configuration -------------------------------------------

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinswitchPerpetualAuth:
        return CoinswitchPerpetualAuth(
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
        return CONSTANTS.INSTRUMENT_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.INSTRUMENT_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # The cancel response is CANCELLATION_RAISED; the poll confirms CANCELLED.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return CONSTANTS.FUNDING_FEE_POLL_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule = self._trading_rules.get(trading_pair)
        return trading_rule.buy_order_collateral_token if trading_rule is not None else CONSTANTS.COLLATERAL_TOKEN

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule = self._trading_rules.get(trading_pair)
        return trading_rule.sell_order_collateral_token if trading_rule is not None else CONSTANTS.COLLATERAL_TOKEN

    # ---- factories -----------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> CoinswitchPerpetualAPIOrderBookDataSource:
        return CoinswitchPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinswitchPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
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

    @staticmethod
    def _http_status(exception: Exception) -> Optional[int]:
        text = str(exception)
        marker = "HTTP status is "
        position = text.find(marker)
        if position < 0:
            return None
        digits = text[position + len(marker):position + len(marker) + 3]
        return int(digits) if digits.isdigit() else None

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """
        A 401 "Invalid Access" is also what an epoch outside the drift window
        gets. Resync and retry once, at most once a minute, so bad keys do not
        double every failing call.
        """
        if self._http_status(request_exception) != 401:
            return False
        now = time.time()
        if now - self._last_forced_time_sync < CONSTANTS.ONE_MINUTE:
            return False
        self._last_forced_time_sync = now
        return True

    def _is_not_found_error(self, exception: Exception) -> bool:
        status = self._http_status(exception)
        if status not in (400, 404, 422):
            return False
        text = str(exception).lower()
        return any(marker in text for marker in ("not found", "does not exist", "doesn't exist", "invalid order",
                                                 "no order", "not exist"))

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found_error(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found_error(cancelation_exception)

    def _is_outcome_unknown(self, exception: Exception) -> bool:
        """
        True when a create request may or may not have reached the exchange: a
        timeout, a dropped connection or an HTTP 5xx. The docs ask for a lookup by
        client_order_id rather than assuming failure.
        """
        if isinstance(exception, (asyncio.TimeoutError, ConnectionError, aiohttp.ClientConnectionError)):
            return True
        status = self._http_status(exception)
        return status is not None and status >= 500

    # ---- instruments / trading rules -----------------------------------------

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(
            path_url=CONSTANTS.INSTRUMENT_INFO_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE},
            is_auth_required=True,
            limit_id=CONSTANTS.INSTRUMENT_INFO_PATH_URL,
        )

    async def _make_trading_pairs_request(self) -> Any:
        return await self._make_trading_rules_request()

    @staticmethod
    def _instruments_from(exchange_info: Any) -> Dict[str, Dict[str, Any]]:
        """``{"data": {"BTCUSDT": {...}, ...}}``, possibly nested under the exchange id."""
        data = utils.unwrap_data(exchange_info)
        if not isinstance(data, dict):
            return {}
        nested = data.get(CONSTANTS.EXCHANGE)
        if isinstance(nested, dict) and len(data) == 1:
            data = nested
        return {str(symbol).upper(): info for symbol, info in data.items() if isinstance(info, dict)}

    def _valid_instruments(self, exchange_info: Any) -> Iterable[Tuple[str, str, Dict[str, Any]]]:
        """(exchange symbol, trading pair, instrument) for every tradable perpetual."""
        for symbol, instrument in self._instruments_from(exchange_info).items():
            if not utils.is_exchange_information_valid(symbol, instrument):
                continue
            base, quote = utils.base_and_quote(symbol, instrument)
            yield symbol, combine_to_hb_trading_pair(base, quote), instrument

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        for symbol, trading_pair, instrument in self._valid_instruments(exchange_info):
            if trading_pair in mapping.inverse:
                continue
            mapping[symbol] = trading_pair
            self._instruments[trading_pair] = instrument
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for symbol, trading_pair, instrument in self._valid_instruments(exchange_info):
            try:
                self._instruments[trading_pair] = instrument
                maker = utils.to_decimal(instrument.get("maker_fee_rate"), Decimal("-1"))
                taker = utils.to_decimal(instrument.get("taker_fee_rate"), Decimal("-1"))
                if maker >= 0 and taker >= 0:
                    self._fee_rates[trading_pair] = (maker, taker)
                max_size = utils.to_decimal(instrument.get("max_base_quantity"))
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=utils.to_decimal(instrument.get("min_base_quantity")),
                    max_order_size=max_size if max_size > 0 else Decimal("1e56"),
                    min_price_increment=utils.price_increment(instrument),
                    min_base_amount_increment=utils.quantity_increment(instrument),
                    buy_order_collateral_token=CONSTANTS.COLLATERAL_TOKEN,
                    sell_order_collateral_token=CONSTANTS.COLLATERAL_TOKEN,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {symbol}: {instrument}. Skipping.")
        return rules

    async def _update_trading_fees(self):
        """instrument_info carries each contract's maker/taker rates; they load with the trading rules."""
        pass

    def max_leverage(self, trading_pair: str) -> Optional[int]:
        instrument = self._instruments.get(trading_pair)
        if not instrument or instrument.get("max_leverage") in (None, ""):
            return None
        return int(utils.to_decimal(instrument.get("max_leverage")))

    def min_leverage(self, trading_pair: str) -> int:
        instrument = self._instruments.get(trading_pair) or {}
        return max(int(utils.to_decimal(instrument.get("min_leverage"), Decimal("1"))), 1)

    # ---- fees ---------------------------------------------------------------

    def _fee_rate(self, trading_pair: str, is_maker: bool) -> Decimal:
        rates = self._fee_rates.get(trading_pair)
        if rates is not None:
            return rates[0] if is_maker else rates[1]
        return self.estimate_fee_pct(is_maker)

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
        trading_pair = combine_to_hb_trading_pair(base_currency, quote_currency)
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self._fee_rate(trading_pair, is_maker),
        )

    # ---- orders --------------------------------------------------------------

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           position_action: PositionAction = PositionAction.NIL,
                           **kwargs) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        # Amounts are JSON numbers; the auth re-renders them without exponents.
        data: Dict[str, Any] = {
            "exchange": CONSTANTS.EXCHANGE,
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "order_type": CONSTANTS.ORDER_TYPE_MARKET if order_type is OrderType.MARKET else CONSTANTS.ORDER_TYPE_LIMIT,
            "quantity": float(amount),
            "client_order_id": utils.client_order_uuid(order_id),
        }
        if order_type is not OrderType.MARKET:
            data["price"] = float(price)
        if position_action is PositionAction.CLOSE:
            # Can only shrink the position, never flip it.
            data["reduce_only"] = True

        try:
            response = await self._post_order(data)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            if not self._is_outcome_unknown(request_error):
                raise
            self.logger().warning(
                f"Outcome of order {order_id} is unknown ({request_error}); looking it up by client_order_id.")
            order = await self._query_order_by_client_id(order_id)
            if order is not None:
                response = {"data": order}
            else:
                # Never landed: re-send. The exchange de-duplicates on client_order_id,
                # so a request that did land in the meantime is not placed twice.
                response = await self._post_order(data)

        order = self._order_from_response(response)
        if not order or order.get("order_id") in (None, ""):
            raise IOError(f"Error submitting order {order_id} to CoinSwitch: {response}")
        if CONSTANTS.ORDER_STATE.get(str(order.get("status", "")).upper()) is OrderState.FAILED:
            raise IOError(f"CoinSwitch rejected order {order_id}: {order}")

        created = utils.to_decimal(order.get("created_at"))
        transact_time = float(created) * 1e-3 if created > 0 else self._time_synchronizer.time()
        return str(order["order_id"]), transact_time

    async def _post_order(self, data: Dict[str, Any]) -> Any:
        return await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.CREATE_ORDER_LIMIT_ID,
        )

    @staticmethod
    def _order_from_response(response: Any) -> Optional[Dict[str, Any]]:
        """Order responses are ``{"data": {...}}``, except Get Order Status: ``{"data": {"order": {...}}}``."""
        data = utils.unwrap_data(response)
        if isinstance(data, dict) and isinstance(data.get("order"), dict):
            data = data["order"]
        return data if isinstance(data, dict) else None

    @staticmethod
    def _orders_from_list(response: Any) -> List[Dict[str, Any]]:
        data = utils.unwrap_data(response)
        orders = data.get("orders") if isinstance(data, dict) else data
        return [order for order in orders if isinstance(order, dict)] if isinstance(orders, list) else []

    async def _query_order_by_client_id(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        try:
            response = await self._api_get(
                path_url=CONSTANTS.ORDER_PATH_URL,
                params={"exchange": CONSTANTS.EXCHANGE, "client_order_id": utils.client_order_uuid(client_order_id)},
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_STATUS_LIMIT_ID,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            status = self._http_status(exception)
            if status is not None and 400 <= status < 500:
                return None
            raise
        order = self._order_from_response(response)
        return order if order and order.get("order_id") not in (None, "") else None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = tracked_order.exchange_order_id
        if not exchange_order_id:
            # Cancel needs the exchange id; an order whose create response was lost
            # can still be found by its client_order_id.
            order = await self._query_order_by_client_id(order_id)
            if order is None:
                raise IOError(f"Cannot cancel {order_id}: CoinSwitch has no order with its client_order_id yet.")
            exchange_order_id = str(order["order_id"])
            tracked_order.update_exchange_order_id(exchange_order_id)

        response = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data={"exchange": CONSTANTS.EXCHANGE, "order_id": exchange_order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID,
        )
        order = self._order_from_response(response)
        if order is not None and (order.get("order_id") or order.get("status")):
            return True
        raise IOError(f"Unexpected response cancelling order {order_id}: {response}")

    async def _request_order_payload(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        if tracked_order.exchange_order_id:
            params = {"exchange": CONSTANTS.EXCHANGE, "order_id": tracked_order.exchange_order_id}
        else:
            params = {"exchange": CONSTANTS.EXCHANGE,
                      "client_order_id": utils.client_order_uuid(tracked_order.client_order_id)}
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_STATUS_LIMIT_ID,
        )
        order = self._order_from_response(response)
        if not order or order.get("order_id") in (None, ""):
            raise IOError(f"Order {tracked_order.client_order_id} not found on CoinSwitch: {response}")
        return order

    _ORDER_PAYLOAD_CACHE_TTL = 5.0

    def _cache_order_payload(self, client_order_id: str, payload: Dict[str, Any]):
        now = time.monotonic()
        self._order_payload_cache = {key: value for key, value in self._order_payload_cache.items()
                                     if now - value[0] <= self._ORDER_PAYLOAD_CACHE_TTL}
        self._order_payload_cache[client_order_id] = (now, payload)

    def _pop_cached_order_payload(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        cached = self._order_payload_cache.pop(client_order_id, None)
        if cached is not None and time.monotonic() - cached[0] <= self._ORDER_PAYLOAD_CACHE_TTL:
            return cached[1]
        return None

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        payload = self._pop_cached_order_payload(tracked_order.client_order_id)
        if payload is None:
            payload = await self._request_order_payload(tracked_order)
        return self._order_update_from_payload(payload, tracked_order, working=None)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        payload = await self._request_order_payload(order)
        # The base class asks for the state right after the fills; one call serves both.
        self._cache_order_payload(order.client_order_id, payload)
        trade_update = self._trade_update_from_payload(order, payload)
        return [trade_update] if trade_update is not None else []

    def _order_age(self, order: InFlightOrder) -> float:
        now = self.current_timestamp
        if now is None or math.isnan(now) or now <= 0 or order.creation_timestamp <= 0:
            return math.inf
        return now - order.creation_timestamp

    async def _fetch_order_snapshots(self, orders: List[InFlightOrder]) -> List[OrderSnapshot]:
        """
        Current state of ``orders`` for (at most) one open-orders call, one
        closed-orders call and ``MAX_STATUS_LOOKUPS_PER_POLL`` single lookups.
        Orders without an exchange id are skipped; they are found by client id
        elsewhere.
        """
        tracked = {str(order.exchange_order_id): order for order in orders if order.exchange_order_id}
        if not tracked:
            return []

        snapshots: List[OrderSnapshot] = []
        found: Set[str] = set()
        for payload in self._orders_from_list(await self._request_order_list(CONSTANTS.OPEN_ORDERS_PATH_URL)):
            exchange_order_id = str(payload.get("order_id") or "")
            if exchange_order_id in tracked and exchange_order_id not in found:
                snapshots.append((payload, True))
                found.add(exchange_order_id)

        if len(found) < len(tracked):
            for payload in self._orders_from_list(await self._request_order_list(CONSTANTS.CLOSED_ORDERS_PATH_URL)):
                exchange_order_id = str(payload.get("order_id") or "")
                if exchange_order_id in tracked and exchange_order_id not in found:
                    snapshots.append((payload, False))
                    found.add(exchange_order_id)

        # In neither list: just placed, older than the lists reach, or truncated.
        unresolved = [order for exchange_order_id, order in tracked.items()
                      if exchange_order_id not in found and self._order_age(order) > CONSTANTS.ORDER_LIST_GRACE_PERIOD]
        unresolved.sort(key=lambda order: order.last_update_timestamp)
        for order in unresolved[:CONSTANTS.MAX_STATUS_LOOKUPS_PER_POLL]:
            try:
                snapshots.append((await self._request_order_payload(order), None))
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                self.logger().debug(f"Status lookup for {order.client_order_id} failed: {exception}")
        return snapshots

    async def _request_order_list(self, path_url: str) -> Any:
        return await self._api_post(
            path_url=path_url,
            data={"exchange": CONSTANTS.EXCHANGE, "limit": CONSTANTS.ORDER_LIST_LIMIT},
            is_auth_required=True,
            limit_id=path_url,
        )

    def _process_order_snapshots(self, snapshots: List[OrderSnapshot]):
        """Books the new fills of each order, then its state (fills first: a terminal state stops tracking)."""
        updatable = self._order_tracker.all_updatable_orders_by_exchange_order_id
        fillable = self._order_tracker.all_fillable_orders_by_exchange_order_id
        for payload, working in snapshots:
            if not isinstance(payload, dict):
                continue
            exchange_order_id = str(payload.get("order_id") or "")
            tracked_order = updatable.get(exchange_order_id) or fillable.get(exchange_order_id)
            if tracked_order is None:
                continue
            trade_update = self._trade_update_from_payload(tracked_order, payload)
            if trade_update is not None:
                self._order_tracker.process_trade_update(trade_update)
            if exchange_order_id in updatable:
                self._order_tracker.process_order_update(
                    self._order_update_from_payload(payload, tracked_order, working=working))

    async def _update_order_status(self):
        """Batch replacement for the base class's one-call-per-order status loop."""
        active_orders = list(self._order_tracker.active_orders.values())
        self._process_order_snapshots(await self._fetch_order_snapshots(active_orders))
        # Orders that never got an exchange id (create response lost) are found by
        # client id; repeated misses go through the not-found counter.
        unidentified = [order for order in active_orders
                        if not order.exchange_order_id and not order.is_pending_create
                        and self._order_age(order) > CONSTANTS.ORDER_LIST_GRACE_PERIOD]
        await self._update_orders_with_error_handler(
            orders=unidentified, error_handler=self._handle_update_error_for_active_order)

    def _order_state(self, payload: Dict[str, Any], working: Optional[bool], current: OrderState) -> OrderState:
        status = str(payload.get("status") or "").upper()
        executed = utils.to_decimal(payload.get("exec_quantity"))
        quantity = utils.to_decimal(payload.get("quantity"))
        if status == CONSTANTS.STATUS_PARTIALLY_EXECUTED:
            if working:
                return OrderState.PARTIALLY_FILLED
            return OrderState.FILLED if quantity > 0 and executed >= quantity else OrderState.CANCELED
        state = CONSTANTS.ORDER_STATE.get(status)
        if state is None:
            self.logger().debug(f"Unknown CoinSwitch order status {status!r}; keeping {current.name}.")
            return current
        if state is OrderState.OPEN and executed > 0:
            return OrderState.PARTIALLY_FILLED
        return state

    def _order_update_from_payload(self, payload: Dict[str, Any], tracked_order: InFlightOrder,
                                   working: Optional[bool]) -> OrderUpdate:
        updated = utils.to_decimal(payload.get("updated_at") or payload.get("created_at"))
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(updated) * 1e-3 if updated > 0 else self._time_synchronizer.time(),
            new_state=self._order_state(payload, working, tracked_order.current_state),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(payload.get("order_id") or tracked_order.exchange_order_id or ""),
        )

    @staticmethod
    def _booked_fee(order: InFlightOrder) -> Decimal:
        return sum((fee.amount
                    for fill in order.order_fills.values()
                    for fee in fill.fee.flat_fees
                    if fee.token == CONSTANTS.COLLATERAL_TOKEN), s_decimal_0)

    def _trade_update_from_payload(self, order: InFlightOrder, payload: Dict[str, Any]) -> Optional[TradeUpdate]:
        """
        The fill since the last one booked, from the order's cumulative fields.
        The trade id is unique to the cumulative quantity, so seeing the same
        payload twice books nothing. The cancel response names the fields
        ``avg_exec_price`` / ``exec_fee``; both spellings are read.
        """
        executed = utils.to_decimal(payload.get("exec_quantity"))
        if executed <= order.executed_amount_base:
            return None
        fill_base = executed - order.executed_amount_base

        average_price = utils.to_decimal(payload.get("avg_execution_price", payload.get("avg_exec_price")))
        if average_price <= 0:
            if order.price is None or order.price.is_nan() or order.price <= 0:
                self.logger().warning(
                    f"Order {order.client_order_id} reports {executed} executed but no average price; "
                    f"the fill is booked when the price arrives.")
                return None
            average_price = order.price
        fill_quote = executed * average_price - order.executed_amount_quote
        if fill_quote <= 0:
            fill_quote = fill_base * average_price

        fee_field = payload.get("execution_fee", payload.get("exec_fee"))
        if fee_field in (None, ""):
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=order.position,
                percent=self._fee_rate(order.trading_pair, is_maker=False),
                percent_token=CONSTANTS.COLLATERAL_TOKEN,
            )
        else:
            fill_fee = utils.to_decimal(fee_field) - self._booked_fee(order)
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=order.position,
                percent_token=CONSTANTS.COLLATERAL_TOKEN,
                flat_fees=[TokenAmount(amount=fill_fee, token=CONSTANTS.COLLATERAL_TOKEN)] if fill_fee > 0 else [],
            )

        exchange_order_id = str(payload.get("order_id") or order.exchange_order_id)
        updated = utils.to_decimal(payload.get("updated_at") or payload.get("created_at"))
        return TradeUpdate(
            trade_id=f"{exchange_order_id}-{executed.normalize():f}",
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=fill_base,
            fill_quote_amount=fill_quote,
            fill_price=fill_quote / fill_base,
            fill_timestamp=float(updated) * 1e-3 if updated > 0 else self._time_synchronizer.time(),
        )

    # ---- balances ------------------------------------------------------------

    async def _request_wallet_balance(self) -> Any:
        return await self._api_get(
            path_url=CONSTANTS.WALLET_BALANCE_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.WALLET_BALANCE_PATH_URL,
        )

    async def _update_balances(self):
        self._apply_wallet_balance(await self._request_wallet_balance())

    def _apply_wallet_balance(self, response: Any):
        """
        ``base_asset_balances`` holds one row per settlement asset (USDT today):
        ``total_balance`` and ``total_available_balance``. A response without that
        list keeps the last known balances instead of zeroing them.
        """
        data = utils.unwrap_data(response)
        rows = data.get("base_asset_balances") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise IOError(f"Unexpected CoinSwitch futures wallet response: {response}")
        reported: Set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("base_asset") or "").upper()
            if not asset:
                continue
            balances = row.get("balances") or {}
            self._account_balances[asset] = utils.to_decimal(balances.get("total_balance"))
            self._account_available_balances[asset] = utils.to_decimal(balances.get("total_available_balance"))
            reported.add(asset)
        for asset in set(self._account_balances.keys()) - reported:
            self._account_balances.pop(asset, None)
            self._account_available_balances.pop(asset, None)

    # ---- positions -----------------------------------------------------------

    async def _request_positions(self, trading_pair: str) -> Any:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return await self._api_get(
            path_url=CONSTANTS.POSITIONS_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE, "symbol": symbol},
            is_auth_required=True,
            limit_id=CONSTANTS.POSITIONS_PATH_URL,
        )

    async def _update_positions(self):
        # The endpoint requires a symbol, so it is one call per configured pair.
        for trading_pair in self._trading_pairs:
            self._apply_positions(trading_pair, await self._request_positions(trading_pair))

    async def _trading_pair_for_symbol(self, symbol: Any) -> Optional[str]:
        if not symbol:
            return None
        try:
            return await self.trading_pair_associated_to_exchange_symbol(symbol=str(symbol).upper())
        except KeyError:
            return None

    def _apply_positions(self, trading_pair: str, response: Any):
        """
        Open positions of one pair. ``position_size`` is unsigned with the
        direction in ``position_side``; a closed position is simply absent, so
        anything not reported is removed. A snapshot with both ``mark_price`` and
        ``unrealised_pnl`` at 0 is mid-update: the size and entry price are kept,
        the previous PnL too.
        """
        # An error body ({"message": ...}) has no "data" and must not read as a position.
        records = response.get("data") if isinstance(response, dict) and "data" in response else None
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            raise IOError(f"Unexpected CoinSwitch positions response for {trading_pair}: {response}")

        symbol = utils.hb_pair_to_symbol(trading_pair)
        reported: Set[str] = set()
        for record in records:
            if not isinstance(record, dict):
                continue
            if record.get("symbol") and str(record["symbol"]).upper() != symbol:
                continue
            side = (PositionSide.SHORT if str(record.get("position_side", "")).upper() == CONSTANTS.POSITION_SHORT
                    else PositionSide.LONG)
            pos_key = self._perpetual_trading.position_key(trading_pair, side)
            size = abs(utils.to_decimal(record.get("position_size")))
            if str(record.get("status") or "OPEN").upper() != "OPEN" or size <= 0:
                continue
            mark_price = utils.to_decimal(record.get("mark_price"))
            unrealized_pnl = utils.to_decimal(record.get("unrealised_pnl"))
            if mark_price == 0 and unrealized_pnl == 0:
                previous = self._perpetual_trading.account_positions.get(pos_key)
                unrealized_pnl = previous.unrealized_pnl if previous is not None else s_decimal_0
            self._perpetual_trading.set_position(pos_key, Position(
                trading_pair=trading_pair,
                position_side=side,
                unrealized_pnl=unrealized_pnl,
                entry_price=utils.to_decimal(record.get("avg_entry_price")),
                amount=size if side is PositionSide.LONG else -size,
                leverage=utils.to_decimal(record.get("leverage"), Decimal("1")),
            ))
            reported.add(pos_key)

        for pos_key, position in list(self._perpetual_trading.account_positions.items()):
            if position.trading_pair == trading_pair and pos_key not in reported:
                self._perpetual_trading.remove_position(pos_key)

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is PositionMode.ONEWAY:
            return True, ""
        return False, "CoinSwitch futures holds one position per symbol; only ONEWAY mode is supported."

    async def _request_leverage(self, trading_pair: str) -> Optional[int]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.LEVERAGE_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE, "symbol": symbol},
            is_auth_required=True,
            limit_id=CONSTANTS.GET_LEVERAGE_LIMIT_ID,
        )
        data = utils.unwrap_data(response)
        leverage = utils.to_decimal(data.get("leverage")) if isinstance(data, dict) else s_decimal_0
        return int(leverage) if leverage > 0 else None

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Leverage is a per-symbol account setting. The current value is read first:
        re-applying the same leverage is a no-op, which matters because any change
        is refused while the symbol has an open position or open orders.
        """
        minimum = self.min_leverage(trading_pair)
        maximum = self.max_leverage(trading_pair)
        if leverage < minimum or (maximum is not None and leverage > maximum):
            return False, f"{trading_pair} allows leverage from {minimum}x to {maximum}x, got {leverage}x."
        try:
            if await self._request_leverage(trading_pair) == leverage:
                return True, ""
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().debug(f"Could not read the current leverage of {trading_pair}: {exception}")

        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        try:
            response = await self._api_post(
                path_url=CONSTANTS.LEVERAGE_PATH_URL,
                data={"exchange": CONSTANTS.EXCHANGE, "symbol": symbol, "leverage": int(leverage)},
                is_auth_required=True,
                limit_id=CONSTANTS.SET_LEVERAGE_LIMIT_ID,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            return False, (f"CoinSwitch refused {leverage}x on {trading_pair} ({exception}). Leverage cannot "
                           f"change while the symbol has an open position or open orders.")
        data = utils.unwrap_data(response)
        applied = utils.to_decimal(data.get("leverage")) if isinstance(data, dict) else s_decimal_0
        if applied > 0 and int(applied) != leverage:
            return False, f"CoinSwitch reports {applied}x on {trading_pair} after asking for {leverage}x."
        return True, ""

    # ---- funding -------------------------------------------------------------

    async def _request_ticker(self, trading_pair: str) -> Dict[str, Any]:
        """Get Ticker answers ``{"data": {"EXCHANGE_2": {...ticker...}}}``."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE, "symbol": symbol},
            is_auth_required=True,
            limit_id=CONSTANTS.TICKER_PATH_URL,
        )
        data = utils.unwrap_data(response)
        if isinstance(data, dict):
            for key in (CONSTANTS.EXCHANGE, symbol):
                if isinstance(data.get(key), dict):
                    return data[key]
            if "last_price" in data or "mark_price" in data:
                return data
        raise IOError(f"Unexpected CoinSwitch futures ticker for {trading_pair}: {response}")

    async def build_funding_info(self, trading_pair: str) -> FundingInfo:
        ticker = await self._request_ticker(trading_pair)
        mark_price = utils.to_decimal(ticker.get("mark_price"))
        if mark_price <= 0:
            # A zero mark price would silently corrupt PnL and funding maths.
            raise ValueError(f"CoinSwitch reported no mark price for {trading_pair}: {ticker}")
        next_funding = int(utils.to_decimal(ticker.get("next_funding_timestamp")) * Decimal("1e-3"))
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=utils.to_decimal(ticker.get("index_price"), mark_price),
            mark_price=mark_price,
            next_funding_utc_timestamp=next_funding or self._next_funding_boundary(),
            rate=utils.to_decimal(ticker.get("funding_rate")),
        )

    def _next_funding_boundary(self) -> int:
        interval = CONSTANTS.DEFAULT_FUNDING_INTERVAL_HOURS * 3600
        now = int(self._time_synchronizer.time() or time.time())
        return ((now // interval) + 1) * interval

    @staticmethod
    def _transaction_time(row: Dict[str, Any]) -> float:
        for key in ("created_at", "timestamp", "time", "updated_at", "transaction_time"):
            value = utils.to_decimal(row.get(key))
            if value > 0:
                return float(value) * 1e-3
        return 0.0

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Latest funding payment as (timestamp, rate, amount); ``amount`` is negative
        when funding was paid.

        The documented FUNDING_FEE rows carry no timestamp and no rate. A row
        that has a time field is used as is; otherwise new payments are spotted by
        transaction id: the first call only records the ids already there, and a
        later call that sees new ids reports their sum, stamped with the current
        time. The rate is the current one from the funding info (as Binance and
        CoinDCX do).
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.TRANSACTIONS_PATH_URL,
            params={
                "exchange": CONSTANTS.EXCHANGE,
                "symbol": symbol,
                "type": CONSTANTS.TRANSACTION_FILTER_FUNDING_FEE,
                "limit": CONSTANTS.FUNDING_TRANSACTIONS_LIMIT,
            },
            is_auth_required=True,
            limit_id=CONSTANTS.TRANSACTIONS_PATH_URL,
        )
        rows = utils.unwrap_data(response)
        rows = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        rows = [row for row in rows
                if str(row.get("type", "")).upper().replace(" ", "_") == CONSTANTS.TRANSACTION_TYPE_FUNDING_FEE
                and (not row.get("symbol") or str(row["symbol"]).upper() == symbol)]
        no_payment = (0, Decimal("-1"), Decimal("-1"))

        timed = [row for row in rows if self._transaction_time(row) > 0]
        if timed:
            latest = max(timed, key=self._transaction_time)
            return (self._transaction_time(latest), await self._current_funding_rate(trading_pair),
                    utils.to_decimal(latest.get("amount")))

        ids = {str(row.get("transaction_id")) for row in rows if row.get("transaction_id")}
        seen = self._funding_transactions_seen.get(trading_pair)
        if seen is None:
            self._funding_transactions_seen[trading_pair] = ids
            return self._last_funding_payment.get(trading_pair, no_payment)
        new_rows = [row for row in rows if row.get("transaction_id") and str(row["transaction_id"]) not in seen]
        seen.update(ids)
        if new_rows:
            amount = sum((utils.to_decimal(row.get("amount")) for row in new_rows), s_decimal_0)
            self._last_funding_payment[trading_pair] = (
                float(self._time_synchronizer.time()), await self._current_funding_rate(trading_pair), amount)
        return self._last_funding_payment.get(trading_pair, no_payment)

    async def _current_funding_rate(self, trading_pair: str) -> Decimal:
        funding_info = self._perpetual_trading.funding_info.get(trading_pair)
        if funding_info is not None:
            return funding_info.rate
        try:
            return (await self.build_funding_info(trading_pair)).rate
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().debug(f"Could not read the funding rate for {trading_pair}: {exception}")
            return Decimal("-1")

    # ---- prices --------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        ticker = await self._request_ticker(trading_pair)
        last = ticker.get("last_price")
        if last in (None, ""):
            raise ValueError(f"No last price for {trading_pair} in the CoinSwitch futures ticker: {ticker}")
        return float(last)

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """One all-pairs ticker call instead of one call per pair."""
        wanted = set(trading_pairs)
        results: Dict[str, float] = {}
        for ticker in await self.get_all_pairs_prices():
            trading_pair = await self._trading_pair_for_symbol(ticker.get("symbol"))
            if trading_pair in wanted and ticker.get("last_price") not in (None, ""):
                results[trading_pair] = float(ticker["last_price"])
        return results

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        """The 24h ticker of every contract, each with its ``symbol`` (rate and volume oracles)."""
        response = await self._api_get(
            path_url=CONSTANTS.ALL_PAIRS_TICKER_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE},
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_PAIRS_TICKER_PATH_URL,
        )
        data = utils.unwrap_data(response)
        if isinstance(data, dict):
            return [{**ticker, "symbol": ticker.get("symbol") or symbol}
                    for symbol, ticker in data.items() if isinstance(ticker, dict)]
        if isinstance(data, list):
            return [ticker for ticker in data if isinstance(ticker, dict)]
        return []

    # ---- user stream ---------------------------------------------------------

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                channel = event_message.get("channel") if isinstance(event_message, dict) else None
                if channel == BALANCE_CHANNEL:
                    self._apply_wallet_balance(event_message.get("data"))
                elif channel == POSITIONS_CHANNEL:
                    self._apply_positions(event_message.get("trading_pair"), event_message.get("data"))
                elif channel == ORDERS_CHANNEL:
                    self._process_order_snapshots([
                        (entry.get("order"), entry.get("working"))
                        for entry in event_message.get("data") or []
                        if isinstance(entry, dict) and isinstance(entry.get("order"), dict)
                    ])
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(1.0)
