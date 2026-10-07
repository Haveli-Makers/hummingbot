import asyncio
import re
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import aiohttp
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.wazirx_perpetual import (
    wazirx_perpetual_constants as CONSTANTS,
    wazirx_perpetual_utils as utils,
    wazirx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_api_order_book_data_source import (
    WazirxPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_api_user_stream_data_source import (
    WazirxPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_auth import WazirxPerpetualAuth
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_decimal_0 = Decimal("0")

_ERROR_CODE_PATTERN = re.compile(r'"code"\s*:\s*(\d+)')


class WazirxPerpetualDerivative(PerpetualDerivativePyBase):
    """
    WazirX perpetual futures connector (FAPI).

    Exchange traits handled here:

    * **One margin wallet, two quote currencies.** Contracts are quoted in INR
      (``BTC-INR``) or USDT (``BTC-USDT``), but every contract margins from the
      INR futures wallet (``marginAsset: INR`` on all of them). Collateral for an
      order is its contract's quote currency, the unit the API reports margin,
      fees and PnL in. For USDT-quoted contracts the connector therefore also
      publishes a **derived** USDT balance: the INR wallet divided by the
      exchange's own fixed margin rate (``conversionRates.INR_MARGIN_USDT`` in
      exchangeInfo). It is the same money as the INR row, not extra funds, and
      it is only published when a USDT-quoted pair is configured.
    * **Leverage is per order**, not an account setting: it is sent with every
      opening order (1-150, capped by the contract's ``maxLeverage``). While a
      position is open an order must carry the position's leverage or it is
      rejected (3113), so the position's leverage wins over the configured one.
    * **One position per symbol** (ONEWAY). A ``CLOSE`` order is sent with the
      ``positionId``, which makes it reduce-only on the exchange: it can never
      flip or open a position, and ``side`` is taken from the position.
    * **requestId is the client order id.** WazirX accepts a client-chosen,
      alphanumeric ``requestId`` and echoes it as ``c`` on the order stream, so
      orders are matched by client id from the first frame; a create whose
      outcome is unknown (5xx, timeout) is looked up by it instead of failing.
    * **Cancel acknowledgements are not trusted** as terminal: a cancel moves the
      order to PENDING_CANCEL and the order stream or the REST poll settles it,
      so a fill racing the cancel is not lost.
    * LIMIT and MARKET only; there is no post-only flag.
    """

    web_utils = web_utils

    def __init__(
        self,
        wazirx_perpetual_api_key: str = None,
        wazirx_perpetual_api_secret: str = None,
        wazirx_perpetual_proxy_url: str = "",
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = wazirx_perpetual_api_key or ""
        self.secret_key = wazirx_perpetual_api_secret or ""
        self._proxy_url = wazirx_perpetual_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        # exchangeInfo records keyed by Hummingbot trading pair.
        self._symbol_info: Dict[str, Dict[str, Any]] = {}
        # "INR_MARGIN_USDT" -> 102: INR charged per 1 USDT of margin.
        self._conversion_rates: Dict[str, Decimal] = {}
        # Open position id per trading pair; a CLOSE order must name it.
        self._position_ids: Dict[str, int] = {}
        self._derived_balance_logged = False

        # ownTrade frames that arrived before their order's exchange id was
        # known, held as (exchange_order_id, payload, buffered_at).
        self._pending_trade_events: List[Tuple[str, Dict[str, Any], float]] = []
        self._pending_trade_events_task: Optional[asyncio.Task] = None

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---- identity / configuration -------------------------------------------

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> WazirxPerpetualAuth:
        return WazirxPerpetualAuth(
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
        # A cancel acknowledgement is not proof the order is gone (the spot API
        # answers with the pre-cancel state). Wait for the stream or the poll.
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
        if trading_rule is not None:
            return trading_rule.buy_order_collateral_token
        return split_hb_trading_pair(trading_pair)[1]

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule = self._trading_rules.get(trading_pair)
        if trading_rule is not None:
            return trading_rule.sell_order_collateral_token
        return split_hb_trading_pair(trading_pair)[1]

    # ---- factories -----------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> WazirxPerpetualAPIOrderBookDataSource:
        return WazirxPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return WazirxPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def stop_network(self):
        await super().stop_network()

        if self._pending_trade_events_task is not None:
            self._pending_trade_events_task.cancel()
            try:
                await self._pending_trade_events_task
            except (asyncio.CancelledError, Exception):
                pass
            self._pending_trade_events_task = None
        self._pending_trade_events.clear()

        # A proxied factory owns a dedicated aiohttp session; the default factory
        # is a shared singleton and must be left alone.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Failed closing the proxy connections factory.", exc_info=True)

    # ---- error classification -------------------------------------------------

    @staticmethod
    def _error_code(exception: Exception) -> Optional[int]:
        """WazirX error bodies are {"code": <int>, "message": ...}; pull the code out."""
        match = _ERROR_CODE_PATTERN.search(str(exception))
        return int(match.group(1)) if match else None

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return self._error_code(request_exception) == CONSTANTS.OUT_OF_RECV_WINDOW_ERROR_CODE

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._error_code(status_update_exception) in (
            CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE, CONSTANTS.UNKNOWN_REQUEST_ID_ERROR_CODE)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # 2194 ("cannot be cancelled in the current state") is deliberately NOT
        # treated as gone: it is also what an order still being submitted
        # returns. The status poll settles those.
        return self._error_code(cancelation_exception) in (
            CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE, CONSTANTS.UNKNOWN_REQUEST_ID_ERROR_CODE)

    @staticmethod
    def _is_outcome_unknown(exception: Exception) -> bool:
        """
        True when a create request may or may not have reached the matching
        engine: a timeout, a dropped connection or an HTTP 5xx. The docs ask for
        the order to be looked up by requestId rather than assumed failed.
        """
        if isinstance(exception, (asyncio.TimeoutError, ConnectionError, aiohttp.ClientConnectionError)):
            return True
        text = str(exception)
        if "HTTP status is 5" in text:
            return True
        return f'"code":{CONSTANTS.REQUEST_ID_USED_ERROR_CODE}' in text.replace(" ", "")

    # ---- exchange info / trading rules ---------------------------------------

    def _store_exchange_metadata(self, exchange_info: Dict[str, Any]):
        rates = exchange_info.get("conversionRates") if isinstance(exchange_info, dict) else None
        if isinstance(rates, dict):
            parsed = {key: utils.to_decimal(value) for key, value in rates.items()}
            self._conversion_rates = {key: value for key, value in parsed.items() if value > 0}
        for symbol_info in self._valid_symbols(exchange_info):
            trading_pair = combine_to_hb_trading_pair(symbol_info["baseAsset"], symbol_info["quoteAsset"])
            self._symbol_info[trading_pair] = symbol_info

    @staticmethod
    def _valid_symbols(exchange_info: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        symbols = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else []
        return filter(utils.is_exchange_information_valid, symbols)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        self._store_exchange_metadata(exchange_info)
        mapping = bidict()
        for symbol_info in self._valid_symbols(exchange_info):
            trading_pair = combine_to_hb_trading_pair(symbol_info["baseAsset"], symbol_info["quoteAsset"])
            if trading_pair in mapping.inverse:
                continue
            mapping[symbol_info["symbol"]] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
        self._store_exchange_metadata(exchange_info)
        rules: List[TradingRule] = []
        for symbol_info in self._valid_symbols(exchange_info):
            try:
                quote = symbol_info["quoteAsset"]
                trading_pair = combine_to_hb_trading_pair(symbol_info["baseAsset"], quote)
                qty_filter = utils.get_filter(symbol_info, "limit_qty_size") or {}
                notional_filter = utils.get_filter(symbol_info, "min_notional") or {}
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=utils.to_decimal(qty_filter.get("minQty")),
                    max_order_size=utils.to_decimal(qty_filter.get("maxQty")),
                    min_price_increment=utils.precision_to_increment(symbol_info.get("pricePrecision")),
                    min_base_amount_increment=utils.precision_to_increment(symbol_info.get("quantityPrecision")),
                    min_notional_size=utils.to_decimal(notional_filter.get("notional")),
                    # Margin, fees and PnL are reported in the quote currency.
                    buy_order_collateral_token=quote,
                    sell_order_collateral_token=quote,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {symbol_info}. Skipping.")
        return rules

    async def _update_trading_fees(self):
        """WazirX publishes no per-account fee endpoint; fills carry the actual commission."""
        pass

    def margin_conversion_rate(self, quote: str, margin: str = CONSTANTS.DEFAULT_MARGIN_ASSET) -> Optional[Decimal]:
        """Units of ``margin`` charged per unit of ``quote`` (INR per USDT), from exchangeInfo."""
        if quote == margin:
            return Decimal("1")
        return self._conversion_rates.get(f"{margin}_MARGIN_{quote}")

    def _margin_asset(self, trading_pair: str) -> str:
        symbol_info = self._symbol_info.get(trading_pair) or {}
        return str(symbol_info.get("marginAsset") or CONSTANTS.DEFAULT_MARGIN_ASSET).upper()

    def max_leverage(self, trading_pair: str) -> Optional[int]:
        symbol_info = self._symbol_info.get(trading_pair)
        if not symbol_info or symbol_info.get("maxLeverage") in (None, ""):
            return None
        return int(utils.to_decimal(symbol_info.get("maxLeverage")))

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
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self.estimate_fee_pct(is_maker),
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
        data: Dict[str, Any] = {
            "requestId": order_id,
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": CONSTANTS.ORDER_TYPE_MARKET if order_type is OrderType.MARKET else CONSTANTS.ORDER_TYPE_LIMIT,
            "quantity": f"{amount:f}",
        }
        if order_type is not OrderType.MARKET:
            data["price"] = f"{price:f}"

        if position_action is PositionAction.CLOSE:
            data["positionId"] = await self._position_id_to_close(trading_pair, trade_type)
        else:
            data["leverage"] = self._order_leverage(trading_pair)

        try:
            response = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.CREATE_ORDER_LIMIT_ID,
            )
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            if not self._is_outcome_unknown(request_error):
                raise
            self.logger().warning(
                f"Outcome of order {order_id} is unknown ({request_error}); looking it up by requestId.")
            response = await self._query_order_by_request_id(order_id)
            if response is None:
                raise

        if not isinstance(response, dict) or response.get("id") is None:
            raise IOError(f"Error submitting order {order_id} to WazirX: {response}")
        if str(response.get("status", "")).lower() == "reject":
            raise IOError(f"WazirX rejected order {order_id}: {response}")

        exchange_order_id = str(response["id"])
        created = response.get("createdTime")
        transact_time = float(created) * 1e-3 if created else self._time_synchronizer.time()
        return exchange_order_id, transact_time

    def _order_leverage(self, trading_pair: str) -> int:
        """
        Leverage for an order that opens or adds to a position. With a position
        already open WazirX only accepts that position's leverage (3113), so it
        overrides the configured value.
        """
        configured = int(self.get_leverage(trading_pair) or 1)
        position = self._perpetual_trading.get_position(trading_pair)
        if position is not None and position.leverage:
            position_leverage = int(position.leverage)
            if position_leverage != configured:
                self.logger().info(
                    f"{trading_pair} has an open position at {position_leverage}x; using that instead of "
                    f"the configured {configured}x (WazirX rejects a leverage change while it is open).")
            return position_leverage
        return configured

    async def _position_id_to_close(self, trading_pair: str, trade_type: TradeType) -> int:
        """
        The id of the open position a CLOSE order acts on. Sending it makes the
        order reduce-only: WazirX takes the side from the position, so the order
        side Hummingbot tracks must be the one that actually reduces it.
        """
        position = self._perpetual_trading.get_position(trading_pair)
        position_id = self._position_ids.get(trading_pair)
        if position is None or position_id is None:
            await self._update_positions_for_symbol(trading_pair)
            position = self._perpetual_trading.get_position(trading_pair)
            position_id = self._position_ids.get(trading_pair)
        if position is None or position_id is None:
            raise ValueError(f"Cannot close {trading_pair}: WazirX reports no open position.")

        reducing_side = TradeType.SELL if position.position_side is PositionSide.LONG else TradeType.BUY
        if trade_type is not reducing_side:
            raise ValueError(
                f"Cannot close the {position.position_side.name} {trading_pair} position with a "
                f"{trade_type.name} order; WazirX would {reducing_side.name} instead.")
        return position_id

    async def _query_order_by_request_id(self, request_id: str) -> Optional[Dict[str, Any]]:
        try:
            order = await self._api_get(
                path_url=CONSTANTS.ORDER_PATH_URL,
                params={"requestId": request_id},
                is_auth_required=True,
                limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            if self._error_code(exception) in (CONSTANTS.UNKNOWN_REQUEST_ID_ERROR_CODE,
                                               CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE):
                return None
            raise
        return order if isinstance(order, dict) and order.get("id") is not None else None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        params: Dict[str, Any] = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair),
        }
        if tracked_order.exchange_order_id:
            params["orderId"] = tracked_order.exchange_order_id
        else:
            params["requestId"] = order_id

        response = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=params,
            is_auth_required=True,
            limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID,
        )
        if isinstance(response, dict) and response.get("id") is not None:
            return True
        raise IOError(f"Unexpected response cancelling order {order_id}: {response}")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id:
            params = {"orderId": tracked_order.exchange_order_id}
        else:
            # The requestId IS the client order id, so an order whose create
            # response never arrived can still be found.
            params = {"requestId": tracked_order.client_order_id}
        order = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID,
        )
        return self._order_update_from_rest(order, tracked_order)

    @staticmethod
    def _order_state(status: Any, executed_quantity: Any, default: OrderState) -> OrderState:
        state = CONSTANTS.ORDER_STATE.get(str(status or "").lower(), default)
        if state is OrderState.OPEN and utils.to_decimal(executed_quantity) > 0:
            return OrderState.PARTIALLY_FILLED
        return state

    def _order_update_from_rest(self, order: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        updated = order.get("updatedTime") or order.get("createdTime")
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(updated) * 1e-3 if updated else self._time_synchronizer.time(),
            new_state=self._order_state(order.get("status"), order.get("executedQty"), tracked_order.current_state),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order.get("id", tracked_order.exchange_order_id or "")),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        fills = await self._api_get(
            path_url=CONSTANTS.USER_TRADES_PATH_URL,
            params={"symbol": symbol, "orderId": order.exchange_order_id, "limit": 1000},
            is_auth_required=True,
            limit_id=CONSTANTS.USER_TRADES_PATH_URL,
        )
        trade_updates: List[TradeUpdate] = []
        for fill in fills if isinstance(fills, list) else []:
            if str(fill.get("orderId")) != str(order.exchange_order_id):
                continue
            trade_updates.append(self._trade_update(
                order=order,
                trade_id=fill.get("id"),
                price=fill.get("price"),
                quantity=fill.get("qty"),
                fee_amount=fill.get("commission"),
                fee_asset=fill.get("commissionAsset"),
                timestamp_ms=fill.get("createdTime"),
            ))
        return trade_updates

    def _trade_update(self, order: InFlightOrder, trade_id: Any, price: Any, quantity: Any,
                      fee_amount: Any, fee_asset: Any, timestamp_ms: Any) -> TradeUpdate:
        fill_price = utils.to_decimal(price)
        fill_amount = utils.to_decimal(quantity)
        fee_value = utils.to_decimal(fee_amount)
        fee_token = str(fee_asset or split_hb_trading_pair(order.trading_pair)[1]).upper()
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=order.position,
            percent_token=fee_token,
            flat_fees=[TokenAmount(amount=fee_value, token=fee_token)] if fee_value else [],
        )
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=fill_amount,
            fill_quote_amount=fill_amount * fill_price,
            fill_price=fill_price,
            fill_timestamp=float(timestamp_ms) * 1e-3 if timestamp_ms else self._time_synchronizer.time(),
        )

    # ---- balances ------------------------------------------------------------

    async def _update_balances(self):
        response = await self._api_get(
            path_url=CONSTANTS.FUNDS_PATH_URL,
            params={"wallets": CONSTANTS.FUTURES_WALLET},
            is_auth_required=True,
            limit_id=CONSTANTS.FUNDS_PATH_URL,
        )
        rows = response.get(CONSTANTS.FUTURES_WALLET) if isinstance(response, dict) else None
        if not isinstance(rows, list):
            raise IOError(
                "WazirX returned no futures wallet. Futures must be activated on the account "
                f"(error {CONSTANTS.FUTURES_NOT_ACTIVATED_ERROR_CODE}). Response: {response}")
        self._apply_wallet_rows(rows, full_snapshot=True)

    def _apply_wallet_rows(self, rows: List[Dict[str, Any]], full_snapshot: bool):
        """
        Publish wallet rows (REST ``asset/free/locked`` or stream ``a/b/l``).
        ``full_snapshot`` drops assets the exchange no longer reports; stream
        frames only carry the assets that changed, so they never drop anything.
        """
        native: Set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset", row.get("a", "")) or "").upper()
            if not asset:
                continue
            free = utils.to_decimal(row.get("free", row.get("b")))
            locked = utils.to_decimal(row.get("locked", row.get("l")))
            self._account_available_balances[asset] = free
            self._account_balances[asset] = free + locked
            native.add(asset)

        published = native | self._publish_derived_balances(native_assets=native, full_snapshot=full_snapshot)

        if full_snapshot:
            for asset in set(self._account_balances.keys()) - published:
                self._account_balances.pop(asset, None)
                self._account_available_balances.pop(asset, None)

    def _derived_collateral_assets(self) -> Dict[str, str]:
        """quote asset -> margin asset, for configured pairs whose quote is not the margin asset."""
        derived: Dict[str, str] = {}
        for trading_pair in self._trading_pairs:
            quote = split_hb_trading_pair(trading_pair)[1]
            margin = self._margin_asset(trading_pair)
            if quote != margin:
                derived[quote] = margin
        return derived

    def _publish_derived_balances(self, native_assets: Set[str], full_snapshot: bool) -> Set[str]:
        """
        Express the margin wallet in a USDT-quoted contract's own currency so the
        budget checker can size collateral for it. Skipped when the wallet holds
        that currency natively, and when the rate is unknown (better no figure
        than a wrong one).
        """
        published: Set[str] = set()
        for quote, margin in self._derived_collateral_assets().items():
            if quote in native_assets:
                continue
            rate = self.margin_conversion_rate(quote, margin)
            if rate is None or rate <= 0:
                # Before exchangeInfo has loaded there are no rates at all; only a
                # loaded table that lacks this pair's rate is worth a warning.
                if full_snapshot and self._conversion_rates:
                    self.logger().warning(
                        f"exchangeInfo has no {margin}_MARGIN_{quote} rate; no {quote} balance is published.")
                continue
            if margin not in self._account_balances:
                continue
            self._account_available_balances[quote] = self._account_available_balances.get(margin, s_decimal_0) / rate
            self._account_balances[quote] = self._account_balances[margin] / rate
            published.add(quote)
            if not self._derived_balance_logged:
                self._derived_balance_logged = True
                self.logger().info(
                    f"{quote}-quoted contracts margin from the {margin} wallet; publishing it as "
                    f"{self._account_balances[quote]} {quote} at WazirX's margin rate of {rate} {margin}/{quote}. "
                    f"This is the same money as the {margin} balance, not additional funds.")
        return published

    # ---- positions -----------------------------------------------------------

    async def _update_positions(self):
        records = await self._api_get(
            path_url=CONSTANTS.POSITION_RISK_PATH_URL,
            params={"limit": 1000},
            is_auth_required=True,
            limit_id=CONSTANTS.POSITION_RISK_PATH_URL,
        )
        reported: Set[str] = set()
        for record in records if isinstance(records, list) else []:
            if isinstance(record, dict):
                pos_key = await self._process_position_record(record)
                if pos_key is not None:
                    reported.add(pos_key)

        # positionRisk lists open positions only, so anything missing is flat. A
        # position closed outside this connector would otherwise stay forever.
        for pos_key, position in list(self._perpetual_trading.account_positions.items()):
            if pos_key not in reported:
                self._perpetual_trading.remove_position(pos_key)
                self._position_ids.pop(position.trading_pair, None)

    async def _update_positions_for_symbol(self, trading_pair: str):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        records = await self._api_get(
            path_url=CONSTANTS.POSITION_RISK_PATH_URL,
            params={"symbol": symbol},
            is_auth_required=True,
            limit_id=CONSTANTS.POSITION_RISK_PATH_URL,
        )
        for record in records if isinstance(records, list) else []:
            if isinstance(record, dict):
                await self._process_position_record(record)

    async def _trading_pair_for_symbol(self, symbol: Any) -> Optional[str]:
        if not symbol:
            return None
        try:
            return await self.trading_pair_associated_to_exchange_symbol(symbol=str(symbol).upper())
        except KeyError:
            return None

    async def _process_position_record(self, record: Dict[str, Any]) -> Optional[str]:
        """
        One positionRisk row. ``positionAmt`` is always positive with the
        direction in ``positionType``; there is no unrealised PnL field, so it is
        derived from the mark price. Returns the position key when recorded.
        """
        trading_pair = await self._trading_pair_for_symbol(record.get("symbol"))
        if trading_pair is None:
            return None
        size = abs(utils.to_decimal(record.get("positionAmt")))
        side = PositionSide.SHORT if str(record.get("positionType", "")).upper() == CONSTANTS.POSITION_SHORT \
            else PositionSide.LONG
        return self._set_position(
            trading_pair=trading_pair,
            side=side,
            size=size,
            entry_price=utils.to_decimal(record.get("entryPrice")),
            mark_price=utils.to_decimal(record.get("markPrice")),
            leverage=utils.to_decimal(record.get("leverage"), Decimal("1")),
            position_id=record.get("id"),
        )

    def _set_position(self, trading_pair: str, side: PositionSide, size: Decimal, entry_price: Decimal,
                      mark_price: Decimal, leverage: Decimal, position_id: Any) -> Optional[str]:
        pos_key = self._perpetual_trading.position_key(trading_pair, side)
        if size <= s_decimal_0:
            self._perpetual_trading.remove_position(pos_key)
            self._position_ids.pop(trading_pair, None)
            return None
        signed_amount = size if side is PositionSide.LONG else -size
        unrealized_pnl = (mark_price - entry_price) * signed_amount if mark_price > 0 else s_decimal_0
        self._perpetual_trading.set_position(pos_key, Position(
            trading_pair=trading_pair,
            position_side=side,
            unrealized_pnl=unrealized_pnl,
            entry_price=entry_price,
            amount=signed_amount,
            leverage=leverage,
        ))
        if position_id not in (None, ""):
            try:
                self._position_ids[trading_pair] = int(position_id)
            except (TypeError, ValueError):
                self.logger().debug(f"Ignoring non-numeric position id {position_id!r} for {trading_pair}.")
        return pos_key

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is PositionMode.ONEWAY:
            return True, ""
        return False, "WazirX futures holds one position per symbol; only ONEWAY mode is supported."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        WazirX has no set-leverage endpoint: leverage rides on each opening order.
        This validates the value and records it for the next order.
        """
        if leverage < 1:
            return False, f"Leverage must be at least 1, got {leverage}."
        maximum = self.max_leverage(trading_pair)
        if maximum is not None and leverage > maximum:
            return False, f"{trading_pair} allows at most {maximum}x leverage."
        position = self._perpetual_trading.get_position(trading_pair)
        if position is not None and position.leverage and int(position.leverage) != leverage:
            self.logger().warning(
                f"{trading_pair} has an open position at {int(position.leverage)}x; orders keep using that "
                f"until it is closed, then {leverage}x applies.")
        return True, ""

    # ---- funding -------------------------------------------------------------

    async def build_funding_info(self, trading_pair: str) -> FundingInfo:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        info = await self._api_get(
            path_url=CONSTANTS.MARK_PRICE_PATH_URL,
            params={"symbol": symbol},
            limit_id=CONSTANTS.MARK_PRICE_PATH_URL,
        )
        mark_price = utils.to_decimal(info.get("markPrice")) if isinstance(info, dict) else s_decimal_0
        if mark_price <= 0:
            # A zero mark price would silently corrupt PnL and funding maths.
            raise ValueError(f"WazirX reported no mark price for {trading_pair}: {info}")
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=utils.to_decimal(info.get("indexPrice"), mark_price),
            mark_price=mark_price,
            next_funding_utc_timestamp=int(int(info.get("nextFundingTime") or 0) * 1e-3)
            or self._next_funding_boundary(),
            rate=utils.to_decimal(info.get("lastFundingRate")),
        )

    def _next_funding_boundary(self) -> int:
        interval = CONSTANTS.DEFAULT_FUNDING_INTERVAL_HOURS * 3600
        now = int(self._time_synchronizer.time() or time.time())
        return ((now // interval) + 1) * interval

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Most recent funding payment as (timestamp, rate, amount), from the income
        history (newest first). ``amount`` is negative when funding was paid.
        The record carries no rate, so the current one is reported, as the
        Binance and CoinDCX connectors do. GST on funding is a separate income
        type and is not included.
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        records = await self._api_get(
            path_url=CONSTANTS.INCOME_PATH_URL,
            params={"symbol": symbol, "incomeType": CONSTANTS.INCOME_TYPE_FUNDING_FEE, "limit": 1},
            is_auth_required=True,
            limit_id=CONSTANTS.INCOME_PATH_URL,
        )
        records = [r for r in records if isinstance(r, dict)] if isinstance(records, list) else []
        if not records:
            return 0, Decimal("-1"), Decimal("-1")

        latest = max(records, key=lambda record: int(record.get("id") or 0))
        timestamp = float(latest.get("time") or latest.get("createdAt") or 0) * 1e-3
        payment = utils.to_decimal(latest.get("amount"))

        rate = Decimal("-1")
        funding_info = self._perpetual_trading.funding_info.get(trading_pair)
        if funding_info is not None:
            rate = funding_info.rate
        else:
            try:
                rate = (await self.build_funding_info(trading_pair)).rate
            except Exception as exception:
                self.logger().debug(f"Could not read the funding rate for {trading_pair}: {exception}")
        return timestamp, rate, payment

    # ---- prices --------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        ticker = await self._api_get(
            path_url=CONSTANTS.TICKER_24HR_PATH_URL,
            params={"symbol": symbol},
            limit_id=CONSTANTS.TICKER_24HR_PATH_URL,
        )
        last = ticker.get("lastPrice") if isinstance(ticker, dict) else None
        if last in (None, ""):
            raise ValueError(f"No last price for {trading_pair} in the WazirX futures ticker: {ticker}")
        return float(last)

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """One all-symbols ticker call instead of one rate-limited call per pair."""
        wanted = set(trading_pairs)
        results: Dict[str, float] = {}
        for ticker in await self.get_all_pairs_prices():
            trading_pair = await self._trading_pair_for_symbol(ticker.get("symbol"))
            if trading_pair in wanted and ticker.get("lastPrice") not in (None, ""):
                results[trading_pair] = float(ticker["lastPrice"])
        return results

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        """
        The 24h ticker for every contract (rate and volume oracles). ``volume`` is
        the QUOTE volume: live BTCINR reports ~1.2e12, which only makes sense in INR.
        """
        tickers = await self._api_get(
            path_url=CONSTANTS.TICKER_24HR_PATH_URL,
            limit_id=CONSTANTS.TICKER_24HR_PATH_URL,
        )
        return [ticker for ticker in tickers if isinstance(ticker, dict)] if isinstance(tickers, list) else []

    # ---- user stream ---------------------------------------------------------

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                stream = event_message.get("stream")
                payload = event_message.get("data")
                records = payload if isinstance(payload, list) else [payload]
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    if stream == CONSTANTS.ORDER_UPDATE_STREAM:
                        self._process_order_event(record)
                    elif stream == CONSTANTS.OWN_TRADE_STREAM:
                        self._process_trade_event(record)
                    elif stream == CONSTANTS.POSITION_UPDATE_STREAM:
                        await self._process_position_event(record)
                    elif stream == CONSTANTS.BALANCE_UPDATE_STREAM:
                        self._apply_wallet_rows(record.get("B") or [], full_snapshot=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)

    def _process_order_event(self, order: Dict[str, Any]):
        client_order_id = str(order.get("c") or "")
        exchange_order_id = str(order.get("i") or "")
        tracked_order = (self._order_tracker.all_updatable_orders.get(client_order_id)
                         or self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id))
        if tracked_order is None:
            # TP/SL legs, other sessions, manual orders.
            return

        # Record the exchange id NOW. process_order_update applies it only after
        # waiting for the fills of a "done" order, and those fills (ownTrade) can
        # only be matched by exchange id, so deferring it would stall both.
        if tracked_order.exchange_order_id is None and exchange_order_id:
            tracked_order.update_exchange_order_id(exchange_order_id)

        position_id = order.get("pi")
        if position_id not in (None, "", 0):
            try:
                self._position_ids[tracked_order.trading_pair] = int(position_id)
            except (TypeError, ValueError):
                pass

        event_time = order.get("E")
        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(event_time) * 1e-3 if event_time else self._time_synchronizer.time(),
            new_state=self._order_state(order.get("X"), order.get("z"), tracked_order.current_state),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id or tracked_order.exchange_order_id,
        ))

    def _process_trade_event(self, trade: Dict[str, Any]):
        exchange_order_id = str(trade.get("o") or "")
        if not exchange_order_id:
            return
        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is None:
            self._defer_trade_event(exchange_order_id, trade)
            return
        self._apply_trade_event(trade, tracked_order)

    def _apply_trade_event(self, trade: Dict[str, Any], tracked_order: InFlightOrder):
        self._order_tracker.process_trade_update(self._trade_update(
            order=tracked_order,
            trade_id=trade.get("i"),
            price=trade.get("p"),
            quantity=trade.get("q"),
            fee_amount=trade.get("f"),
            fee_asset=trade.get("fc"),
            timestamp_ms=trade.get("t") or trade.get("E"),
        ))

    def _defer_trade_event(self, exchange_order_id: str, trade: Dict[str, Any]):
        """
        Hold a fill whose order is not matchable yet. Only worth doing while some
        order still awaits its exchange id; anything else belongs to another
        session or a manual trade.
        """
        if not any(order.exchange_order_id is None
                   for order in self._order_tracker.all_updatable_orders.values()):
            self.logger().debug(f"Ignoring {CONSTANTS.OWN_TRADE_STREAM} for untracked order {exchange_order_id}.")
            return
        if len(self._pending_trade_events) >= CONSTANTS.MAX_PENDING_TRADE_EVENTS:
            dropped_id = self._pending_trade_events.pop(0)[0]
            self.logger().warning(
                f"Pending fill buffer is full; dropped the oldest fill for {dropped_id}. "
                f"The REST poll will reconcile it.")
        self._pending_trade_events.append((exchange_order_id, trade, time.time()))
        if self._pending_trade_events_task is None or self._pending_trade_events_task.done():
            self._pending_trade_events_task = safe_ensure_future(self._replay_pending_trade_events())

    async def _replay_pending_trade_events(self):
        try:
            while self._pending_trade_events:
                await self._sleep(CONSTANTS.PENDING_TRADE_EVENT_RETRY_INTERVAL)
                still_pending: List[Tuple[str, Dict[str, Any], float]] = []
                orders = self._order_tracker.all_fillable_orders_by_exchange_order_id
                for exchange_order_id, trade, buffered_at in self._pending_trade_events:
                    tracked_order = orders.get(exchange_order_id)
                    if tracked_order is not None:
                        self._apply_trade_event(trade, tracked_order)
                    elif time.time() - buffered_at < CONSTANTS.PENDING_TRADE_EVENT_TTL:
                        still_pending.append((exchange_order_id, trade, buffered_at))
                    else:
                        self.logger().debug(
                            f"Deferred fill for {exchange_order_id} expired unmatched; leaving it to the REST poll.")
                self._pending_trade_events = still_pending
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error replaying deferred fills.")

    async def _process_position_event(self, event: Dict[str, Any]):
        """
        positionUpdate: ``X`` is open / closed / liquidated, ``ps`` the side,
        ``q`` the remaining amount (``S`` the size), ``ep`` the entry price and
        ``l`` the leverage. It has no mark price, so unrealised PnL uses the
        streamed funding-info mark price when available.
        """
        trading_pair = await self._trading_pair_for_symbol(event.get("s"))
        if trading_pair is None:
            return
        side = PositionSide.SHORT if str(event.get("ps", "")).upper() == CONSTANTS.POSITION_SHORT \
            else PositionSide.LONG
        size = utils.to_decimal(event.get("q", event.get("S")))
        if str(event.get("X", "open")).lower() != "open":
            size = s_decimal_0
        entry_price = utils.to_decimal(event.get("ep"))
        funding_info = self._perpetual_trading.funding_info.get(trading_pair)
        mark_price = funding_info.mark_price if funding_info is not None else s_decimal_0
        self._set_position(
            trading_pair=trading_pair,
            side=side,
            size=abs(size),
            entry_price=entry_price,
            mark_price=mark_price,
            leverage=utils.to_decimal(event.get("l"), Decimal("1")),
            position_id=event.get("i"),
        )
