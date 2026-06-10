import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.delta_perpetual import (
    delta_perpetual_constants as CONSTANTS,
    delta_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_api_order_book_data_source import (
    DeltaPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_api_user_stream_data_source import (
    DeltaPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_auth import DeltaPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    pass


def _result(response: Any) -> Any:
    """
    Delta wraps payloads as {"success": true, "result": …, "meta": …}.
    Raise on an explicit failure envelope; otherwise return `result` (or the
    response itself if there is no wrapper).
    """
    if isinstance(response, dict):
        if response.get("success") is False:
            err = response.get("error") or response.get("message") or response
            raise IOError(f"Delta API error: {err}")
        if "result" in response:
            return response["result"]
    return response


class DeltaPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        delta_perpetual_api_key: str,
        delta_perpetual_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = delta_perpetual_api_key
        self.secret_key = delta_perpetual_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._position_mode = None
        # Delta orders reference an integer product_id and a contract size.
        self._product_id_by_symbol: Dict[str, int] = {}
        self._contract_value_by_symbol: Dict[str, Decimal] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def authenticator(self):
        return DeltaPerpetualAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def name(self) -> str:
        return "delta_perpetual"

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
        return CONSTANTS.PRODUCTS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.PRODUCTS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self) -> List[PositionMode]:
        # Delta perpetuals operate in one-way (single) position mode.
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].sell_order_collateral_token

    # ── Factory methods ────────────────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return DeltaPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return DeltaPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ── Exception classification ───────────────────────────────────────────────

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        msg = str(request_exception).lower()
        return "expired_signature" in msg or "signature expired" in msg or "timestamp" in msg

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        msg = str(status_update_exception).lower()
        return "not found" in msg or "open_order_not_found" in msg or "404" in msg

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        msg = str(cancelation_exception).lower()
        return "not found" in msg or "open_order_not_found" in msg or "404" in msg

    # ── Contract-size helpers ──────────────────────────────────────────────────

    def _format_amount_to_size(self, trading_pair: str, amount: Decimal) -> Decimal:
        """Convert a base-asset amount into integer Delta contracts."""
        contract_value = Decimal(self._trading_rules[trading_pair].min_base_amount_increment)
        if contract_value <= 0:
            return amount
        return amount / contract_value

    def _format_size_to_amount(self, trading_pair: str, size: Decimal) -> Decimal:
        """Convert a Delta contract size back into a base-asset amount."""
        contract_value = Decimal(self._trading_rules[trading_pair].min_base_amount_increment)
        return size * contract_value

    async def _product_id_for_pair(self, trading_pair: str) -> Optional[int]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return self._product_id_by_symbol.get(symbol)

    # ── Trading-pair initialisation ────────────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        self._product_id_by_symbol = {}
        self._contract_value_by_symbol = {}
        for product in _result(exchange_info) or []:
            try:
                if not isinstance(product, dict):
                    continue
                if product.get("contract_type") != CONSTANTS.PERPETUAL_CONTRACT_TYPE:
                    continue
                if product.get("state") not in (None, "live"):
                    continue
                symbol = product.get("symbol")
                base = (product.get("underlying_asset") or {}).get("symbol")
                quote = (product.get("quoting_asset") or {}).get("symbol")
                if not symbol or not base or not quote:
                    continue
                hb_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
                mapping[symbol] = hb_pair
                self._product_id_by_symbol[symbol] = int(product.get("id"))
                self._contract_value_by_symbol[symbol] = Decimal(str(product.get("contract_value", "1")))
            except Exception as exc:
                self.logger().debug(f"Error parsing Delta product '{product}': {exc}")
        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(
            path_url=CONSTANTS.PRODUCTS_PATH_URL,
            params={"contract_types": CONSTANTS.PERPETUAL_CONTRACT_TYPE},
            is_auth_required=False,
        )

    async def _make_trading_rules_request(self) -> Any:
        return await self._make_trading_pairs_request()

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for product in _result(exchange_info) or []:
            try:
                if not isinstance(product, dict):
                    continue
                if product.get("contract_type") != CONSTANTS.PERPETUAL_CONTRACT_TYPE:
                    continue
                if product.get("state") not in (None, "live"):
                    continue
                base = (product.get("underlying_asset") or {}).get("symbol")
                quote = (product.get("quoting_asset") or {}).get("symbol")
                settling = (product.get("settling_asset") or {}).get("symbol") or quote
                if not base or not quote:
                    continue
                trading_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())

                contract_value = Decimal(str(product.get("contract_value", "1")))
                tick_size = Decimal(str(product.get("tick_size", "0.5")))

                rules.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=contract_value,
                        min_price_increment=tick_size,
                        min_base_amount_increment=contract_value,
                        min_notional_size=Decimal("0"),
                        buy_order_collateral_token=settling.upper(),
                        sell_order_collateral_token=settling.upper(),
                    )
                )
            except Exception as exc:
                self.logger().debug(f"Error parsing Delta trading rule for '{product}': {exc}")
        return rules

    # ── Pricing ────────────────────────────────────────────────────────────────

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        response = await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL, is_auth_required=False)
        result = _result(response)
        return result if isinstance(result, list) else []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return tickers
        requested = {tp.replace("-", "").upper() for tp in trading_pairs}
        return [t for t in tickers if str(t.get("symbol", "")).upper() in requested]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair.replace("-", "")
        try:
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL.format(symbol=symbol),
                is_auth_required=False,
                limit_id=CONSTANTS.TICKER_PATH_URL,
            )
            data = _result(response)
            if isinstance(data, dict):
                return float(data.get("close") or data.get("mark_price") or data.get("spot_price") or 0)
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
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        product_id = await self._product_id_for_pair(trading_pair)
        size = int(self._format_amount_to_size(trading_pair, amount))

        payload: Dict[str, Any] = {
            "product_id": product_id,
            "size": abs(size),
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "order_type": CONSTANTS.ORDER_TYPE_MARKET if order_type == OrderType.MARKET
            else CONSTANTS.ORDER_TYPE_LIMIT,
            "client_order_id": order_id,
            "reduce_only": position_action == PositionAction.CLOSE,
        }
        if order_type != OrderType.MARKET:
            payload["limit_price"] = str(price)
            payload["time_in_force"] = CONSTANTS.TIME_IN_FORCE_GTC
        if order_type == OrderType.LIMIT_MAKER:
            payload["post_only"] = True

        response = await self._api_post(
            path_url=CONSTANTS.ORDERS_PATH_URL,
            data=payload,
            is_auth_required=True,
        )
        order = _result(response)
        if isinstance(order, list) and order:
            order = order[0]
        exchange_order_id = str(order.get("id", "")) if isinstance(order, dict) else ""
        if not exchange_order_id:
            raise IOError(f"Delta did not return an order id: {response}")
        ts_raw = order.get("created_at", 0) if isinstance(order, dict) else 0
        transact_time = self._parse_delta_timestamp(ts_raw)
        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        product_id = await self._product_id_for_pair(tracked_order.trading_pair)
        payload = {"id": int(tracked_order.exchange_order_id), "product_id": product_id}
        response = await self._api_delete(
            path_url=CONSTANTS.ORDERS_PATH_URL,
            data=payload,
            is_auth_required=True,
        )
        order = _result(response)
        if isinstance(order, dict):
            state = str(order.get("state", "")).lower()
            return state in ("cancelled", "canceled") or order.get("id") is not None
        return True

    # ── Order & trade status ───────────────────────────────────────────────────

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_BY_ID_PATH_URL.format(order_id=tracked_order.exchange_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
        )
        order = _result(response)
        if isinstance(order, list) and order:
            order = order[0]
        if not isinstance(order, dict):
            raise ValueError(f"Unexpected Delta order status response: {response}")

        state_str = str(order.get("state", "")).lower()
        new_state = CONSTANTS.ORDER_STATE.get(state_str)
        if new_state is None:
            raise ValueError(f"Unknown Delta order state '{state_str}' for {tracked_order.exchange_order_id}")

        # A 'closed' order is fully filled only if there is no unfilled size.
        unfilled = order.get("unfilled_size")
        if new_state == OrderState.OPEN and unfilled is not None and float(unfilled) < float(order.get("size", 0)):
            new_state = OrderState.PARTIALLY_FILLED

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._parse_delta_timestamp(order.get("updated_at") or order.get("created_at") or 0),
            new_state=new_state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # Fills are delivered via the private WebSocket (v2/user_trades) and turned
        # into TradeUpdates in the user-stream listener; the periodic poll has no
        # dedicated per-order fills endpoint, so nothing to fetch here.
        return []

    # ── Balance ────────────────────────────────────────────────────────────────

    async def _update_balances(self) -> None:
        local_assets = set(self._account_balances.keys())
        remote_assets: set = set()
        try:
            response = await self._api_get(path_url=CONSTANTS.WALLET_PATH_URL, is_auth_required=True)
            for entry in _result(response) or []:
                if not isinstance(entry, dict):
                    continue
                asset = str(entry.get("asset_symbol") or (entry.get("asset") or {}).get("symbol") or "").upper()
                if not asset:
                    continue
                free = Decimal(str(entry.get("available_balance", "0")))
                total = Decimal(str(entry.get("balance", "0")))
                self._account_balances[asset] = total
                self._account_available_balances[asset] = free
                remote_assets.add(asset)
            for stale in local_assets - remote_assets:
                del self._account_balances[stale]
                del self._account_available_balances[stale]
        except Exception as exc:
            self.logger().error(f"Error updating Delta balances: {exc}", exc_info=True)

    # ── Positions / leverage / funding ─────────────────────────────────────────

    async def _update_positions(self):
        response = await self._api_get(path_url=CONSTANTS.POSITIONS_PATH_URL, is_auth_required=True)
        for position in _result(response) or []:
            if not isinstance(position, dict):
                continue
            ex_symbol = position.get("product_symbol")
            try:
                hb_pair = await self.trading_pair_associated_to_exchange_symbol(ex_symbol)
            except KeyError:
                continue

            size = Decimal(str(position.get("size", "0")))  # signed contracts
            position_side = PositionSide.LONG if size > 0 else PositionSide.SHORT
            pos_key = self._perpetual_trading.position_key(hb_pair, position_side)

            if size != 0:
                contract_value = self._contract_value_by_symbol.get(ex_symbol, Decimal("1"))
                amount = abs(size) * contract_value * (Decimal("1") if position_side == PositionSide.LONG else Decimal("-1"))
                unrealized_pnl = Decimal(str(position.get("unrealized_pnl", position.get("realized_pnl", "0")) or "0"))
                entry_price = Decimal(str(position.get("entry_price", "0") or "0"))
                leverage = Decimal(str(position.get("leverage", "1") or "1"))
                self._perpetual_trading.set_position(
                    pos_key,
                    Position(
                        trading_pair=hb_pair,
                        position_side=position_side,
                        unrealized_pnl=unrealized_pnl,
                        entry_price=entry_price,
                        amount=amount,
                        leverage=leverage,
                    ),
                )
            else:
                self._perpetual_trading.remove_position(pos_key)

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        # Delta perpetuals are one-way only; accept ONEWAY, reject HEDGE.
        if mode == PositionMode.ONEWAY:
            return True, ""
        return False, "Delta perpetuals only support ONEWAY position mode."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        product_id = await self._product_id_for_pair(trading_pair)
        try:
            response = await self._api_post(
                path_url=CONSTANTS.SET_LEVERAGE_PATH_URL.format(product_id=product_id),
                data={"leverage": str(leverage)},
                is_auth_required=True,
                limit_id=CONSTANTS.SET_LEVERAGE_PATH_URL,
            )
            _result(response)  # raises on failure envelope
            return True, ""
        except Exception as exc:
            return False, str(exc)

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        # Funding payments are not polled per-pair here; return the "no payment" sentinel.
        return 0, Decimal("-1"), Decimal("-1")

    async def _update_funding_payment(self, trading_pair: str, fire_event_on_new: bool) -> bool:
        return True

    # ── Fees ───────────────────────────────────────────────────────────────────

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self.estimate_fee_pct(is_maker),
        )

    async def _update_trading_fees(self):
        pass

    # ── User-stream event listener ─────────────────────────────────────────────

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                channel = event.get("type") or event.get("channel")

                if channel == CONSTANTS.WS_WALLET_CHANNEL:
                    # margins/wallet update
                    asset = str(event.get("asset_symbol") or "").upper()
                    if asset:
                        bal = event.get("balance")
                        avail = event.get("available_balance")
                        if bal is not None:
                            self._account_balances[asset] = Decimal(str(bal))
                        if avail is not None:
                            self._account_available_balances[asset] = Decimal(str(avail))

                elif channel == CONSTANTS.WS_ORDERS_CHANNEL:
                    self._process_order_event(event)

                elif channel == CONSTANTS.WS_USER_TRADES_CHANNEL:
                    self._process_trade_event(event)

                elif channel == CONSTANTS.WS_POSITIONS_CHANNEL:
                    # Positions are also refreshed by the periodic _update_positions poll.
                    pass

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in Delta user-stream listener.", exc_info=True)
                await self._sleep(5.0)

    def _process_order_event(self, order_data: Dict[str, Any]):
        client_order_id = order_data.get("client_order_id")
        exchange_order_id = str(order_data.get("id", ""))
        state_str = str(order_data.get("state", "")).lower()

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

        new_state = CONSTANTS.ORDER_STATE.get(state_str)
        if new_state is None:
            return
        unfilled = order_data.get("unfilled_size")
        if new_state == OrderState.OPEN and unfilled is not None and float(unfilled) < float(order_data.get("size", 0)):
            new_state = OrderState.PARTIALLY_FILLED

        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked.trading_pair,
            update_timestamp=self._parse_delta_timestamp(order_data.get("updated_at") or order_data.get("created_at") or 0),
            new_state=new_state,
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    def _process_trade_event(self, fill: Dict[str, Any]):
        client_order_id = fill.get("client_order_id")
        exchange_order_id = str(fill.get("order_id", ""))
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

        ex_symbol = fill.get("product_symbol")
        contract_value = self._contract_value_by_symbol.get(ex_symbol, Decimal("1"))
        fill_size = Decimal(str(fill.get("size", "0")))
        fill_base = abs(fill_size) * contract_value
        fill_price = Decimal(str(fill.get("price", "0")))
        fee_token = str(fill.get("commission_asset") or tracked.quote_asset)
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=tracked.position if hasattr(tracked, "position") else PositionAction.NIL,
            flat_fees=[TokenAmount(amount=Decimal(str(fill.get("commission", "0"))), token=fee_token)],
        )
        self._order_tracker.process_trade_update(TradeUpdate(
            trade_id=str(fill.get("id", fill.get("fill_id", ""))),
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked.trading_pair,
            fee=fee,
            fill_base_amount=fill_base,
            fill_quote_amount=fill_base * fill_price,
            fill_price=fill_price,
            fill_timestamp=self._parse_delta_timestamp(fill.get("created_at") or 0),
        ))

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_delta_timestamp(raw) -> float:
        """Delta timestamps are microseconds since epoch; normalise to seconds."""
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if value > 1e15:        # microseconds
            return value / 1e6
        if value > 1e12:        # milliseconds
            return value / 1e3
        return value
