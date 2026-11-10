import asyncio
import hashlib
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.delta_options import (
    delta_options_constants as CONSTANTS,
    delta_options_utils as utils,
    delta_options_web_utils as web_utils,
)
from hummingbot.connector.derivative.delta_options.delta_options_api_order_book_data_source import (
    DeltaOptionsAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.delta_options.delta_options_api_user_stream_data_source import (
    DeltaOptionsAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.delta_options.delta_options_auth import DeltaOptionsAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.option_derivative_py_base import OptionDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.option_info import OptionInstrumentInfo
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _result(response: Any) -> Any:
    """
    Delta wraps payloads as {"success": true, "result": ...}; an explicit failure raises.
    """
    if isinstance(response, dict):
        if response.get("success") is False:
            raise IOError(f"Delta API error: {response.get('error') or response}")
        if "result" in response:
            return response["result"]
    return response


class DeltaOptionsDerivative(OptionDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        delta_options_api_key: str,
        delta_options_api_secret: str,
        delta_options_proxy_url: str = "",
        delta_options_allow_selling: bool = False,
        delta_options_max_short_contracts: int = 1,
        delta_options_dead_man_switch: bool = True,
        delta_options_stop_before_expiry_minutes: int = 30,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = delta_options_api_key
        self.secret_key = delta_options_api_secret
        self._proxy_url = delta_options_proxy_url or ""
        self._dead_man_switch = utils.as_bool(delta_options_dead_man_switch)
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        if len(self._trading_pairs) > CONSTANTS.WS_MAX_SYMBOLS:
            raise ValueError(f"delta_options streams at most {CONSTANTS.WS_MAX_SYMBOLS} options per connector.")
        self._contract_values: Dict[str, Decimal] = {}
        self._product_ids: Dict[str, int] = {}
        self._fee_rates: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
        # Delta's symbol for each option the connector has seen listed, kept for its own options after they expire.
        self._symbols: Dict[str, str] = {}
        # Fills announced on the private stream, per client order id, until REST has recorded them.
        self._ws_fill_counts: Dict[str, int] = defaultdict(int)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_id: Optional[str] = None
        self._stop_lock = asyncio.Lock()
        self._authenticator: Optional[DeltaOptionsAuth] = None
        super().__init__(
            balance_asset_limit,
            rate_limits_share_pct,
            allow_selling=utils.as_bool(delta_options_allow_selling),
            max_short_contracts=Decimal(str(delta_options_max_short_contracts)),
            stop_before_expiry=float(str(delta_options_stop_before_expiry_minutes)) * 60,
        )

    @property
    def authenticator(self) -> DeltaOptionsAuth:
        if self._authenticator is None:
            self._authenticator = DeltaOptionsAuth(
                api_key=self.api_key,
                secret_key=self.secret_key,
                time_provider=self._time_synchronizer,
            )
        return self._authenticator

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

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
        return CONSTANTS.PRODUCTS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PRODUCTS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PRODUCTS_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # A cancel only counts once Delta's reply shows the order cancelled (see _place_cancel).
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        # No market orders: option books are thin, so a market order can fill far from fair value.
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].sell_order_collateral_token

    def contract_size(self, trading_pair: str) -> Decimal:
        return self._contract_values[trading_pair]

    def size_to_amount(self, trading_pair: str, size: Decimal) -> Decimal:
        """
        Converts a size in Delta contracts into an amount of the underlying.
        """
        return size * self._contract_values[trading_pair]

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return DeltaOptionsAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return DeltaOptionsAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def start_network(self):
        await super().start_network()
        # Never a second loop next to a running one: a stop would then cancel one, and the other would keep the
        # switch armed after the stop had switched it off.
        heartbeat_running = self._heartbeat_task is not None and not self._heartbeat_task.done()
        if self._trading_required and self._dead_man_switch and self._trading_pairs and not heartbeat_running:
            self._heartbeat_task = safe_ensure_future(self._heartbeat_loop())

    async def stop_network(self):
        # One stop at a time: stopping the clock starts one in the background, and a second one closing the proxy
        # session would cut off the first one's switch-off of the dead-man switch.
        async with self._stop_lock:
            await self._stop_heartbeat()
            await super().stop_network()
            # A proxy gets its own aiohttp session; the default connections factory is shared and stays open.
            if self._proxy_url:
                try:
                    await self._web_assistants_factory.close()
                except Exception:
                    self.logger().debug("Error closing the proxy connections factory.", exc_info=True)

    def resync_after_stream_gap(self):
        """
        Delta doesn't replay what the private stream sent while it was down (orders its dead-man switch cancelled,
        fills), so orders, positions and balances are read over REST at once rather than at the next poll, which can
        be two minutes away.
        """
        self._poll_notifier.set()

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return "signatureexpired" in str(request_exception).lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # Delta answers {"error":{"code":"open_order_not_found"}} for an order that is no longer open.
        return self._is_not_found(cancelation_exception)

    @staticmethod
    def _is_not_found(exception: Exception) -> bool:
        message = str(exception).lower()
        return "open_order_not_found" in message or "not found" in message or "404" in message

    async def _make_network_check_request(self):
        await self._api_get(path_url=CONSTANTS.PRODUCTS_PATH_URL, params={"page_size": 1})

    async def _make_trading_pairs_request(self) -> List[Dict[str, Any]]:
        params = {
            "contract_types": CONSTANTS.OPTION_CONTRACT_TYPES,
            "states": CONSTANTS.LIVE_STATE,
            "page_size": CONSTANTS.PRODUCTS_PAGE_SIZE,
        }
        products = []
        for _ in range(CONSTANTS.MAX_PRODUCT_PAGES):
            response = await self._api_get(path_url=CONSTANTS.PRODUCTS_PATH_URL, params=params)
            products.extend(response.get("result") or [])
            after = (response.get("meta") or {}).get("after")
            if not after:
                return products
            params = {**params, "after": after}
        self.logger().warning(f"Stopped reading Delta products after {CONSTANTS.MAX_PRODUCT_PAGES} pages.")
        return products

    async def _make_trading_rules_request(self) -> List[Dict[str, Any]]:
        return await self._make_trading_pairs_request()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        mapping = bidict()
        instruments = []
        contract_values = {}
        product_ids = {}
        fee_rates = {}
        for product in exchange_info:
            if not utils.is_live_vanilla_option(product):
                continue
            try:
                trading_pair = utils.trading_pair_from_symbol(product["symbol"], product["quoting_asset"]["symbol"])
                instrument = utils.instrument_from_product(product, trading_pair)
                contract_value = Decimal(str(product["contract_value"]))
                product_id = int(product["id"])
                premium_cap = (product.get("product_specs") or {}).get("premium_commission_rate")
                fee_rates[trading_pair] = (
                    Decimal(str(product.get("maker_commission_rate") or utils.DEFAULT_FEES.maker_percent_fee_decimal)),
                    Decimal(str(product.get("taker_commission_rate") or utils.DEFAULT_FEES.taker_percent_fee_decimal)),
                    Decimal(str(premium_cap)) if premium_cap is not None else CONSTANTS.DEFAULT_PREMIUM_CAP_RATE,
                )
            except Exception:
                self.logger().error(f"Could not read the Delta product {product.get('symbol')}.", exc_info=True)
                continue
            mapping[product["symbol"]] = trading_pair
            instruments.append(instrument)
            contract_values[trading_pair] = contract_value
            product_ids[trading_pair] = product_id
        self._keep_unlisted_trading_pairs(mapping, instruments, contract_values, product_ids, fee_rates)
        self._symbols = dict(mapping.inverse)
        self._contract_values = contract_values
        self._product_ids = product_ids
        self._fee_rates = fee_rates
        self._options_trading.set_instruments(instruments)
        self._set_trading_pair_symbol_map(mapping)

    def _keep_unlisted_trading_pairs(self, mapping: bidict, instruments: List[OptionInstrumentInfo],
                                     contract_values: Dict[str, Decimal], product_ids: Dict[str, int],
                                     fee_rates: Dict[str, Tuple[Decimal, Decimal, Decimal]]):
        """
        Delta lists live options only, so the bot's own options drop out of the list once they expire. They stay
        known: orders on them are refused, while cancels, late fills and the settlement still need their ids and sizes.
        One the connector never saw listed (the bot started after it expired) is known from its name, which is enough
        to tell that it has expired.
        """
        listed = {instrument.trading_pair for instrument in instruments}
        for trading_pair in self._trading_pairs:
            if trading_pair in listed:
                continue
            instrument = self._options_trading.get_instrument(trading_pair)
            if instrument is None or trading_pair not in self._contract_values:
                try:
                    instruments.append(utils.instrument_from_trading_pair(trading_pair))
                except Exception:
                    self.logger().error(f"{trading_pair} is not listed on Delta, and its name doesn't describe an "
                                        f"option.")
                continue
            instruments.append(instrument)
            contract_values[trading_pair] = self._contract_values[trading_pair]
            product_ids[trading_pair] = self._product_ids[trading_pair]
            if trading_pair in self._fee_rates:
                fee_rates[trading_pair] = self._fee_rates[trading_pair]
            if trading_pair in self._symbols:
                mapping[self._symbols[trading_pair]] = trading_pair

    async def _format_trading_rules(self, exchange_info_dict: List[Dict[str, Any]]) -> List[TradingRule]:
        rules = []
        for product in exchange_info_dict:
            if not utils.is_live_vanilla_option(product):
                continue
            try:
                quote = product["quoting_asset"]["symbol"]
                settling_asset = ((product.get("settling_asset") or {}).get("symbol") or quote).upper()
                contract_value = Decimal(str(product["contract_value"]))
                rules.append(TradingRule(
                    trading_pair=utils.trading_pair_from_symbol(product["symbol"], quote),
                    min_order_size=contract_value,
                    min_price_increment=Decimal(str(product["tick_size"])),
                    min_base_amount_increment=contract_value,
                    min_notional_size=Decimal("0"),
                    buy_order_collateral_token=settling_asset,
                    sell_order_collateral_token=settling_asset,
                ))
            except Exception:
                self.logger().error(f"Could not read the trading rule of {product.get('symbol')}.", exc_info=True)
        # The base class clears the old rules only after this returns: the bot's own options keep theirs once they
        # expire and drop out of Delta's list.
        listed = {rule.trading_pair for rule in rules}
        rules.extend(rule for trading_pair, rule in self._trading_rules.items()
                     if trading_pair in self._trading_pairs and trading_pair not in listed)
        return rules

    async def _update_trading_fees(self):
        pass

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
        """
        Delta charges the lower of (rate x underlying value) and (cap x premium), plus GST. As a share of the premium
        that is min(rate x underlying price / option price, cap), and the cap alone when either price is unknown.
        """
        is_maker = is_maker if is_maker is not None else order_type is OrderType.LIMIT_MAKER
        trading_pair = combine_to_hb_trading_pair(base_currency, quote_currency)
        maker_rate, taker_rate, premium_cap = self._fee_rates.get(trading_pair, (
            utils.DEFAULT_FEES.maker_percent_fee_decimal,
            utils.DEFAULT_FEES.taker_percent_fee_decimal,
            CONSTANTS.DEFAULT_PREMIUM_CAP_RATE,
        ))
        share_of_premium = premium_cap
        try:
            underlying_price = self.get_underlying_price(trading_pair)
        except KeyError:
            underlying_price = None
        if underlying_price and not price.is_nan() and price > 0:
            rate = maker_rate if is_maker else taker_rate
            share_of_premium = min(rate * underlying_price / price, premium_cap)
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=share_of_premium * (Decimal("1") + CONSTANTS.GST_RATE),
        )

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
        payload: Dict[str, Any] = {
            "product_id": self._product_ids[trading_pair],
            "size": int(amount / self.contract_size(trading_pair)),
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "order_type": CONSTANTS.ORDER_TYPE_LIMIT,
            "limit_price": str(price),
            "time_in_force": CONSTANTS.TIME_IN_FORCE_GTC,
            "client_order_id": order_id,
            "reduce_only": position_action == PositionAction.CLOSE,
        }
        if order_type == OrderType.LIMIT_MAKER:
            payload["post_only"] = True
        order = _result(await self._api_post(path_url=CONSTANTS.ORDERS_PATH_URL, data=payload, is_auth_required=True))
        if isinstance(order, list) and order:
            order = order[0]
        exchange_order_id = str(order.get("id") or "") if isinstance(order, dict) else ""
        if not exchange_order_id:
            raise IOError(f"Delta did not return an order id: {order}")
        return exchange_order_id, self._parse_timestamp(order.get("created_at"))

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        # An order still being placed is waited for (up to 10s), so a cancel, or the kill switch, never misses it.
        exchange_order_id = await tracked_order.get_exchange_order_id()
        try:
            response = await self._api_delete(
                path_url=CONSTANTS.ORDERS_PATH_URL,
                data={"id": int(exchange_order_id), "product_id": self._product_ids[tracked_order.trading_pair]},
                is_auth_required=True,
            )
        except IOError as error:
            if not self._is_not_found(error):
                raise
            return await self._closed_order_was_cancelled(tracked_order)
        order = _result(response)
        # Only a "cancelled" state confirms it; an order that filled first comes back "closed" and keeps its fills.
        return isinstance(order, dict) and str(order.get("state", "")).lower() in ("cancelled", "canceled")

    async def _closed_order_was_cancelled(self, order: InFlightOrder) -> bool:
        """
        Delta answers "not found" for an order that is no longer open: one it cancelled itself (its dead-man switch,
        an expiry), or one that filled. Its own state decides, so neither is left open or marked the wrong way.
        """
        order_update = await self._request_order_status(order)
        if order_update.new_state == OrderState.CANCELED:
            return True
        for trade_update in await self._all_trade_updates_for_order(order):
            self._order_tracker.process_trade_update(trade_update)
        self._order_tracker.process_order_update(order_update)
        return False

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        # Waits briefly for an order still being placed; a timeout counts towards marking it lost.
        exchange_order_id = await tracked_order.get_exchange_order_id()
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_BY_ID_PATH_URL.format(order_id=exchange_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
        )
        order = _result(response)
        if not isinstance(order, dict):
            raise ValueError(f"Unexpected Delta order status response: {response}")
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._parse_timestamp(order.get("updated_at") or order.get("created_at")),
            new_state=self._order_state(order, tracked_order.current_state),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Fills come from REST /v2/fills, the only source that carries the commission (v2/user_trades has none).
        The endpoint has no order filter, so it reads the option's fills since the order was created.
        """
        if order.exchange_order_id is None:
            return []
        params: Dict[str, Any] = {
            "product_ids": str(self._product_ids[order.trading_pair]),
            "page_size": CONSTANTS.FILLS_PAGE_SIZE,
        }
        if order.creation_timestamp > 0:
            params["start_time"] = int((order.creation_timestamp - 60) * 1e6)
        trade_updates = []
        for _ in range(CONSTANTS.MAX_FILL_PAGES):
            response = await self._api_get(path_url=CONSTANTS.FILLS_PATH_URL, params=params, is_auth_required=True)
            for fill in _result(response) or []:
                if str(fill.get("order_id")) == order.exchange_order_id:
                    trade_updates.append(self._trade_update_from_fill(order, fill))
            after = (response.get("meta") or {}).get("after") if isinstance(response, dict) else None
            if not after:
                break
            params = {**params, "after": after}
        return trade_updates

    def _trade_update_from_fill(self, order: InFlightOrder, fill: Dict[str, Any]) -> TradeUpdate:
        fill_base = self.size_to_amount(order.trading_pair, abs(Decimal(str(fill.get("size", "0")))))
        fill_price = Decimal(str(fill.get("price", "0")))
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=getattr(order, "position", PositionAction.NIL),
            flat_fees=[TokenAmount(
                amount=Decimal(str(fill.get("commission") or "0")),
                token=str(fill.get("settling_asset_symbol") or order.quote_asset),
            )],
        )
        return TradeUpdate(
            trade_id=str(fill.get("id")),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=fill_base,
            fill_quote_amount=fill_base * fill_price,
            fill_price=fill_price,
            fill_timestamp=self._parse_timestamp(fill.get("created_at")),
        )

    async def _update_balances(self):
        response = await self._api_get(path_url=CONSTANTS.WALLET_PATH_URL, is_auth_required=True)
        balances = {}
        for entry in response.get("result") or []:
            asset = str(entry.get("asset_symbol") or "").upper()
            if asset:
                balances[asset] = (Decimal(str(entry.get("balance", "0"))), Decimal(str(entry.get("available_balance", "0"))))
        if not balances:
            # An empty wallet payload would otherwise wipe every balance we track.
            self.logger().warning("Delta returned no wallet balances; keeping the last known balances.")
            return
        for asset in set(self._account_balances) - set(balances):
            self._account_balances.pop(asset, None)
            self._account_available_balances.pop(asset, None)
        for asset, (total, available) in balances.items():
            self._account_balances[asset] = total
            self._account_available_balances[asset] = available

    async def _update_positions(self):
        response = await self._api_get(
            path_url=CONSTANTS.POSITIONS_PATH_URL,
            params={"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES},
            is_auth_required=True,
        )
        open_pairs = set()
        for entry in response.get("result") or []:
            trading_pair = await self._trading_pair_or_none(entry.get("product_symbol"))
            size = Decimal(str(entry.get("size") or "0"))
            if trading_pair is None or size == 0:
                continue
            open_pairs.add(trading_pair)
            self._options_trading.set_position(trading_pair, self._position(
                trading_pair=trading_pair,
                size=size,
                entry_price=Decimal(str(entry.get("entry_price") or "0")),
                unrealized_pnl=Decimal(str(entry.get("unrealized_pnl") or "0")),
            ))
        # Delta stops listing a position once it is closed or its option has expired.
        for trading_pair in set(self.account_positions) - open_pairs:
            self._options_trading.remove_position(trading_pair)

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                channel = event.get("type")
                if channel == CONSTANTS.WS_MARGINS_CHANNEL:
                    self._process_balance_event(event)
                elif channel == CONSTANTS.WS_POSITIONS_CHANNEL:
                    await self._process_position_event(event)
                elif channel == CONSTANTS.WS_ORDERS_CHANNEL:
                    entries = (event.get("result") or []) if event.get("action") == "snapshot" else [event]
                    for order_data in entries:
                        self._process_order_event(order_data)
                elif channel == CONSTANTS.WS_USER_TRADES_CHANNEL:
                    self._process_trade_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in the Delta options user stream listener.", exc_info=True)
                await self._sleep(5.0)

    def _process_balance_event(self, event: Dict[str, Any]):
        asset = str(event.get("asset_symbol") or "").upper()
        if not asset:
            return
        if event.get("balance") is not None:
            self._account_balances[asset] = Decimal(str(event["balance"]))
        if event.get("available_balance") is not None:
            self._account_available_balances[asset] = Decimal(str(event["available_balance"]))

    async def _process_position_event(self, event: Dict[str, Any]):
        entries = (event.get("result") or []) if event.get("action") == "snapshot" else [event]
        for entry in entries:
            trading_pair = await self._trading_pair_or_none(entry.get("symbol") or entry.get("product_symbol"))
            if trading_pair is None:
                continue
            size = Decimal(str(entry.get("size") or "0"))
            if entry.get("action") == "delete" or size == 0:
                self._options_trading.remove_position(trading_pair)
                continue
            current = self.get_position(trading_pair)
            self._options_trading.set_position(trading_pair, self._position(
                trading_pair=trading_pair,
                size=size,
                entry_price=Decimal(str(entry.get("entry_price") or "0")),
                unrealized_pnl=current.unrealized_pnl if current is not None else Decimal("0"),
            ))

    def _process_order_event(self, order_data: Dict[str, Any]):
        # Stream updates carry "order_id"; snapshot entries carry "id".
        exchange_order_id = str(order_data.get("order_id") or order_data.get("id") or "")
        tracked = self._find_tracked_order(
            self._order_tracker.all_updatable_orders, order_data.get("client_order_id"), exchange_order_id)
        if tracked is None:
            return
        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked.trading_pair,
            update_timestamp=self._parse_timestamp(order_data.get("timestamp") or order_data.get("updated_at")),
            new_state=self._order_state(order_data, tracked.current_state),
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id or tracked.exchange_order_id,
        ))

    def _process_trade_event(self, fill: Dict[str, Any]):
        """
        v2/user_trades uses compact keys ("c" client order id, "o" order id) and has no commission, so it only
        signals the fill; the fill itself, with its fee, is read from REST.
        """
        tracked = self._find_tracked_order(
            self._order_tracker.all_fillable_orders, fill.get("c"), str(fill.get("o") or ""))
        if tracked is None:
            return
        self._ws_fill_counts[tracked.client_order_id] += 1
        safe_ensure_future(self._fetch_fills_after_trade_event(tracked))

    async def _fetch_fills_after_trade_event(self, order: InFlightOrder):
        for delay in CONSTANTS.FILL_FETCH_DELAYS:
            await self._sleep(delay)
            try:
                for trade_update in await self._all_trade_updates_for_order(order):
                    self._order_tracker.process_trade_update(trade_update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(f"Error fetching the fills of {order.client_order_id}.", exc_info=True)
                continue
            if len(order.order_fills) >= self._ws_fill_counts[order.client_order_id]:
                break
        if order.is_done:
            self._ws_fill_counts.pop(order.client_order_id, None)

    @staticmethod
    def _find_tracked_order(orders: Dict[str, InFlightOrder], client_order_id: Optional[str],
                            exchange_order_id: str) -> Optional[InFlightOrder]:
        tracked = orders.get(client_order_id) if client_order_id else None
        if tracked is None and exchange_order_id:
            tracked = next((o for o in orders.values() if o.exchange_order_id == exchange_order_id), None)
        return tracked

    @staticmethod
    def _order_state(order: Dict[str, Any], current: OrderState) -> OrderState:
        state = CONSTANTS.ORDER_STATE.get(str(order.get("state", "")).lower(), current)
        unfilled = order.get("unfilled_size")
        if state == OrderState.OPEN and unfilled is not None and Decimal(str(unfilled)) < Decimal(str(order.get("size", 0))):
            state = OrderState.PARTIALLY_FILLED
        return state

    def _position(self, trading_pair: str, size: Decimal, entry_price: Decimal, unrealized_pnl: Decimal) -> Position:
        return Position(
            trading_pair=trading_pair,
            position_side=PositionSide.LONG if size > 0 else PositionSide.SHORT,
            unrealized_pnl=unrealized_pnl,
            entry_price=entry_price,
            amount=self.size_to_amount(trading_pair, size),
            leverage=Decimal("1"),
        )

    async def _trading_pair_or_none(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        try:
            return await self.trading_pair_associated_to_exchange_symbol(symbol)
        except KeyError:
            return None

    async def _heartbeat_loop(self):
        """
        Delta's dead-man switch: while this loop keeps sending heartbeats, nothing happens; if they stop for longer
        than the TTL (crash, hang, lost network), Delta cancels the open orders on this bot's options.
        """
        while True:
            try:
                if self._heartbeat_id is None:
                    self._heartbeat_id = await self._register_heartbeat()
                await self._send_heartbeat(self._heartbeat_id, ttl=CONSTANTS.HEARTBEAT_TTL_MS)
            except asyncio.CancelledError:
                raise
            except Exception:
                registered = self._heartbeat_id is not None
                self.logger().network(
                    f"Error {'refreshing' if registered else 'setting up'} Delta's dead-man switch.",
                    exc_info=True,
                    app_warning_msg=(
                        "Delta's dead-man switch could not be refreshed; if this persists, Delta will cancel this "
                        "account's open orders on the bot's options." if registered
                        else "Delta's dead-man switch could not be set up, so orders stay open if the bot stops; "
                             "retrying."),
                )
            await self._sleep(CONSTANTS.HEARTBEAT_INTERVAL)

    async def _register_heartbeat(self) -> str:
        symbols = sorted(await self.exchange_symbols_for_live_pairs(self._trading_pairs))
        # One heartbeat per key and set of options, so a restarted bot picks up its own heartbeat again.
        heartbeat_id = "hb" + hashlib.sha256(f"{self.api_key}:{','.join(symbols)}".encode()).hexdigest()[:24]
        try:
            _result(await self._api_post(
                path_url=CONSTANTS.HEARTBEAT_CREATE_PATH_URL,
                data={
                    "heartbeat_id": heartbeat_id,
                    "impact": "products",
                    "product_symbols": symbols,
                    "default_ttl": CONSTANTS.HEARTBEAT_TTL_MS,
                    "config": [{"action": "cancel_orders", "unhealthy_count": 1}],
                },
                is_auth_required=True,
            ))
        except IOError as create_error:
            # Expected after a restart: the heartbeat already exists, which an acknowledgment confirms. (Looking it up
            # instead needs the account's user id, per Delta's API reference.)
            try:
                await self._send_heartbeat(heartbeat_id, ttl=CONSTANTS.HEARTBEAT_TTL_MS)
            except IOError as ack_error:
                raise IOError(f"Could not create the heartbeat ({create_error}) nor acknowledge it ({ack_error}).")
        self.logger().info(f"Delta's dead-man switch is on for {len(symbols)} options: their orders are cancelled if "
                           f"this bot goes quiet for {CONSTANTS.HEARTBEAT_TTL_MS // 1000}s.")
        return heartbeat_id

    async def _send_heartbeat(self, heartbeat_id: str, ttl: int) -> Dict[str, Any]:
        return _result(await self._api_post(
            path_url=CONSTANTS.HEARTBEAT_PATH_URL,
            data={"heartbeat_id": heartbeat_id, "ttl": ttl},
            is_auth_required=True,
        ))

    async def _stop_heartbeat(self):
        if self._heartbeat_task is None:
            return
        self._heartbeat_task.cancel()
        self._heartbeat_task = None
        if self._heartbeat_id is None:
            return
        try:
            # A TTL of 0 switches the heartbeat off, so a clean stop doesn't make Delta cancel anything.
            await asyncio.wait_for(self._send_heartbeat(self._heartbeat_id, ttl=0), timeout=CONSTANTS.HEARTBEAT_STOP_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Could not switch off Delta's dead-man switch ({error!r}); when it expires, Delta "
                                  f"will cancel this account's open orders on the bot's options.")

    @staticmethod
    def _parse_timestamp(raw: Any) -> float:
        """
        Delta sends microseconds since epoch, or ISO 8601 strings in some REST payloads; returns seconds.
        """
        try:
            value = float(raw)
        except (TypeError, ValueError):
            try:
                return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
            except ValueError:
                return 0.0
        return value / 1e6 if value > 1e14 else value / 1e3 if value > 1e11 else value
