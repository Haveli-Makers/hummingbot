import asyncio
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
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

TRADING_NOT_ENABLED = "delta_options is read-only for now: order placement is not enabled yet."


class DeltaOptionsDerivative(OptionDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        delta_options_api_key: str,
        delta_options_api_secret: str,
        delta_options_proxy_url: str = "",
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = delta_options_api_key
        self.secret_key = delta_options_api_secret
        self._proxy_url = delta_options_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        if len(self._trading_pairs) > CONSTANTS.WS_MAX_SYMBOLS:
            raise ValueError(f"delta_options streams at most {CONSTANTS.WS_MAX_SYMBOLS} options per connector.")
        self._contract_values: Dict[str, Decimal] = {}
        self._fee_rates: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
        self._authenticator: Optional[DeltaOptionsAuth] = None
        super().__init__(balance_asset_limit, rate_limits_share_pct)

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
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].sell_order_collateral_token

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

    async def stop_network(self):
        await super().stop_network()
        # A proxy gets its own aiohttp session; the default connections factory is shared and stays open.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Error closing the proxy connections factory.", exc_info=True)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return "signatureexpired" in str(request_exception).lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

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
        fee_rates = {}
        for product in exchange_info:
            if not utils.is_live_vanilla_option(product):
                continue
            try:
                trading_pair = utils.trading_pair_from_symbol(product["symbol"], product["quoting_asset"]["symbol"])
                instrument = utils.instrument_from_product(product, trading_pair)
                contract_value = Decimal(str(product["contract_value"]))
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
        self._contract_values = contract_values
        self._fee_rates = fee_rates
        self._options_trading.set_instruments(instruments)
        self._set_trading_pair_symbol_map(mapping)

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
        raise NotImplementedError(TRADING_NOT_ENABLED)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        raise NotImplementedError(TRADING_NOT_ENABLED)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        raise NotImplementedError(TRADING_NOT_ENABLED)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        raise NotImplementedError(TRADING_NOT_ENABLED)
