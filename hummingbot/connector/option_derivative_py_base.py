import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Dict, List, Optional, Set, Tuple

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.option_budget_checker import OptionBudgetChecker
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.options_trading import OptionsTrading
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import PerpetualDerivativeInFlightOrder
from hummingbot.core.data_type.option_api_order_book_data_source import OptionAPIOrderBookDataSource
from hummingbot.core.data_type.option_info import OptionInfo, OptionInstrumentInfo, OptionType
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.event.events import MarketEvent, OptionExpiredEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather


class OptionDerivativePyBase(ExchangePyBase, ABC):
    """
    Base class for options connectors. Options are listed and expire continuously, so on top of the usual exchange
    state this keeps an index of the instruments, their live option info (mark price, IV, Greeks) and net positions.

    Selling an option to open a short position is refused unless `allow_selling` is set, and is capped at
    `max_short_contracts` per option, counting open sell orders as if they filled.

    From `stop_before_expiry` seconds before an option expires, orders that would open a position in it are refused
    and resting ones are cancelled; closing orders are still accepted until it expires. At expiry, `OptionExpired` is
    raised for each of the connector's options.
    """
    VALID_POSITION_ACTIONS = [PositionAction.OPEN, PositionAction.CLOSE]
    # Conservative initial margin for a short option: max(15% - out-of-the-money share, 10%) of the underlying's value,
    # plus the premium. Exchanges set their own; this only sizes budget checks.
    SHORT_MARGIN_RATE = Decimal("0.15")
    SHORT_MARGIN_FLOOR_RATE = Decimal("0.10")
    EXPIRY_CHECK_INTERVAL = 5.0

    def __init__(self,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 allow_selling: bool = False,
                 max_short_contracts: Decimal = Decimal("0"),
                 stop_before_expiry: float = 0):
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._allow_selling = allow_selling
        self._max_short_contracts = Decimal(str(max_short_contracts))
        self._stop_before_expiry = float(stop_before_expiry)
        self._options_trading = OptionsTrading(self.trading_pairs)
        self._option_info_listener_task: Optional[asyncio.Task] = None
        self._expiry_watch_task: Optional[asyncio.Task] = None
        # Options whose closing window has started, and options whose expiry has been announced.
        self._closing_only: Set[str] = set()
        self._expiry_announced: Set[str] = set()
        self._orderbook_ds: OptionAPIOrderBookDataSource = self._orderbook_ds  # for type-hinting
        self._budget_checker = OptionBudgetChecker(self)

    @property
    def status_dict(self) -> Dict[str, bool]:
        status_d = super().status_dict
        # An expired option has no market data any more, and the others must not wait for it.
        status_d["option_info"] = self._options_trading.is_option_info_initialized(self._live_trading_pairs())
        return status_d

    @property
    def budget_checker(self) -> OptionBudgetChecker:
        return self._budget_checker

    @property
    def position_mode(self) -> PositionMode:
        return PositionMode.ONEWAY

    @property
    def account_positions(self) -> Dict[str, Position]:
        return self._options_trading.account_positions

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def set_position_mode(self, mode: PositionMode):
        if mode != PositionMode.ONEWAY:
            self.logger().error(f"Position mode {mode} is not supported. Options positions are always net (ONEWAY).")

    def get_leverage(self, trading_pair: str) -> int:
        return 1

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        if leverage != 1:
            self.logger().warning("Leverage does not apply to options; the setting is ignored.")

    def get_position(self, trading_pair: str) -> Optional[Position]:
        return self._options_trading.get_position(trading_pair)

    def get_option_info(self, trading_pair: str) -> OptionInfo:
        return self._options_trading.get_option_info(trading_pair)

    def get_instrument(self, trading_pair: str) -> Optional[OptionInstrumentInfo]:
        return self._options_trading.get_instrument(trading_pair)

    def get_underlying_price(self, trading_pair: str) -> Decimal:
        return self.get_option_info(trading_pair).underlying_price

    def get_option_chain(self, underlying: str, expiry_timestamp: Optional[float] = None) -> List[OptionInstrumentInfo]:
        """
        Returns the live (not expired) instruments of an underlying, optionally for one expiry only,
        sorted by expiry, option type and strike.
        """
        now = self._time()
        chain = [
            instrument for instrument in self._options_trading.instruments.values()
            if instrument.underlying == underlying
            and not instrument.is_expired(now)
            and (expiry_timestamp is None or instrument.expiry_timestamp == expiry_timestamp)
        ]
        return sorted(chain, key=lambda i: (i.expiry_timestamp, i.option_type.value, i.strike))

    def is_expired(self, trading_pair: str) -> bool:
        instrument = self.get_instrument(trading_pair)
        return instrument is not None and instrument.is_expired(self._time())

    def is_closing_only(self, trading_pair: str) -> bool:
        """
        True once the option is within `stop_before_expiry` of its expiry: only closing orders are accepted.
        """
        instrument = self.get_instrument(trading_pair)
        return instrument is not None and self._time() >= instrument.expiry_timestamp - self._stop_before_expiry

    async def exchange_symbols_for_live_pairs(self, trading_pairs: List[str]) -> List[str]:
        """
        Returns the exchange symbols of the pairs that are still listed and not expired. Expired or delisted pairs are
        skipped, so one expired instrument never breaks the subscriptions of the others.
        """
        symbols = []
        for trading_pair in trading_pairs:
            if self.is_expired(trading_pair):
                continue
            try:
                symbols.append(await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair))
            except KeyError:
                self.logger().warning(f"{trading_pair} is no longer listed on {self.name}; skipping it.")
        return symbols

    def estimate_short_margin(self, trading_pair: str, amount: Decimal, price: Decimal) -> Decimal:
        """
        A conservative estimate of the margin needed to sell `amount` of an option to open, in the collateral token.
        """
        premium = price if price is not None and not price.is_nan() else Decimal("0")
        instrument = self.get_instrument(trading_pair)
        if instrument is None:
            return premium * amount
        try:
            underlying = self.get_underlying_price(trading_pair)
        except KeyError:
            underlying = instrument.strike
        if underlying <= 0:
            return premium * amount
        if instrument.option_type == OptionType.CALL:
            out_of_the_money = max(instrument.strike - underlying, Decimal("0"))
        else:
            out_of_the_money = max(underlying - instrument.strike, Decimal("0"))
        rate = max(self.SHORT_MARGIN_RATE - out_of_the_money / underlying, self.SHORT_MARGIN_FLOOR_RATE)
        return (rate * underlying + premium) * amount

    async def flatten(self, slippage: Decimal = Decimal("0.05")) -> List[str]:
        """
        Kill switch: cancels every open order, then closes every position on this connector's trading pairs with a
        reduce-only limit order that crosses the book by `slippage` (kept inside the exchange's price band). Positions
        on other options, or on expired ones that are settling, are left alone. Returns the client ids of the closing
        orders.
        """
        await self.cancel_all(timeout_seconds=10)
        order_ids = []
        for position in list(self.account_positions.values()):
            trading_pair = position.trading_pair
            if position.amount == 0:
                continue
            if trading_pair not in self.trading_pairs or self.is_expired(trading_pair):
                self.logger().warning(f"Not closing the {position.amount} {trading_pair} position: it is not one of "
                                      f"this connector's live trading pairs.")
                continue
            is_buy = position.amount < 0
            price = self._crossing_price(trading_pair, is_buy, slippage)
            if price.is_nan():
                self.logger().error(f"Cannot close the {position.amount} {trading_pair} position: no price to cross.")
                continue
            place = self.buy if is_buy else self.sell
            order_ids.append(place(trading_pair, abs(position.amount), OrderType.LIMIT, price,
                                   position_action=PositionAction.CLOSE))
        return order_ids

    @abstractmethod
    def contract_size(self, trading_pair: str) -> Decimal:
        """
        Amount of the underlying in one contract.
        """
        raise NotImplementedError

    @abstractmethod
    def get_buy_collateral_token(self, trading_pair: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_sell_collateral_token(self, trading_pair: str) -> str:
        raise NotImplementedError

    def start_tracking_order(
        self,
        order_id: str,
        exchange_order_id: Optional[str],
        trading_pair: str,
        trade_type: TradeType,
        price: Decimal,
        amount: Decimal,
        order_type: OrderType,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ):
        self._order_tracker.start_tracking_order(
            PerpetualDerivativeInFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                amount=amount,
                price=price,
                creation_timestamp=self.current_timestamp,
                leverage=1,
                position=position_action,
            )
        )

    @abstractmethod
    def _create_order_book_data_source(self) -> OptionAPIOrderBookDataSource:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    async def _update_positions(self):
        raise NotImplementedError

    async def start_network(self):
        await super().start_network()
        self._options_trading.start()
        self._option_info_listener_task = safe_ensure_future(self._listen_for_option_info())
        if self.is_trading_required:
            self._expiry_watch_task = safe_ensure_future(self._expiry_watch_loop())

    async def stop_network(self):
        self._options_trading.stop()
        for task in (self._option_info_listener_task, self._expiry_watch_task):
            if task is not None:
                task.cancel()
        self._option_info_listener_task = self._expiry_watch_task = None
        await super().stop_network()

    async def _create_order(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ):
        if position_action not in self.VALID_POSITION_ACTIONS:
            raise ValueError(
                f"Invalid position action {position_action}. Must be one of {self.VALID_POSITION_ACTIONS}"
            )
        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER) and price is not None:
            # Checked as it will be sent: the base class rounds the price down to the tick.
            price = self.quantize_order_price(trading_pair, price)
        refusal = self._order_refusal(trading_pair, trade_type, order_type, amount, price, position_action)
        if refusal is not None:
            self.start_tracking_order(order_id, None, trading_pair, trade_type, price, amount, order_type, position_action)
            self.logger().error(f"Order {order_id} for {amount} {trading_pair} refused: {refusal}.")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=ValueError(refusal))
            return
        await super()._create_order(
            trade_type,
            order_id,
            trading_pair,
            amount,
            order_type,
            price,
            position_action=position_action,
            **kwargs,
        )

    def get_fee(
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
        return self._get_fee(
            base_currency, quote_currency, order_type, order_side, position_action, amount, price, is_maker
        )

    @abstractmethod
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
        raise NotImplementedError

    def _order_refusal(
        self, trading_pair: str, trade_type: TradeType, order_type: OrderType, amount: Decimal,
        price: Optional[Decimal], position_action: PositionAction,
    ) -> Optional[str]:
        """
        Why an order must not be sent, or None: an unsupported order type, an expired option, an opening order too
        close to expiry, the selling guard, or the exchange's price band.
        """
        if order_type not in self.supported_order_types():
            # Refused here: the base class would first look up a price for it, which fails on an empty book.
            return f"{order_type.name} orders are not supported"
        instrument = self.get_instrument(trading_pair)
        if self.is_expired(trading_pair):
            return f"the option expired at {self._utc(instrument.expiry_timestamp)}"
        if position_action == PositionAction.OPEN and self.is_closing_only(trading_pair):
            return (f"the option expires at {self._utc(instrument.expiry_timestamp)}, and positions are not opened in "
                    f"its last {self._stop_before_expiry / 60:g} minutes; closing orders are still accepted")
        if trade_type == TradeType.SELL and position_action == PositionAction.OPEN:
            short = self._short_after_sell(trading_pair, amount)
            if short > 0 and not self._allow_selling:
                return "selling options to open a short position is not enabled for this connector"
            limit = self._max_short_contracts * self.contract_size(trading_pair)
            if short > limit:
                return (f"the short position could reach {short} (counting open sell orders), above the "
                        f"{self._max_short_contracts}-contract limit ({limit})")
        option_info = self._option_info_or_none(trading_pair)
        if price is not None and not price.is_nan() and option_info is not None:
            lower, upper = option_info.price_band_lower, option_info.price_band_upper
            if (lower is not None and price < lower) or (upper is not None and price > upper):
                return f"price {price} is outside the exchange's price band ({lower} to {upper})"
        return None

    def _short_after_sell(self, trading_pair: str, amount: Decimal) -> Decimal:
        """
        The worst-case short position after this sell, as if every open sell order on the option also filled.
        """
        position = self.get_position(trading_pair)
        held = position.amount if position is not None else Decimal("0")
        pending_sells = sum(
            (order.amount - order.executed_amount_base for order in self.in_flight_orders.values()
             if order.trading_pair == trading_pair and order.trade_type == TradeType.SELL and not order.is_done),
            Decimal("0"),
        )
        return max(Decimal("0"), pending_sells + amount - held)

    def _crossing_price(self, trading_pair: str, is_buy: bool, slippage: Decimal) -> Decimal:
        option_info = self._option_info_or_none(trading_pair)
        try:
            reference = Decimal(str(self.get_price(trading_pair, is_buy)))
        except Exception:
            reference = Decimal("NaN")
        if (reference.is_nan() or reference <= 0) and option_info is not None:
            reference = option_info.mark_price
        if reference is None or reference.is_nan():
            return Decimal("NaN")
        price = reference * (Decimal("1") + slippage if is_buy else Decimal("1") - slippage)
        lower = option_info.price_band_lower if option_info is not None else None
        upper = option_info.price_band_upper if option_info is not None else None
        if upper is not None:
            price = min(price, upper)
        if lower is not None:
            price = max(price, lower)
        # Onto the tick grid: away from the book so it still crosses, but never out of the band.
        tick = self.get_order_price_quantum(trading_pair, price)
        steps = price / tick
        rounded = steps.to_integral_value(rounding=ROUND_CEILING if is_buy else ROUND_FLOOR) * tick
        if upper is not None and rounded > upper:
            rounded = steps.to_integral_value(rounding=ROUND_FLOOR) * tick
        if lower is not None and rounded < lower:
            rounded = steps.to_integral_value(rounding=ROUND_CEILING) * tick
        return rounded

    def _option_info_or_none(self, trading_pair: str) -> Optional[OptionInfo]:
        try:
            return self.get_option_info(trading_pair)
        except KeyError:
            return None

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_positions(),
            self._update_balances(),
            self._update_order_status(),
        )

    async def _listen_for_option_info(self):
        await self._init_option_info()
        await self._orderbook_ds.listen_for_option_info(output=self._options_trading.option_info_stream)

    def _live_trading_pairs(self) -> List[str]:
        return [trading_pair for trading_pair in self.trading_pairs if not self.is_expired(trading_pair)]

    @staticmethod
    def _utc(timestamp: float) -> str:
        return f"{datetime.fromtimestamp(timestamp, tz=timezone.utc):%d %b %H:%M} UTC"

    async def _expiry_watch_loop(self):
        while True:
            try:
                self._check_expiries()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error while checking option expiries.", exc_info=True)
            await self._sleep(self.EXPIRY_CHECK_INTERVAL)

    def _check_expiries(self):
        """
        When an option's closing window starts, cancels the resting orders that would open a position in it; when it
        expires, raises OptionExpired and hands over the position held at that moment for settlement.
        """
        now = self._time()
        for trading_pair in self.trading_pairs:
            instrument = self.get_instrument(trading_pair)
            if instrument is None:
                continue
            if trading_pair not in self._closing_only and self.is_closing_only(trading_pair):
                self._closing_only.add(trading_pair)
                cancelled = self._cancel_opening_orders(trading_pair)
                self.logger().info(f"{trading_pair} expires at {self._utc(instrument.expiry_timestamp)}: no new "
                                   f"positions are opened in it from now on ({cancelled} resting opening orders "
                                   f"cancelled); closing orders are still accepted.")
            if trading_pair not in self._expiry_announced and instrument.is_expired(now):
                self._expiry_announced.add(trading_pair)
                self.trigger_event(MarketEvent.OptionExpired, OptionExpiredEvent(
                    timestamp=now, market=self.name, trading_pair=trading_pair,
                    expiry_timestamp=instrument.expiry_timestamp))
                self._on_option_expired(trading_pair, self.get_position(trading_pair))

    def _cancel_opening_orders(self, trading_pair: str) -> int:
        opening = [order for order in self.in_flight_orders.values()
                   if order.trading_pair == trading_pair and not order.is_done
                   and getattr(order, "position", None) == PositionAction.OPEN]
        for order in opening:
            self.cancel(trading_pair, order.client_order_id)
        return len(opening)

    def _on_option_expired(self, trading_pair: str, position: Optional[Position]):
        """
        Called once when one of the connector's options expires, with the position held at that moment (or None).
        Connectors look for the exchange's settlement record here and raise OptionSettled.
        """

    async def _init_option_info(self):
        for trading_pair in self._live_trading_pairs():
            while True:
                try:
                    option_info = await self._orderbook_ds.get_option_info(trading_pair)
                    self._options_trading.initialize_option_info(option_info)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().network(
                        f"Error fetching option info for {trading_pair}. Retrying in 5 seconds.",
                        exc_info=True,
                    )
                    await self._sleep(5.0)
