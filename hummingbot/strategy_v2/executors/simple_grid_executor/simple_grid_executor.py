import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class SimpleGridExecutor(ExecutorBase):
    """
    Runs a single leg of the simple grid strategy.

    A leg is one complete round trip: a passive entry order, then whichever exit comes
    first out of the take profit, the stop loss and the time limit. The controller reads
    ``close_price`` back off this executor to decide where the next leg is anchored.

    Two behaviours distinguish it from the other executors in this package:

    * **Entry chasing.** A resting order that the market walks away from never fills, so
      the entry is re-placed as the touch price drifts, bounded by a drift cap, a re-post
      count and a timeout.
    * **First fill wins.** In ``both_oco`` mode an entry is placed on each side and the
      first one to fill cancels the other, so only one position is ever open.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: SimpleGridExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)
        self.config: SimpleGridExecutorConfig = config
        self.trading_rules = self.get_trading_rules(config.connector_name, config.trading_pair)

        # Entry state, tracked per side so both_oco can run two candidates at once.
        self._entry_orders: Dict[TradeType, Optional[TrackedOrder]] = {side: None for side in config.sides()}
        self._quoted_price: Dict[TradeType, Optional[Decimal]] = {side: None for side in config.sides()}

        # The side whose level triggered first, set the instant we commit rather than when
        # the fill lands, and the side that has actually filled.
        self._triggered_side: Optional[TradeType] = None
        self._filled_side: Optional[TradeType] = None
        self._entry_remainder_cancelled = False
        self._entry_placed_at: Dict[TradeType, float] = {}
        self._entry_crossed = False

        self._take_profit_order: Optional[TrackedOrder] = None
        self._close_order: Optional[TrackedOrder] = None
        self._failed_orders: List[TrackedOrder] = []

        # Orders already warned about in _order_filled_base, so the warning is logged once
        # each rather than on every tick.
        self._assumed_full_fills: List[str] = []

        self._current_retries = 0
        self._max_retries = max_retries

    # ------------------------------------------------------------------ properties

    @property
    def is_perpetual(self) -> bool:
        return self.is_perpetual_connector(self.config.connector_name)

    @property
    def is_trading(self) -> bool:
        return self.status == RunnableStatus.RUNNING and self.open_filled_amount > Decimal("0")

    @property
    def side(self) -> Optional[TradeType]:
        """
        The side actually taken.

        Falls back to the triggered side while a fill is in flight, and to the only side on
        offer when the leg is single-sided. None means the direction is still undecided.
        """
        if self._filled_side is not None:
            return self._filled_side
        if self._triggered_side is not None:
            return self._triggered_side
        sides = self.config.sides()
        return sides[0] if len(sides) == 1 else None

    @property
    def close_order_side(self) -> Optional[TradeType]:
        if self.side is None:
            return None
        return TradeType.SELL if self.side == TradeType.BUY else TradeType.BUY

    def _order_filled_base(self, order: Optional[TrackedOrder]) -> Decimal:
        """
        How much of an order filled, trusting the venue's terminal state when the fills
        themselves never arrived.

        executed_amount_base is populated only by trade updates, so a venue whose trade feed
        lags can report an order FILLED with zero executed amount. Every size derived from it
        then collapses to zero and no exit is armed while a real position sits on the venue.
        The completion event cannot help — it is built from the same field. A terminal FILLED
        state means the venue considers the order fully filled, so fall back to the requested
        amount and log the assumption.
        """
        if order is None:
            return Decimal("0")
        live = order.executed_amount_base or Decimal("0")
        if live > Decimal("0"):
            return live
        in_flight = order.order
        if in_flight is not None and in_flight.current_state == OrderState.FILLED \
                and in_flight.amount > Decimal("0"):
            if order.order_id not in self._assumed_full_fills:
                self._assumed_full_fills.append(order.order_id)
                self.logger().warning(
                    f"Executor ID: {self.config.id} - order {order.order_id} is FILLED but no "
                    f"fills arrived; assuming the full {in_flight.amount} at {in_flight.price}. "
                    f"Sizes and PnL for this leg are approximate.")
            return in_flight.amount
        return Decimal("0")

    def _order_avg_price(self, order: Optional[TrackedOrder]) -> Optional[Decimal]:
        """Average fill price, falling back to the order's own price when fills are missing."""
        if order is None:
            return None
        if order.executed_amount_base and order.executed_amount_base > Decimal("0"):
            price = order.average_executed_price
            if price and not price.is_nan() and price > Decimal("0"):
                return price
        in_flight = order.order
        if in_flight is not None and in_flight.current_state == OrderState.FILLED \
                and in_flight.price and in_flight.price > Decimal("0"):
            return in_flight.price
        return None

    def _executed_amount(self, side: TradeType) -> Decimal:
        return self._order_filled_base(self._entry_orders.get(side))

    @property
    def open_filled_amount(self) -> Decimal:
        """
        Net entry exposure in base units.

        Normally only one side ever fills. If a cancel loses a race and both sides get
        filled, the venue nets them, so the position we actually hold is the difference.
        """
        if self._filled_side is None:
            return Decimal("0")
        opposing = TradeType.SELL if self._filled_side == TradeType.BUY else TradeType.BUY
        net = self._executed_amount(self._filled_side) - self._executed_amount(opposing)
        if net <= Decimal("0"):
            return Decimal("0")
        return self.connectors[self.config.connector_name].quantize_order_amount(
            trading_pair=self.config.trading_pair, amount=net)

    @property
    def close_filled_amount(self) -> Decimal:
        return self._order_filled_base(self._close_order)

    @property
    def amount_to_close(self) -> Decimal:
        return self.open_filled_amount - self.close_filled_amount

    @property
    def entry_price(self) -> Decimal:
        """Average price actually paid, falling back to the price we are quoting at."""
        if self._filled_side is not None:
            price = self._order_avg_price(self._entry_orders[self._filled_side])
            if price is not None:
                return price
        return self._entry_target_price(self.side or TradeType.BUY)

    @property
    def close_price(self) -> Decimal:
        price = self._order_avg_price(self._close_order)
        if price is not None:
            return price
        return self._exit_reference_price()

    @property
    def open_filled_amount_quote(self) -> Decimal:
        return self.open_filled_amount * self.entry_price

    @property
    def close_filled_amount_quote(self) -> Decimal:
        return self.close_filled_amount * self.close_price

    @property
    def filled_amount_quote(self) -> Decimal:
        return self.open_filled_amount_quote + self.close_filled_amount_quote

    @property
    def trade_pnl_pct(self) -> Decimal:
        if self.open_filled_amount == Decimal("0") or self.close_type in [CloseType.FAILED]:
            return Decimal("0")
        if self.side == TradeType.BUY:
            return (self.close_price - self.entry_price) / self.entry_price
        return (self.entry_price - self.close_price) / self.entry_price

    @property
    def trade_pnl_quote(self) -> Decimal:
        return self.trade_pnl_pct * self.open_filled_amount * self.entry_price

    def get_net_pnl_quote(self) -> Decimal:
        return self.trade_pnl_quote - self.cum_fees_quote

    def get_net_pnl_pct(self) -> Decimal:
        if self.open_filled_amount_quote == Decimal("0"):
            return Decimal("0")
        return self.net_pnl_quote / self.open_filled_amount_quote

    def get_cum_fees_quote(self) -> Decimal:
        orders = list(self._entry_orders.values()) + [self._take_profit_order, self._close_order]
        return sum([order.cum_fees_quote for order in orders if order], Decimal("0"))

    @property
    def is_expired(self) -> bool:
        return self.end_time is not None and self.end_time <= self._strategy.current_timestamp

    @property
    def end_time(self) -> Optional[float]:
        if not self.config.barriers.time_limit:
            return None
        return self.config.timestamp + self.config.barriers.time_limit

    # ------------------------------------------------------------------ prices

    def _touch_price(self, side: TradeType) -> Decimal:
        """Passive price for the given side: best bid to buy, best ask to sell."""
        price_type = PriceType.BestBid if side == TradeType.BUY else PriceType.BestAsk
        return self.get_price(self.config.connector_name, self.config.trading_pair, price_type=price_type)

    def _entry_target_price(self, side: TradeType) -> Decimal:
        """
        The price level at which this side would be entered.

        Measured from the grid anchor when the controller supplies one, otherwise from the
        live touch price. The long level sits one step ABOVE the anchor and the short level
        one step BELOW it, because this strategy enters in the direction the market is
        already moving: we go long once the price has risen to our level, short once it has
        fallen to ours. A step of zero means enter here and now.
        """
        base = self.config.entry_price if self.config.entry_price is not None \
            else self._touch_price(side)
        if self.config.entry_offset_pct == Decimal("0"):
            return base
        if side == TradeType.BUY:
            return base * (1 + self.config.entry_offset_pct)
        return base * (1 - self.config.entry_offset_pct)

    def _exit_reference_price(self) -> Decimal:
        """
        The price the stop loss is measured against.

        BestBid/BestAsk are mapped to whichever side we would actually exit into, so a
        config of BestBid behaves sensibly on a short leg too. LastTrade and MidPrice are
        used as configured.
        """
        price_type = self.config.trigger_price_type
        if price_type in (PriceType.BestBid, PriceType.BestAsk):
            price_type = PriceType.BestBid if self.side == TradeType.BUY else PriceType.BestAsk
        price = self._usable_price(price_type)
        # Nothing usable: fall back to the entry price so callers get a number rather than a
        # NaN. control_stop_loss checks _usable_price itself and skips instead of comparing.
        return price if price is not None else self.entry_price

    def _usable_price(self, price_type: PriceType) -> Optional[Decimal]:
        """
        A price we can actually compare against, or None.

        LastTrade stays at NaN until a trade prints into the order book, and some venues
        never feed trades in at all — CoinDCX perpetuals among them. A NaN Decimal raises on
        comparison rather than returning False, so this falls back to the mid price and
        reports None only when nothing is usable. Never let a NaN reach a comparison: it
        either crashes the executor or, worse, silently decides the stop loss was not hit.
        """
        for candidate in (price_type, PriceType.MidPrice):
            try:
                price = self.get_price(self.config.connector_name, self.config.trading_pair,
                                       price_type=candidate)
            except Exception:
                continue
            if price is None:
                continue
            price = Decimal(price)
            if not price.is_nan() and price > Decimal("0"):
                return price
        return None

    @property
    def take_profit_price(self) -> Decimal:
        if self.side == TradeType.BUY:
            return self.entry_price * (1 + self.config.barriers.take_profit)
        return self.entry_price * (1 - self.config.barriers.take_profit)

    @property
    def stop_loss_price(self) -> Decimal:
        if self.side == TradeType.BUY:
            return self.entry_price * (1 - self.config.barriers.stop_loss)
        return self.entry_price * (1 + self.config.barriers.stop_loss)

    # ------------------------------------------------------------------ control loop

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            self._detect_entry_fill()
            if self._filled_side is None:
                self.control_entry_orders()
            else:
                self.control_barriers()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()
        self.evaluate_max_retries()

    async def on_start(self):
        await super().on_start()
        if self.is_expired:
            self.close_type = CloseType.EXPIRED
            self.stop()
            return
        if self.config.entry_mode == SimpleGridEntryMode.BOTH_OCO and not self.is_perpetual:
            self.logger().error(
                f"Executor ID: {self.config.id} - both_oco requires a perpetual connector; "
                f"{self.config.connector_name} cannot open a short. Stopping.")
            self.close_type = CloseType.FAILED
            self.stop()

    def evaluate_max_retries(self):
        if self._current_retries > self._max_retries:
            self.close_type = CloseType.FAILED
            self.stop()

    # ------------------------------------------------------------------ entry

    def _detect_entry_fill(self):
        """
        Latch the winning side.

        The fill event handler normally gets here first; this is the defensive path for a
        fill we only learn about through the in-flight order state.
        """
        if self._filled_side is not None:
            return
        for side, order in self._entry_orders.items():
            if self._order_filled_base(order) > Decimal("0"):
                self._on_entry_filled(side)
                return

    def _on_entry_filled(self, side: TradeType):
        if self._filled_side is not None:
            return
        self._filled_side = side
        self.logger().info(f"Executor ID: {self.config.id} - entry filled on {side}; cancelling the opposite side")
        self._cancel_losing_entries()

    def _cancel_losing_entries(self):
        """Cancel every entry order that is not the winning side."""
        for side, order in self._entry_orders.items():
            if side == self._filled_side:
                continue
            if order and order.order and order.order.is_open:
                self._cancel_order(order)

    def control_entry_orders(self):
        """
        Watch the entry levels and go with whichever the price reaches first.

        Nothing rests in the book while we wait. The levels sit on the far side of the
        market by design — a long entry above it, a short entry below — so a resting order
        there would fill instantly at the wrong price. We watch instead, and send the order
        once the market has actually got there.
        """
        if self.config.entry_timeout is not None and \
                self._strategy.current_timestamp - self.config.timestamp >= self.config.entry_timeout:
            self.logger().info(f"Executor ID: {self.config.id} - entry timed out, no level reached")
            self._give_up_on_entry()
            return

        if self._triggered_side is not None:
            # Direction already settled; the other level is off the table. The entry order
            # may still be sitting there unfilled though.
            self._cross_unfilled_entry_if_stale()
            return

        reference = self._usable_price(self.config.trigger_price_type)
        if reference is None:
            self.logger().warning(
                f"Executor ID: {self.config.id} - no usable {self.config.trigger_price_type.name} "
                f"price; not entering this tick")
            return
        for side in self.config.sides():
            if self._entry_orders[side] is not None:
                continue
            if self._entry_level_reached(side, reference):
                self.logger().info(
                    f"Executor ID: {self.config.id} - {side.name} level "
                    f"{self._entry_target_price(side)} reached at {reference}; entering")
                self.place_entry_order(side)
                # Direction is settled the moment one level triggers; the other is dropped.
                self._on_entry_triggered(side)
                return

    def _entry_level_reached(self, side: TradeType, reference: Decimal) -> bool:
        """
        A long triggers once the price has risen to its level, a short once it has fallen.

        With enter_on_either_level a long also triggers on the level below, because a
        buy-only chain has to be able to act on a fall as well as a rise.
        """
        level = self._entry_target_price(side)
        if side == TradeType.BUY:
            if reference >= level:
                return True
            return self.config.enter_on_either_level and reference <= self._opposite_level()
        return reference <= level

    def _opposite_level(self) -> Decimal:
        """The level a step the other way, used when a buy-only chain watches both."""
        base = self.config.entry_price
        if base is None:
            base = self._touch_price(TradeType.BUY)
        return base * (1 - self.config.entry_offset_pct)

    def _on_entry_triggered(self, side: TradeType):
        """
        Commit to a direction before the fill lands.

        Waiting for the fill event would leave the opposite level live for another tick,
        and in a fast move both could trigger.
        """
        if self._triggered_side is None:
            self._triggered_side = side

    def _cross_unfilled_entry_if_stale(self):
        """
        Take the price if a passive entry has waited too long.

        A resting order only fills when the market comes back to it. When the market is
        moving away — exactly the move a passive opening order is trying to join — it never
        does, and the leg would expire having done nothing. After the configured wait we
        cancel and cross the spread instead, accepting the taker fee to actually get in.
        """
        if self.config.entry_cross_after is None or self._entry_crossed:
            return
        side = self._triggered_side
        order = self._entry_orders.get(side)
        if order is None or order.order is None or not order.order.is_open:
            return
        if order.executed_amount_base > Decimal("0"):
            return  # already going in; let the fill path finish it
        placed_at = self._entry_placed_at.get(side)
        if placed_at is None or \
                self._strategy.current_timestamp - placed_at < self.config.entry_cross_after:
            return

        self.logger().info(
            f"Executor ID: {self.config.id} - passive {side.name} entry unfilled after "
            f"{self.config.entry_cross_after}s; crossing the spread")
        self._cancel_order(order)
        self._entry_crossed = True
        # Overwrites the tracked order, so the cancel event for the old id no longer
        # matches anything and cannot clear the new one.
        self.place_entry_order(side, order_type=OrderType.MARKET)

    def place_entry_order(self, side: TradeType, order_type: Optional[OrderType] = None):
        # The level is already through the market, so this order crosses. Price is only a
        # reference for limit types; market orders ignore it.
        price = self._entry_target_price(side)
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=order_type or self.config.entry_order_type,
            amount=self.config.amount,
            price=price,
            side=side,
            position_action=PositionAction.OPEN,
        )
        self._entry_orders[side] = TrackedOrder(order_id=order_id)
        self._quoted_price[side] = price
        self._entry_placed_at[side] = self._strategy.current_timestamp
        self.logger().debug(f"Executor ID: {self.config.id} - placed {side} entry {order_id} at {price}")

    def _give_up_on_entry(self):
        """No level was reached, so no position was ever opened and nothing needs unwinding."""
        for order in self._entry_orders.values():
            if order and order.order and order.order.is_open:
                self._cancel_order(order)
        self.close_type = CloseType.EXPIRED
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

    # ------------------------------------------------------------------ barriers

    def control_barriers(self):
        self._cancel_entry_remainder()
        self.control_time_limit()
        if self._status != RunnableStatus.RUNNING:
            return
        if self.open_filled_amount >= self.trading_rules.min_order_size and \
                self.open_filled_amount_quote >= self.trading_rules.min_notional_size:
            self.control_stop_loss()
            if self._status != RunnableStatus.RUNNING:
                return
            self.control_take_profit()

    def _cancel_entry_remainder(self):
        """
        Drop the unfilled part of the winning entry.

        Leaving it open would keep averaging the entry price while the position is live,
        which would move the take profit and stop loss under our feet. Guarded by a flag
        because the order stays open until the cancel is acknowledged, and re-sending on
        every tick would burn rate limit for nothing.
        """
        if not self.config.cancel_remainder_on_partial_fill or self._filled_side is None:
            return
        if self._entry_remainder_cancelled:
            return
        order = self._entry_orders[self._filled_side]
        if order and order.order and order.order.is_open:
            self._cancel_order(order)
            self._entry_remainder_cancelled = True

    def control_stop_loss(self):
        price_type = self.config.trigger_price_type
        if price_type in (PriceType.BestBid, PriceType.BestAsk):
            price_type = PriceType.BestBid if self.side == TradeType.BUY else PriceType.BestAsk
        reference = self._usable_price(price_type)
        if reference is None:
            # Skip rather than guess. Deciding "not breached" from missing data would leave a
            # live position unprotected without saying so.
            self.logger().warning(
                f"Executor ID: {self.config.id} - no usable price to check the stop loss "
                f"against; position is unprotected this tick")
            return
        stop_price = self.stop_loss_price
        breached = reference <= stop_price if self.side == TradeType.BUY else reference >= stop_price
        if breached:
            self.logger().info(
                f"Executor ID: {self.config.id} - stop loss hit ({reference} vs {stop_price})")
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

    def control_take_profit(self):
        if self._take_profit_order is None:
            self.place_take_profit_order()
        elif self._take_profit_order.order and self._take_profit_order.order.is_open:
            # A partial fill that lands after the take profit is placed leaves the resting
            # amount short of the position; re-issue it at the right size.
            if self._take_profit_order.order.amount != self.amount_to_close and \
                    self.amount_to_close >= self.trading_rules.min_order_size:
                self.renew_take_profit_order()

    def place_take_profit_order(self):
        if self.amount_to_close < self.trading_rules.min_order_size:
            return
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            amount=self.amount_to_close,
            price=self.take_profit_price,
            order_type=self.config.barriers.take_profit_order_type,
            position_action=PositionAction.CLOSE,
            side=self.close_order_side,
        )
        self._take_profit_order = TrackedOrder(order_id=order_id)
        self.logger().debug(
            f"Executor ID: {self.config.id} - placed take profit {order_id} at {self.take_profit_price}")

    def renew_take_profit_order(self):
        self._cancel_order(self._take_profit_order)
        self._take_profit_order = None
        self.place_take_profit_order()

    def control_time_limit(self):
        if self.is_expired:
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.TIME_LIMIT)

    # ------------------------------------------------------------------ closing

    def place_close_order_and_cancel_open_orders(self, close_type: CloseType, price: Decimal = Decimal("NaN")):
        self.cancel_open_orders()
        if self.amount_to_close >= self.trading_rules.min_order_size:
            order_id = self.place_order(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_type=OrderType.MARKET,
                amount=self.amount_to_close,
                price=price,
                side=self.close_order_side,
                position_action=PositionAction.CLOSE,
            )
            self._close_order = TrackedOrder(order_id=order_id)
            self.logger().debug(f"Executor ID: {self.config.id} - placed close order {order_id}")
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

    def cancel_open_orders(self):
        for order in list(self._entry_orders.values()) + [self._take_profit_order]:
            if order and order.order and order.order.is_open:
                self._cancel_order(order)

    def _cancel_order(self, order: TrackedOrder):
        self._strategy.cancel(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_id=order.order_id,
        )

    def all_orders_completed(self) -> bool:
        tracked = list(self._entry_orders.values()) + [self._take_profit_order, self._close_order]
        return all(order is None or order.is_done for order in tracked)

    def open_and_close_volume_match(self) -> bool:
        if self.open_filled_amount == Decimal("0"):
            return True
        if self._close_order is None:
            return False
        if self._close_order.is_filled:
            return True
        # is_filled reads executed_amount_base, which stays zero when the close order's trade
        # updates never arrived. Compare the amounts we trust, or shutdown never completes.
        return self.close_filled_amount >= self.open_filled_amount

    async def control_shutdown_process(self):
        self.close_timestamp = self._strategy.current_timestamp
        if self.all_orders_completed():
            if self.open_and_close_volume_match():
                self.stop()
            else:
                await self.control_close_order()
                self._current_retries += 1
        else:
            self.cancel_open_orders()
        await self._sleep(5.0)

    async def control_close_order(self):
        if self._close_order:
            in_flight_order = self.get_in_flight_order(self.config.connector_name, self._close_order.order_id) \
                if not self._close_order.order else self._close_order.order
            if in_flight_order:
                self._close_order.order = in_flight_order
                connector = self.connectors[self.config.connector_name]
                await connector._update_orders_with_error_handler(
                    orders=[in_flight_order],
                    error_handler=connector._handle_update_error_for_lost_order)
            else:
                self._failed_orders.append(self._close_order)
                self._close_order = None
        else:
            self.place_close_order_and_cancel_open_orders(close_type=self.close_type)

    def early_stop(self, keep_position: bool = False):
        self.close_type = CloseType.POSITION_HOLD if keep_position else CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    async def validate_sufficient_balance(self):
        """
        Check each side we might take on its own, never the sum of them.

        Only one entry order is ever sent: control_entry_orders latches _triggered_side on
        the first level reached and puts the other off the table, and nothing rests in the
        book before that. So the collateral needed is one leg's worth — but either side could
        be the one that triggers, so each has to be affordable individually.
        """
        for side in self.config.sides():
            price = self.config.entry_price if self.config.entry_price is not None else self._touch_price(side)
            if self.is_perpetual:
                candidate = PerpetualOrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self.config.entry_order_type,
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                    leverage=Decimal(self.config.leverage),
                )
            else:
                candidate = OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self.config.entry_order_type,
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                )
            adjusted = self.adjust_order_candidates(self.config.connector_name, [candidate])
            if any(entry.amount == Decimal("0") for entry in adjusted):
                self.close_type = CloseType.INSUFFICIENT_BALANCE
                self.logger().error(
                    f"Not enough budget to open the position: one {side.name} leg of "
                    f"{self.config.amount} at {price} does not fit.")
                self.stop()
                return

    # ------------------------------------------------------------------ events

    def _tracked_orders(self) -> List[Optional[TrackedOrder]]:
        return list(self._entry_orders.values()) + [self._take_profit_order, self._close_order]

    def update_tracked_orders_with_order_id(self, order_id: str):
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        for order in self._tracked_orders():
            if order and order.order_id == order_id:
                order.order = in_flight_order
                return

    def _entry_side_for_order_id(self, order_id: str) -> Optional[TradeType]:
        for side, order in self._entry_orders.items():
            if order and order.order_id == order_id:
                return side
        return None

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        self.update_tracked_orders_with_order_id(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._on_entry_filled(side)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._on_entry_filled(side)
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self.close_type = CloseType.TAKE_PROFIT
            self._close_order = self._take_profit_order
            self.close_timestamp = self._strategy.current_timestamp
            self._status = RunnableStatus.SHUTTING_DOWN

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            # Cancelling the unfilled remainder of an entry is routine. The tracked order
            # has to survive it, because its fills are the record of the position we hold.
            if self._order_filled_base(self._entry_orders[side]) == Decimal("0"):
                self._failed_orders.append(self._entry_orders[side])
                self._entry_orders[side] = None
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self._failed_orders.append(self._take_profit_order)
            self._take_profit_order = None
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._failed_orders.append(self._entry_orders[side])
            self._entry_orders[side] = None
            self._current_retries += 1
            self.logger().error(f"Entry order failed {event.order_id}. "
                                f"Retrying {self._current_retries}/{self._max_retries}")
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self._failed_orders.append(self._take_profit_order)
            self._take_profit_order = None
            self.logger().error(f"Take profit order failed {event.order_id}.")
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None
            self._current_retries += 1
            self.logger().error(f"Close order failed {event.order_id}. "
                                f"Retrying {self._current_retries}/{self._max_retries}")

    # ------------------------------------------------------------------ reporting

    def get_custom_info(self) -> Dict:
        return {
            "level_id": self.config.level_id,
            "side": self.side,
            "entry_mode": self.config.entry_mode,
            "entry_price": self.entry_price,
            # The controller re-anchors the grid on this pair of fields.
            "close_price": self.close_price,
            "close_type": self.close_type,
            "take_profit_price": self.take_profit_price if self.side else None,
            "stop_loss_price": self.stop_loss_price if self.side else None,
            "trigger_price_type": self.config.trigger_price_type,
            "entry_levels": {s.name: self._entry_target_price(s) for s in self.config.sides()},
            "filled_amount": self.open_filled_amount,
            "current_retries": self._current_retries,
            "max_retries": self._max_retries,
            "order_ids": [order.order_id for order in self._tracked_orders() if order],
            "held_position_orders": self._held_position_orders,
        }

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
