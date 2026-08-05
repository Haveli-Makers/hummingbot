import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
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
        self._first_quoted_price: Dict[TradeType, Optional[Decimal]] = {side: None for side in config.sides()}
        self._repost_count: Dict[TradeType, int] = {side: 0 for side in config.sides()}
        self._last_repost_timestamp: Dict[TradeType, float] = {side: 0.0 for side in config.sides()}

        # The side that won the race; None until something fills.
        self._filled_side: Optional[TradeType] = None
        self._entry_remainder_cancelled = False

        self._take_profit_order: Optional[TrackedOrder] = None
        self._close_order: Optional[TrackedOrder] = None
        self._failed_orders: List[TrackedOrder] = []

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
        """The side actually taken, or the only side on offer if nothing has filled yet."""
        if self._filled_side is not None:
            return self._filled_side
        sides = self.config.sides()
        return sides[0] if len(sides) == 1 else None

    @property
    def close_order_side(self) -> Optional[TradeType]:
        if self.side is None:
            return None
        return TradeType.SELL if self.side == TradeType.BUY else TradeType.BUY

    def _executed_amount(self, side: TradeType) -> Decimal:
        order = self._entry_orders.get(side)
        return order.executed_amount_base if order else Decimal("0")

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
        return self._close_order.executed_amount_base if self._close_order else Decimal("0")

    @property
    def amount_to_close(self) -> Decimal:
        return self.open_filled_amount - self.close_filled_amount

    @property
    def entry_price(self) -> Decimal:
        """Average price actually paid, falling back to the price we are quoting at."""
        if self._filled_side is not None:
            order = self._entry_orders[self._filled_side]
            if order and order.executed_amount_base > Decimal("0"):
                return order.average_executed_price
        if self.config.entry_price is not None:
            return self.config.entry_price
        side = self.side or TradeType.BUY
        return self._touch_price(side)

    @property
    def close_price(self) -> Decimal:
        if self._close_order and self._close_order.executed_amount_base > Decimal("0"):
            return self._close_order.average_executed_price
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
        return self.get_price(self.config.connector_name, self.config.trading_pair, price_type=price_type)

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
            if order and order.executed_amount_base > Decimal("0"):
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
        if self.config.entry_timeout is not None and \
                self._strategy.current_timestamp - self.config.timestamp >= self.config.entry_timeout:
            self.logger().info(f"Executor ID: {self.config.id} - entry timed out with no fill")
            self._give_up_on_entry()
            return

        for side in self.config.sides():
            order = self._entry_orders[side]
            if order is None:
                self.place_entry_order(side)
            elif self.config.chase_entry:
                self._maybe_repost_entry(side)

    def place_entry_order(self, side: TradeType):
        price = self.config.entry_price if self.config.entry_price is not None else self._touch_price(side)
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.entry_order_type,
            amount=self.config.amount,
            price=price,
            side=side,
            position_action=PositionAction.OPEN,
        )
        self._entry_orders[side] = TrackedOrder(order_id=order_id)
        self._quoted_price[side] = price
        if self._first_quoted_price[side] is None:
            self._first_quoted_price[side] = price
        self._last_repost_timestamp[side] = self._strategy.current_timestamp
        self.logger().debug(f"Executor ID: {self.config.id} - placed {side} entry {order_id} at {price}")

    def _maybe_repost_entry(self, side: TradeType):
        """Re-place the entry when the touch price has drifted away from our quote."""
        order = self._entry_orders[side]
        if order is None or order.order is None or not order.order.is_open:
            return
        if order.executed_amount_base > Decimal("0"):
            return  # partially filled: the fill path owns this order now

        now = self._strategy.current_timestamp
        if now - self._last_repost_timestamp[side] < self.config.min_repost_interval:
            return

        quoted = self._quoted_price[side]
        touch = self._touch_price(side)
        if quoted is None or quoted == Decimal("0"):
            return
        if abs(touch - quoted) / quoted < self.config.entry_repost_threshold:
            return

        if self._max_reposts_reached(side) or self._drift_cap_exceeded(side, touch):
            self._give_up_on_entry()
            return

        self._cancel_order(order)
        self._entry_orders[side] = None
        self._repost_count[side] += 1
        self._last_repost_timestamp[side] = now
        self.logger().debug(
            f"Executor ID: {self.config.id} - {side} touch moved {quoted} -> {touch}, re-quoting "
            f"({self._repost_count[side]} re-posts)")

    def _max_reposts_reached(self, side: TradeType) -> bool:
        return self.config.max_entry_reposts is not None and \
            self._repost_count[side] >= self.config.max_entry_reposts

    def _drift_cap_exceeded(self, side: TradeType, touch: Decimal) -> bool:
        """
        True once the market has run far enough from our first quote that chasing it would
        mean entering at a materially worse price than the leg was sized for.
        """
        if self.config.max_entry_drift is None:
            return False
        first = self._first_quoted_price[side]
        if first is None or first == Decimal("0"):
            return False
        return abs(touch - first) / first > self.config.max_entry_drift

    def _give_up_on_entry(self):
        """No position was ever opened, so there is nothing to unwind."""
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
        reference = self._exit_reference_price()
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
        return self._close_order is not None and self._close_order.is_filled

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
        # In both_oco only one side can end up filled, but both are live until one does,
        # so the balance check has to cover every side we are about to quote.
        candidates = []
        for side in self.config.sides():
            price = self.config.entry_price if self.config.entry_price is not None else self._touch_price(side)
            if self.is_perpetual:
                candidates.append(PerpetualOrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self.config.entry_order_type,
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                    leverage=Decimal(self.config.leverage),
                ))
            else:
                candidates.append(OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self.config.entry_order_type,
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                ))
        adjusted = self.adjust_order_candidates(self.config.connector_name, candidates)
        if any(candidate.amount == Decimal("0") for candidate in adjusted):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open the position.")
            self.stop()

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
            if self._entry_orders[side].executed_amount_base == Decimal("0"):
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
            "entry_reposts": dict(self._repost_count),
            "filled_amount": self.open_filled_amount,
            "current_retries": self._current_retries,
            "max_retries": self._max_retries,
            "order_ids": [order.order_id for order in self._tracked_orders() if order],
            "held_position_orders": self._held_position_orders,
        }

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
