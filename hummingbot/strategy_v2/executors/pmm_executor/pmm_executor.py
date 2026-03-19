import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PositionAction, PriceType, TradeType
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
from hummingbot.strategy_v2.executors.pmm_executor.data_types import PMMExecutorConfig, PMMLevel, PMMOrderState
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class PMMExecutor(ExecutorBase):
    """
    Pure Market Making Executor.

    Places symmetric buy and sell limit orders at specified percentage distances from a fair price.
    When any order fills, it is automatically re-places at the same price level.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: PMMExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        """
        Initialize the PMMExecutor.

        :param strategy: The strategy instance.
        :param config: The PMM executor configuration.
        :param update_interval: Control loop interval in seconds.
        :param max_retries: Maximum retries before failing.
        """
        self.config: PMMExecutorConfig = config
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)

        self.trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)

        self.fair_price = self.get_fair_price()
        self.mid_price = self.fair_price

        self.pmm_levels = self._generate_levels()

        self._filled_orders: List[dict] = []
        self._failed_orders: List[str] = []
        self._canceled_orders: List[str] = []
        self._close_order: Optional[TrackedOrder] = None

        self._level_failures: Dict[str, tuple] = {}
        self._max_level_failures = 5
        self._level_failure_cooldown = 60
        self._level_insufficient_funds: Dict[str, bool] = {} 

        self.total_buy_quote = Decimal("0")
        self.total_sell_quote = Decimal("0")
        self.total_fees_quote = Decimal("0")
        self.net_inventory_base = Decimal("0")
        self.net_inventory_quote = Decimal("0")
        self.realized_pnl_quote = Decimal("0")
        self.realized_pnl_pct = Decimal("0")
        self.unrealized_pnl_quote = Decimal("0")
        self.open_buy_liquidity_quote = Decimal("0")
        self.open_sell_liquidity_quote = Decimal("0")

        self.max_order_creation_timestamp = 0
        self._current_retries = 0
        self._max_retries = max_retries
        self._last_refresh_timestamp: float = 0
        self._refresh_cooldown: float = 1.5  
        self._refresh_pending_levels: List[int] = [] 
        self._refresh_canceling_level: Optional[int] = None  

    def get_fair_price(self) -> Decimal:
        """Calculate the fair price based on config: use explicit fair_price if set, otherwise fetch by fair_price_type."""
        if self.config.fair_price is not None:
            return self.config.fair_price
        return self.get_price(self.config.connector_name, self.config.trading_pair, self.config.fair_price_type)

    @property
    def is_perpetual(self) -> bool:
        """Check if the exchange connector is perpetual."""
        return self.is_perpetual_connector(self.config.connector_name)

    def _generate_levels(self) -> List[PMMLevel]:
        """Generate PMM levels from the configuration."""
        levels = []
        for i, (spread, amount) in enumerate(zip(self.config.spread_percentages, self.config.order_amounts_quote)):
            levels.append(
                PMMLevel(
                    id=f"L{i}",
                    spread_pct=spread,
                    amount_quote=amount,
                )
            )
        self.logger().info(
            f"Created {len(levels)} PMM levels | "
            f"spreads: {[f'{float(s) * 100:.2f}%' for s in self.config.spread_percentages]} | "
            f"fair price: {self.fair_price} | "
            f"buy prices: {[float(lv.get_buy_price(self.fair_price)) for lv in levels]} | "
            f"sell prices: {[float(lv.get_sell_price(self.fair_price)) for lv in levels]}"
        )
        return levels

    async def validate_sufficient_balance(self):
        """Validate that there is enough balance to place orders on both sides (buy and sell)."""
        mid_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)

        total_amount_quote = sum(self.config.order_amounts_quote)
        total_amount_base = total_amount_quote / mid_price

        if self.is_perpetual:
            buy_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.order_type.is_limit_type(),
                order_type=self.config.order_type,
                order_side=TradeType.BUY,
                amount=total_amount_base,
                price=mid_price,
                leverage=Decimal(self.config.leverage),
            )
        else:
            buy_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.order_type.is_limit_type(),
                order_type=self.config.order_type,
                order_side=TradeType.BUY,
                amount=total_amount_base,
                price=mid_price,
            )
        adjusted_buy = self.adjust_order_candidates(self.config.connector_name, [buy_candidate])
        if adjusted_buy[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough quote balance to open PMM buy positions.")
            self.stop()
            return

        if not self.is_perpetual:
            sell_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.order_type.is_limit_type(),
                order_type=self.config.order_type,
                order_side=TradeType.SELL,
                amount=total_amount_base,
                price=mid_price,
            )
            adjusted_sell = self.adjust_order_candidates(self.config.connector_name, [sell_candidate])
            if adjusted_sell[0].amount == Decimal("0"):
                self.logger().warning(
                    f"Not enough base balance for all sell levels. "
                    f"Need ~{total_amount_base:.4f} {self.config.trading_pair.split('-')[0]} "
                    f"but have less. Sells will be placed only up to available balance."
                )

    @property
    def end_time(self) -> Optional[float]:
        """Calculate the end time based on the time limit."""
        if not self.config.time_limit:
            return None
        return self.config.timestamp + self.config.time_limit

    @property
    def is_expired(self) -> bool:
        """Check if the executor has exceeded its time limit."""
        return self.end_time is not None and self.end_time <= self._strategy.current_timestamp

    @property
    def is_trading(self):
        return self.status == RunnableStatus.RUNNING

    @property
    def is_active(self):
        return self._status in [RunnableStatus.RUNNING, RunnableStatus.NOT_STARTED, RunnableStatus.SHUTTING_DOWN]

    # ─── Control Loop ────────────────────────────────────────────────────

    async def control_task(self):
        """Main control loop: update metrics, refresh fair price, process fills, manage orders or shutdown."""
        self.update_metrics()
        self.process_filled_orders()

        if self.status == RunnableStatus.RUNNING:
            if self.check_barriers():
                self.cancel_all_orders()
                self._status = RunnableStatus.SHUTTING_DOWN
                return
            if self.config.fair_price is None:
                self.refresh_orders_on_price_change()
            self.manage_orders()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

        self.evaluate_max_retries()

    def check_barriers(self) -> bool:
        """Check risk management barriers (stop loss, time limit)."""
        if self.stop_loss_condition():
            self.close_type = CloseType.STOP_LOSS
            return True
        elif self.is_expired:
            self.close_type = CloseType.TIME_LIMIT
            return True
        return False

    def stop_loss_condition(self) -> bool:
        """Check if the net PnL has breached the stop loss threshold."""
        if self.config.stop_loss:
            net_pnl_pct = self.get_net_pnl_pct()
            return net_pnl_pct <= -self.config.stop_loss
        return False

    # ─── Filled Order Processing ─────────────────────────────────────────

    def process_filled_orders(self):
        """
        Detect filled orders, record them, and switch the level to place both buy and sell.

        When any order fills at a level, both a new buy and sell are placed at that level's
        spread percentage from fair price (subject to available balance).
        """
        for level in self.pmm_levels:
            if level.buy_state == PMMOrderState.ORDER_FILLED and level.buy_order:
                if level.buy_order.order:
                    order_json = level.buy_order.order.to_json()
                else:
                    order_json = {"trade_type": "BUY"}

                if Decimal(order_json.get("executed_amount_base", "0")) == Decimal("0"):
                    self.logger().warning(
                        f"PMM buy filled but amounts are 0 for level={level.id} "
                        f"(fill data arrived late). Using order amount for tracking."
                    )
                    buy_price = level.get_buy_price(self.fair_price)
                    amount_base = level.amount_quote / buy_price
                    order_json["executed_amount_base"] = str(amount_base)
                    order_json["executed_amount_quote"] = str(level.amount_quote)
                    order_json.setdefault("trade_type", "BUY")
                if "cumulative_fee_paid_quote" not in order_json or order_json["cumulative_fee_paid_quote"] == "0":
                    order_json["cumulative_fee_paid_quote"] = "0"

                self._filled_orders.append(order_json)
                self.logger().info(
                    f"PMM buy filled | level={level.id} "
                    f"spread={float(level.spread_pct) * 100:.2f}% "
                    f"price={level.buy_order.average_executed_price} | "
                    f"Re-placing BUY at {level.get_buy_price(self.fair_price):.4f} "
                    f"and SELL at {level.get_sell_price(self.fair_price):.4f}"
                )
                level.reset_buy_order()
                level.pending_side = "both"
                for key in list(self._level_insufficient_funds):
                    if key.endswith("_sell"):
                        del self._level_insufficient_funds[key]

            if level.sell_state == PMMOrderState.ORDER_FILLED and level.sell_order:
                if level.sell_order.order:
                    order_json = level.sell_order.order.to_json()
                else:
                    order_json = {"trade_type": "SELL"}

                if Decimal(order_json.get("executed_amount_base", "0")) == Decimal("0"):
                    self.logger().warning(
                        f"PMM sell filled but amounts are 0 for level={level.id} "
                        f"(fill data arrived late). Using order amount for tracking."
                    )
                    sell_price = level.get_sell_price(self.fair_price)
                    amount_base = level.amount_quote / sell_price
                    order_json["executed_amount_base"] = str(amount_base)
                    order_json["executed_amount_quote"] = str(level.amount_quote)
                    order_json.setdefault("trade_type", "SELL")
                if "cumulative_fee_paid_quote" not in order_json or order_json["cumulative_fee_paid_quote"] == "0":
                    order_json["cumulative_fee_paid_quote"] = "0"

                self._filled_orders.append(order_json)
                self.logger().info(
                    f"PMM sell filled | level={level.id} "
                    f"spread={float(level.spread_pct) * 100:.2f}% "
                    f"price={level.sell_order.average_executed_price} | "
                    f"Re-placing BUY at {level.get_buy_price(self.fair_price):.4f} "
                    f"and SELL at {level.get_sell_price(self.fair_price):.4f}"
                )
                level.reset_sell_order()
                level.pending_side = "both"
                for key in list(self._level_insufficient_funds):
                    if key.endswith("_buy"):
                        del self._level_insufficient_funds[key]

    # ─── Fair Price Refresh ────────────────────────────────────────────

    def refresh_orders_on_price_change(self):
        """Staggered refresh: cancel and replace ONE level per tick to minimize downtime.

        When price moves beyond tolerance, all levels are queued for refresh. Each tick
        cancels the next pending level. When the cancel confirms (via process_order_canceled_event),
        the level is reset to NOT_ACTIVE, and manage_orders() places the replacement immediately.
        """
        if self._refresh_pending_levels or self._refresh_canceling_level is not None:
            self._cancel_next_pending_level()
            return

        new_fair_price = self.get_fair_price()
        if self.fair_price == Decimal("0"):
            self.fair_price = new_fair_price
            return

        price_change_pct = abs(new_fair_price - self.fair_price) / self.fair_price
        self.logger().debug(
            f"PMM: Price check | stored={self.fair_price:.4f} | current={new_fair_price:.4f} | "
            f"change={float(price_change_pct) * 100:.4f}% | tolerance={float(self.config.price_refresh_tolerance) * 100:.4f}%"
        )
        if price_change_pct <= self.config.price_refresh_tolerance:
            return

        now = self._strategy.current_timestamp
        if now - self._last_refresh_timestamp < self._refresh_cooldown:
            return

        self.logger().info(
            f"PMM: Fair price changed {self.fair_price} → {new_fair_price} "
            f"({float(price_change_pct) * 100:.4f}% > {float(self.config.price_refresh_tolerance) * 100:.4f}% tolerance). "
            f"Starting staggered refresh."
        )
        self.fair_price = new_fair_price
        self._last_refresh_timestamp = now

        for level in self.pmm_levels:
            self.logger().info(
                f"PMM: Level {level.id} new prices | "
                f"buy: {level.get_buy_price(self.fair_price):.4f} | "
                f"sell: {level.get_sell_price(self.fair_price):.4f}"
            )

        self._refresh_pending_levels = [
            i for i, level in enumerate(self.pmm_levels)
            if (level.buy_order and level.buy_state == PMMOrderState.ORDER_PLACED)
            or (level.sell_order and level.sell_state == PMMOrderState.ORDER_PLACED)
        ]
        self._refresh_canceling_level = None
        self._cancel_next_pending_level()

    def _cancel_next_pending_level(self):
        """Cancel orders for the next level in the refresh queue."""
        if self._refresh_canceling_level is not None:
            idx = self._refresh_canceling_level
            level = self.pmm_levels[idx]
            buy_done = level.buy_state != PMMOrderState.ORDER_PLACED
            sell_done = level.sell_state != PMMOrderState.ORDER_PLACED
            if not (buy_done and sell_done):
                return 

        if not self._refresh_pending_levels:
            self._refresh_canceling_level = None
            return

        idx = self._refresh_pending_levels.pop(0)
        self._refresh_canceling_level = idx
        level = self.pmm_levels[idx]

        if level.buy_order and level.buy_state == PMMOrderState.ORDER_PLACED:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=level.buy_order.order_id,
            )
            self.logger().debug(f"PMM: Refresh canceling buy for {level.id}")

        if level.sell_order and level.sell_state == PMMOrderState.ORDER_PLACED:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=level.sell_order.order_id,
            )
            self.logger().debug(f"PMM: Refresh canceling sell for {level.id}")

    # ─── Order Management ────────────────────────────────────────────────

    def _is_level_on_cooldown(self, level_id: str) -> bool:
        """Check if a level is in failure cooldown or permanently disabled due to insufficient funds."""
        if self._level_insufficient_funds.get(level_id, False):
            return True
        if level_id not in self._level_failures:
            return False
        count, last_ts = self._level_failures[level_id]
        if count >= self._max_level_failures:
            elapsed = self._strategy.current_timestamp - last_ts
            if elapsed < self._level_failure_cooldown:
                return True
            self._level_failures[level_id] = (0, 0)
        return False

    def _record_level_failure(self, level_id: str):
        """Record a failure for a level."""
        count, _ = self._level_failures.get(level_id, (0, 0))
        self._level_failures[level_id] = (count + 1, self._strategy.current_timestamp)

    def manage_orders(self):
        """
        Place orders for levels that don't have active orders, respecting pending_side.

        Grid alternation:
          - pending_side="buy"  → only place a buy order
          - pending_side="sell" → only place a sell order
          - pending_side="both" → place both buy and sell
        """
        if self.max_order_creation_timestamp > self._strategy.current_timestamp - self.config.order_frequency:
            return

        orders_placed = 0
        max_batch = self.config.max_orders_per_batch or (len(self.pmm_levels) * 2)

        refreshing_indices = set(self._refresh_pending_levels)
        if self._refresh_canceling_level is not None:
            refreshing_indices.add(self._refresh_canceling_level)

        for i, level in enumerate(self.pmm_levels):
            if orders_placed >= max_batch:
                break

            if i in refreshing_indices:
                continue

            should_place_buy = (
                level.pending_side in ("buy", "both")
                and level.buy_state == PMMOrderState.NOT_ACTIVE
                and not self._is_level_on_cooldown(f"{level.id}_buy")
            )
            should_place_sell = (
                level.pending_side in ("sell", "both")
                and level.sell_state == PMMOrderState.NOT_ACTIVE
                and not self._is_level_on_cooldown(f"{level.id}_sell")
            )

            if should_place_buy:
                self._place_buy_order(level)
                orders_placed += 1

            if orders_placed >= max_batch:
                break

            if should_place_sell:
                self._place_sell_order(level)
                orders_placed += 1

        if orders_placed > 0:
            self.max_order_creation_timestamp = self._strategy.current_timestamp

    def _place_buy_order(self, level: PMMLevel):
        """Place a buy order for the given level at fair_price * (1 - spread_pct)."""
        buy_price = level.get_buy_price(self.fair_price)
        amount_base = level.amount_quote / buy_price

        best_bid = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestBid)
        if buy_price >= best_bid:
            buy_price = best_bid * (1 - self.config.safe_extra_spread)

        min_notional = max(self.config.min_order_amount_quote, self.trading_rules.min_notional_size)
        notional = amount_base * buy_price
        if notional < min_notional:
            self.logger().debug(
                f"PMM: Skipping buy L{level.id} — notional {notional:.2f} < min {min_notional:.2f}"
            )
            self._record_level_failure(f"{level.id}_buy")
            return

        order_candidate = self._create_order_candidate(
            side=TradeType.BUY,
            amount=amount_base,
            price=buy_price,
        )
        adjusted = self.adjust_order_candidates(self.config.connector_name, [order_candidate])
        if adjusted:
            order_candidate = adjusted[0]

        adjusted_notional = order_candidate.amount * order_candidate.price
        if order_candidate.amount <= Decimal("0") or adjusted_notional < min_notional:
            self.logger().debug(
                f"PMM: Skipping buy L{level.id} — adjusted notional {adjusted_notional:.2f} < min {min_notional:.2f} "
                f"(insufficient quote balance to place buy order)"
            )
            self._record_level_failure(f"{level.id}_buy")
            return

        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.order_type,
            amount=order_candidate.amount,
            price=order_candidate.price,
            side=TradeType.BUY,
            position_action=PositionAction.OPEN if self.is_perpetual else PositionAction.NIL,
        )
        level.buy_order = TrackedOrder(order_id=order_id)
        self.logger().info(
            f"PMM: Placed BUY order {order_id} | level={level.id} | "
            f"price={order_candidate.price:.4f} | amount={order_candidate.amount:.4f} | "
            f"fair_price={self.fair_price:.4f} | spread={float(level.spread_pct) * 100:.2f}%"
        )

    def _place_sell_order(self, level: PMMLevel):
        """Place a sell order for the given level at fair_price * (1 + spread_pct)."""
        sell_price = level.get_sell_price(self.fair_price)
        amount_base = level.amount_quote / sell_price

        best_ask = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestAsk)
        if sell_price <= best_ask:
            sell_price = best_ask * (1 + self.config.safe_extra_spread)

        min_notional = max(self.config.min_order_amount_quote, self.trading_rules.min_notional_size)
        notional = amount_base * sell_price
        if notional < min_notional:
            self.logger().debug(
                f"PMM: Skipping sell L{level.id} — notional {notional:.2f} < min {min_notional:.2f}"
            )
            self._record_level_failure(f"{level.id}_sell")
            return

        order_candidate = self._create_order_candidate(
            side=TradeType.SELL,
            amount=amount_base,
            price=sell_price,
        )
        adjusted = self.adjust_order_candidates(self.config.connector_name, [order_candidate])
        if adjusted:
            order_candidate = adjusted[0]

        adjusted_notional = order_candidate.amount * order_candidate.price
        if order_candidate.amount <= Decimal("0") or adjusted_notional < min_notional:
            self.logger().debug(
                f"PMM: Skipping sell L{level.id} — adjusted notional {adjusted_notional:.2f} < min {min_notional:.2f} "
                f"(insufficient base balance to place sell order — need {amount_base:.4f} {self.config.trading_pair.split('-')[0]})"
            )
            self._record_level_failure(f"{level.id}_sell")
            return

        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self.config.order_type,
            amount=order_candidate.amount,
            price=order_candidate.price,
            side=TradeType.SELL,
            position_action=PositionAction.OPEN if self.is_perpetual else PositionAction.NIL,
        )
        level.sell_order = TrackedOrder(order_id=order_id)
        self.logger().info(
            f"PMM: Placed SELL order {order_id} | level={level.id} | "
            f"price={order_candidate.price:.4f} | amount={order_candidate.amount:.4f} | "
            f"fair_price={self.fair_price:.4f} | spread={float(level.spread_pct) * 100:.2f}%"
        )

    def _create_order_candidate(self, side: TradeType, amount: Decimal, price: Decimal):
        """Create an OrderCandidate (or PerpetualOrderCandidate) for the given parameters."""
        if self.is_perpetual:
            return PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=self.config.order_type.is_limit_type(),
                order_type=self.config.order_type,
                order_side=side,
                amount=amount,
                price=price,
                leverage=Decimal(self.config.leverage),
            )
        return OrderCandidate(
            trading_pair=self.config.trading_pair,
            is_maker=self.config.order_type.is_limit_type(),
            order_type=self.config.order_type,
            order_side=side,
            amount=amount,
            price=price,
        )

    # ─── Order Cancellation ──────────────────────────────────────────────

    def cancel_all_orders(self):
        """Cancel all active buy and sell orders across all levels."""
        for level in self.pmm_levels:
            if level.buy_order and level.buy_state == PMMOrderState.ORDER_PLACED:
                self._strategy.cancel(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_id=level.buy_order.order_id
                )
                self.logger().debug(f"PMM: Canceling buy order {level.buy_order.order_id}")
            if level.sell_order and level.sell_state == PMMOrderState.ORDER_PLACED:
                self._strategy.cancel(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_id=level.sell_order.order_id
                )
                self.logger().debug(f"PMM: Canceling sell order {level.sell_order.order_id}")

    def early_stop(self, keep_position: bool = False):
        """Stop the executor early, canceling all open orders."""
        self.cancel_all_orders()
        self._status = RunnableStatus.SHUTTING_DOWN
        self.close_type = CloseType.EARLY_STOP

    # ─── Shutdown ────────────────────────────────────────────────────────

    async def control_shutdown_process(self):
        """
        Handle the shutdown process:
        1. Cancel all remaining open orders
        2. Stop the executor
        """
        self.close_timestamp = self._strategy.current_timestamp

        active_orders = [
            order for level in self.pmm_levels
            for order in [level.buy_order, level.sell_order]
            if order is not None and not order.is_done and not order.is_filled
        ]

        if len(active_orders) == 0:
            self.update_realized_pnl_metrics()
            self.logger().info(
                f"PMM: Shutdown complete | net inventory: {self.net_inventory_base} base"
            )
            self.stop()
        else:
            self.cancel_all_orders()
            self._current_retries += 1

        await self._sleep(5.0)

    # ─── Metrics ─────────────────────────────────────────────────────────

    def update_metrics(self):
        """Update mid price and all PnL/inventory metrics."""
        self.mid_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        self.update_realized_pnl_metrics()
        self.update_inventory_metrics()

    def update_inventory_metrics(self):
        """Track open buy/sell liquidity across all levels."""
        self.open_buy_liquidity_quote = sum(
            level.amount_quote for level in self.pmm_levels
            if level.buy_state == PMMOrderState.ORDER_PLACED
            and level.buy_order
            and level.buy_order.executed_amount_base == Decimal("0")
        )
        self.open_sell_liquidity_quote = sum(
            level.amount_quote for level in self.pmm_levels
            if level.sell_state == PMMOrderState.ORDER_PLACED
            and level.sell_order
            and level.sell_order.executed_amount_base == Decimal("0")
        )

    def update_realized_pnl_metrics(self):
        """Calculate realized and unrealized PnL from filled orders."""
        if len(self._filled_orders) == 0:
            self._reset_metrics()
            return

        self.total_buy_quote = sum(
            Decimal(order["executed_amount_quote"])
            for order in self._filled_orders if order["trade_type"] == TradeType.BUY.name
        )
        self.total_sell_quote = sum(
            Decimal(order["executed_amount_quote"])
            for order in self._filled_orders if order["trade_type"] == TradeType.SELL.name
        )
        self.total_fees_quote = sum(
            Decimal(order["cumulative_fee_paid_quote"])
            for order in self._filled_orders
        )

        buy_amount_base = sum(
            Decimal(order["executed_amount_base"])
            for order in self._filled_orders if order["trade_type"] == TradeType.BUY.name
        )
        sell_amount_base = sum(
            Decimal(order["executed_amount_base"])
            for order in self._filled_orders if order["trade_type"] == TradeType.SELL.name
        )

        self.net_inventory_base = buy_amount_base - sell_amount_base
        self.net_inventory_quote = self.net_inventory_base * self.mid_price

        self.realized_pnl_quote = (
            self.total_sell_quote - self.total_buy_quote - self.total_fees_quote
        )

        if buy_amount_base > 0 and self.net_inventory_base > 0:
            avg_buy_price = self.total_buy_quote / buy_amount_base
            self.unrealized_pnl_quote = self.net_inventory_base * (self.mid_price - avg_buy_price)
        elif sell_amount_base > 0 and self.net_inventory_base < 0:
            avg_sell_price = self.total_sell_quote / sell_amount_base
            self.unrealized_pnl_quote = abs(self.net_inventory_base) * (avg_sell_price - self.mid_price)
        else:
            self.unrealized_pnl_quote = Decimal("0")

        total_volume = self.total_buy_quote + self.total_sell_quote
        self.realized_pnl_pct = self.realized_pnl_quote / total_volume if total_volume > 0 else Decimal("0")

    def _reset_metrics(self):
        """Reset all PnL metrics to zero."""
        self.total_buy_quote = Decimal("0")
        self.total_sell_quote = Decimal("0")
        self.total_fees_quote = Decimal("0")
        self.realized_pnl_quote = Decimal("0")
        self.realized_pnl_pct = Decimal("0")
        self.unrealized_pnl_quote = Decimal("0")
        self.net_inventory_base = Decimal("0")
        self.net_inventory_quote = Decimal("0")

    # ─── PnL Interface ───────────────────────────────────────────────────

    def get_net_pnl_quote(self) -> Decimal:
        """Net PnL = realized + unrealized."""
        return self.realized_pnl_quote + self.unrealized_pnl_quote

    def get_net_pnl_pct(self) -> Decimal:
        """Net PnL as a percentage of total volume."""
        total_volume = self.total_buy_quote + self.total_sell_quote
        return self.get_net_pnl_quote() / total_volume if total_volume > 0 else Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        return self.total_fees_quote

    @property
    def filled_amount_quote(self) -> Decimal:
        return self.total_buy_quote + self.total_sell_quote

    # ─── Custom Info ─────────────────────────────────────────────────────

    def get_custom_info(self) -> Dict:
        return {
            "fair_price": self.fair_price,
            "levels": [
                {
                    "id": level.id,
                    "spread_pct": float(level.spread_pct),
                    "amount_quote": float(level.amount_quote),
                    "buy_price": float(level.get_buy_price(self.fair_price)),
                    "sell_price": float(level.get_sell_price(self.fair_price)),
                    "buy_state": level.buy_state.value,
                    "sell_state": level.sell_state.value,
                    "pending_side": level.pending_side,
                }
                for level in self.pmm_levels
            ],
            "filled_orders": self._filled_orders,
            "failed_orders": self._failed_orders,
            "canceled_orders": self._canceled_orders,
            "total_buy_quote": self.total_buy_quote,
            "total_sell_quote": self.total_sell_quote,
            "total_fees_quote": self.total_fees_quote,
            "realized_pnl_quote": self.realized_pnl_quote,
            "realized_pnl_pct": self.realized_pnl_pct,
            "unrealized_pnl_quote": self.unrealized_pnl_quote,
            "net_inventory_base": self.net_inventory_base,
            "net_inventory_quote": self.net_inventory_quote,
            "open_buy_liquidity_quote": self.open_buy_liquidity_quote,
            "open_sell_liquidity_quote": self.open_sell_liquidity_quote,
        }

    # ─── Lifecycle ───────────────────────────────────────────────────────

    async def on_start(self):
        """Start the executor, validate balance, and check barriers."""
        await super().on_start()
        self.update_metrics()
        if self.check_barriers():
            self.logger().error(f"PMM executor already expired by {self.close_type}.")
            self._status = RunnableStatus.SHUTTING_DOWN

    def evaluate_max_retries(self):
        """Stop the executor if the maximum number of retries is reached."""
        if self._current_retries > self._max_retries:
            self.close_type = CloseType.FAILED
            self.stop()

    # ─── Event Handlers ──────────────────────────────────────────────────

    def update_tracked_orders_with_order_id(self, order_id: str):
        """Update tracked orders with the InFlightOrder from the connector."""
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        if in_flight_order:
            for level in self.pmm_levels:
                if level.buy_order and level.buy_order.order_id == order_id:
                    level.buy_order.order = in_flight_order
                if level.sell_order and level.sell_order.order_id == order_id:
                    level.sell_order.order = in_flight_order
            if self._close_order and self._close_order.order_id == order_id:
                self._close_order.order = in_flight_order

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        for i, level in enumerate(self.pmm_levels):
            if level.buy_order and event.order_id == level.buy_order.order_id:
                self._canceled_orders.append(level.buy_order.order_id)
                saved_side = level.pending_side
                level.reset_buy_order()
                level.pending_side = saved_side  
            if level.sell_order and event.order_id == level.sell_order.order_id:
                self._canceled_orders.append(level.sell_order.order_id)
                saved_side = level.pending_side
                level.reset_sell_order()
                level.pending_side = saved_side  

            if i == self._refresh_canceling_level:
                buy_done = level.buy_state != PMMOrderState.ORDER_PLACED
                sell_done = level.sell_state != PMMOrderState.ORDER_PLACED
                if buy_done and sell_done:
                    self.logger().info(f"PMM: Refresh cancel confirmed for {level.id}, placing replacements.")
                    if level.pending_side in ("buy", "both") and not self._is_level_on_cooldown(f"{level.id}_buy"):
                        self._place_buy_order(level)
                    if level.pending_side in ("sell", "both") and not self._is_level_on_cooldown(f"{level.id}_sell"):
                        self._place_sell_order(level)
                    self._refresh_canceling_level = None
                    self._cancel_next_pending_level()

        if self._close_order and event.order_id == self._close_order.order_id:
            self._canceled_orders.append(self._close_order.order_id)
            self._close_order = None

    def _is_insufficient_funds_error(self, event: MarketOrderFailureEvent) -> bool:
        """Check if the failure is due to insufficient funds."""
        if event.error_message:
            msg = event.error_message.lower()
            return "insufficient funds" in msg or "insufficient balance" in msg
        return False

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        is_balance_error = self._is_insufficient_funds_error(event)
        for level in self.pmm_levels:
            if level.buy_order and event.order_id == level.buy_order.order_id:
                self._failed_orders.append(level.buy_order.order_id)
                level_key = f"{level.id}_buy"
                self._record_level_failure(level_key)
                if is_balance_error:
                    self._level_insufficient_funds[level_key] = True
                    self.logger().warning(
                        f"PMM: Disabling buy for level {level.id} — insufficient funds. "
                        f"Will re-enable when a sell fills and releases balance."
                    )
                saved_side = level.pending_side
                level.reset_buy_order()
                level.pending_side = saved_side
            if level.sell_order and event.order_id == level.sell_order.order_id:
                self._failed_orders.append(level.sell_order.order_id)
                level_key = f"{level.id}_sell"
                self._record_level_failure(level_key)
                if is_balance_error:
                    self._level_insufficient_funds[level_key] = True
                    self.logger().warning(
                        f"PMM: Disabling sell for level {level.id} — insufficient funds. "
                        f"Will re-enable when a buy fills and creates inventory."
                    )
                saved_side = level.pending_side
                level.reset_sell_order()
                level.pending_side = saved_side
        if self._close_order and event.order_id == self._close_order.order_id:
            self._failed_orders.append(self._close_order.order_id)
            self._close_order = None

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
