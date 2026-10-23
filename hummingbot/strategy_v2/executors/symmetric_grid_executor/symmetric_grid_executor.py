import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PositionAction, PositionMode, PriceType, TradeType
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
from hummingbot.strategy_v2.executors.symmetric_grid_executor.data_types import (
    FairPriceType,
    SymmetricGridExecutorConfig,
    SymmetricGridLevel,
    SymmetricGridOrderState,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class SymmetricGridExecutor(ExecutorBase):
    """
    Symmetric Grid Executor.

    Places symmetric buy and sell limit orders at specified percentage distances from a fair price.
    Each level alternates sides once it starts filling: after its buy fills it re-places only its
    sell, and after its sell fills only its buy.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: SymmetricGridExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        """
        Initialize the SymmetricGridExecutor.

        :param strategy: The strategy instance.
        :param config: The symmetric grid executor configuration.
        :param update_interval: Control loop interval in seconds.
        :param max_retries: Maximum retries before failing.
        """
        self.config: SymmetricGridExecutorConfig = config
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)

        self.trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)

        self.fair_price = self.get_fair_price()
        self.mid_price = self.fair_price
        self._log_seq: int = 0

        self.grid_levels = self._generate_levels()

        self._filled_orders: List[dict] = []
        self._failed_orders: List[str] = []
        self._canceled_orders: List[str] = []

        self._filled_orders_processed_idx = 0
        self._buy_amount_quote_sum = Decimal("0")
        self._sell_amount_quote_sum = Decimal("0")
        self._fees_quote_sum = Decimal("0")
        self._buy_amount_base_sum = Decimal("0")
        self._sell_amount_base_sum = Decimal("0")

        self._level_failures: Dict[str, tuple] = {}
        self._max_level_failures = 5
        self._level_failure_cooldown = 60
        self._level_insufficient_funds: Dict[str, float] = {}
        self._sell_base_pending: Dict[str, Decimal] = {}
        # Orders whose cancel was confirmed, by confirmation time. CoinDCX has confirmed cancels for
        # orders that went on to fill, so fills for these ids are still counted for a while.
        self._cancel_watch: Dict[str, float] = {}
        self._cancel_watch_seconds: float = 60.0
        # A fill at a level leaves slightly less base than its full sell needs (fee taken in base,
        # amount rounded down to the step); sell what there is when it is at least this share.
        self._base_shortfall_tolerance = Decimal("0.98")
        self._last_position_mode_warning_ts: float = 0
        self._sell_skip_logged: set = set()  # level ids whose "Skipping sell" was already logged

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
        self._refresh_pending_levels: set = set()
        self._refresh_cancel_sent_ts: float = 0
        self._refresh_cancel_timeout: float = 10.0
        self._last_price_log_timestamp: float = 0
        self._price_log_interval: float = 30.0
        self._insufficient_funds_cooldown: float = 5.0

    def _get_mid_price(self) -> Decimal:
        """Fetch the mid price from the connector."""
        return self.connectors[self.config.connector_name].get_price_by_type(
            self.config.trading_pair, PriceType.MidPrice)

    def _seq(self) -> int:
        """Return the next log sequence number."""
        self._log_seq += 1
        return self._log_seq

    _FAIR_PRICE_FETCHERS = {
        FairPriceType.MidPrice: _get_mid_price,
    }

    def get_fair_price(self) -> Decimal:
        """Calculate the fair price based on config: use explicit fair_price if set, otherwise fetch by fair_price_type."""
        if self.config.fair_price is not None:
            return self.config.fair_price
        fetcher = self._FAIR_PRICE_FETCHERS[self.config.fair_price_type]
        return fetcher(self)

    @staticmethod
    def _is_valid_fair_price(price: Decimal) -> bool:
        """Return True when a fair price can safely be used to calculate grid levels."""
        return price is not None and price.is_finite() and price > Decimal("0")

    @property
    def is_perpetual(self) -> bool:
        """Check if the exchange connector is perpetual."""
        return self.is_perpetual_connector(self.config.connector_name)

    def _generate_levels(self) -> List[SymmetricGridLevel]:
        """Generate grid levels from the configuration."""
        levels = []
        for i, (spread, amount) in enumerate(zip(self.config.spread_percentages, self.config.order_amounts_quote)):
            levels.append(
                SymmetricGridLevel(
                    id=f"L{i}",
                    spread_pct=spread,
                    amount_quote=amount,
                )
            )
        self.logger().info(
            f"[#{self._seq()}] Grid created | {len(levels)} levels | "
            f"spreads={[f'{float(s) * 100:.2f}%' for s in self.config.spread_percentages]} | "
            f"fair={self.fair_price}"
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
            quote_asset = self.config.trading_pair.split("-")[1]
            message = (f"SymGrid {self.config.trading_pair}: not enough {quote_asset} to place the buy levels "
                       f"(need ~{total_amount_quote} {quote_asset}). Executor stopped.")
            self.logger().error(message)
            self._strategy.notify_hb_app_with_timestamp(message)
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
                self.logger().warning(f"[#{self._seq()}] Not enough base balance for all sell levels. "f"Need ~{total_amount_base:.4f} {self.config.trading_pair.split('-')[0]}.")

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
        self._prune_cancel_watch()

        if self.status == RunnableStatus.RUNNING:
            if self.check_barriers():
                self.cancel_all_orders()
                self._status = RunnableStatus.SHUTTING_DOWN
                return
            if self.config.fair_price is None:
                self.refresh_orders_on_price_change()
            self._advance_refresh()
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
        Record filled orders and decide what each level places next.

        Sides alternate: after a level's buy fills, it waits for that level's sell to fill before
        buying again, and the reverse. That caps the inventory a level can build up at one order.
        """
        for level in self.grid_levels:
            buy_filled = level.buy_state == SymmetricGridOrderState.ORDER_FILLED
            sell_filled = level.sell_state == SymmetricGridOrderState.ORDER_FILLED
            if not (buy_filled or sell_filled):
                continue
            if buy_filled:
                self._record_fill(level, level.buy_order)
                level.reset_buy_order()
                self._clear_insufficient_funds("_sell")
            if sell_filled:
                self._record_fill(level, level.sell_order)
                level.reset_sell_order()
                self._clear_insufficient_funds("_buy")
            if buy_filled and sell_filled:
                level.pending_side = "both"
            else:
                level.pending_side = "sell" if buy_filled else "buy"
            self.logger().info(f"[#{self._seq()}] {level.id} next: {level.pending_side}")

    def _record_fill(self, level: SymmetricGridLevel, tracked: TrackedOrder, note: str = "filled"):
        """Add what an order traded to the fill history used for PnL and inventory."""
        order = tracked.order
        order_json = order.to_json()
        if Decimal(order_json["executed_amount_base"]) == Decimal("0"):
            # FILLED arrived before the trade updates. Use the order's own size and price: the
            # level's amount and the current fair price can both differ from what traded.
            self.logger().warning(
                f"[#{self._seq()}] {level.id} {order.trade_type.name} reported filled before its trades arrived; "
                f"recording the order's {order.amount} @ {order.price}"
            )
            order_json["executed_amount_base"] = str(order.amount)
            order_json["executed_amount_quote"] = str(order.amount * order.price)
        self._filled_orders.append(order_json)
        self.logger().info(
            f"[#{self._seq()}] {order.trade_type.name} {note} | {level.id} @ {tracked.average_executed_price} | "
            f"qty={order_json['executed_amount_base']}"
        )

    def _clear_insufficient_funds(self, side_suffix: str):
        """A fill frees up the asset the other side needs, so let those levels retry at once."""
        for key in list(self._level_insufficient_funds):
            if key.endswith(side_suffix):
                del self._level_insufficient_funds[key]

    # ─── Fair Price Refresh ────────────────────────────────────────────

    def refresh_orders_on_price_change(self):
        """Parallel refresh: cancel ALL stale levels at once, replace each as its cancels confirm.

        When price moves beyond tolerance, cancels are sent for every active level in one tick.
        Each level is re-placed as soon as it has no live orders left (see _advance_refresh),
        without waiting for the other levels.
        """
        if self._refresh_pending_levels:
            now = self._strategy.current_timestamp
            elapsed = now - self._refresh_cancel_sent_ts
            if self._refresh_cancel_sent_ts > 0 and elapsed > self._refresh_cancel_timeout:
                # Never drop an order whose cancel was not confirmed: it may still be live on the
                # exchange. Re-send the cancel and keep tracking the order until the exchange
                # reports it cancelled, filled or failed.
                for idx in sorted(self._refresh_pending_levels):
                    level = self.grid_levels[idx]
                    for tracked in (level.buy_order, level.sell_order):
                        if tracked is not None and not tracked.is_done:
                            self.logger().warning(
                                f"[#{self._seq()}] Refresh cancel of {tracked.order_id} ({level.id}) not confirmed "
                                f"after {elapsed:.1f}s. Re-sending the cancel."
                            )
                            self._strategy.cancel(
                                connector_name=self.config.connector_name,
                                trading_pair=self.config.trading_pair,
                                order_id=tracked.order_id,
                            )
                self._refresh_cancel_sent_ts = now
            return

        new_fair_price = self.get_fair_price()
        if not self._is_valid_fair_price(new_fair_price):
            self.logger().warning(
                f"[#{self._seq()}] Ignoring invalid fair price during refresh: {new_fair_price}"
            )
            return

        if not self._is_valid_fair_price(self.fair_price):
            self.fair_price = new_fair_price
            return

        price_change_pct = abs(new_fair_price - self.fair_price) / self.fair_price
        max_refresh_price_change_pct = Decimal("0.5")
        if price_change_pct > max_refresh_price_change_pct:
            self.logger().warning(
                f"[#{self._seq()}] Ignoring fair price refresh from {self.fair_price} to {new_fair_price}: "
                f"change {float(price_change_pct) * 100:.4f}% exceeds "
                f"{float(max_refresh_price_change_pct) * 100:.4f}% sanity limit."
            )
            return

        now = self._strategy.current_timestamp

        if now - self._last_price_log_timestamp >= self._price_log_interval:
            self.logger().info(
                f"[#{self._seq()}] Price check | fair={self.fair_price:.4f} → {new_fair_price:.4f} | "
                f"delta={float(price_change_pct) * 100:.4f}% | tol={float(self.config.price_refresh_tolerance) * 100:.4f}%"
            )
            self._last_price_log_timestamp = now

        if price_change_pct <= self.config.price_refresh_tolerance:
            return

        if now - self._last_refresh_timestamp < self._refresh_cooldown:
            return

        self.logger().info(
            f"[#{self._seq()}] Fair price {self.fair_price:.4f} → {new_fair_price:.4f} "
            f"({float(price_change_pct) * 100:.4f}% > {float(self.config.price_refresh_tolerance) * 100:.4f}%). "
            f"Canceling all levels for refresh."
        )
        self.fair_price = new_fair_price
        self._last_refresh_timestamp = now

        levels_to_refresh = set()
        for i, level in enumerate(self.grid_levels):
            has_active_buy = level.buy_order and level.buy_state == SymmetricGridOrderState.ORDER_PLACED
            has_active_sell = level.sell_order and level.sell_state == SymmetricGridOrderState.ORDER_PLACED
            if has_active_buy or has_active_sell:
                levels_to_refresh.add(i)
                if has_active_buy:
                    self._strategy.cancel(
                        connector_name=self.config.connector_name,
                        trading_pair=self.config.trading_pair,
                        order_id=level.buy_order.order_id,
                    )
                if has_active_sell:
                    self._strategy.cancel(
                        connector_name=self.config.connector_name,
                        trading_pair=self.config.trading_pair,
                        order_id=level.sell_order.order_id,
                    )

        if levels_to_refresh:
            self._refresh_pending_levels = levels_to_refresh
            self._refresh_cancel_sent_ts = now
            self.logger().info(
                f"[#{self._seq()}] Sent cancels for {len(levels_to_refresh)} levels: "
                f"{[self.grid_levels[i].id for i in sorted(levels_to_refresh)]}"
            )

    # ─── Order Management ────────────────────────────────────────────────

    def _is_level_on_cooldown(self, level_id: str) -> bool:
        """Check if a level is in failure cooldown (including insufficient-funds cooldown)."""
        if level_id in self._level_insufficient_funds:
            ts = self._level_insufficient_funds[level_id]
            if self._strategy.current_timestamp - ts < self._insufficient_funds_cooldown:
                return True
            del self._level_insufficient_funds[level_id]
            return False
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
        max_batch = self.config.max_orders_per_batch or (len(self.grid_levels) * 2)

        refreshing_indices = set(self._refresh_pending_levels)

        for i, level in enumerate(self.grid_levels):
            if orders_placed >= max_batch:
                break

            if i in refreshing_indices:
                continue

            should_place_buy = (
                level.pending_side in ("buy", "both")
                and level.buy_state == SymmetricGridOrderState.NOT_ACTIVE
                and not self._is_level_on_cooldown(f"{level.id}_buy")
            )
            should_place_sell = (
                level.pending_side in ("sell", "both")
                and level.sell_state == SymmetricGridOrderState.NOT_ACTIVE
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

    def _place_level(self, level: SymmetricGridLevel) -> int:
        """Place whichever of the level's orders are missing and due. Returns how many were placed."""
        placed = 0
        if (level.pending_side in ("buy", "both")
                and level.buy_state == SymmetricGridOrderState.NOT_ACTIVE
                and not self._is_level_on_cooldown(f"{level.id}_buy")
                and self._place_buy_order(level)):
            placed += 1
        if (level.pending_side in ("sell", "both")
                and level.sell_state == SymmetricGridOrderState.NOT_ACTIVE
                and not self._is_level_on_cooldown(f"{level.id}_sell")
                and self._place_sell_order(level)):
            placed += 1
        return placed

    def _position_mode_ok(self) -> bool:
        """
        Perpetuals are traded in one-way mode only. In hedge mode the grid's buys and sells would
        open opposite positions that never net off.
        """
        if not self.is_perpetual:
            return True
        position_mode = getattr(self.connectors[self.config.connector_name], "position_mode", None)
        if position_mode == PositionMode.ONEWAY:
            return True
        now = self._strategy.current_timestamp
        if now - self._last_position_mode_warning_ts >= self._price_log_interval:
            self.logger().warning(
                f"[#{self._seq()}] {self.config.connector_name} is in {getattr(position_mode, 'name', position_mode)} "
                f"position mode; this grid trades perpetuals in ONEWAY mode only. Not placing orders."
            )
            self._last_position_mode_warning_ts = now
        return False

    def _place_buy_order(self, level: SymmetricGridLevel) -> bool:
        """Place a buy order for the given level at fair_price * (1 - spread_pct). Returns True if placed."""
        if not self._position_mode_ok():
            return False
        buy_price = level.get_buy_price(self.fair_price)
        amount_base = level.amount_quote / buy_price

        best_bid = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestBid)
        if buy_price >= best_bid:
            if self.config.fair_price is not None:
                # A fixed fair price that is already through the market: wait for the price to come
                # back instead of chasing the best bid down.
                return False
            buy_price = best_bid * (1 - self.config.safe_extra_spread)

        min_notional = max(self.config.min_order_amount_quote, self.trading_rules.min_notional_size)
        notional = amount_base * buy_price
        if notional < min_notional:
            self.logger().debug(
                f"SymGrid: Skipping buy {level.id} — notional {notional:.2f} < min {min_notional:.2f}"
            )
            self._record_level_failure(f"{level.id}_buy")
            return False

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
                f"SymGrid: Skipping buy {level.id} — adjusted notional {adjusted_notional:.2f} < min {min_notional:.2f} "
                f"(insufficient quote balance to place buy order)"
            )
            self._record_level_failure(f"{level.id}_buy")
            return False

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
            f"[#{self._seq()}] BUY {level.id} | price={order_candidate.price:.4f} | "
            f"qty={order_candidate.amount:.4f} | fair={self.fair_price:.4f} | spread={float(level.spread_pct) * 100:.2f}%"
        )
        return True

    def _get_pending_sell_base(self) -> Decimal:
        """Return total base amount committed to sell orders not yet confirmed by the exchange."""
        return sum(self._sell_base_pending.values(), Decimal("0"))

    def _place_sell_order(self, level: SymmetricGridLevel) -> bool:
        """Place a sell order for the given level at fair_price * (1 + spread_pct). Returns True if placed."""
        if not self._position_mode_ok():
            return False
        sell_price = level.get_sell_price(self.fair_price)
        amount_base = level.amount_quote / sell_price

        # A perpetual sell opens a short against margin, which the budget check below covers.
        # Only a spot sell needs the base asset in the wallet.
        if not self.is_perpetual:
            base_asset = self.config.trading_pair.split("-")[0]
            available_base = self.get_available_balance(self.config.connector_name, base_asset)
            effective_available = available_base - self._get_pending_sell_base()
            if effective_available < amount_base:
                if effective_available > 0 and effective_available >= amount_base * self._base_shortfall_tolerance:
                    amount_base = effective_available
                else:
                    # Logged once per level until its sell goes out; this check runs every tick.
                    if level.id not in self._sell_skip_logged:
                        self.logger().info(
                            f"SymGrid: Skipping sell {level.id} — effective available base "
                            f"{effective_available:.4f} < needed {amount_base:.4f} {base_asset}. "
                            f"Waiting for {base_asset}; not logged again until this sell is placed."
                        )
                        self._sell_skip_logged.add(level.id)
                    return False

        best_ask = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.BestAsk)
        if sell_price <= best_ask:
            if self.config.fair_price is not None:
                # A fixed fair price that is already through the market: wait for the price to come
                # back instead of chasing the best ask up.
                return False
            sell_price = best_ask * (1 + self.config.safe_extra_spread)

        min_notional = max(self.config.min_order_amount_quote, self.trading_rules.min_notional_size)
        notional = amount_base * sell_price
        if notional < min_notional:
            self.logger().debug(
                f"SymGrid: Skipping sell {level.id} — notional {notional:.2f} < min {min_notional:.2f}"
            )
            self._record_level_failure(f"{level.id}_sell")
            return False

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
                f"SymGrid: Skipping sell {level.id} — adjusted notional {adjusted_notional:.2f} < min {min_notional:.2f} "
                f"(insufficient base balance to place sell order — need {amount_base:.4f} {self.config.trading_pair.split('-')[0]})"
            )
            self._record_level_failure(f"{level.id}_sell")
            return False

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
        self._sell_base_pending[order_id] = order_candidate.amount
        self._sell_skip_logged.discard(level.id)
        self.logger().info(
            f"[#{self._seq()}] SELL {level.id} | price={order_candidate.price:.4f} | "
            f"qty={order_candidate.amount:.4f} | fair={self.fair_price:.4f} | spread={float(level.spread_pct) * 100:.2f}%"
        )
        return True

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
        for level in self.grid_levels:
            if level.buy_order and level.buy_state == SymmetricGridOrderState.ORDER_PLACED:
                self._strategy.cancel(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_id=level.buy_order.order_id
                )
                self.logger().debug(f"SymGrid: Canceling buy order {level.buy_order.order_id}")
            if level.sell_order and level.sell_state == SymmetricGridOrderState.ORDER_PLACED:
                self._strategy.cancel(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_id=level.sell_order.order_id
                )
                self.logger().debug(f"SymGrid: Canceling sell order {level.sell_order.order_id}")

    def early_stop(self, keep_position: bool = False):
        """Stop the executor early, canceling all open orders."""
        if self.status == RunnableStatus.TERMINATED:
            # The kill switch stops every executor of the controller, finished ones included. Moving a
            # finished one back to SHUTTING_DOWN leaves it stuck there (nothing runs its shutdown) and,
            # since is_active counts SHUTTING_DOWN, the controller would never start a new executor.
            return
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
            order for level in self.grid_levels
            for order in [level.buy_order, level.sell_order]
            if order is not None and not order.is_done and not order.is_filled
        ]

        if len(active_orders) == 0:
            self.update_realized_pnl_metrics()
            self.logger().info(
                f"[#{self._seq()}] Shutdown complete | net_inv={self.net_inventory_base} base"
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
            level.amount_quote for level in self.grid_levels
            if level.buy_state == SymmetricGridOrderState.ORDER_PLACED
            and level.buy_order
            and level.buy_order.executed_amount_base == Decimal("0")
        )
        self.open_sell_liquidity_quote = sum(
            level.amount_quote for level in self.grid_levels
            if level.sell_state == SymmetricGridOrderState.ORDER_PLACED
            and level.sell_order
            and level.sell_order.executed_amount_base == Decimal("0")
        )

    def update_realized_pnl_metrics(self):
        """Calculate realized and unrealized PnL, summing only fills not yet accumulated."""
        if len(self._filled_orders) == 0:
            self._reset_metrics()
            self._filled_orders_processed_idx = 0
            return

        if self._filled_orders_processed_idx == 0 or self._filled_orders_processed_idx > len(self._filled_orders):
            self._buy_amount_quote_sum = Decimal("0")
            self._sell_amount_quote_sum = Decimal("0")
            self._fees_quote_sum = Decimal("0")
            self._buy_amount_base_sum = Decimal("0")
            self._sell_amount_base_sum = Decimal("0")
            self._filled_orders_processed_idx = 0

        for order in self._filled_orders[self._filled_orders_processed_idx:]:
            self._fees_quote_sum += Decimal(order["cumulative_fee_paid_quote"])
            if order["trade_type"] == TradeType.BUY.name:
                self._buy_amount_quote_sum += Decimal(order["executed_amount_quote"])
                self._buy_amount_base_sum += Decimal(order["executed_amount_base"])
            else:
                self._sell_amount_quote_sum += Decimal(order["executed_amount_quote"])
                self._sell_amount_base_sum += Decimal(order["executed_amount_base"])
        self._filled_orders_processed_idx = len(self._filled_orders)

        self.total_buy_quote = self._buy_amount_quote_sum
        self.total_sell_quote = self._sell_amount_quote_sum
        self.total_fees_quote = self._fees_quote_sum

        buy_amount_base = self._buy_amount_base_sum
        sell_amount_base = self._sell_amount_base_sum
        avg_buy_price = self.total_buy_quote / buy_amount_base if buy_amount_base > 0 else Decimal("0")
        avg_sell_price = self.total_sell_quote / sell_amount_base if sell_amount_base > 0 else Decimal("0")

        self.net_inventory_base = buy_amount_base - sell_amount_base
        self.net_inventory_quote = self.net_inventory_base * self.mid_price

        # Realise only the quantity that has been both bought and sold; whatever is still held is
        # valued at mid below. Booking the cost of held inventory as realised made the first buy
        # fill read as a -100% loss.
        matched_base = min(buy_amount_base, sell_amount_base)
        self.realized_pnl_quote = matched_base * (avg_sell_price - avg_buy_price) - self.total_fees_quote

        if self.net_inventory_base > 0:
            self.unrealized_pnl_quote = self.net_inventory_base * (self.mid_price - avg_buy_price)
        elif self.net_inventory_base < 0:
            self.unrealized_pnl_quote = -self.net_inventory_base * (avg_sell_price - self.mid_price)
        else:
            self.unrealized_pnl_quote = Decimal("0")

        budget = self.budget_quote
        self.realized_pnl_pct = self.realized_pnl_quote / budget if budget > 0 else Decimal("0")

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

    @property
    def budget_quote(self) -> Decimal:
        """The capital the grid works with: the sum of its level amounts."""
        return sum(self.config.order_amounts_quote, Decimal("0"))

    def get_net_pnl_pct(self) -> Decimal:
        """
        Net PnL as a fraction of the grid's budget.

        Measured against the budget rather than traded volume, so the stop-loss doesn't get less
        sensitive the longer the grid trades.
        """
        budget = self.budget_quote
        return self.get_net_pnl_quote() / budget if budget > 0 else Decimal("0")

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
                for level in self.grid_levels
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
        await super().on_start()  # runs validate_sufficient_balance()
        if self.status == RunnableStatus.TERMINATED:
            return  # not enough balance: stay stopped rather than flipping to SHUTTING_DOWN below
        self.update_metrics()
        if self.check_barriers():
            self.logger().error(f"SymGrid executor already expired by {self.close_type}.")
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
            for level in self.grid_levels:
                if level.buy_order and level.buy_order.order_id == order_id:
                    level.buy_order.order = in_flight_order
                if level.sell_order and level.sell_order.order_id == order_id:
                    level.sell_order.order = in_flight_order

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)
        self._sell_base_pending.pop(event.order_id, None)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        if event.order_id in self._cancel_watch:
            self._record_late_fill(event)
            return
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        self._sell_base_pending.pop(event.order_id, None)
        # An order can fill completely just before its cancel is confirmed; handle it as a fill so
        # the level alternates sides as usual.
        self.process_filled_orders()
        for level in self.grid_levels:
            if level.buy_order and event.order_id == level.buy_order.order_id:
                self._release_cancelled(level, level.buy_order)
                level.reset_buy_order()
            if level.sell_order and event.order_id == level.sell_order.order_id:
                self._release_cancelled(level, level.sell_order)
                level.reset_sell_order()

        self._advance_refresh()

    def _release_cancelled(self, level: SymmetricGridLevel, tracked: TrackedOrder):
        """
        Keep what a cancelled order traded, and watch its id for fills that arrive afterwards:
        CoinDCX has confirmed cancels for orders that went on to fill.
        """
        self._canceled_orders.append(tracked.order_id)
        if tracked.order is not None and tracked.executed_amount_base > Decimal("0"):
            self._record_fill(level, tracked, note="partly filled before cancel")
        self._cancel_watch[tracked.order_id] = self._strategy.current_timestamp

    def _record_late_fill(self, event: OrderFilledEvent):
        """Count a fill for an order whose cancel was already confirmed."""
        quote_asset = self.config.trading_pair.split("-")[1]
        try:
            fee_quote = event.trade_fee.fee_amount_in_token(
                trading_pair=self.config.trading_pair, price=event.price, order_amount=event.amount,
                token=quote_asset, exchange=self.connectors[self.config.connector_name])
        except Exception:
            fee_quote = Decimal("0")
        self.logger().warning(
            f"[#{self._seq()}] Fill for {event.order_id} after its cancel was confirmed: "
            f"{event.trade_type.name} {event.amount} @ {event.price}. Counting it."
        )
        self._filled_orders.append({
            "client_order_id": event.order_id,
            "trade_type": event.trade_type.name,
            "executed_amount_base": str(event.amount),
            "executed_amount_quote": str(event.amount * event.price),
            "cumulative_fee_paid_quote": str(fee_quote),
        })

    def _prune_cancel_watch(self):
        now = self._strategy.current_timestamp
        for order_id, cancelled_at in list(self._cancel_watch.items()):
            if now - cancelled_at > self._cancel_watch_seconds:
                del self._cancel_watch[order_id]

    def _advance_refresh(self):
        """
        Re-place each refreshing level as soon as it has no live orders left.

        A level is done once each of its orders is cancelled, filled or failed, so a fill that races
        a cancel no longer holds the refresh up. Fills are recorded first, so a filled order is never
        overwritten by its replacement.
        """
        if not self._refresh_pending_levels:
            return
        self.process_filled_orders()
        for i in sorted(self._refresh_pending_levels):
            level = self.grid_levels[i]
            if SymmetricGridOrderState.ORDER_PLACED in (level.buy_state, level.sell_state):
                continue
            self._refresh_pending_levels.discard(i)
            self._place_level(level)
            self.logger().info(
                f"[#{self._seq()}] Refreshed {level.id} | "
                f"buy={level.get_buy_price(self.fair_price):.4f} | "
                f"sell={level.get_sell_price(self.fair_price):.4f} | "
                f"remaining={len(self._refresh_pending_levels)}"
            )
        if not self._refresh_pending_levels:
            self._refresh_cancel_sent_ts = 0

    def _is_insufficient_funds_error(self, event: MarketOrderFailureEvent) -> bool:
        """Check if the failure is due to insufficient funds."""
        if event.error_message:
            msg = event.error_message.lower()
            return "insufficient funds" in msg or "insufficient balance" in msg
        return False

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        is_balance_error = self._is_insufficient_funds_error(event)
        self._sell_base_pending.pop(event.order_id, None)
        for level in self.grid_levels:
            if level.buy_order and event.order_id == level.buy_order.order_id:
                self._failed_orders.append(level.buy_order.order_id)
                if level.buy_order.order is not None and level.buy_order.executed_amount_base > Decimal("0"):
                    self._record_fill(level, level.buy_order, note="partly filled before failing")
                level_key = f"{level.id}_buy"
                self._record_level_failure(level_key)
                if is_balance_error:
                    self._level_insufficient_funds[level_key] = self._strategy.current_timestamp
                    self.logger().warning(
                        f"SymGrid: Cooling down buy for level {level.id} — insufficient funds. "
                        f"Will retry in {self._insufficient_funds_cooldown:.0f}s."
                    )
                level.reset_buy_order()
            if level.sell_order and event.order_id == level.sell_order.order_id:
                self._failed_orders.append(level.sell_order.order_id)
                if level.sell_order.order is not None and level.sell_order.executed_amount_base > Decimal("0"):
                    self._record_fill(level, level.sell_order, note="partly filled before failing")
                level_key = f"{level.id}_sell"
                self._record_level_failure(level_key)
                if is_balance_error:
                    self._level_insufficient_funds[level_key] = self._strategy.current_timestamp
                    self.logger().warning(
                        f"SymGrid: Cooling down sell for level {level.id} — insufficient funds. "
                        f"Will retry in {self._insufficient_funds_cooldown:.0f}s."
                    )
                level.reset_sell_order()
        self._advance_refresh()

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
