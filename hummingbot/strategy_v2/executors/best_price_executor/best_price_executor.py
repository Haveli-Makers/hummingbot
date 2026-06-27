import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
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
from hummingbot.strategy_v2.executors.best_price_executor.data_types import BestPriceExecutorConfig
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class BestPriceExecutor(ExecutorBase):
    """
    Executor that keeps orders at the best price in the order book with one-tick improvement.
    Provides fast execution by staying at the top of the book.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: BestPriceExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10,
                 wait_on_executor_id: Optional[str] = None):
        """
        Initialize the BestPriceExecutor instance.

        :param strategy: The strategy to be used by the executor.
        :param config: The configuration for the executor.
        :param max_retries: The maximum number of retries for the executor.
        :param wait_on_executor_id: If set, delays start until the specified executor terminates.
        """
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval, wait_on_executor_id=wait_on_executor_id)
        self.config: BestPriceExecutorConfig = config

        # Order tracking
        self._order: Optional[TrackedOrder] = None
        self._failed_orders: list[TrackedOrder] = []
        self._canceled_orders: list[TrackedOrder] = []
        self._partial_filled_orders: list[TrackedOrder] = []
        self._renewal_task: Optional[asyncio.Task] = None  # Task for async order renewal

    @property
    def filled_amount_quote(self) -> Decimal:
        total = Decimal("0")
        for order_json in self._held_position_orders:
            total += Decimal(order_json["executed_amount_quote"])
        return total

    @property
    def best_price(self) -> Decimal:
        """
        Get the best price from the order book level 0.
        Only moves to next level if both price and qty match our order exactly.
        """
        level0 = self.get_orderbook_level(self.config.connector_name, self.config.trading_pair, self.config.side, 0)
        if level0 is None:
            price_type = PriceType.BestBid if self.config.side == TradeType.BUY else PriceType.BestAsk
            return self.get_price(self.config.connector_name, self.config.trading_pair, price_type=price_type)

        book_price, book_qty = level0
        if self._order and self._order.order:
            if book_price == self._order.order.price and book_qty == self._order.order.remaining_amount:
                next_level = self.get_orderbook_level(self.config.connector_name, self.config.trading_pair, self.config.side, 1)
                if next_level is not None:
                    return next_level[0]
        return book_price

    async def control_task(self):
        """
        Control the order execution process for best price strategy.
        """
        if self.status == RunnableStatus.RUNNING:
            self.control_best_price_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    def control_best_price_order(self):
        """
        Control the best price order execution.
        Places new order if none exists, or renews existing order if price needs adjustment.
        """
        if not self._order:
            self.logger().info("No order to control, placing a new one.")
            self.place_best_price_order()
        else:
            self.maintain_best_price_position()

    def maintain_best_price_position(self):
        """
        Keep the order at the best price in the order book with one-tick improvement defined by price_diff.
        Renew immediately whenever the best price changes beyond our current price.
        """
        if not self._order or not self._order.order or not self._order.order.is_open:
            self.logger().debug("No order to maintain")
            return

        desired_price = self._compute_best_price()
        if desired_price is None:
            return

        current_order_price = self._order.order.price
        # If our order is not at desired best price, renew it
        if current_order_price != desired_price:
            self.renew_order()

    def _compute_best_price(self) -> Decimal:
        """
        Compute best price with one tick improvement defined by price_diff.
        If our order is already at the best price, look at the next level.
        """
        # Apply one tick improvement
        if self.config.side == TradeType.BUY:
            return self.best_price + self.config.price_diff
        else:
            return self.best_price - self.config.price_diff

    def place_best_price_order(self):
        """
        Place a new best price order.
        """
        order_price = self._compute_best_price()
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            amount=self.config.amount,
            price=order_price,
            side=self.config.side,
            position_action=self.config.position_action,
        )
        self._order = TrackedOrder(order_id=order_id)
        self.logger().debug(f"Best Price Executor ID: {self.config.id} - Placing order {order_id} at {order_price}")

    def renew_order(self):
        """
        Renew the order with a new best price.

        This method initiates order renewal by scheduling an async task that
        will wait for cancellation before placing the new order.
        """
        if self._renewal_task is None or self._renewal_task.done():
            self._renewal_task = asyncio.create_task(self._async_renew_order())
        else:
            self.logger().debug("Renewal already in progress, skipping")

    async def _async_renew_order(self):
        """
        Async implementation of order renewal that waits for cancellation.
        """
        self.cancel_order()

        # Wait for the order to be canceled (self._order becomes None)
        max_wait_time = 10.0  # Maximum time to wait in seconds
        wait_interval = 0.1   # Check every 100ms
        elapsed_time = 0.0

        while self._order is not None and elapsed_time < max_wait_time:
            await asyncio.sleep(wait_interval)
            elapsed_time += wait_interval

        if self._order is not None:
            self.logger().warning(f"Timeout waiting for order cancellation after {max_wait_time}s")
            # Force clear the order to proceed with renewal
            self._order = None
        else:
            self.logger().debug("Order successfully canceled, proceeding with renewal")

        # Place the new order now that the old one is canceled and balance is freed
        self.place_best_price_order()
        self.logger().debug("Best price order renewal completed")

    def cancel_order(self):
        """
        Cancel the current order.
        """
        if self._order and self._order.order and self._order.order.is_open:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=self._order.order_id
            )
            self.logger().debug("Cancelling best price order")

    def early_stop(self, keep_position: bool = True):
        """
        This method allows strategy to stop the executor early.
        """
        self._status = RunnableStatus.SHUTTING_DOWN

    async def control_shutdown_process(self):
        """
        Control the shutdown process of the executor.
        """
        if self._order:
            if self._order.is_open:
                self.cancel_order()
            elif self._order.is_filled:
                self.close_type = CloseType.POSITION_HOLD
                if self._order.order is not None:
                    self._held_position_orders.append(self._order.order.to_json())
                self._held_position_orders.extend([order.order.to_json() for order in self._partial_filled_orders if order.order is not None])
                self.stop()
        elif self._partial_filled_orders:
            self._held_position_orders.extend([order.order.to_json() for order in self._partial_filled_orders if order.order is not None])
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
        else:
            self.close_type = CloseType.EARLY_STOP
            self.stop()
        await self._sleep(5.0)

    def update_tracked_order_with_order_id(self, order_id: str):
        """
        Update the tracked order with the information from the InFlightOrder.
        """
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        if self._order and self._order.order_id == order_id:
            self._order.order = in_flight_order

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """Process the order created event."""
        self.update_tracked_order_with_order_id(event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        """Process the order filled event."""
        self.update_tracked_order_with_order_id(event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        """Process the order completed event."""
        self.update_tracked_order_with_order_id(event.order_id)
        if self._order and self._order.order_id == event.order_id:
            if self._order.order is not None:
                self._held_position_orders.append(self._order.order.to_json())
            self.close_type = CloseType.POSITION_HOLD
            self.stop()

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        """Process the order canceled event."""
        if self._order and event.order_id == self._order.order_id:
            if self._order.executed_amount_base > Decimal("0"):
                self._partial_filled_orders.append(self._order)
            else:
                self._canceled_orders.append(self._order)
            self._order = None

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """Process the order failed event."""
        if self._order and event.order_id == self._order.order_id:
            self._failed_orders.append(self._order)
            self._order = None

    async def validate_sufficient_balance(self):
        """Validate that there is sufficient balance for the order."""
        current_price = self._compute_best_price()

        if self.is_perpetual_connector(self.config.connector_name):
            order_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,  # BestPrice always uses LIMIT_MAKER
                order_type=OrderType.LIMIT_MAKER,
                order_side=self.config.side,
                amount=self.config.amount,
                price=current_price,
                leverage=Decimal(self.config.leverage),
            )
        else:
            order_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,  # BestPrice always uses LIMIT_MAKER
                order_type=OrderType.LIMIT_MAKER,
                order_side=self.config.side,
                amount=self.config.amount,
                price=current_price,
            )

        adjusted_order_candidates = self.adjust_order_candidates(self.config.connector_name, [order_candidate])
        if adjusted_order_candidates[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            base, quote = self.config.trading_pair.split("-")
            collateral_token = quote if self.config.side == TradeType.BUY else base
            available = self.connectors[self.config.connector_name].get_available_balance(collateral_token)
            self.logger().error(
                f"Not enough budget to open best price position. "
                f"Available {collateral_token}: {available}, required notional: {self.config.amount * current_price}"
            )
            self.stop()
            return

        trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)
        notional_value = self.config.amount * current_price
        if trading_rules and notional_value < trading_rules.min_notional_size:
            self.close_type = CloseType.BELOW_MIN_NOTIONAL
            self.logger().error(
                f"Order notional {notional_value} is below min notional {trading_rules.min_notional_size}."
            )
            self.stop()

    def get_custom_info(self) -> Dict:
        """Get custom information about the executor."""
        return {
            "level_id": self.config.level_id,
            "order_id": self._order.order_id if self._order else None,
            "order_last_update": self._order.last_update_timestamp if self._order else None,
            "held_position_orders": self._held_position_orders,
            "price_diff": self.config.price_diff,
            "side": self.config.side
        }

    def to_format_status(self, scale=1.0):
        """Format the status of the executor."""
        lines = [f"""
| Trading Pair: {self.config.trading_pair} | Exchange: {self.config.connector_name}
| Amount: {self.config.amount} | Price: {self._order.order.price if self._order and self._order.order else 'N/A'}
| Strategy: BEST_PRICE | Price Diff: {self.config.price_diff}
"""]
        return lines

    async def _sleep(self, delay: float):
        """Sleep for a specified delay."""
        await asyncio.sleep(delay)

    def get_net_pnl_pct(self) -> Decimal:
        """Get the net profit and loss percentage."""
        return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        """Get the net profit and loss in quote currency."""
        return Decimal("0")

    def get_cum_fees_quote(self) -> Decimal:
        """Get the cumulative fees in quote currency."""
        return Decimal("0")
