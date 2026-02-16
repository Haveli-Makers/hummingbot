"""
Configuration (set in conf/scripts/conf_order_edit_example_<exchange>.yml):
- exchange: coindcx, wazirx, etc.
- trading_pair: e.g. USDT-INR
- order_amount: base amount for the order
- bid_spread: initial spread below mid price (0.002 = 0.2%)
- edit_price: the new price to set when editing the order
- edit_after_seconds: seconds to wait before editing the order
"""

import logging
import os
from decimal import Decimal
from typing import Dict, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import OrderCancelledEvent, OrderEditedEvent, OrderEditFailedEvent, OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class OrderEditExampleConfig(BaseClientModel):
    """Configuration for the Order Edit Example script."""
    script_file_name: str = os.path.basename(__file__)

    exchange: str = Field(
        default="coindcx",
        description="Exchange to use (coindcx, wazirx, etc.)"
    )
    trading_pair: str = Field(
        default="USDT-INR",
        description="Trading pair to trade"
    )

    order_amount: Decimal = Field(
        default=Decimal("1.2"),
        description="Amount for the BUY order"
    )
    bid_spread: Decimal = Field(
        default=Decimal("0.002"),
        description="Spread below mid price for initial BUY order (0.002 = 0.2%)"
    )

    edit_price: Decimal = Field(
        default=Decimal("0"),
        description="Target price to edit the order to. Set 0 to skip editing."
    )
    edit_after_seconds: int = Field(
        default=15,
        description="Seconds to wait after placing order before editing its price"
    )

    refresh_time: int = Field(
        default=10,
        description="Time between order status checks (seconds)"
    )
    post_edit_cooldown_seconds: int = Field(
        default=10,
        description="Seconds to wait after an edit before placing new orders"
    )


class OrderEditExample(ScriptStrategyBase):
    """
    A script that places a BUY order and edits its price.

    The script:
    1. Places a BUY limit order at bid_spread % below mid price
    2. After edit_after_seconds, edits the order to edit_price
    3. Uses native edit if supported, otherwise cancel-and-replace
    """

    _last_check_timestamp: float = 0
    _order_placed_ts: Optional[float] = None
    _order_client_id: Optional[str] = None
    _order_price: Optional[Decimal] = None
    _edit_done: bool = False
    _post_edit_cooldown_until: Optional[float] = None

    @classmethod
    def init_markets(cls, config: OrderEditExampleConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: OrderEditExampleConfig):
        super().__init__(connectors)
        self.config = config
        self._last_check_timestamp = 0
        self._order_placed_ts = None
        self._order_client_id = None
        self._order_price = None
        self._edit_done = False
        self._post_edit_cooldown_until = None

    @property
    def connector(self) -> ConnectorBase:
        """Get the configured connector."""
        return self.connectors[self.config.exchange]

    def on_tick(self):
        """Called on each tick of the strategy."""
        if self.current_timestamp - self._last_check_timestamp < self.config.refresh_time:
            return

        self._last_check_timestamp = self.current_timestamp

        mid_price = self.connector.get_price_by_type(
            self.config.trading_pair,
            PriceType.MidPrice,
        )
        if mid_price is None or mid_price <= 0:
            self.logger().warning("Could not get valid mid price")
            return

        in_cooldown = (
            self._post_edit_cooldown_until is not None
            and self.current_timestamp < self._post_edit_cooldown_until
        )

        active_orders = self.get_active_orders(connector_name=self.config.exchange)
        buy_orders = [o for o in active_orders if o.is_buy]

        if not buy_orders and self._order_client_id is None and not in_cooldown:
            self._place_buy_order(mid_price)
            return

        if (
            not self._edit_done
            and self._order_placed_ts is not None
            and self.current_timestamp - self._order_placed_ts >= self.config.edit_after_seconds
            and self.config.edit_price > 0
            and buy_orders
        ):
            order = buy_orders[0]
            self._do_edit(order, self.config.edit_price)

    def _place_buy_order(self, mid_price: Decimal) -> Optional[str]:
        """Place a BUY order at bid_spread % below mid price."""
        buy_price = mid_price * (Decimal("1") - self.config.bid_spread)
        buy_price = self.connector.quantize_order_price(self.config.trading_pair, buy_price)

        base_asset, quote_asset = self.config.trading_pair.split("-")
        available = self.connector.get_available_balance(quote_asset) or Decimal("0")
        required = self.config.order_amount * buy_price
        if available < required:
            self.log_with_clock(
                logging.WARNING,
                f"Insufficient {quote_asset} balance. Available: {available}, Required: {required}",
            )
            return None

        order_id = self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order_amount,
            order_type=OrderType.LIMIT,
            price=buy_price,
        )

        if order_id:
            self._order_client_id = order_id
            self._order_price = buy_price
            self._order_placed_ts = self.current_timestamp
            self._edit_done = False
            self.log_with_clock(
                logging.INFO,
                f"Placed BUY order {order_id} at {buy_price} "
                f"(mid: {mid_price}, spread: {self.config.bid_spread:.4%})",
            )
            if self.config.edit_price > 0:
                self.log_with_clock(
                    logging.INFO,
                    f"Will edit price to {self.config.edit_price} after {self.config.edit_after_seconds}s",
                )
        return order_id

    def _do_edit(self, order: LimitOrder, new_price: Decimal):
        """Edit the order price using the connector's edit_order.

        For exchanges with native edit (e.g. CoinDCX), this calls the exchange's
        edit endpoint directly. For exchanges without native edit (e.g. WazirX),
        the connector automatically uses cancel-and-replace strategy.
        """
        new_price = self.connector.quantize_order_price(self.config.trading_pair, new_price)

        self.log_with_clock(
            logging.INFO,
            f"Editing BUY order {order.client_order_id}: "
            f"{order.price} -> {new_price} (amount unchanged: {order.quantity})",
        )
        try:
            self.connector.edit_order(
                client_order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                new_price=new_price,
                new_amount=order.quantity,
            )
            self._edit_done = True
        except Exception as e:
            self.logger().error(f"Failed to edit order {order.client_order_id}: {e}")

    def did_fill_order(self, event: OrderFilledEvent):
        """Called when an order is filled."""
        msg = (
            f"Order filled: {event.trade_type.name} {event.amount:.6f} "
            f"{event.trading_pair} @ {event.price:.2f}"
        )
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        self._reset_tracking()

    def did_cancel_order(self, event: OrderCancelledEvent):
        """Called when an order is cancelled."""
        self.log_with_clock(logging.INFO, f"Order cancelled: {event.order_id}")
        if event.order_id == self._order_client_id and self._edit_done:
            pass
        elif event.order_id == self._order_client_id:
            self._reset_tracking()

    def did_edit_order(self, event: OrderEditedEvent):
        """Called when an order is successfully edited."""
        msg = (
            f"Order edited successfully: {event.order_id} "
            f"Price: {event.original_price:.2f} -> {event.new_price:.2f}, "
            f"Amount: {event.original_amount:.6f} -> {event.new_amount:.6f}"
        )
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

        if event.new_order_id:
            self._order_client_id = event.new_order_id
        self._order_price = event.new_price
        self._edit_done = True
        self._post_edit_cooldown_until = (
            self.current_timestamp + self.config.post_edit_cooldown_seconds
        )

    def did_fail_order_edit(self, event: OrderEditFailedEvent):
        """Called when an order edit fails."""
        msg = f"Order edit FAILED: {event.order_id} - {event.error_message}"
        if not event.recoverable:
            msg += " (CRITICAL: Order was cancelled but replacement failed!)"
            self._reset_tracking()
        self.log_with_clock(logging.ERROR, msg)
        self.notify_hb_app_with_timestamp(msg)

    def _reset_tracking(self):
        """Reset order tracking so a new order can be placed."""
        self._order_client_id = None
        self._order_price = None
        self._order_placed_ts = None
        self._edit_done = False

    def format_status(self) -> str:
        """Return formatted status for the HB client."""
        lines = []
        lines.append(f"\n  Exchange: {self.config.exchange}")
        lines.append(f"  Trading Pair: {self.config.trading_pair}")

        mid_price = self.connector.get_price_by_type(
            self.config.trading_pair,
            PriceType.MidPrice,
        )
        if mid_price:
            lines.append(f"  Mid Price: {mid_price:.2f}")

        lines.append(f"  Edit Price Target: {self.config.edit_price}")
        lines.append(f"  Edit Done: {self._edit_done}")

        active_orders = self.get_active_orders(connector_name=self.config.exchange)
        if active_orders:
            lines.append(f"\n  Active Orders ({len(active_orders)}):")
            for order in active_orders:
                side = "BUY" if order.is_buy else "SELL"
                lines.append(
                    f"    {side} {order.quantity:.6f} @ {order.price:.2f} "
                    f"(ID: {order.client_order_id[:8]}...)"
                )
        else:
            lines.append("\n  No active orders")

        if self._order_placed_ts and not self._edit_done and self.config.edit_price > 0:
            remaining = max(
                0,
                self.config.edit_after_seconds
                - (self.current_timestamp - self._order_placed_ts),
            )
            lines.append(f"\n  Edit in: {remaining:.0f}s")

        return "\n".join(lines)
