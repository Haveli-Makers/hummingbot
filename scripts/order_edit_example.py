"""
Order Edit Example Script for Binance and CoinDCX

This script demonstrates how to use the order editing functionality in Hummingbot.
It places limit orders and then edits them (adjusts price/amount) based on market conditions.

Key Features:
- Places initial limit orders with configurable spreads
- Monitors orders and edits them when price moves beyond threshold
- Uses native order edit APIs for atomic order editing
  - Binance: Uses cancel-replace API
  - CoinDCX: Uses edit price API
- Handles edit events and failures gracefully

Configuration:
- exchange: The exchange to use (binance, binance_paper_trade, coindcx)
- trading_pair: The trading pair to trade
- order_amount: Base amount for orders
- bid_spread: Initial spread below mid price for buy orders
- ask_spread: Initial spread above mid price for sell orders
- edit_threshold: Price movement threshold to trigger order edit (as decimal, e.g., 0.002 = 0.2%)
- refresh_time: Time between order status checks (seconds)

Author: Hummingbot Team
"""

import logging
import os
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    OrderCancelledEvent,
    OrderEditedEvent,
    OrderEditFailedEvent,
    OrderFilledEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class OrderEditExampleConfig(BaseClientModel):
    """Configuration for the Order Edit Example script."""
    script_file_name: str = os.path.basename(__file__)
    
    # Exchange and trading pair
    exchange: str = Field(
        default="binance_paper_trade",
        description="Exchange to use (binance, binance_paper_trade, coindcx)"
    )
    trading_pair: str = Field(
        default="BTC-USDT",
        description="Trading pair to trade"
    )
    
    # Order parameters
    order_amount: Decimal = Field(
        default=Decimal("0.001"),
        description="Amount for each order"
    )
    bid_spread: Decimal = Field(
        default=Decimal("0.002"),
        description="Spread below mid price for buy orders (0.002 = 0.2%)"
    )
    ask_spread: Decimal = Field(
        default=Decimal("0.002"),
        description="Spread above mid price for sell orders (0.002 = 0.2%)"
    )
    
    # Edit parameters
    edit_threshold: Decimal = Field(
        default=Decimal("0.001"),
        description="Price movement threshold to trigger order edit (0.001 = 0.1%)"
    )
    refresh_time: int = Field(
        default=10,
        description="Time between order checks (seconds)"
    )
    force_edit_after_seconds: int = Field(
        default=15,
        description="If no edit triggered naturally, force one after this many seconds"
    )
    
    # Strategy behavior
    max_orders_per_side: int = Field(
        default=1,
        description="Maximum number of orders per side"
    )


class OrderEditExample(ScriptStrategyBase):
    """
    A script that demonstrates order editing functionality.
    
    The script:
    1. Places buy and sell limit orders at configured spreads
    2. Monitors mid price and edits orders when price moves beyond threshold
    3. Uses native Binance cancel-replace for efficient order editing
    """
    
    # Tracking variables
    _last_check_timestamp: float = 0
    _order_prices: Dict[str, Decimal] = {}  # client_order_id -> original price
    _last_mid_price: Optional[Decimal] = None
    _force_edit_start_ts: Optional[float] = None
    
    @classmethod
    def init_markets(cls, config: OrderEditExampleConfig):
        cls.markets = {config.exchange: {config.trading_pair}}
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: OrderEditExampleConfig):
        super().__init__(connectors)
        self.config = config
        self._order_prices = {}
        self._last_mid_price = None
        self._last_check_timestamp = 0
        self._force_edit_start_ts = None
    
    @property
    def connector(self) -> ConnectorBase:
        """Get the configured connector."""
        return self.connectors[self.config.exchange]

    def _has_sufficient_balance(self, is_buy: bool, amount: Decimal, price: Decimal) -> bool:
        """Check if there is enough balance to place the order."""
        base_asset, quote_asset = self.config.trading_pair.split("-")
        if is_buy:
            required_asset = quote_asset
            required_amount = amount * price
        else:
            required_asset = base_asset
            required_amount = amount

        available_balance = self.connector.get_available_balance(required_asset) or Decimal("0")

        # Add a small buffer for fees/slippage
        required_amount *= Decimal("1.001")

        if available_balance < required_amount:
            self.log_with_clock(
                logging.WARNING,
                f"Insufficient balance to place {'BUY' if is_buy else 'SELL'} order. "
                f"Available: {available_balance} {required_asset}, Required: {required_amount} {required_asset}"
            )
            return False

        return True
    
    def on_tick(self):
        """Called on each tick of the strategy."""
        
        # Check if it's time to evaluate orders
        if self.current_timestamp - self._last_check_timestamp < self.config.refresh_time:
            return
        
        self._last_check_timestamp = self.current_timestamp
        
        # Get current mid price
        mid_price = self.connector.get_price_by_type(
            self.config.trading_pair, 
            PriceType.MidPrice
        )
        
        if mid_price is None or mid_price <= 0:
            self.logger().warning("Could not get valid mid price")
            return
        
        # Get active orders
        active_orders = self.get_active_orders(connector_name=self.config.exchange)
        buy_orders = [o for o in active_orders if o.is_buy]
        sell_orders = [o for o in active_orders if not o.is_buy]
        
        # Place new orders if needed
        if len(buy_orders) < self.config.max_orders_per_side:
            self._place_buy_order(mid_price)
        
        if len(sell_orders) < self.config.max_orders_per_side:
            self._place_sell_order(mid_price)
        
        # Check if we need to edit existing orders
        if self._last_mid_price is not None:
            price_change = abs(mid_price - self._last_mid_price) / self._last_mid_price
            
            if price_change >= self.config.edit_threshold:
                self.logger().info(
                    f"Price moved {price_change:.4%} (threshold: {self.config.edit_threshold:.4%}). "
                    f"Editing orders..."
                )
                self._edit_orders_for_new_price(mid_price, active_orders)
        
        self._last_mid_price = mid_price
        if self._force_edit_start_ts is None:
            self._force_edit_start_ts = self.current_timestamp

        # Force a one-time edit if no natural trigger occurs (to validate edit flow quickly)
        if self.current_timestamp - self._force_edit_start_ts >= self.config.force_edit_after_seconds:
            self._force_edit_start_ts = float('inf')  # disable further forced edits
            self._force_edit_any_order(active_orders)
    
    def _place_buy_order(self, mid_price: Decimal) -> Optional[str]:
        """Place a buy order below the mid price."""
        buy_price = mid_price * (Decimal("1") - self.config.bid_spread)
        buy_price = self.connector.quantize_order_price(self.config.trading_pair, buy_price)

        if not self._has_sufficient_balance(True, self.config.order_amount, buy_price):
            return None
        
        order_id = self.buy(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order_amount,
            order_type=OrderType.LIMIT,
            price=buy_price
        )
        
        if order_id:
            self._order_prices[order_id] = buy_price
            self.log_with_clock(
                logging.INFO,
                f"Placed BUY order {order_id} at {buy_price}"
            )
        
        return order_id

    def _force_edit_any_order(self, active_orders: List[LimitOrder]):
        """Force a small edit on the first active order to validate edit events."""
        if not active_orders:
            return
        order = active_orders[0]
        # Nudge price by 0.1% in the same direction
        delta = Decimal("0.001")
        if order.is_buy:
            new_price = order.price * (Decimal("1") + delta)
        else:
            new_price = order.price * (Decimal("1") - delta)

        new_price = self.connector.quantize_order_price(self.config.trading_pair, new_price)

        self.log_with_clock(
            logging.INFO,
            f"Forced edit for quick validation on order {order.client_order_id}: {order.price} -> {new_price}"
        )
        self._edit_order(order, new_price, order.quantity)
    
    def _place_sell_order(self, mid_price: Decimal) -> Optional[str]:
        """Place a sell order above the mid price."""
        sell_price = mid_price * (Decimal("1") + self.config.ask_spread)
        sell_price = self.connector.quantize_order_price(self.config.trading_pair, sell_price)

        if not self._has_sufficient_balance(False, self.config.order_amount, sell_price):
            return None
        
        order_id = self.sell(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            amount=self.config.order_amount,
            order_type=OrderType.LIMIT,
            price=sell_price
        )
        
        if order_id:
            self._order_prices[order_id] = sell_price
            self.log_with_clock(
                logging.INFO,
                f"Placed SELL order {order_id} at {sell_price}"
            )
        
        return order_id
    
    def _edit_orders_for_new_price(self, mid_price: Decimal, active_orders: List[LimitOrder]):
        """Edit orders to adjust to new mid price."""
        
        for order in active_orders:
            if order.is_buy:
                new_price = mid_price * (Decimal("1") - self.config.bid_spread)
            else:
                new_price = mid_price * (Decimal("1") + self.config.ask_spread)
            
            new_price = self.connector.quantize_order_price(
                self.config.trading_pair, 
                new_price
            )
            
            # Get original price
            original_price = self._order_prices.get(order.client_order_id, order.price)
            
            # Only edit if price changed significantly
            price_diff = abs(new_price - original_price) / original_price
            if price_diff < Decimal("0.0001"):  # Less than 0.01% change
                continue
            
            self.log_with_clock(
                logging.INFO,
                f"Editing {'BUY' if order.is_buy else 'SELL'} order {order.client_order_id}: "
                f"{original_price} -> {new_price}"
            )
            
            # Use the edit_order method
            self._edit_order(order, new_price, order.quantity)
    
    def _edit_order(self, order: LimitOrder, new_price: Decimal, new_amount: Decimal):
        """
        Edit an existing order using the connector's edit_order method.
        
        This uses the exchange's native edit order API when supported.
        """
        try:
            # Call the edit_order method on the connector
            self.connector.edit_order(
                client_order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                new_price=new_price,
                new_amount=new_amount
            )
            
            # Update our tracking
            self._order_prices[order.client_order_id] = new_price
            
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
        
        # Clean up tracking
        if event.order_id in self._order_prices:
            del self._order_prices[event.order_id]
    
    def did_cancel_order(self, event: OrderCancelledEvent):
        """Called when an order is cancelled."""
        self.log_with_clock(
            logging.INFO,
            f"Order cancelled: {event.order_id}"
        )
        
        # Clean up tracking
        if event.order_id in self._order_prices:
            del self._order_prices[event.order_id]
    
    def did_edit_order(self, event: OrderEditedEvent):
        """Called when an order is successfully edited."""
        msg = (
            f"Order edited: {event.order_id} "
            f"Price: {event.original_price:.2f} -> {event.new_price:.2f}, "
            f"Amount: {event.original_amount:.6f} -> {event.new_amount:.6f}"
        )
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        
        # Update tracking with new price
        if event.new_order_id:
            # Cancel-replace: new order ID was created
            if event.order_id in self._order_prices:
                del self._order_prices[event.order_id]
            self._order_prices[event.new_order_id] = event.new_price
        else:
            # Native edit: same order ID
            self._order_prices[event.order_id] = event.new_price
    
    def did_fail_order_edit(self, event: OrderEditFailedEvent):
        """Called when an order edit fails."""
        msg = (
            f"Order edit FAILED: {event.order_id} - {event.error_message}"
        )
        if not event.recoverable:
            msg += " (CRITICAL: Order was cancelled but replacement failed!)"
        
        self.log_with_clock(logging.ERROR, msg)
        self.notify_hb_app_with_timestamp(msg)
    
    def format_status(self) -> str:
        """Return formatted status for the HB client."""
        lines = []
        lines.append(f"\n  Exchange: {self.config.exchange}")
        lines.append(f"  Trading Pair: {self.config.trading_pair}")
        
        # Current price
        mid_price = self.connector.get_price_by_type(
            self.config.trading_pair, 
            PriceType.MidPrice
        )
        if mid_price:
            lines.append(f"  Mid Price: {mid_price:.2f}")
        
        # Active orders
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
        
        # Edit threshold info
        if self._last_mid_price and mid_price:
            price_change = abs(mid_price - self._last_mid_price) / self._last_mid_price
            lines.append(f"\n  Price change since last edit: {price_change:.4%}")
            lines.append(f"  Edit threshold: {self.config.edit_threshold:.4%}")
        
        return "\n".join(lines)
