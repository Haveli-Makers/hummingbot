"""
Test cases for CoinDCX exchange logic.
These tests verify order handling, balance parsing, and trading rules.
"""
import unittest
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional


class OrderSide(Enum):
    """Order side enumeration."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type enumeration."""
    LIMIT = "limit_order"
    MARKET = "market_order"


class OrderStatus(Enum):
    """Order status enumeration."""
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class TestCoinDCXOrderCreation(unittest.TestCase):
    """Test cases for order creation logic."""

    def create_order_payload(self,
                            symbol: str,
                            side: OrderSide,
                            order_type: OrderType,
                            quantity: Decimal,
                            price: Optional[Decimal] = None,
                            client_order_id: str = "") -> dict:
        """Create an order payload for CoinDCX API."""
        payload = {
            "market": symbol,
            "side": side.value,
            "order_type": order_type.value,
            "total_quantity": str(quantity),
            "timestamp": 1620000000000
        }
        
        if price is not None and order_type == OrderType.LIMIT:
            payload["price_per_unit"] = str(price)
        
        if client_order_id:
            payload["client_order_id"] = client_order_id
        
        return payload

    def test_create_limit_buy_order(self):
        """Test creating a limit buy order."""
        payload = self.create_order_payload(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00")
        )
        
        self.assertEqual(payload["market"], "BTCUSDT")
        self.assertEqual(payload["side"], "buy")
        self.assertEqual(payload["order_type"], "limit_order")
        self.assertEqual(payload["total_quantity"], "1.0")
        self.assertEqual(payload["price_per_unit"], "50000.00")

    def test_create_limit_sell_order(self):
        """Test creating a limit sell order."""
        payload = self.create_order_payload(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.5"),
            price=Decimal("51000.00")
        )
        
        self.assertEqual(payload["side"], "sell")
        self.assertEqual(payload["total_quantity"], "0.5")
        self.assertEqual(payload["price_per_unit"], "51000.00")

    def test_create_market_order_no_price(self):
        """Test that market orders don't include price."""
        payload = self.create_order_payload(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00")  # Should be ignored
        )
        
        self.assertEqual(payload["order_type"], "market_order")
        self.assertNotIn("price_per_unit", payload)

    def test_create_order_with_client_id(self):
        """Test creating order with client order ID."""
        payload = self.create_order_payload(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            client_order_id="my-order-123"
        )
        
        self.assertEqual(payload["client_order_id"], "my-order-123")


class TestCoinDCXOrderCancellation(unittest.TestCase):
    """Test cases for order cancellation logic."""

    def create_cancel_payload(self, order_id: str) -> dict:
        """Create a cancel order payload."""
        return {
            "id": order_id,
            "timestamp": 1620000000000
        }

    def test_create_cancel_payload(self):
        """Test creating cancel order payload."""
        payload = self.create_cancel_payload("order123")
        
        self.assertEqual(payload["id"], "order123")
        self.assertIn("timestamp", payload)

    def test_cancel_payload_has_required_fields(self):
        """Test cancel payload has all required fields."""
        payload = self.create_cancel_payload("abc123")
        
        self.assertIn("id", payload)
        self.assertIn("timestamp", payload)


class TestCoinDCXBalanceParsing(unittest.TestCase):
    """Test cases for balance parsing."""

    def parse_balance(self, balance_entry: dict) -> dict:
        """Parse a single balance entry."""
        return {
            "currency": balance_entry.get("currency", ""),
            "available": Decimal(str(balance_entry.get("balance", "0"))),
            "locked": Decimal(str(balance_entry.get("locked_balance", "0"))),
            "total": Decimal(str(balance_entry.get("balance", "0"))) + 
                     Decimal(str(balance_entry.get("locked_balance", "0")))
        }

    def parse_all_balances(self, balances: list) -> Dict[str, dict]:
        """Parse all balance entries."""
        result = {}
        for entry in balances:
            parsed = self.parse_balance(entry)
            if parsed["currency"]:
                result[parsed["currency"]] = parsed
        return result

    def test_parse_single_balance(self):
        """Test parsing a single balance entry."""
        entry = {
            "currency": "BTC",
            "balance": "1.5",
            "locked_balance": "0.5"
        }
        
        result = self.parse_balance(entry)
        
        self.assertEqual(result["currency"], "BTC")
        self.assertEqual(result["available"], Decimal("1.5"))
        self.assertEqual(result["locked"], Decimal("0.5"))
        self.assertEqual(result["total"], Decimal("2.0"))

    def test_parse_balance_zero_locked(self):
        """Test parsing balance with no locked funds."""
        entry = {
            "currency": "USDT",
            "balance": "10000.00",
            "locked_balance": "0"
        }
        
        result = self.parse_balance(entry)
        
        self.assertEqual(result["available"], Decimal("10000.00"))
        self.assertEqual(result["locked"], Decimal("0"))
        self.assertEqual(result["total"], Decimal("10000.00"))

    def test_parse_all_balances(self):
        """Test parsing multiple balance entries."""
        balances = [
            {"currency": "BTC", "balance": "1.0", "locked_balance": "0.1"},
            {"currency": "USDT", "balance": "5000", "locked_balance": "1000"},
            {"currency": "ETH", "balance": "10", "locked_balance": "0"}
        ]
        
        result = self.parse_all_balances(balances)
        
        self.assertEqual(len(result), 3)
        self.assertIn("BTC", result)
        self.assertIn("USDT", result)
        self.assertIn("ETH", result)
        self.assertEqual(result["BTC"]["available"], Decimal("1.0"))
        self.assertEqual(result["USDT"]["total"], Decimal("6000"))

    def test_parse_empty_balances(self):
        """Test parsing empty balance list."""
        result = self.parse_all_balances([])
        self.assertEqual(len(result), 0)


class TestCoinDCXOrderStatusParsing(unittest.TestCase):
    """Test cases for order status parsing."""

    def parse_order_status(self, status_str: str) -> OrderStatus:
        """Parse order status string to enum."""
        status_map = {
            "open": OrderStatus.OPEN,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELLED,
            "canceled": OrderStatus.CANCELLED,  # Handle both spellings
            "rejected": OrderStatus.REJECTED
        }
        return status_map.get(status_str.lower(), OrderStatus.OPEN)

    def test_parse_open_status(self):
        """Test parsing open status."""
        result = self.parse_order_status("open")
        self.assertEqual(result, OrderStatus.OPEN)

    def test_parse_filled_status(self):
        """Test parsing filled status."""
        result = self.parse_order_status("filled")
        self.assertEqual(result, OrderStatus.FILLED)

    def test_parse_cancelled_status(self):
        """Test parsing cancelled status (British spelling)."""
        result = self.parse_order_status("cancelled")
        self.assertEqual(result, OrderStatus.CANCELLED)

    def test_parse_canceled_status(self):
        """Test parsing canceled status (American spelling)."""
        result = self.parse_order_status("canceled")
        self.assertEqual(result, OrderStatus.CANCELLED)

    def test_parse_partially_filled(self):
        """Test parsing partially filled status."""
        result = self.parse_order_status("partially_filled")
        self.assertEqual(result, OrderStatus.PARTIALLY_FILLED)

    def test_parse_case_insensitive(self):
        """Test that status parsing is case insensitive."""
        result1 = self.parse_order_status("FILLED")
        result2 = self.parse_order_status("Filled")
        result3 = self.parse_order_status("filled")
        
        self.assertEqual(result1, OrderStatus.FILLED)
        self.assertEqual(result2, OrderStatus.FILLED)
        self.assertEqual(result3, OrderStatus.FILLED)


class TestCoinDCXTradingRules(unittest.TestCase):
    """Test cases for trading rules parsing."""

    def parse_trading_rule(self, market_info: dict) -> dict:
        """Parse trading rules from market info."""
        return {
            "symbol": market_info.get("coindcx_name", ""),
            "base_asset": market_info.get("target_currency_name", ""),
            "quote_asset": market_info.get("base_currency_name", ""),
            "min_order_size": Decimal(str(market_info.get("min_quantity", "0"))),
            "max_order_size": Decimal(str(market_info.get("max_quantity", "999999999"))),
            "min_price_increment": Decimal(str(market_info.get("step", "0.00000001"))),
            "min_base_increment": Decimal(str(market_info.get("target_currency_precision", "0.00000001"))),
            "min_notional": Decimal(str(market_info.get("min_notional", "0")))
        }

    def test_parse_trading_rule_basic(self):
        """Test parsing basic trading rules."""
        market_info = {
            "coindcx_name": "BTCUSDT",
            "target_currency_name": "BTC",
            "base_currency_name": "USDT",
            "min_quantity": "0.0001",
            "max_quantity": "100",
            "step": "0.01",
            "target_currency_precision": "0.00000001",
            "min_notional": "10"
        }
        
        rule = self.parse_trading_rule(market_info)
        
        self.assertEqual(rule["symbol"], "BTCUSDT")
        self.assertEqual(rule["base_asset"], "BTC")
        self.assertEqual(rule["quote_asset"], "USDT")
        self.assertEqual(rule["min_order_size"], Decimal("0.0001"))
        self.assertEqual(rule["max_order_size"], Decimal("100"))
        self.assertEqual(rule["min_notional"], Decimal("10"))

    def test_parse_trading_rule_defaults(self):
        """Test that defaults are applied for missing fields."""
        market_info = {
            "coindcx_name": "BTCUSDT"
        }
        
        rule = self.parse_trading_rule(market_info)
        
        self.assertEqual(rule["symbol"], "BTCUSDT")
        self.assertEqual(rule["min_order_size"], Decimal("0"))
        self.assertEqual(rule["max_order_size"], Decimal("999999999"))


class TestCoinDCXOrderQuantityValidation(unittest.TestCase):
    """Test cases for order quantity validation."""

    def validate_order_quantity(self, 
                                quantity: Decimal,
                                min_size: Decimal,
                                max_size: Decimal,
                                step_size: Decimal) -> tuple:
        """
        Validate order quantity against trading rules.
        Returns (is_valid, error_message).
        """
        if quantity < min_size:
            return False, f"Quantity {quantity} is below minimum {min_size}"
        
        if quantity > max_size:
            return False, f"Quantity {quantity} is above maximum {max_size}"
        
        # Check step size
        if step_size > 0:
            remainder = quantity % step_size
            if remainder != Decimal("0"):
                return False, f"Quantity {quantity} is not a multiple of step size {step_size}"
        
        return True, ""

    def test_valid_quantity(self):
        """Test a valid order quantity."""
        is_valid, error = self.validate_order_quantity(
            quantity=Decimal("1.0"),
            min_size=Decimal("0.001"),
            max_size=Decimal("100"),
            step_size=Decimal("0.001")
        )
        
        self.assertTrue(is_valid)
        self.assertEqual(error, "")

    def test_quantity_below_minimum(self):
        """Test quantity below minimum."""
        is_valid, error = self.validate_order_quantity(
            quantity=Decimal("0.0001"),
            min_size=Decimal("0.001"),
            max_size=Decimal("100"),
            step_size=Decimal("0.001")
        )
        
        self.assertFalse(is_valid)
        self.assertIn("below minimum", error)

    def test_quantity_above_maximum(self):
        """Test quantity above maximum."""
        is_valid, error = self.validate_order_quantity(
            quantity=Decimal("150"),
            min_size=Decimal("0.001"),
            max_size=Decimal("100"),
            step_size=Decimal("0.001")
        )
        
        self.assertFalse(is_valid)
        self.assertIn("above maximum", error)

    def test_quantity_not_step_multiple(self):
        """Test quantity that's not a multiple of step size."""
        is_valid, error = self.validate_order_quantity(
            quantity=Decimal("1.0005"),
            min_size=Decimal("0.001"),
            max_size=Decimal("100"),
            step_size=Decimal("0.001")
        )
        
        self.assertFalse(is_valid)
        self.assertIn("step size", error)


class TestCoinDCXPriceValidation(unittest.TestCase):
    """Test cases for price validation."""

    def validate_price(self,
                       price: Decimal,
                       tick_size: Decimal) -> tuple:
        """
        Validate price against tick size.
        Returns (is_valid, rounded_price).
        """
        if tick_size <= 0:
            return True, price
        
        # Round to tick size
        rounded = (price / tick_size).quantize(Decimal("1")) * tick_size
        is_valid = price == rounded
        
        return is_valid, rounded

    def test_valid_price(self):
        """Test a valid price."""
        is_valid, rounded = self.validate_price(
            price=Decimal("50000.00"),
            tick_size=Decimal("0.01")
        )
        
        self.assertTrue(is_valid)
        self.assertEqual(rounded, Decimal("50000.00"))

    def test_price_rounded(self):
        """Test price that needs rounding."""
        is_valid, rounded = self.validate_price(
            price=Decimal("50000.005"),
            tick_size=Decimal("0.01")
        )
        
        self.assertFalse(is_valid)
        # The rounding uses banker's rounding by default, so 50000.005 rounds to 50000.00
        self.assertEqual(rounded, Decimal("50000.00"))


if __name__ == "__main__":
    unittest.main()
