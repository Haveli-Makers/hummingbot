import unittest
from decimal import Decimal

from hummingbot.connector.exchange.coinswitch.coinswitch_utils import CoinswitchUtils


class CoinswitchUtilsTests(unittest.TestCase):
    """Test CoinSwitch utility functions"""

    def test_order_type_to_string(self):
        """Test converting order type to string"""
        result = CoinswitchUtils.order_type_to_string("limit")
        self.assertEqual("limit", result)

    def test_string_to_order_type(self):
        """Test converting string to order type"""
        from hummingbot.core.data_type.common import OrderType
        result = CoinswitchUtils.string_to_order_type("limit")
        self.assertEqual(OrderType.LIMIT, result)

    def test_str_to_float(self):
        """Test string to float conversion"""
        result = CoinswitchUtils.str_to_float("123.456")
        self.assertEqual(123.456, result)

    def test_str_to_float_invalid(self):
        """Test string to float conversion with invalid input"""
        result = CoinswitchUtils.str_to_float("invalid")
        self.assertEqual(0.0, result)

    def test_str_to_decimal(self):
        """Test string to Decimal conversion"""
        result = CoinswitchUtils.str_to_decimal("123.456")
        self.assertEqual(Decimal("123.456"), result)

    def test_str_to_decimal_invalid(self):
        """Test string to Decimal conversion with invalid input"""
        result = CoinswitchUtils.str_to_decimal("invalid")
        self.assertEqual(Decimal("0"), result)

    def test_parse_order_response(self):
        """Test parsing order response"""
        order_response = {
            "order_id": "123",
            "symbol": "btc/inr",
            "price": "91.50",
            "orig_qty": "1.0",
            "executed_qty": "0.5",
            "status": "PARTIALLY_EXECUTED",
            "side": "buy",
            "exchange": "coinswitchx"
        }

        result = CoinswitchUtils.parse_order_response(order_response)

        self.assertEqual("123", result["order_id"])
        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(Decimal("91.50"), result["price"])
        self.assertEqual(Decimal("1.0"), result["quantity"])
        self.assertEqual("buy", result["side"])

    def test_parse_ticker_response(self):
        """Test parsing ticker response"""
        ticker_response = {
            "symbol": "btc/inr",
            "bidPrice": "91.40",
            "askPrice": "91.60",
            "lastPrice": "91.50",
            "highPrice": "92.00",
            "lowPrice": "91.00",
            "baseVolume": "1000",
            "quoteVolume": "91000",
            "at": 1234567890000
        }

        result = CoinswitchUtils.parse_ticker_response(ticker_response)

        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(Decimal("91.40"), result["bid_price"])
        self.assertEqual(Decimal("91.60"), result["ask_price"])
        self.assertEqual(Decimal("91.50"), result["last_price"])

    def test_parse_balance_response(self):
        """Test parsing balance response"""
        balance_response = [
            {
                "currency": "usdt",
                "main_balance": "1000",
                "blocked_balance_order": "100"
            },
            {
                "currency": "inr",
                "main_balance": "91000",
                "blocked_balance_order": "0"
            }
        ]

        result = CoinswitchUtils.parse_balance_response(balance_response)

        self.assertIn("USDT", result)
        self.assertEqual(Decimal("1000"), result["USDT"]["total"])
        self.assertEqual(Decimal("100"), result["USDT"]["locked"])
        self.assertIn("INR", result)

    def test_parse_depth_response(self):
        """Test parsing depth (order book) response"""
        depth_response = {
            "symbol": "btc/inr",
            "timestamp": 1234567890000,
            "bids": [[91.0, 10], [90.5, 20]],
            "asks": [[91.5, 10], [92.0, 20]]
        }

        result = CoinswitchUtils.parse_depth_response(depth_response)

        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(2, len(result["bids"]))
        self.assertEqual(2, len(result["asks"]))
        self.assertEqual(Decimal("91.0"), result["bids"][0][0])

    def test_parse_trade_response(self):
        """Test parsing trade response"""
        trade_response = {
            "E": 1234567890000,
            "m": False,
            "p": "91.50",
            "q": "1.0",
            "s": "btc/inr",
            "t": "trade_123",
            "e": "coinswitchx"
        }

        result = CoinswitchUtils.parse_trade_response(trade_response)

        self.assertEqual(1234567890000, result["event_time"])
        self.assertEqual(Decimal("91.50"), result["price"])
        self.assertEqual(Decimal("1.0"), result["quantity"])
        self.assertEqual("BTC/INR", result["symbol"])

    def test_decimal_precision(self):
        """Test Decimal precision handling"""
        value = "0.000000001"
        result = CoinswitchUtils.str_to_decimal(value)
        self.assertEqual(Decimal("0.000000001"), result)

    def test_large_number_conversion(self):
        """Test handling large numbers"""
        value = "999999999.99"
        result = CoinswitchUtils.str_to_decimal(value)
        self.assertEqual(Decimal("999999999.99"), result)

    def test_parse_empty_depth_response(self):
        """Test parsing empty depth response"""
        depth_response = {
            "symbol": "btc/inr",
            "timestamp": 1234567890000,
            "bids": [],
            "asks": []
        }

        result = CoinswitchUtils.parse_depth_response(depth_response)

        self.assertEqual(0, len(result["bids"]))
        self.assertEqual(0, len(result["asks"]))


if __name__ == "__main__":
    unittest.main()
