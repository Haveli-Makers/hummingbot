import unittest
from decimal import Decimal

from hummingbot.connector.exchange.coinswitch.coinswitch_utils import CoinswitchUtils
from hummingbot.core.data_type.common import OrderType


class CoinswitchUtilsTests(unittest.TestCase):
    """Test CoinSwitch utility functions."""

    def test_order_type_to_string_always_returns_limit(self):
        """CoinSwitch only supports limit orders."""
        result = CoinswitchUtils.order_type_to_string(OrderType.LIMIT)
        self.assertEqual("limit", result)

    def test_string_to_order_type_limit(self):
        result = CoinswitchUtils.string_to_order_type("limit")
        self.assertEqual(OrderType.LIMIT, result)

    def test_string_to_order_type_unknown_defaults_to_limit(self):
        result = CoinswitchUtils.string_to_order_type("market")
        self.assertEqual(OrderType.LIMIT, result)

    def test_str_to_float_valid(self):
        self.assertAlmostEqual(123.456, CoinswitchUtils.str_to_float("123.456"))

    def test_str_to_float_integer_string(self):
        self.assertEqual(100.0, CoinswitchUtils.str_to_float("100"))

    def test_str_to_float_invalid_returns_zero(self):
        self.assertEqual(0.0, CoinswitchUtils.str_to_float("invalid"))

    def test_str_to_float_none_returns_zero(self):
        self.assertEqual(0.0, CoinswitchUtils.str_to_float(None))

    def test_str_to_decimal_valid(self):
        self.assertEqual(Decimal("123.456"), CoinswitchUtils.str_to_decimal("123.456"))

    def test_str_to_decimal_integer_string(self):
        self.assertEqual(Decimal("100"), CoinswitchUtils.str_to_decimal("100"))

    def test_str_to_decimal_invalid_returns_zero(self):
        self.assertEqual(Decimal("0"), CoinswitchUtils.str_to_decimal("invalid"))

    def test_str_to_decimal_none_returns_zero(self):
        self.assertEqual(Decimal("0"), CoinswitchUtils.str_to_decimal(None))

    def test_parse_order_response_basic_fields(self):
        order_response = {
            "order_id": "order_123",
            "symbol": "btc/inr",
            "price": "91.50",
            "orig_qty": "1.0",
            "executed_qty": "0.5",
            "status": "PARTIALLY_EXECUTED",
            "side": "buy",
            "exchange": "coinswitchx",
        }

        result = CoinswitchUtils.parse_order_response(order_response)

        self.assertEqual("order_123", result["order_id"])
        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(Decimal("91.50"), result["price"])
        self.assertEqual(Decimal("1.0"), result["quantity"])
        self.assertEqual(Decimal("0.5"), result["executed_qty"])
        self.assertEqual("PARTIALLY_EXECUTED", result["status"])
        self.assertEqual("buy", result["side"])

    def test_parse_order_response_missing_fields_use_defaults(self):
        result = CoinswitchUtils.parse_order_response({})
        self.assertIsNone(result["order_id"])
        self.assertEqual("", result["symbol"])
        self.assertEqual(Decimal("0"), result["price"])

    def test_parse_ticker_response_basic_fields(self):
        ticker = {
            "symbol": "btc/inr",
            "bidPrice": "91.40",
            "askPrice": "91.60",
            "lastPrice": "91.50",
            "highPrice": "92.00",
            "lowPrice": "91.00",
            "baseVolume": "1000",
            "quoteVolume": "91000",
            "at": 1234567890000,
        }
        result = CoinswitchUtils.parse_ticker_response(ticker)

        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(Decimal("91.40"), result["bid_price"])
        self.assertEqual(Decimal("91.60"), result["ask_price"])
        self.assertEqual(Decimal("91.50"), result["last_price"])
        self.assertEqual(Decimal("92.00"), result["high_price"])
        self.assertEqual(Decimal("91.00"), result["low_price"])
        self.assertEqual(1234567890000, result["timestamp"])

    def test_parse_balance_response(self):
        balance_response = [
            {"currency": "btc", "main_balance": "1.5", "blocked_balance_order": "0.5"},
            {"currency": "inr", "main_balance": "100000", "blocked_balance_order": "0"},
        ]
        result = CoinswitchUtils.parse_balance_response(balance_response)

        self.assertIn("BTC", result)
        self.assertEqual(Decimal("1.5"), result["BTC"]["total"])
        self.assertEqual(Decimal("0.5"), result["BTC"]["locked"])
        self.assertIn("INR", result)

    def test_parse_balance_response_empty_list(self):
        result = CoinswitchUtils.parse_balance_response([])
        self.assertEqual({}, result)

    def test_parse_depth_response(self):
        depth = {
            "symbol": "btc/inr",
            "timestamp": 1234567890000,
            "bids": [["5000000", "0.001"], ["4999000", "0.002"]],
            "asks": [["5001000", "0.001"], ["5002000", "0.002"]],
        }
        result = CoinswitchUtils.parse_depth_response(depth)

        self.assertEqual("BTC/INR", result["symbol"])
        self.assertEqual(2, len(result["bids"]))
        self.assertEqual(2, len(result["asks"]))
        self.assertEqual(Decimal("5000000"), result["bids"][0][0])

    def test_parse_trade_response(self):
        trade = {
            "E": 1234567890000,
            "m": True,
            "p": "91.50",
            "q": "10.0",
            "s": "BTC,INR",
            "t": "trade_789",
            "e": "coinswitchx",
        }
        result = CoinswitchUtils.parse_trade_response(trade)

        self.assertEqual(1234567890000, result["event_time"])
        self.assertTrue(result["is_buyer_maker"])
        self.assertEqual(Decimal("91.50"), result["price"])
        self.assertEqual(Decimal("10.0"), result["quantity"])
        self.assertEqual("BTC,INR", result["symbol"])
        self.assertEqual("trade_789", result["trade_id"])
        self.assertEqual("coinswitchx", result["exchange"])

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

    def test_parse_balance_response_usdt(self):
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

    def test_parse_depth_response_numeric_input(self):
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

    def test_parse_trade_response_slash_symbol(self):
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
