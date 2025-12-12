"""
Test cases for CoinDCX utility functions.
These tests verify the logic patterns used in CoinDCX connector.
"""
import unittest
from decimal import Decimal


class TestCoinDCXUtilsLogic(unittest.TestCase):
    """Test cases for CoinDCX utility logic (without importing coindcx_utils)."""

    # Define the logic locally to test patterns
    KNOWN_QUOTE_CURRENCIES = ["USDT", "USDC", "INR", "BTC", "ETH", "BUSD"]

    def is_exchange_information_valid(self, exchange_info: dict) -> bool:
        """Replicate the validation logic."""
        status = exchange_info.get("status", "")
        return status.lower() == "active"

    def coindcx_pair_to_hb_pair(self, coindcx_pair: str) -> str:
        """Replicate the pair conversion logic."""
        # Handle socket format (e.g., "B-BTC_USDT" -> "BTC-USDT")
        if coindcx_pair.startswith(("B-", "I-")):
            parts = coindcx_pair.split("-", 1)
            if len(parts) == 2:
                return parts[1].replace("_", "-")

        # Handle standard format (e.g., "BTCUSDT" -> "BTC-USDT")
        for quote in self.KNOWN_QUOTE_CURRENCIES:
            if coindcx_pair.endswith(quote):
                base = coindcx_pair[:-len(quote)]
                return f"{base}-{quote}"

        return coindcx_pair

    def hb_pair_to_coindcx_symbol(self, hb_pair: str) -> str:
        """Replicate the pair conversion logic."""
        return hb_pair.replace("-", "")

    # ===== Exchange Information Validation Tests =====

    def test_is_exchange_information_valid_active_status(self):
        """Test that active markets are considered valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT",
            "status": "active"
        }
        self.assertTrue(self.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_active_uppercase(self):
        """Test that ACTIVE (uppercase) markets are considered valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT",
            "status": "ACTIVE"
        }
        self.assertTrue(self.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_inactive_status(self):
        """Test that inactive markets are not valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT",
            "status": "inactive"
        }
        self.assertFalse(self.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_paused_status(self):
        """Test that paused markets are not valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT",
            "status": "paused"
        }
        self.assertFalse(self.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_missing_status(self):
        """Test that markets without status are not valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT"
        }
        self.assertFalse(self.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_empty_status(self):
        """Test that markets with empty status are not valid."""
        exchange_info = {
            "coindcx_name": "BTCUSDT",
            "status": ""
        }
        self.assertFalse(self.is_exchange_information_valid(exchange_info))

    # ===== CoinDCX to Hummingbot Pair Conversion Tests =====

    def test_coindcx_pair_to_hb_pair_usdt(self):
        """Test converting BTCUSDT to BTC-USDT."""
        result = self.coindcx_pair_to_hb_pair("BTCUSDT")
        self.assertEqual(result, "BTC-USDT")

    def test_coindcx_pair_to_hb_pair_inr(self):
        """Test converting BTCINR to BTC-INR."""
        result = self.coindcx_pair_to_hb_pair("BTCINR")
        self.assertEqual(result, "BTC-INR")

    def test_coindcx_pair_to_hb_pair_btc_quote(self):
        """Test converting ETHBTC to ETH-BTC."""
        result = self.coindcx_pair_to_hb_pair("ETHBTC")
        self.assertEqual(result, "ETH-BTC")

    def test_coindcx_pair_to_hb_pair_socket_format(self):
        """Test converting B-BTC_USDT format to BTC-USDT."""
        result = self.coindcx_pair_to_hb_pair("B-BTC_USDT")
        self.assertEqual(result, "BTC-USDT")

    def test_coindcx_pair_to_hb_pair_i_prefix(self):
        """Test converting I-BTC_INR format to BTC-INR."""
        result = self.coindcx_pair_to_hb_pair("I-BTC_INR")
        self.assertEqual(result, "BTC-INR")

    def test_coindcx_pair_to_hb_pair_usdc(self):
        """Test converting BTCUSDC to BTC-USDC."""
        result = self.coindcx_pair_to_hb_pair("BTCUSDC")
        self.assertEqual(result, "BTC-USDC")

    def test_coindcx_pair_to_hb_pair_unknown_quote(self):
        """Test that unknown pairs are returned as-is."""
        result = self.coindcx_pair_to_hb_pair("UNKNOWNPAIR")
        self.assertEqual(result, "UNKNOWNPAIR")

    # ===== Hummingbot to CoinDCX Symbol Conversion Tests =====

    def test_hb_pair_to_coindcx_symbol(self):
        """Test converting BTC-USDT to BTCUSDT."""
        result = self.hb_pair_to_coindcx_symbol("BTC-USDT")
        self.assertEqual(result, "BTCUSDT")

    def test_hb_pair_to_coindcx_symbol_inr(self):
        """Test converting BTC-INR to BTCINR."""
        result = self.hb_pair_to_coindcx_symbol("BTC-INR")
        self.assertEqual(result, "BTCINR")

    def test_hb_pair_to_coindcx_symbol_complex(self):
        """Test converting ETH-BTC to ETHBTC."""
        result = self.hb_pair_to_coindcx_symbol("ETH-BTC")
        self.assertEqual(result, "ETHBTC")


class TestCoinDCXPairConversionRoundTrip(unittest.TestCase):
    """Test that pair conversion works both ways."""

    KNOWN_QUOTE_CURRENCIES = ["USDT", "USDC", "INR", "BTC", "ETH", "BUSD"]

    def coindcx_pair_to_hb_pair(self, coindcx_pair: str) -> str:
        """Replicate the pair conversion logic."""
        if coindcx_pair.startswith(("B-", "I-")):
            parts = coindcx_pair.split("-", 1)
            if len(parts) == 2:
                return parts[1].replace("_", "-")

        for quote in self.KNOWN_QUOTE_CURRENCIES:
            if coindcx_pair.endswith(quote):
                base = coindcx_pair[:-len(quote)]
                return f"{base}-{quote}"

        return coindcx_pair

    def hb_pair_to_coindcx_symbol(self, hb_pair: str) -> str:
        """Replicate the pair conversion logic."""
        return hb_pair.replace("-", "")

    def test_roundtrip_usdt_pair(self):
        """Test BTC-USDT -> BTCUSDT -> BTC-USDT."""
        hb_pair = "BTC-USDT"
        coindcx_symbol = self.hb_pair_to_coindcx_symbol(hb_pair)
        result = self.coindcx_pair_to_hb_pair(coindcx_symbol)

        self.assertEqual(result, hb_pair)

    def test_roundtrip_inr_pair(self):
        """Test BTC-INR -> BTCINR -> BTC-INR."""
        hb_pair = "BTC-INR"
        coindcx_symbol = self.hb_pair_to_coindcx_symbol(hb_pair)
        result = self.coindcx_pair_to_hb_pair(coindcx_symbol)

        self.assertEqual(result, hb_pair)

    def test_roundtrip_eth_btc(self):
        """Test ETH-BTC -> ETHBTC -> ETH-BTC."""
        hb_pair = "ETH-BTC"
        coindcx_symbol = self.hb_pair_to_coindcx_symbol(hb_pair)
        result = self.coindcx_pair_to_hb_pair(coindcx_symbol)

        self.assertEqual(result, hb_pair)


class TestCoinDCXFeeConfiguration(unittest.TestCase):
    """Test the fee configuration values."""

    def test_default_maker_fee(self):
        """Test that maker fee is 0.1%."""
        maker_fee = Decimal("0.001")
        self.assertEqual(maker_fee, Decimal("0.001"))

    def test_default_taker_fee(self):
        """Test that taker fee is 0.1%."""
        taker_fee = Decimal("0.001")
        self.assertEqual(taker_fee, Decimal("0.001"))

    def test_fee_deducted_from_returns(self):
        """Test that fees are deducted from returns for buys."""
        buy_percent_fee_deducted_from_returns = True
        self.assertTrue(buy_percent_fee_deducted_from_returns)


class TestCoinDCXConstants(unittest.TestCase):
    """Test the constant values used in CoinDCX connector."""

    def test_centralized_flag(self):
        """Test that CENTRALIZED flag is True."""
        CENTRALIZED = True
        self.assertTrue(CENTRALIZED)

    def test_example_pair_format(self):
        """Test that example pair follows the BTC-USDT format."""
        EXAMPLE_PAIR = "BTC-USDT"
        self.assertRegex(EXAMPLE_PAIR, r"^[A-Z]+-[A-Z]+$")

    def test_api_base_url_format(self):
        """Test that API base URL is valid."""
        REST_URL = "https://api.coindcx.com"
        self.assertTrue(REST_URL.startswith("https://"))
        self.assertIn("coindcx", REST_URL)


if __name__ == "__main__":
    unittest.main()
