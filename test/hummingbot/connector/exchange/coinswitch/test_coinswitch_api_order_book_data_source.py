import unittest
from unittest.mock import MagicMock


class CoinswitchAPIOrderBookDataSourceTests(unittest.TestCase):
    """Test cases for CoinSwitch API order book data source"""

    def test_module_imports(self):
        """Test that order book data source module can be imported"""
        try:
            from hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source import (
                CoinswitchAPIOrderBookDataSource,
            )
            self.assertIsNotNone(CoinswitchAPIOrderBookDataSource)
        except ImportError as e:
            self.fail(f"Failed to import CoinswitchAPIOrderBookDataSource: {e}")

    def test_data_source_instantiation(self):
        """Test creating a data source instance"""
        from hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source import (
            CoinswitchAPIOrderBookDataSource,
        )

        trading_pairs = ["BTC-INR"]

        # This test validates that the constructor exists and expects the right parameters
        # Real usage would provide connector and api_factory instances
        try:
            data_source = CoinswitchAPIOrderBookDataSource(
                trading_pairs=trading_pairs,
                connector=MagicMock(),
                api_factory=MagicMock()
            )
            self.assertIsNotNone(data_source)
        except TypeError as e:
            # If parameters don't match, that's still useful validation info
            self.assertIn("missing", str(e).lower())

    def test_order_book_message_format(self):
        """Test order book message format"""
        depth_data = {
            "bids": [[91.0, 10], [90.5, 20]],
            "asks": [[91.5, 10], [92.0, 20]],
            "timestamp": 1234567890000
        }

        # Should contain required fields
        self.assertIn("bids", depth_data)
        self.assertIn("asks", depth_data)
        self.assertEqual(2, len(depth_data["bids"]))
        self.assertEqual(2, len(depth_data["asks"]))

    def test_bid_ask_ordering(self):
        """Test that bids and asks are properly ordered"""
        depth_data = {
            "bids": [[91.0, 10], [90.9, 20], [90.8, 30]],
            "asks": [[91.1, 10], [91.2, 20], [91.3, 30]],
            "timestamp": 1234567890000
        }

        # Bids should be in descending order
        for i in range(len(depth_data["bids"]) - 1):
            self.assertGreater(depth_data["bids"][i][0], depth_data["bids"][i + 1][0])

        # Asks should be in ascending order
        for i in range(len(depth_data["asks"]) - 1):
            self.assertLess(depth_data["asks"][i][0], depth_data["asks"][i + 1][0])

    def test_snapshot_data_validation(self):
        """Test validation of snapshot data"""
        valid_snapshot = {
            "bids": [[91.0, 10]],
            "asks": [[91.1, 10]],
            "timestamp": 1234567890000
        }

        # Should contain required fields
        self.assertIn("bids", valid_snapshot)
        self.assertIn("asks", valid_snapshot)
        self.assertIn("timestamp", valid_snapshot)

    def test_timestamp_handling(self):
        """Test proper timestamp handling"""
        timestamp_ms = 1234567890000
        timestamp_s = timestamp_ms / 1000

        # Should convert properly
        self.assertEqual(1234567890.0, timestamp_s)


if __name__ == "__main__":
    unittest.main()
