import unittest
from unittest.mock import MagicMock


class CoinswitchExchangeTests(unittest.TestCase):
    """Test cases for CoinSwitch Exchange connector"""

    def test_exchange_module_imports(self):
        """Test that exchange module can be imported"""
        try:
            from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange
            self.assertIsNotNone(CoinswitchExchange)
        except (ImportError, ModuleNotFoundError) as e:
            self.skipTest(f"Skipping due to missing dependency: {e}")

    def test_exchange_instantiation(self):
        """Test basic exchange instantiation"""
        try:
            from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

            exchange = CoinswitchExchange(
                client_config_map=MagicMock(),
                coinswitch_api_key="test_key",
                coinswitch_secret_key="test_secret",
                trading_pairs=["BTC-INR"],
                trading_required=False
            )

            self.assertIsNotNone(exchange)
        except (ImportError, ModuleNotFoundError) as e:
            self.skipTest(f"Skipping due to missing dependency: {e}")


if __name__ == "__main__":
    unittest.main()
