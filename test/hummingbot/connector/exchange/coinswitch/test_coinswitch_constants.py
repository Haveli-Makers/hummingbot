import unittest

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as cs_constants


class CoinswitchConstantsTests(unittest.TestCase):
    """Test CoinSwitch connector constants"""

    def test_exchange_name_prefix(self):
        """Test exchange name prefix constant"""
        self.assertEqual("x-CS", cs_constants.HBOT_ORDER_ID_PREFIX)

    def test_api_endpoints(self):
        """Test that API endpoint constants are properly defined"""
        self.assertTrue(hasattr(cs_constants, "REST_URL"))
        self.assertTrue(hasattr(cs_constants, "WSS_URL"))
        self.assertIn("https", cs_constants.REST_URL)
        self.assertIn("wss", cs_constants.WSS_URL)

    def test_api_versions(self):
        """Test API version constants"""
        self.assertEqual("v2", cs_constants.PUBLIC_API_VERSION)
        self.assertEqual("v2", cs_constants.PRIVATE_API_VERSION)

    def test_order_path_urls(self):
        """Test order-related path URLs"""
        self.assertTrue(hasattr(cs_constants, "CREATE_ORDER_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "CANCEL_ORDER_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "GET_ORDER_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "OPEN_ORDERS_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "CLOSED_ORDERS_PATH_URL"))

    def test_account_path_urls(self):
        """Test account-related path URLs"""
        self.assertTrue(hasattr(cs_constants, "GET_PORTFOLIO_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "TRADING_FEE_PATH_URL"))

    def test_market_data_path_urls(self):
        """Test market data path URLs"""
        self.assertTrue(hasattr(cs_constants, "DEPTH_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "TICKER_PATH_URL"))
        self.assertTrue(hasattr(cs_constants, "TRADES_PATH_URL"))

    def test_rate_limits(self):
        """Test rate limit configuration"""
        rate_limits = cs_constants.RATE_LIMITS
        self.assertIsInstance(rate_limits, list)
        self.assertGreater(len(rate_limits), 0)

    def test_order_sides(self):
        """Test order side constants"""
        self.assertEqual("buy", cs_constants.SIDE_BUY)
        self.assertEqual("sell", cs_constants.SIDE_SELL)

    def test_order_type(self):
        """Test order type constants"""
        self.assertEqual("limit", cs_constants.ORDER_TYPE_LIMIT)

    def test_order_state_mapping(self):
        """Test order state mapping"""
        self.assertTrue(hasattr(cs_constants, "ORDER_STATE"))
        order_state = cs_constants.ORDER_STATE
        self.assertIn("OPEN", order_state)
        self.assertIn("EXECUTED", order_state)

    def test_supported_exchanges(self):
        """Test supported exchanges list"""
        self.assertIn("coinswitchx", cs_constants.SUPPORTED_EXCHANGES)
        self.assertEqual("coinswitchx", cs_constants.DEFAULT_EXCHANGE)

    def test_ws_settings(self):
        """Test WebSocket settings"""
        self.assertEqual(30, cs_constants.WS_HEARTBEAT_TIME_INTERVAL)


if __name__ == "__main__":
    unittest.main()
