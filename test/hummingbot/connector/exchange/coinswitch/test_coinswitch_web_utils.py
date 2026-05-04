import asyncio
import unittest

from typing_extensions import Awaitable

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as cs_constants, coinswitch_web_utils


class CoinswitchWebUtilsTests(unittest.TestCase):
    """Test CoinSwitch web utilities"""

    def setUp(self) -> None:
        self.web_utils = coinswitch_web_utils

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_public_rest_url_building(self):
        """Test building public REST URLs"""
        endpoint = "/trade/api/v2/depth"
        url = self.web_utils.public_rest_url(endpoint)

        self.assertTrue(url.startswith("https://"))
        self.assertIn(endpoint, url)

    def test_private_rest_url_building(self):
        """Test building private REST URLs"""
        endpoint = "/trade/api/v2/order"
        url = self.web_utils.private_rest_url(endpoint)

        self.assertTrue(url.startswith("https://"))
        self.assertIn(endpoint, url)

    def test_build_api_url(self):
        """Test building API URLs"""
        endpoint = "/trade/api/v2/depth"
        url = self.web_utils.build_api_url(endpoint)

        self.assertTrue(url.startswith("https://"))
        self.assertIn(endpoint, url)

    def test_rest_api_factory_creation(self):
        """Test REST API factory creation"""
        api_factory = self.web_utils.build_api_factory()
        self.assertIsNotNone(api_factory)

    def test_rest_api_factory_without_sync(self):
        """Test REST API factory without time synchronizer"""
        throttler = self.web_utils.create_throttler()
        api_factory = self.web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)

        self.assertIsNotNone(api_factory)

    def test_create_throttler(self):
        """Test throttler creation"""
        throttler = self.web_utils.create_throttler()

        self.assertIsNotNone(throttler)

    def test_build_ws_url(self):
        """Test WebSocket URL building"""
        path = "/spot"
        ws_url = self.web_utils.CoinswitchWebUtils.build_ws_url(path)

        self.assertTrue(ws_url.startswith("wss://"))

    def test_parse_trading_pair_hyphen_format(self):
        """Test parsing trading pair in hyphen format"""
        base, quote = self.web_utils.CoinswitchWebUtils.parse_trading_pair("BTC-INR")
        self.assertEqual("BTC", base)
        self.assertEqual("INR", quote)

    def test_parse_trading_pair_slash_format(self):
        """Test parsing trading pair in slash format"""
        base, quote = self.web_utils.CoinswitchWebUtils.parse_trading_pair("BTC/INR")
        self.assertEqual("BTC", base)
        self.assertEqual("INR", quote)

    def test_format_trading_pair(self):
        """Test formatting trading pair"""
        result = self.web_utils.CoinswitchWebUtils.format_trading_pair("BTC", "INR")
        self.assertEqual("BTC/INR", result)

    def test_normalize_symbol(self):
        """Test symbol normalization"""
        result = self.web_utils.CoinswitchWebUtils.normalize_symbol("btc")
        self.assertEqual("BTC", result)

    def test_order_url_building(self):
        """Test order endpoint URL building"""
        order_url = self.web_utils.build_api_url(cs_constants.CREATE_ORDER_PATH_URL)
        self.assertIn("/order", order_url)

    def test_depth_url_building(self):
        """Test depth/order book endpoint URL building"""
        depth_url = self.web_utils.build_api_url(cs_constants.DEPTH_PATH_URL)
        self.assertIn("/depth", depth_url)

    def test_trades_url_building(self):
        """Test recent trades endpoint URL building"""
        trades_url = self.web_utils.build_api_url(cs_constants.TRADES_PATH_URL)
        self.assertIn("/trades", trades_url)

    def test_portfolio_url_building(self):
        """Test portfolio endpoint URL building"""
        portfolio_url = self.web_utils.build_api_url(cs_constants.GET_PORTFOLIO_PATH_URL)
        self.assertIn("/portfolio", portfolio_url)


if __name__ == "__main__":
    unittest.main()
