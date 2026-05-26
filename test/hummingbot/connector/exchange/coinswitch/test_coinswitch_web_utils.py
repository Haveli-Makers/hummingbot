import unittest

from hummingbot.connector.exchange.coinswitch import (
    coinswitch_constants as CONSTANTS,
    coinswitch_web_utils as web_utils,
)


class CoinswitchWebUtilsTests(unittest.TestCase):
    """Test CoinSwitch web utility functions."""

    def test_public_rest_url_starts_with_https(self):
        url = web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL)
        self.assertTrue(url.startswith("https://"))
        self.assertIn(CONSTANTS.DEPTH_PATH_URL, url)

    def test_private_rest_url_starts_with_https(self):
        url = web_utils.private_rest_url(CONSTANTS.CREATE_ORDER_PATH_URL)
        self.assertTrue(url.startswith("https://"))
        self.assertIn(CONSTANTS.CREATE_ORDER_PATH_URL, url)

    def test_build_api_url(self):
        url = web_utils.build_api_url(CONSTANTS.DEPTH_PATH_URL)
        self.assertTrue(url.startswith("https://"))
        self.assertIn(CONSTANTS.DEPTH_PATH_URL, url)

    def test_public_and_private_rest_url_same_base(self):
        """CoinSwitch uses the same base for public and private endpoints."""
        pub = web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL)
        priv = web_utils.private_rest_url(CONSTANTS.DEPTH_PATH_URL)
        self.assertEqual(pub, priv)

    def test_trade_info_url(self):
        url = web_utils.build_api_url(CONSTANTS.TRADE_INFO_PATH_URL)
        self.assertIn("/trade/api/v2/tradeInfo", url)

    def test_order_url(self):
        url = web_utils.build_api_url(CONSTANTS.CREATE_ORDER_PATH_URL)
        self.assertIn("/order", url)

    def test_ticker_all_url(self):
        url = web_utils.build_api_url(CONSTANTS.TICKER_ALL_PATH_URL)
        self.assertIn("ticker", url)

    def test_trading_fee_url(self):
        url = web_utils.build_api_url(CONSTANTS.TRADING_FEE_PATH_URL)
        self.assertIn("tradingFee", url)

    def test_depth_url(self):
        url = web_utils.build_api_url(CONSTANTS.DEPTH_PATH_URL)
        self.assertIn("/depth", url)

    def test_trades_url(self):
        url = web_utils.build_api_url(CONSTANTS.TRADES_PATH_URL)
        self.assertIn("/trades", url)

    def test_portfolio_url(self):
        url = web_utils.build_api_url(CONSTANTS.GET_PORTFOLIO_PATH_URL)
        self.assertIn("/portfolio", url)

    def test_create_throttler_returns_non_none(self):
        throttler = web_utils.create_throttler()
        self.assertIsNotNone(throttler)

    def test_build_api_factory_returns_non_none(self):
        api_factory = web_utils.build_api_factory()
        self.assertIsNotNone(api_factory)

    def test_build_api_factory_without_sync_returns_non_none(self):
        throttler = web_utils.create_throttler()
        api_factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
        self.assertIsNotNone(api_factory)


if __name__ == "__main__":
    unittest.main()
