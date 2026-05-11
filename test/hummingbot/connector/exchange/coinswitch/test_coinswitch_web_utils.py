import unittest

from hummingbot.connector.exchange.coinswitch import (
    coinswitch_constants as CONSTANTS,
    coinswitch_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_web_utils import CoinswitchWebUtils


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

    def test_build_ws_url_starts_with_wss(self):
        ws_url = CoinswitchWebUtils.build_ws_url("/spot")
        self.assertTrue(ws_url.startswith("wss://"))
        self.assertIn("/spot", ws_url)

    def test_get_ws_path_for_client(self):
        path = CoinswitchWebUtils.get_ws_path_for_client()
        self.assertIn("spot", path)

    def test_parse_trading_pair_hyphen_format(self):
        base, quote = CoinswitchWebUtils.parse_trading_pair("BTC-INR")
        self.assertEqual("BTC", base)
        self.assertEqual("INR", quote)

    def test_parse_trading_pair_slash_format(self):
        base, quote = CoinswitchWebUtils.parse_trading_pair("BTC/INR")
        self.assertEqual("BTC", base)
        self.assertEqual("INR", quote)

    def test_parse_trading_pair_invalid_raises_value_error(self):
        with self.assertRaises(ValueError):
            CoinswitchWebUtils.parse_trading_pair("BTCINR")

    def test_parse_trading_pair_empty_raises_value_error(self):
        with self.assertRaises(ValueError):
            CoinswitchWebUtils.parse_trading_pair("")

    def test_format_trading_pair(self):
        result = CoinswitchWebUtils.format_trading_pair("BTC", "INR")
        self.assertEqual("BTC/INR", result)

    def test_normalize_symbol_lowercase(self):
        result = CoinswitchWebUtils.normalize_symbol("btc")
        self.assertEqual("BTC", result)

    def test_normalize_symbol_already_upper(self):
        result = CoinswitchWebUtils.normalize_symbol("BTC")
        self.assertEqual("BTC", result)


if __name__ == "__main__":
    unittest.main()
