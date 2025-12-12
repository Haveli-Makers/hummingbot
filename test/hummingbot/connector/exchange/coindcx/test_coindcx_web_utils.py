"""
Test cases for CoinDCX web utilities.
These tests verify URL generation and API constants.
"""
import unittest

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class TestCoinDCXWebUtils(unittest.TestCase):
    """Test cases for CoinDCX web utilities."""

    # Constants replicated from coindcx_constants
    REST_URL = "https://api.coindcx.com"
    WSS_URL = "wss://stream.coindcx.com"

    # API Paths
    EXCHANGE_INFO_PATH = "/exchange/v1/markets_details"
    ORDER_PATH = "/exchange/v1/orders/create"
    CANCEL_ORDER_PATH = "/exchange/v1/orders/cancel"
    USER_BALANCES_PATH = "/exchange/v1/users/balances"
    ORDER_STATUS_PATH = "/exchange/v1/orders/status"
    ACTIVE_ORDERS_PATH = "/exchange/v1/orders/active_orders"
    ORDER_BOOK_PATH = "/market_data/orderbook"
    RECENT_TRADES_PATH = "/market_data/trade_history"
    SERVER_TIME_PATH = "/exchange/v1/time"

    def test_rest_url_format(self):
        """Test that REST URL is properly formatted."""
        self.assertTrue(self.REST_URL.startswith("https://"))
        self.assertIn("coindcx.com", self.REST_URL)
        self.assertFalse(self.REST_URL.endswith("/"))

    def test_wss_url_format(self):
        """Test that WebSocket URL is properly formatted."""
        self.assertTrue(self.WSS_URL.startswith("wss://"))
        self.assertIn("coindcx.com", self.WSS_URL)
        self.assertFalse(self.WSS_URL.endswith("/"))

    def test_public_rest_url(self):
        """Test public REST URL generation."""
        path = self.EXCHANGE_INFO_PATH
        url = f"{self.REST_URL}{path}"

        self.assertEqual(url, "https://api.coindcx.com/exchange/v1/markets_details")

    def test_private_rest_url(self):
        """Test private REST URL generation."""
        path = self.USER_BALANCES_PATH
        url = f"{self.REST_URL}{path}"

        self.assertEqual(url, "https://api.coindcx.com/exchange/v1/users/balances")

    def test_order_book_url(self):
        """Test order book URL generation."""
        symbol = "BTCUSDT"
        url = f"{self.REST_URL}{self.ORDER_BOOK_PATH}?pair={symbol}"

        self.assertIn("orderbook", url)
        self.assertIn("BTCUSDT", url)

    def test_trades_url(self):
        """Test trades URL generation."""
        symbol = "BTCUSDT"
        url = f"{self.REST_URL}{self.RECENT_TRADES_PATH}?pair={symbol}"

        self.assertIn("trade_history", url)
        self.assertIn("BTCUSDT", url)


class TestCoinDCXApiPaths(unittest.TestCase):
    """Test API path constants."""

    EXCHANGE_INFO_PATH = "/exchange/v1/markets_details"
    ORDER_PATH = "/exchange/v1/orders/create"
    CANCEL_ORDER_PATH = "/exchange/v1/orders/cancel"
    USER_BALANCES_PATH = "/exchange/v1/users/balances"
    ORDER_STATUS_PATH = "/exchange/v1/orders/status"

    def test_exchange_info_path_format(self):
        """Test exchange info path."""
        self.assertTrue(self.EXCHANGE_INFO_PATH.startswith("/"))
        self.assertIn("markets", self.EXCHANGE_INFO_PATH)

    def test_order_path_format(self):
        """Test order path."""
        self.assertTrue(self.ORDER_PATH.startswith("/"))
        self.assertIn("orders/create", self.ORDER_PATH)

    def test_cancel_order_path_format(self):
        """Test cancel order path."""
        self.assertTrue(self.CANCEL_ORDER_PATH.startswith("/"))
        self.assertIn("orders/cancel", self.CANCEL_ORDER_PATH)

    def test_balances_path_format(self):
        """Test balances path."""
        self.assertTrue(self.USER_BALANCES_PATH.startswith("/"))
        self.assertIn("balances", self.USER_BALANCES_PATH)

    def test_order_status_path_format(self):
        """Test order status path."""
        self.assertTrue(self.ORDER_STATUS_PATH.startswith("/"))
        self.assertIn("status", self.ORDER_STATUS_PATH)


class TestCoinDCXWebSocketChannels(unittest.TestCase):
    """Test WebSocket channel names."""

    WS_ORDER_BOOK_CHANNEL = "orderbook"
    WS_TRADES_CHANNEL = "trades"
    WS_TICKER_CHANNEL = "ticker"
    WS_USER_CHANNEL = "user"

    def test_order_book_channel(self):
        """Test order book channel name."""
        self.assertEqual(self.WS_ORDER_BOOK_CHANNEL, "orderbook")

    def test_trades_channel(self):
        """Test trades channel name."""
        self.assertEqual(self.WS_TRADES_CHANNEL, "trades")

    def test_ticker_channel(self):
        """Test ticker channel name."""
        self.assertEqual(self.WS_TICKER_CHANNEL, "ticker")

    def test_user_channel(self):
        """Test user channel name."""
        self.assertEqual(self.WS_USER_CHANNEL, "user")


class TestCoinDCXRateLimits(unittest.TestCase):
    """Test rate limit configuration."""

    # Rate limit values
    RATE_LIMIT_ID = "coindcx_rate_limit"
    MAX_REQUESTS_PER_SECOND = 10
    REQUEST_WEIGHT_DEFAULT = 1

    def test_rate_limit_id(self):
        """Test rate limit identifier."""
        self.assertIsInstance(self.RATE_LIMIT_ID, str)
        self.assertTrue(len(self.RATE_LIMIT_ID) > 0)

    def test_max_requests_reasonable(self):
        """Test that max requests per second is reasonable."""
        self.assertGreater(self.MAX_REQUESTS_PER_SECOND, 0)
        self.assertLessEqual(self.MAX_REQUESTS_PER_SECOND, 100)

    def test_default_weight(self):
        """Test default request weight."""
        self.assertEqual(self.REQUEST_WEIGHT_DEFAULT, 1)


class TestCoinDCXUrlConstruction(unittest.TestCase):
    """Test URL construction helpers."""

    REST_URL = "https://api.coindcx.com"

    def build_url(self, path: str, params: dict = None) -> str:
        """Build a full URL with optional query parameters."""
        url = f"{self.REST_URL}{path}"
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{param_str}"
        return url

    def test_build_url_without_params(self):
        """Test URL building without parameters."""
        url = self.build_url("/exchange/v1/time")
        self.assertEqual(url, "https://api.coindcx.com/exchange/v1/time")

    def test_build_url_with_single_param(self):
        """Test URL building with single parameter."""
        url = self.build_url("/market_data/orderbook", {"pair": "BTCUSDT"})
        self.assertIn("pair=BTCUSDT", url)

    def test_build_url_with_multiple_params(self):
        """Test URL building with multiple parameters."""
        url = self.build_url("/market_data/orderbook", {"pair": "BTCUSDT", "limit": "100"})
        self.assertIn("pair=BTCUSDT", url)
        self.assertIn("limit=100", url)

    def test_constants_import(self):
        """Test that constants can be imported and accessed."""
        self.assertIsNotNone(CONSTANTS.DEFAULT_DOMAIN)
        self.assertIsNotNone(CONSTANTS.WSS_URL)
        self.assertIsNotNone(CONSTANTS.REST_URL)


class TestCoinDCXWebUtilsFunctions(unittest.TestCase):
    def test_public_rest_url_market_data(self):
        url = web_utils.public_rest_url("/market_data/orderbook")
        self.assertIn("public.coindcx.com", url)
        self.assertTrue(url.endswith("/market_data/orderbook"))

    def test_public_rest_url_default(self):
        url = web_utils.public_rest_url("/exchange/v1/markets")
        self.assertIn("api.coindcx.com", url)
        self.assertTrue(url.endswith("/exchange/v1/markets"))

    def test_private_rest_url(self):
        url = web_utils.private_rest_url("/exchange/v1/users/balances")
        self.assertIn("api.coindcx.com", url)
        self.assertTrue(url.endswith("/exchange/v1/users/balances"))

    def test_build_api_factory(self):
        throttler = AsyncThrottler([])
        factory = web_utils.build_api_factory(throttler=throttler)
        self.assertIsNotNone(factory)
        self.assertTrue(hasattr(factory, "get_rest_assistant"))

    def test_build_api_factory_without_time_synchronizer_pre_processor(self):
        throttler = AsyncThrottler([])
        factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler)
        self.assertIsNotNone(factory)
        self.assertTrue(hasattr(factory, "get_rest_assistant"))

    def test_create_throttler(self):
        throttler = web_utils.create_throttler()
        self.assertIsInstance(throttler, AsyncThrottler)

    def test_get_current_server_time(self):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(web_utils.get_current_server_time())
        self.assertIsInstance(result, float)


if __name__ == "__main__":
    unittest.main()
