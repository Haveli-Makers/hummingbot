
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.core.rate_oracle.sources.wazirx_rate_source import WazirxRateSource


class WazirxRateSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.source = WazirxRateSource()

    def test_name(self):
        self.assertEqual("wazirx", self.source.name)

    def test_parse_trading_pair_with_base_quote_units(self):
        ticker_obj = {
            "base_unit": "btc",
            "quote_unit": "usdt"
        }
        parsed = self.source._parse_trading_pair("btcusdt", ticker_obj)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get("trading_pair"), "BTC-USDT")
        self.assertEqual(parsed.get("base"), "BTC")
        self.assertEqual(parsed.get("quote"), "USDT")

    def test_parse_trading_pair_with_base_quote_fields(self):
        ticker_obj = {
            "base": "eth",
            "quote": "btc"
        }
        parsed = self.source._parse_trading_pair("ethbtc", ticker_obj)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get("trading_pair"), "ETH-BTC")

    def test_parse_trading_pair_fallback_parsing(self):
        ticker_obj = {}
        parsed = self.source._parse_trading_pair("btcusdt", ticker_obj)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get("trading_pair"), "BTC-USDT")

    def test_parse_trading_pair_fallback_inr(self):
        ticker_obj = {}
        parsed = self.source._parse_trading_pair("btcinr", ticker_obj)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get("trading_pair"), "BTC-INR")

    def test_parse_trading_pair_invalid_key(self):
        ticker_obj = {}
        parsed = self.source._parse_trading_pair("invalid", ticker_obj)
        self.assertIsNone(parsed)

    def test_parse_trading_pair_exception_handling(self):
        ticker_obj = None
        parsed = self.source._parse_trading_pair("btcusdt", ticker_obj)
        self.assertIsNone(parsed)

    @aioresponses()
    async def test_get_prices_success(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "50000.0",
                "sell": "50010.0"
            },
            "ethusdt": {
                "base_unit": "eth",
                "quote_unit": "usdt",
                "buy": "3000.0",
                "sell": "3010.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_prices()
        self.assertIn("BTC-USDT", prices)
        self.assertIn("ETH-USDT", prices)
        self.assertEqual(Decimal("50005.0"), prices["BTC-USDT"])  
        self.assertEqual(Decimal("3005.0"), prices["ETH-USDT"])   

    @aioresponses()
    async def test_get_prices_with_quote_filter(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "50000.0",
                "sell": "50010.0"
            },
            "btcinr": {
                "base_unit": "btc",
                "quote_unit": "inr",
                "buy": "4000000.0",
                "sell": "4001000.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_prices(quote_token="USDT")
        self.assertIn("BTC-USDT", prices)
        self.assertNotIn("BTC-INR", prices)

    @aioresponses()
    async def test_get_prices_api_error(self, mock_api):
        mock_api.get("https://api.wazirx.com/api/v2/tickers", status=500)

        prices = await self.source.get_prices()
        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_prices_invalid_data(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "invalid",
                "sell": "50010.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_prices()
        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_prices_missing_buy_sell(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": None,
                "sell": "50010.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_prices()
        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_bid_ask_prices_success(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "50000.0",
                "sell": "50010.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_bid_ask_prices()
        self.assertIn("BTC-USDT", prices)
        data = prices["BTC-USDT"]
        self.assertEqual(Decimal("50000.0"), data["bid"])
        self.assertEqual(Decimal("50010.0"), data["ask"])
        self.assertEqual(Decimal("50005.0"), data["mid"])
        self.assertEqual(Decimal("10.0"), data["spread"])
        self.assertAlmostEqual(Decimal("0.02"), data["spread_pct"], places=2)

    @aioresponses()
    async def test_get_bid_ask_prices_with_quote_filter(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "50000.0",
                "sell": "50010.0"
            },
            "btcinr": {
                "base_unit": "btc",
                "quote_unit": "inr",
                "buy": "4000000.0",
                "sell": "4001000.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_bid_ask_prices(quote_token="INR")
        self.assertNotIn("BTC-USDT", prices)
        self.assertIn("BTC-INR", prices)

    @aioresponses()
    async def test_get_bid_ask_prices_api_error(self, mock_api):
        mock_api.get("https://api.wazirx.com/api/v2/tickers", status=404)

        prices = await self.source.get_bid_ask_prices()
        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_bid_ask_prices_invalid_bid_ask(self, mock_api):
        mock_data = {
            "btcusdt": {
                "base_unit": "btc",
                "quote_unit": "usdt",
                "buy": "50010.0",  # bid > ask
                "sell": "50000.0"
            }
        }

        mock_api.get("https://api.wazirx.com/api/v2/tickers", payload=mock_data)

        prices = await self.source.get_bid_ask_prices()
        self.assertEqual({}, prices)