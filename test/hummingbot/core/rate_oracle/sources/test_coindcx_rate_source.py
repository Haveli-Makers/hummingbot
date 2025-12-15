
import time
from decimal import Decimal
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import patch

from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource


class FakeResponse:
    def __init__(self, status: int, json_data):
        self.status = status
        self._json = json_data

    async def json(self):
        return self._json

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, mapping):
        self._mapping = mapping

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url):
        resp = self._mapping.get(url)
        if resp is None:
            return FakeResponse(404, {})
        return FakeResponse(resp.get("status", 200), resp.get("json"))


class CoindcxRateSourceTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.source = CoindcxRateSource()

    def test_parse_trading_pair(self):
        parsed = self.source._parse_trading_pair("BTCUSDT")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.get("trading_pair"), "BTC-USDT")

        # Test invalid symbol
        parsed_invalid = self.source._parse_trading_pair("INVALID")
        self.assertIsNone(parsed_invalid)

    def test_name(self):
        self.assertEqual(self.source.name, "coindcx")

    async def test_fetch_markets_cache_hit(self):
        # Set cache
        self.source._markets_cache = {"test": "data"}
        self.source._markets_cache_time = time.time()

        markets = await self.source._fetch_markets()
        self.assertEqual(markets, {"test": "data"})

    async def test_fetch_markets_bad_response(self):
        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 404, "json": {}},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            markets = await self.source._fetch_markets()
            self.assertEqual(markets, {})

    async def test_fetch_markets_exception(self):
        with patch("aiohttp.ClientSession", side_effect=Exception("Network error")):
            markets = await self.source._fetch_markets()
            self.assertEqual(markets, {})

    async def test_get_prices_and_bid_ask_prices(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        tickers = [
            {"market": "BTCUSDT", "bid": "100", "ask": "102"}
        ]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices(quote_token="USDT")
            self.assertIn("BTC-USDT", prices)
            self.assertEqual(prices["BTC-USDT"], Decimal("101"))

            bid_asks = await self.source.get_bid_ask_prices(quote_token="USDT")
            self.assertIn("BTC-USDT", bid_asks)
            entry = bid_asks["BTC-USDT"]
            self.assertEqual(entry["bid"], Decimal("100"))
            self.assertEqual(entry["ask"], Decimal("102"))
            self.assertEqual(entry["mid"], Decimal("101"))

    async def test_get_prices_with_dict_data(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        # Data as dict with "data" key
        tickers = {"data": [{"market": "BTCUSDT", "bid": "100", "ask": "102"}]}

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertIn("BTC-USDT", prices)

    async def test_get_prices_with_nested_dict_tickers(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        # Tickers as dict with string values
        tickers = {"market1": "string_value"}

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_prices_with_string_ticker(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        tickers = ["string_ticker"]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_prices_with_parse_trading_pair_fallback(self):
        # No markets data, should use _parse_trading_pair
        tickers = [{"market": "BTCUSDT", "bid": "100", "ask": "102"}]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": []},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession") as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertIn("BTC-USDT", prices)

    async def test_get_prices_with_quote_token_filter(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"},
            {"coindcx_name": "BTCINR", "base_currency_short_name": "INR", "target_currency_short_name": "BTC"}
        ]
        tickers = [
            {"market": "BTCUSDT", "bid": "100", "ask": "102"},
            {"market": "BTCINR", "bid": "200", "ask": "202"}
        ]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices(quote_token="USDT")
            self.assertIn("BTC-USDT", prices)
            self.assertNotIn("BTC-INR", prices)

    async def test_get_prices_with_none_bid_ask(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        tickers = [{"market": "BTCUSDT", "bid": None, "ask": "102"}]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_prices_with_invalid_decimal(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        tickers = [{"market": "BTCUSDT", "bid": "invalid", "ask": "102"}]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            prices = await self.source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_bid_ask_prices_with_bid_greater_than_ask(self):
        markets_data = [
            {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"}
        ]
        tickers = [{"market": "BTCUSDT", "bid": "102", "ask": "100"}]

        mapping = {
            self.source.MARKETS_DETAILS_URL: {"status": 200, "json": markets_data},
            self.source.TICKER_URL: {"status": 200, "json": tickers},
        }

        with patch("aiohttp.ClientSession", autospec=True) as mock_cs:
            mock_cs.return_value = FakeSession(mapping)
            bid_asks = await self.source.get_bid_ask_prices()
            self.assertEqual(bid_asks, {})
