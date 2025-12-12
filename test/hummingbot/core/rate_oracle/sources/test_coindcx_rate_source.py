import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource


class CoindcxRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.target_token = "BTC"
        cls.global_token = "USDT"
        cls.coindcx_symbol = f"{cls.target_token}{cls.global_token}"
        cls.trading_pair = f"{cls.target_token}-{cls.global_token}"

    def setup_coindcx_responses(self, mock_api, expected_rate: Decimal):
        # Mock markets details response
        markets_url = CoindcxRateSource.MARKETS_DETAILS_URL
        markets_response = [
            {
                "coindcx_name": self.coindcx_symbol,
                "base_currency_short_name": self.global_token,  # This is actually quote in CoinDCX API
                "target_currency_short_name": self.target_token,  # This is actually base in CoinDCX API
            }
        ]

        # Mock ticker response
        ticker_url = CoindcxRateSource.TICKER_URL
        ticker_response = [
            {
                "market": self.coindcx_symbol,
                "bid": str(expected_rate - Decimal("0.1")),
                "ask": str(expected_rate + Decimal("0.1")),
            }
        ]

        mock_api.get(markets_url, body=json.dumps(markets_response))
        mock_api.get(ticker_url, body=json.dumps(ticker_response))

    @aioresponses()
    async def test_get_coindcx_prices(self, mock_api):
        expected_rate = Decimal("50000")
        self.setup_coindcx_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])

    @aioresponses()
    async def test_get_coindcx_prices_with_quote_token(self, mock_api):
        expected_rate = Decimal("50000")
        self.setup_coindcx_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_prices(quote_token=self.global_token)

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])

    @aioresponses()
    async def test_get_coindcx_prices_with_wrong_quote_token(self, mock_api):
        expected_rate = Decimal("50000")
        self.setup_coindcx_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_prices(quote_token="BTC")

        self.assertNotIn(self.trading_pair, prices)

    @aioresponses()
    async def test_get_coindcx_bid_ask_prices(self, mock_api):
        expected_bid = Decimal("49999.9")
        expected_ask = Decimal("50000.1")
        expected_mid = (expected_bid + expected_ask) / Decimal("2")
        expected_spread = expected_ask - expected_bid
        expected_spread_pct = (expected_spread / expected_mid) * Decimal("100")

        # Mock markets details response
        markets_url = CoindcxRateSource.MARKETS_DETAILS_URL
        markets_response = [
            {
                "coindcx_name": self.coindcx_symbol,
                "base_currency_short_name": self.global_token,
                "target_currency_short_name": self.target_token,
            }
        ]

        # Mock ticker response
        ticker_url = CoindcxRateSource.TICKER_URL
        ticker_response = [
            {
                "market": self.coindcx_symbol,
                "bid": str(expected_bid),
                "ask": str(expected_ask),
            }
        ]

        mock_api.get(markets_url, body=json.dumps(markets_response))
        mock_api.get(ticker_url, body=json.dumps(ticker_response))

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_bid_ask_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_bid, prices[self.trading_pair]["bid"])
        self.assertEqual(expected_ask, prices[self.trading_pair]["ask"])
        self.assertEqual(expected_mid, prices[self.trading_pair]["mid"])
        self.assertEqual(expected_spread, prices[self.trading_pair]["spread"])
        self.assertAlmostEqual(expected_spread_pct, prices[self.trading_pair]["spread_pct"], places=5)

    def test_name(self):
        rate_source = CoindcxRateSource()
        self.assertEqual("coindcx", rate_source.name)

    def test_parse_trading_pair(self):
        rate_source = CoindcxRateSource()

        # Test valid pairs
        result = rate_source._parse_trading_pair("BTCUSDT")
        self.assertIsNotNone(result)
        self.assertEqual("BTC-USDT", result["trading_pair"])
        self.assertEqual("BTC", result["base"])
        self.assertEqual("USDT", result["quote"])

        result = rate_source._parse_trading_pair("ETHBTC")
        self.assertIsNotNone(result)
        self.assertEqual("ETH-BTC", result["trading_pair"])

        # Test invalid pair
        result = rate_source._parse_trading_pair("INVALID")
        self.assertIsNone(result)

    @aioresponses()
    async def test_get_prices_with_fallback_parsing(self, mock_api):
        expected_rate = Decimal("50000")

        # Mock markets details to return empty (simulate failure)
        markets_url = CoindcxRateSource.MARKETS_DETAILS_URL
        mock_api.get(markets_url, body=json.dumps([]))

        # Mock ticker response
        ticker_url = CoindcxRateSource.TICKER_URL
        ticker_response = [
            {
                "market": self.coindcx_symbol,
                "bid": str(expected_rate - Decimal("0.1")),
                "ask": str(expected_rate + Decimal("0.1")),
            }
        ]
        mock_api.get(ticker_url, body=json.dumps(ticker_response))

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])

    @aioresponses()
    async def test_get_prices_api_error(self, mock_api):
        # Mock API error
        ticker_url = CoindcxRateSource.TICKER_URL
        mock_api.get(ticker_url, status=500)

        rate_source = CoindcxRateSource()
        prices = await rate_source.get_prices()

        self.assertEqual({}, prices)