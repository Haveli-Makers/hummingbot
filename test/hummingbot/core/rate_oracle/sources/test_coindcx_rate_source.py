import asyncio
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
