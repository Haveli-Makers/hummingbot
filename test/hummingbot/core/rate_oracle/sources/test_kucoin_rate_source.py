import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses

from hummingbot.connector.exchange.kucoin import kucoin_constants as CONSTANTS
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.sources.kucoin_rate_source import KucoinRateSource


class KucoinRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.target_token = "COINALPHA"
        cls.global_token = "HBOT"
        cls.trading_pair = combine_to_hb_trading_pair(base=cls.target_token, quote=cls.global_token)
        cls.ignored_trading_pair = combine_to_hb_trading_pair(base="SOME", quote="PAIR")

    def setup_kucoin_responses(self, mock_api, expected_rate: Decimal):
        symbols_url = f"{CONSTANTS.BASE_PATH_URL['main']}{CONSTANTS.SYMBOLS_PATH_URL}"
        symbols_response = {  # truncated response
            "data": [
                {
                    "symbol": self.trading_pair,
                    "baseCurrency": self.target_token,
                    "quoteCurrency": self.global_token,
                    "enableTrading": True,
                },
                {
                    "symbol": self.ignored_trading_pair,
                    "baseCurrency": "SOME",
                    "quoteCurrency": "PAIR",
                    "enableTrading": False,
                },
            ],
        }
        mock_api.get(url=symbols_url, body=json.dumps(symbols_response))
        prices_url = f"{CONSTANTS.BASE_PATH_URL['main']}{CONSTANTS.ALL_TICKERS_PATH_URL}"
        prices_response = {  # truncated response
            "data": {
                "time": 1602832092060,
                "ticker": [
                    {
                        "symbol": self.trading_pair,
                        "symbolName": self.trading_pair,
                        "buy": str(expected_rate - Decimal("0.1")),
                        "sell": str(expected_rate + Decimal("0.1")),
                    },
                    {
                        "symbol": self.ignored_trading_pair,
                        "symbolName": self.ignored_trading_pair,
                        "buy": str(expected_rate - Decimal("0.1")),
                        "sell": str(expected_rate + Decimal("0.1")),
                    }
                ],
            },
        }
        mock_api.get(url=prices_url, body=json.dumps(prices_response))

    @aioresponses()
    async def test_get_prices(self, mock_api):
        expected_rate = Decimal("10")
        self.setup_kucoin_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = KucoinRateSource()
        prices = await rate_source.get_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])
        self.assertNotIn(self.ignored_trading_pair, prices)

    @aioresponses()
    async def test_get_bid_ask_prices(self, mock_api):
        expected_rate = Decimal("10")
        self.setup_kucoin_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = KucoinRateSource()
        rate_source._ensure_exchange()  # ensure exchange is created
        with patch.object(rate_source._exchange, 'trading_pair_associated_to_exchange_symbol', new_callable=AsyncMock) as mock_method:
            mock_method.return_value = self.trading_pair
            prices = await rate_source.get_bid_ask_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(Decimal("9.9"), prices[self.trading_pair]["bid"])
        self.assertEqual(Decimal("10.1"), prices[self.trading_pair]["ask"])
        self.assertEqual(expected_rate, prices[self.trading_pair]["mid"])
        self.assertEqual(Decimal("0.2"), prices[self.trading_pair]["spread"])
        self.assertEqual(Decimal("2"), prices[self.trading_pair]["spread_pct"])
        self.assertNotIn(self.ignored_trading_pair, prices)

    @aioresponses()
    async def test_get_prices_exception(self, mock_api):
        prices_url = f"{CONSTANTS.BASE_PATH_URL['main']}{CONSTANTS.ALL_TICKERS_PATH_URL}"
        mock_api.get(url=prices_url, exception=Exception("API Error"))

        rate_source = KucoinRateSource()
        prices = await rate_source.get_prices()

        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_bid_ask_prices_exception(self, mock_api):
        prices_url = f"{CONSTANTS.BASE_PATH_URL['main']}{CONSTANTS.ALL_TICKERS_PATH_URL}"
        mock_api.get(url=prices_url, exception=Exception("API Error"))

        rate_source = KucoinRateSource()
        prices = await rate_source.get_bid_ask_prices()

        self.assertEqual({}, prices)
