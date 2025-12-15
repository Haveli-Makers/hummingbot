import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses

from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS, mexc_web_utils as web_utils
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.sources.mexc_rate_source import MexcRateSource


class MexcRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.target_token = "COINALPHA"
        cls.global_token = "HBOT"
        cls.mexc_pair = f"{cls.target_token}{cls.global_token}"  # MEXC doesn't use separator
        cls.trading_pair = combine_to_hb_trading_pair(base=cls.target_token, quote=cls.global_token)
        cls.mexc_ignored_pair = "SOMEPAIR"
        cls.ignored_trading_pair = combine_to_hb_trading_pair(base="SOME", quote="PAIR")

    def setup_mexc_responses(self, mock_api, expected_rate: Decimal):
        pairs_url = web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL)
        symbols_response = {
            "symbols": [
                {
                    "symbol": self.mexc_pair,
                    "status": "1",
                    "baseAsset": self.target_token,
                    "quoteAsset": self.global_token,
                    "permissions": ["SPOT"],
                    "isSpotTradingAllowed": True,
                },
                {
                    "symbol": self.mexc_ignored_pair,
                    "status": "0",
                    "baseAsset": "SOME",
                    "quoteAsset": "PAIR",
                    "permissions": ["SPOT"],
                    "isSpotTradingAllowed": False,
                },
            ]
        }

        mexc_prices_url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        mexc_prices_response = [
            {
                "symbol": self.mexc_pair,
                "bidPrice": str(expected_rate - Decimal("0.1")),
                "bidQty": "0.50000000",
                "askPrice": str(expected_rate + Decimal("0.1")),
                "askQty": "0.14500000",
            }
        ]
        mock_api.get(pairs_url, body=json.dumps(symbols_response))
        mock_api.get(mexc_prices_url, body=json.dumps(mexc_prices_response))

    @aioresponses()
    async def test_get_mexc_prices(self, mock_api):
        expected_rate = Decimal("10")
        self.setup_mexc_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = MexcRateSource()
        prices = await rate_source.get_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])
        self.assertNotIn(self.ignored_trading_pair, prices)

    @aioresponses()
    async def test_get_bid_ask_prices(self, mock_api):
        expected_rate = Decimal("10")
        self.setup_mexc_responses(mock_api=mock_api, expected_rate=expected_rate)

        rate_source = MexcRateSource()
        rate_source._ensure_exchanges()  # ensure exchange is created
        with patch.object(rate_source._mexc_exchange, 'trading_pair_associated_to_exchange_symbol', new_callable=AsyncMock) as mock_method:
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
        mexc_prices_url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        mock_api.get(mexc_prices_url, exception=Exception("API error"))

        rate_source = MexcRateSource()
        prices = await rate_source.get_prices()

        self.assertEqual({}, prices)

    @aioresponses()
    async def test_get_bid_ask_prices_exception(self, mock_api):
        mexc_prices_url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        mock_api.get(mexc_prices_url, exception=Exception("API error"))

        rate_source = MexcRateSource()
        prices = await rate_source.get_bid_ask_prices()

        self.assertEqual({}, prices)
