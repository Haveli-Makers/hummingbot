from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from bidict import bidict

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.core.rate_oracle.sources.coindcx_perpetual_rate_source import CoinDCXPerpetualRateSource

# GET /fapi/v1/ticker/bookTicker for every symbol (FAPI guide shape).
BOOK_TICKERS = [
    {"symbol": "BTCUSDT", "bidPrice": "67070.0", "bidQty": "9.866", "askPrice": "67070.2", "askQty": "9.866"},
    {"symbol": "ETHINR", "bidPrice": "300000", "bidQty": "1", "askPrice": "300300", "askQty": "1"},
    {"symbol": "XRPUSDT", "bidPrice": "0", "bidQty": "0", "askPrice": "0.5", "askQty": "10"},
    {"symbol": "SOLUSDT", "bidPrice": "151", "bidQty": "1", "askPrice": "150", "askQty": "1"},
    {"symbol": "DELISTEDUSDT", "bidPrice": "1", "bidQty": "1", "askPrice": "2", "askQty": "1"},
]


class CoinDCXPerpetualRateSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        CoinDCXPerpetualRateSource.get_prices.cache_clear()
        CoinDCXPerpetualRateSource.get_bid_ask_prices.cache_clear()
        self.source = CoinDCXPerpetualRateSource()
        self.exchange = self.source._build_exchange()
        self.exchange._set_trading_pair_symbol_map(bidict({
            "BTCUSDT": "BTC-USDT", "ETHINR": "ETH-INR", "XRPUSDT": "XRP-USDT", "SOLUSDT": "SOL-USDT"}))
        self.exchange._api_get = AsyncMock(return_value=BOOK_TICKERS)
        self.source._exchange = self.exchange

    async def test_bid_ask_comes_from_the_book_ticker(self):
        prices = await self.source.get_bid_ask_prices()

        self.exchange._api_get.assert_awaited_once_with(path_url=CONSTANTS.BOOK_TICKER_PATH_URL)
        btc = prices["BTC-USDT"]
        self.assertEqual(Decimal("67070.0"), btc["bid"])
        self.assertEqual(Decimal("67070.2"), btc["ask"])
        self.assertEqual(Decimal("67070.1"), btc["mid"])
        # Spread in percent of mid, so spread_capture no longer reports zero.
        self.assertEqual(Decimal("0.2") / Decimal("67070.1") * 100, btc["spread"])

    async def test_empty_or_crossed_books_and_unknown_symbols_are_skipped(self):
        prices = await self.source.get_bid_ask_prices()
        self.assertNotIn("XRP-USDT", prices)  # no bid
        self.assertNotIn("SOL-USDT", prices)  # bid above ask
        self.assertEqual({"BTC-USDT", "ETH-INR"}, set(prices))

    async def test_quote_token_filter(self):
        self.assertEqual({"ETH-INR"}, set(await self.source.get_bid_ask_prices(quote_token="INR")))

    async def test_prices_are_mids(self):
        self.assertEqual({"BTC-USDT": Decimal("67070.1"), "ETH-INR": Decimal("300150")},
                         await self.source.get_prices())

    async def test_a_failed_request_returns_no_prices(self):
        self.exchange._api_get = AsyncMock(side_effect=IOError("down"))
        self.assertEqual({}, await self.source.get_bid_ask_prices())

    def test_name(self):
        self.assertEqual("coindcx_perpetual", self.source.name)
