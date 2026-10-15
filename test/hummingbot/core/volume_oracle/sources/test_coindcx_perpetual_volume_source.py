from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from bidict import bidict

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.coindcx_perpetual_volume_source import CoinDCXPerpetualVolumeSource

# GET /fapi/v1/ticker/24hr for every symbol (FAPI guide shape).
TICKERS = [
    {"symbol": "BTCUSDT", "lastPrice": "68450.23", "volume": "45230.500", "quoteVolume": "3068420750.115"},
    {"symbol": "ETHUSDT", "lastPrice": "3000", "volume": "10", "quoteVolume": "30000"},
    {"symbol": "DELISTEDUSDT", "lastPrice": "1", "volume": "1", "quoteVolume": "1"},
    {"symbol": "XRPUSDT", "lastPrice": "0.5"},
]


class CoinDCXPerpetualVolumeSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.source = CoinDCXPerpetualVolumeSource()
        self.source._ensure_exchange()
        self.exchange = self.source._exchange
        self.exchange._set_trading_pair_symbol_map(bidict({
            "BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "XRPUSDT": "XRP-USDT"}))
        self.exchange._api_get = AsyncMock(return_value=TICKERS)

    async def test_volumes_come_from_the_24h_ticker(self):
        volumes = await self.source.get_all_24h_volumes()

        self.exchange._api_get.assert_awaited_once_with(path_url=CONSTANTS.TICKER_24HR_PATH_URL)
        self.assertEqual({
            "exchange": "coindcx_perpetual",
            "symbol": "BTC-USDT",
            "base_volume": Decimal("45230.500"),
            "last_price": Decimal("68450.23"),
            "quote_volume": Decimal("3068420750.115"),
        }, volumes["BTC-USDT"])

    async def test_unknown_symbols_and_incomplete_tickers_are_skipped(self):
        self.assertEqual({"BTC-USDT", "ETH-USDT"}, set(await self.source.get_all_24h_volumes()))

    async def test_requested_pairs_filter_the_result(self):
        self.assertEqual({"ETH-USDT"}, set(await self.source.get_all_24h_volumes(["ETH-USDT"])))

    def test_name(self):
        self.assertEqual("coindcx_perpetual", self.source.name)
