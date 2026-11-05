from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.volume_oracle.sources.wazirx_perpetual_volume_source import WazirxPerpetualVolumeSource
from hummingbot.core.volume_oracle.volume_oracle import VOLUME_ORACLE_SOURCES

TICKERS = [
    {"symbol": "DOGEINR", "lastPrice": "8.605", "volume": "51244703646.894"},
    {"symbol": "BTCUSDT", "lastPrice": "0", "volume": "0"},
    {"symbol": "UNLISTEDINR", "lastPrice": "1", "volume": "1"},
]
SYMBOL_MAP = {"DOGEINR": "DOGE-INR", "BTCUSDT": "BTC-USDT"}


class WazirxPerpetualVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    def _source(self):
        exchange = MagicMock()
        exchange.get_all_pairs_prices = AsyncMock(return_value=TICKERS)

        async def _resolve(symbol):
            return SYMBOL_MAP[symbol]

        exchange.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_resolve)
        source = WazirxPerpetualVolumeSource()
        source._exchange = exchange
        return source

    def test_registered(self):
        self.assertIs(WazirxPerpetualVolumeSource, VOLUME_ORACLE_SOURCES["wazirx_perpetual"])

    async def test_ticker_volume_is_quote_volume(self):
        volumes = await self._source().get_all_24h_volumes()
        doge = volumes["DOGE-INR"]
        self.assertEqual(Decimal("51244703646.894"), doge["quote_volume"])
        self.assertEqual(Decimal("51244703646.894") / Decimal("8.605"), doge["base_volume"])
        self.assertEqual(Decimal("8.605"), doge["last_price"])
        self.assertEqual("wazirx_perpetual", doge["exchange"])
        self.assertEqual(Decimal("0"), volumes["BTC-USDT"]["base_volume"])
        self.assertNotIn("UNLISTED-INR", volumes)

    async def test_pair_filter(self):
        volumes = await self._source().get_all_24h_volumes(trading_pairs=["DOGE-INR"])
        self.assertEqual(["DOGE-INR"], list(volumes))
