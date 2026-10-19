from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.volume_oracle.sources.coinswitch_perpetual_volume_source import CoinswitchPerpetualVolumeSource
from hummingbot.core.volume_oracle.volume_oracle import VOLUME_ORACLE_SOURCES

TICKERS = [
    {"symbol": "BTCUSDT", "last_price": "95136.60", "base_asset_volume_24h": "93707.6800",
     "quote_asset_volume_24h": "8970676685.2608"},
    {"symbol": "DOGEUSDT", "last_price": "0.38", "base_asset_volume_24h": "", "quote_asset_volume_24h": None},
    {"symbol": "BADUSDT", "last_price": "x"},
    {"symbol": "UNLISTEDUSDT", "last_price": "1"},
]
SYMBOL_MAP = {"BTCUSDT": "BTC-USDT", "DOGEUSDT": "DOGE-USDT", "BADUSDT": "BAD-USDT"}


class CoinswitchPerpetualVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    def _source(self):
        exchange = MagicMock()
        exchange.get_all_pairs_prices = AsyncMock(return_value=TICKERS)
        exchange.trading_pair_symbol_map = AsyncMock(return_value=dict(SYMBOL_MAP))
        source = CoinswitchPerpetualVolumeSource()
        source._exchange = exchange
        return source

    def test_registered(self):
        self.assertIs(CoinswitchPerpetualVolumeSource, VOLUME_ORACLE_SOURCES["coinswitch_perpetual"])

    async def test_volumes(self):
        volumes = await self._source().get_all_24h_volumes()
        self.assertEqual({"BTC-USDT", "DOGE-USDT"}, set(volumes))
        btc = volumes["BTC-USDT"]
        self.assertEqual("coinswitch_perpetual", btc["exchange"])
        self.assertEqual(Decimal("93707.6800"), btc["base_volume"])
        self.assertEqual(Decimal("8970676685.2608"), btc["quote_volume"])
        self.assertEqual(Decimal("95136.60"), btc["last_price"])
        self.assertEqual(Decimal("0"), volumes["DOGE-USDT"]["base_volume"])

    async def test_symbol_map_is_read_once(self):
        source = self._source()
        await source.get_all_24h_volumes()
        source._exchange.trading_pair_symbol_map.assert_awaited_once()

    async def test_pair_filter(self):
        self.assertEqual(["BTC-USDT"], list(await self._source().get_all_24h_volumes(["BTC-USDT"])))

    def test_builds_a_keyless_non_trading_connector(self):
        exchange = CoinswitchPerpetualVolumeSource()._build_exchange()
        self.assertFalse(exchange.is_trading_required)
        self.assertEqual("coinswitch_perpetual", exchange.name)
