from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.core.volume_oracle.sources.coinex_volume_source import CoinexVolumeSource
from hummingbot.core.volume_oracle.volume_oracle import VOLUME_ORACLE_SOURCES


class CoinexVolumeSourceTest(IsolatedAsyncioWrapperTestCase):

    def _fake_exchange(self, tickers, resolver=None):
        ex = CoinexVolumeSource()._build_exchange()
        ex.get_all_24h_volume_tickers = AsyncMock(return_value=tickers)

        async def _default(symbol):
            return {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT"}[symbol]

        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=resolver or _default)
        return ex

    def _source_with(self, fake_ex):
        vs = CoinexVolumeSource()
        vs._build_exchange = lambda: fake_ex
        return vs

    async def test_registered(self):
        self.assertIn("coinex", VOLUME_ORACLE_SOURCES)
        self.assertIs(CoinexVolumeSource, VOLUME_ORACLE_SOURCES["coinex"])

    def test_name(self):
        self.assertEqual("coinex", CoinexVolumeSource().name)

    async def test_volumes_normalized(self):
        # volume -> base_volume, value -> quote_volume, last -> last_price.
        vs = self._source_with(self._fake_exchange([
            {"market": "BTCUSDT", "volume": "10", "value": "620000", "last": "62000"}]))
        entry = (await vs.get_all_24h_volumes())["BTC-USDT"]
        self.assertEqual("coinex", entry["exchange"])
        self.assertEqual("BTC-USDT", entry["symbol"])
        self.assertEqual(Decimal("10"), entry["base_volume"])
        self.assertEqual(Decimal("620000"), entry["quote_volume"])
        self.assertEqual(Decimal("62000"), entry["last_price"])

    async def test_resolves_symbol_via_map(self):
        # The exchange symbol (BTCUSDT) must be translated to the HB pair (BTC-USDT).
        vs = self._source_with(self._fake_exchange([
            {"market": "BTCUSDT", "volume": "1", "value": "1", "last": "1"}]))
        vols = await vs.get_all_24h_volumes()
        self.assertIn("BTC-USDT", vols)
        self.assertNotIn("BTCUSDT", vols)

    async def test_skips_unknown_symbol(self):
        async def _resolve(symbol):
            if symbol == "BTCUSDT":
                return "BTC-USDT"
            raise Exception("unknown")
        vs = self._source_with(self._fake_exchange([
            {"market": "BTCUSDT", "volume": "10", "value": "1", "last": "62000"},
            {"market": "FOOBAR", "volume": "5", "value": "1", "last": "5"}], resolver=_resolve))
        vols = await vs.get_all_24h_volumes()
        self.assertIn("BTC-USDT", vols)
        self.assertEqual(1, len(vols))

    async def test_missing_base_volume_skipped(self):
        # A ticker without "volume" raises KeyError in _normalize_ticker → the pair is skipped.
        vs = self._source_with(self._fake_exchange([{"market": "BTCUSDT", "value": "1", "last": "62000"}]))
        self.assertEqual({}, await vs.get_all_24h_volumes())

    async def test_quote_volume_omitted_when_value_missing(self):
        vs = self._source_with(self._fake_exchange([{"market": "BTCUSDT", "volume": "10", "last": "62000"}]))
        entry = (await vs.get_all_24h_volumes())["BTC-USDT"]
        self.assertNotIn("quote_volume", entry)
        self.assertEqual(Decimal("10"), entry["base_volume"])
