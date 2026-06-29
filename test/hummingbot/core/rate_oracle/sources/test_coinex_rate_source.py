from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.coinex_rate_source import CoinexRateSource


class CoinexRateSourceTest(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def _ticker(market, last):
        return {"market": market, "last": last, "volume": "1", "value": "1"}

    def _fake_exchange(self, tickers, resolver=None):
        ex = CoinexRateSource()._build_exchange()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _default(symbol):
            return {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "BTCUSDC": "BTC-USDC"}[symbol]

        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=resolver or _default)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = CoinexRateSource()
        rs._build_exchange = lambda: fake_ex
        return rs

    async def test_registered(self):
        self.assertIn("coinex", RATE_ORACLE_SOURCES)
        self.assertIs(CoinexRateSource, RATE_ORACLE_SOURCES["coinex"])

    async def test_get_prices_uses_last(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSDT", "62000")]))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("62000"), prices["BTC-USDT"])

    async def test_bid_ask_falls_back_to_last(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSDT", "62000")]))
        ba = await rs.get_bid_ask_prices()
        entry = ba["BTC-USDT"]
        self.assertEqual(Decimal("62000"), entry["bid"])
        self.assertEqual(Decimal("62000"), entry["ask"])
        self.assertEqual(Decimal("62000"), entry["mid"])
        self.assertEqual(Decimal("0"), entry["spread"])

    async def test_quote_token_filter(self):
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTCUSDT", "62000"), self._ticker("BTCUSDC", "61990")]))
        prices = await rs.get_prices(quote_token="USDT")
        self.assertIn("BTC-USDT", prices)
        self.assertNotIn("BTC-USDC", prices)

    async def test_skips_invalid_and_unknown(self):
        async def _resolve(symbol):
            if symbol == "BTCUSDT":
                return "BTC-USDT"
            raise Exception("unknown")
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTCUSDT", "0"), self._ticker("FOOBAR", "5")], resolver=_resolve))
        self.assertEqual({}, await rs.get_prices())

    def test_name(self):
        self.assertEqual("coinex", CoinexRateSource().name)
