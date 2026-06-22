from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.valr_rate_source import ValrRateSource


class ValrRateSourceTest(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def _summary(pair, bid, ask, last="0"):
        return {"currencyPair": pair, "bidPrice": bid, "askPrice": ask, "lastTradedPrice": last,
                "baseVolume": "1", "quoteVolume": "1"}

    def _fake_exchange(self, tickers, resolver=None):
        ex = ValrRateSource()._build_exchange()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _default(symbol):
            return {"BTCZAR": "BTC-ZAR", "ETHZAR": "ETH-ZAR", "BTCUSDC": "BTC-USDC"}[symbol]

        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=resolver or _default)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = ValrRateSource()
        rs._build_exchange = lambda: fake_ex
        return rs

    async def test_registered(self):
        self.assertIn("valr", RATE_ORACLE_SOURCES)
        self.assertIs(ValrRateSource, RATE_ORACLE_SOURCES["valr"])

    async def test_get_bid_ask_prices(self):
        rs = self._rate_source_with(self._fake_exchange([self._summary("BTCZAR", "990", "1010")]))
        ba = await rs.get_bid_ask_prices()
        entry = ba["BTC-ZAR"]
        self.assertEqual(Decimal("990"), entry["bid"])
        self.assertEqual(Decimal("1010"), entry["ask"])
        self.assertEqual(Decimal("1000"), entry["mid"])

    async def test_get_prices_uses_mid(self):
        rs = self._rate_source_with(self._fake_exchange([self._summary("BTCZAR", "990", "1010")]))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("1000"), prices["BTC-ZAR"])

    async def test_falls_back_to_last_when_no_bid_ask(self):
        rs = self._rate_source_with(self._fake_exchange([self._summary("BTCZAR", "0", "0", last="1005")]))
        entry = (await rs.get_bid_ask_prices())["BTC-ZAR"]
        self.assertEqual(Decimal("1005"), entry["mid"])
        self.assertEqual(Decimal("0"), entry["spread"])

    async def test_quote_token_filter(self):
        rs = self._rate_source_with(self._fake_exchange(
            [self._summary("BTCZAR", "990", "1010"), self._summary("BTCUSDC", "60000", "60010")]))
        prices = await rs.get_prices(quote_token="ZAR")
        self.assertIn("BTC-ZAR", prices)
        self.assertNotIn("BTC-USDC", prices)

    def test_name(self):
        self.assertEqual("valr", ValrRateSource().name)
