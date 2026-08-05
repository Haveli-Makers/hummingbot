from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.zebpay_rate_source import ZebpayRateSource


class ZebpayRateSourceTest(IsolatedAsyncioWrapperTestCase):

    def _fake_exchange(self, tickers, symbol_map):
        ex = MagicMock()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _resolve(symbol):
            return symbol_map[symbol]  # raises KeyError for unmapped symbols
        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_resolve)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = ZebpayRateSource()
        rs._build_zebpay_connector = lambda: fake_ex
        return rs

    async def test_registered(self):
        self.assertIn("zebpay", RATE_ORACLE_SOURCES)
        self.assertIs(ZebpayRateSource, RATE_ORACLE_SOURCES["zebpay"])

    def test_name(self):
        self.assertEqual("zebpay", ZebpayRateSource().name)

    async def test_non_dashed_symbol_resolved_via_map(self):
        # The latent bug: a non-dashed exchange symbol used to be dropped. It must
        # now be translated through the connector symbol map.
        rs = self._rate_source_with(self._fake_exchange(
            [{"symbol": "BTCINR", "last": "100"}], {"BTCINR": "BTC-INR"}))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("100"), prices["BTC-INR"])

    async def test_unmapped_symbol_falls_back_to_dashed(self):
        # Symbol not in the map → KeyError → dashed-symbol fallback keeps it working.
        rs = self._rate_source_with(self._fake_exchange(
            [{"symbol": "ETH-INR", "last": "50"}], {}))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("50"), prices["ETH-INR"])

    async def test_bid_ask_real_spread(self):
        rs = self._rate_source_with(self._fake_exchange(
            [{"symbol": "BTCINR", "bid": "99", "ask": "101"}], {"BTCINR": "BTC-INR"}))
        ba = await rs.get_bid_ask_prices()
        entry = ba["BTC-INR"]
        self.assertEqual(Decimal("99"), entry["bid"])
        self.assertEqual(Decimal("101"), entry["ask"])
        self.assertEqual(Decimal("100"), entry["mid"])
