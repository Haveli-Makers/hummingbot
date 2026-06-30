from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.csx_rate_source import CsxRateSource


class CsxRateSourceTest(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def _ticker(instrument, last):
        return {"Instrument": instrument, "LastTradedPrice": last}

    def _fake_exchange(self, tickers, depth=None):
        # Stub the connector: the rate source only calls these two coroutines.
        ex = MagicMock()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)
        depth = depth or {}

        async def _snapshot(trading_pair):
            if trading_pair not in depth:
                raise Exception("no depth for pair")
            return depth[trading_pair]

        ex.get_order_book_snapshot = AsyncMock(side_effect=_snapshot)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = CsxRateSource()
        rs._build_csx_connector = lambda: fake_ex
        return rs

    async def test_registered(self):
        self.assertIn("csx", RATE_ORACLE_SOURCES)
        self.assertIs(CsxRateSource, RATE_ORACLE_SOURCES["csx"])

    def test_name(self):
        self.assertEqual("csx", CsxRateSource().name)

    async def test_get_prices_uses_last(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTC/INR", "100")]))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("100"), prices["BTC-INR"])

    async def test_bid_ask_uses_order_book(self):
        # Real top-of-book from the depth endpoint (best bid 99 / best ask 101),
        # NOT bid == ask == last with zero spread.
        depth = {"BTC-INR": {"bids": [["98", "2"], ["99", "1"]], "asks": [["102", "3"], ["101", "1"]]}}
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTC/INR", "100")], depth))
        ba = await rs.get_bid_ask_prices()
        entry = ba["BTC-INR"]
        self.assertEqual(Decimal("99"), entry["bid"])
        self.assertEqual(Decimal("101"), entry["ask"])
        self.assertEqual(Decimal("100"), entry["mid"])
        self.assertEqual(Decimal("2"), entry["spread"])

    async def test_bid_ask_concurrent_isolates_per_pair_failure(self):
        # bid/ask are fetched concurrently (gather); one pair's depth fetch failing
        # must not drop the others — the failing pair falls back to its last price.
        depth = {"BTC-INR": {"bids": [["99", "1"]], "asks": [["101", "1"]]}}  # no ETH depth
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTC/INR", "100"), self._ticker("ETH/INR", "50")], depth))
        ba = await rs.get_bid_ask_prices()
        self.assertEqual(Decimal("99"), ba["BTC-INR"]["bid"])
        self.assertEqual(Decimal("101"), ba["BTC-INR"]["ask"])
        # ETH depth raised → falls back to last traded price (zero spread).
        self.assertEqual(Decimal("50"), ba["ETH-INR"]["mid"])
        self.assertEqual(Decimal("0"), ba["ETH-INR"]["spread"])

    async def test_bid_ask_falls_back_to_last_when_no_depth(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTC/INR", "100")], depth={}))
        ba = await rs.get_bid_ask_prices()
        entry = ba["BTC-INR"]
        self.assertEqual(Decimal("100"), entry["bid"])
        self.assertEqual(Decimal("100"), entry["ask"])
        self.assertEqual(Decimal("100"), entry["mid"])
        self.assertEqual(Decimal("0"), entry["spread"])

    async def test_quote_token_filter(self):
        depth = {
            "BTC-INR": {"bids": [["99", "1"]], "asks": [["101", "1"]]},
            "BTC-USDT": {"bids": [["100", "1"]], "asks": [["102", "1"]]},
        }
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTC/INR", "100"), self._ticker("BTC/USDT", "101")], depth))
        ba = await rs.get_bid_ask_prices(quote_token="INR")
        self.assertIn("BTC-INR", ba)
        self.assertNotIn("BTC-USDT", ba)
