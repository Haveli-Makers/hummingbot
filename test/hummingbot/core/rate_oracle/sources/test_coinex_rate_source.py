from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from bidict import bidict

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.coinex_rate_source import CoinexRateSource


class CoinexRateSourceTest(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        # async_ttl_cache keys on str(args) which embeds the instance's memory
        # address; addresses are reused across tests, so a stale entry from a
        # previous test's freed instance can collide. Clear for determinism.
        CoinexRateSource.get_prices.cache_clear()
        CoinexRateSource.get_bid_ask_prices.cache_clear()

    @staticmethod
    def _ticker(market, last):
        return {"market": market, "last": last, "volume": "1", "value": "1"}

    def _fake_exchange(self, tickers, resolver=None):
        """Exchange stub for the ticker-based get_prices path."""
        ex = CoinexRateSource()._build_exchange()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _default(symbol):
            return {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "BTCUSDC": "BTC-USDC"}[symbol]

        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=resolver or _default)
        return ex

    def _fake_exchange_for_bid_ask(self, symbol_map, depth_by_pair):
        """Exchange stub for the depth-based get_bid_ask_prices path."""
        ex = CoinexRateSource()._build_exchange()
        ex.trading_pair_symbol_map = AsyncMock(return_value=bidict(symbol_map))

        async def _snapshot(trading_pair):
            if trading_pair not in depth_by_pair:
                raise Exception("no depth for pair")
            return depth_by_pair[trading_pair]

        ex.get_order_book_snapshot = AsyncMock(side_effect=_snapshot)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = CoinexRateSource()
        rs._build_exchange = lambda: fake_ex
        return rs

    async def test_registered(self):
        self.assertIn("coinex", RATE_ORACLE_SOURCES)
        self.assertIs(CoinexRateSource, RATE_ORACLE_SOURCES["coinex"])

    def test_name(self):
        self.assertEqual("coinex", CoinexRateSource().name)

    # ── get_prices: ticker-based (last), covers all pairs from one call ──────────
    async def test_get_prices_uses_last(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSDT", "62000")]))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("62000"), prices["BTC-USDT"])

    async def test_get_prices_quote_token_filter(self):
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTCUSDT", "62000"), self._ticker("BTCUSDC", "61990")]))
        prices = await rs.get_prices(quote_token="USDT")
        self.assertIn("BTC-USDT", prices)
        self.assertNotIn("BTC-USDC", prices)

    async def test_get_prices_skips_invalid_and_unknown(self):
        async def _resolve(symbol):
            if symbol == "BTCUSDT":
                return "BTC-USDT"
            raise Exception("unknown")
        rs = self._rate_source_with(self._fake_exchange(
            [self._ticker("BTCUSDT", "0"), self._ticker("FOOBAR", "5")], resolver=_resolve))
        self.assertEqual({}, await rs.get_prices())

    # ── get_bid_ask_prices: REAL top-of-book from /spot/depth ────────────────────
    async def test_bid_ask_uses_order_book(self):
        # Real best bid 61990 / best ask 62010 from the depth snapshot — NOT bid==ask==last.
        depth = {"BTC-USDT": {"bids": [["61990", "1"], ["61980", "2"]],
                              "asks": [["62010", "1"], ["62020", "3"]]}}
        rs = self._rate_source_with(self._fake_exchange_for_bid_ask({"BTCUSDT": "BTC-USDT"}, depth))
        entry = (await rs.get_bid_ask_prices())["BTC-USDT"]
        self.assertEqual(Decimal("61990"), entry["bid"])
        self.assertEqual(Decimal("62010"), entry["ask"])
        self.assertEqual(Decimal("62000"), entry["mid"])
        self.assertGreater(entry["spread"], Decimal("0"))

    async def test_bid_ask_skips_pair_without_depth(self):
        # A pair whose depth fetch fails/empties is skipped (no fake last-price value).
        depth = {"BTC-USDT": {"bids": [["61990", "1"]], "asks": [["62010", "1"]]}}  # no ETH depth
        rs = self._rate_source_with(self._fake_exchange_for_bid_ask(
            {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT"}, depth))
        ba = await rs.get_bid_ask_prices()
        self.assertIn("BTC-USDT", ba)
        self.assertNotIn("ETH-USDT", ba)

    async def test_bid_ask_concurrent_isolates_per_pair_failure(self):
        # Depth is fetched concurrently (safe_gather); one pair's failing fetch must
        # not drop the others.
        depth = {"BTC-USDT": {"bids": [["61990", "1"]], "asks": [["62010", "1"]]},
                 "SOL-USDT": {"bids": [["150", "1"]], "asks": [["151", "1"]]}}  # no ETH depth -> raises
        rs = self._rate_source_with(self._fake_exchange_for_bid_ask(
            {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT"}, depth))
        ba = await rs.get_bid_ask_prices()
        self.assertIn("BTC-USDT", ba)
        self.assertIn("SOL-USDT", ba)
        self.assertNotIn("ETH-USDT", ba)  # its depth fetch raised -> isolated, not fatal

    async def test_bid_ask_empty_depth_skipped(self):
        depth = {"BTC-USDT": {"bids": [], "asks": []}}
        rs = self._rate_source_with(self._fake_exchange_for_bid_ask({"BTCUSDT": "BTC-USDT"}, depth))
        self.assertEqual({}, await rs.get_bid_ask_prices())

    async def test_bid_ask_quote_token_filter(self):
        depth = {"BTC-USDT": {"bids": [["1", "1"]], "asks": [["2", "1"]]},
                 "BTC-USDC": {"bids": [["1", "1"]], "asks": [["2", "1"]]}}
        rs = self._rate_source_with(self._fake_exchange_for_bid_ask(
            {"BTCUSDT": "BTC-USDT", "BTCUSDC": "BTC-USDC"}, depth))
        ba = await rs.get_bid_ask_prices(quote_token="USDT")
        self.assertIn("BTC-USDT", ba)
        self.assertNotIn("BTC-USDC", ba)
