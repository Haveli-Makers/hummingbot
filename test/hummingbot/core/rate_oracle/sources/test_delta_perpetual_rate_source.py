from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.delta_perpetual_rate_source import DeltaPerpetualRateSource


class DeltaPerpetualRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = combine_to_hb_trading_pair(base="BTC", quote="USD")  # BTC-USD

    @staticmethod
    def _ticker(symbol, bid, ask):
        return {"symbol": symbol, "quotes": {"best_bid": bid, "best_ask": ask}}

    def _fake_exchange(self, tickers, resolver=None):
        ex = DeltaPerpetualRateSource()._build_exchange()
        ex.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _default_resolve(symbol):
            return {"BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "BTCINR": "BTC-INR"}[symbol]

        ex.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=resolver or _default_resolve)
        return ex

    def _rate_source_with(self, fake_ex):
        rs = DeltaPerpetualRateSource()
        # _ensure_exchanges() will call _build_exchange(); return our fake instead.
        rs._build_exchange = lambda: fake_ex
        return rs

    async def test_registered_in_rate_oracle_sources(self):
        self.assertIn("delta_perpetual", RATE_ORACLE_SOURCES)
        self.assertIs(DeltaPerpetualRateSource, RATE_ORACLE_SOURCES["delta_perpetual"])

    async def test_get_bid_ask_prices(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSD", "99", "101")]))
        ba = await rs.get_bid_ask_prices()
        self.assertIn("BTC-USD", ba)
        d = ba["BTC-USD"]
        self.assertEqual(Decimal("99"), d["bid"])
        self.assertEqual(Decimal("101"), d["ask"])
        self.assertEqual(Decimal("100"), d["mid"])
        self.assertEqual(Decimal("2"), d["spread"])

    async def test_get_prices_returns_mid(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSD", "99", "101")]))
        prices = await rs.get_prices()
        self.assertEqual(Decimal("100"), prices["BTC-USD"])

    async def test_quote_token_filter(self):
        tickers = [self._ticker("BTCUSD", "100", "102"), self._ticker("BTCINR", "200", "202")]
        rs = self._rate_source_with(self._fake_exchange(tickers))
        prices = await rs.get_prices(quote_token="USD")
        self.assertIn("BTC-USD", prices)
        self.assertNotIn("BTC-INR", prices)

    async def test_missing_quotes_skipped(self):
        rs = self._rate_source_with(self._fake_exchange([{"symbol": "BTCUSD"}]))
        self.assertEqual({}, await rs.get_bid_ask_prices())

    async def test_bid_greater_than_ask_skipped(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSD", "102", "100")]))
        self.assertEqual({}, await rs.get_bid_ask_prices())

    async def test_invalid_decimal_skipped(self):
        rs = self._rate_source_with(self._fake_exchange([self._ticker("BTCUSD", "n/a", "100")]))
        self.assertEqual({}, await rs.get_bid_ask_prices())

    async def test_unknown_symbol_skipped(self):
        async def _resolve(symbol):
            raise Exception("not in symbol map")

        rs = self._rate_source_with(self._fake_exchange([self._ticker("OPTUSD", "1", "2")], resolver=_resolve))
        self.assertEqual({}, await rs.get_bid_ask_prices())

    async def test_static_helper_direct_call(self):
        class _StubExchange:
            async def get_all_pairs_prices(self):
                return [{"symbol": "BTCUSD", "quotes": {"best_bid": "100", "best_ask": "102"}}]

            async def trading_pair_associated_to_exchange_symbol(self, symbol):
                if symbol == "BTCUSD":
                    return "BTC-USD"
                raise Exception("Unknown symbol")

        result = await DeltaPerpetualRateSource._get_delta_bid_ask_prices(exchange=_StubExchange())
        entry = result["BTC-USD"]
        self.assertEqual(Decimal("100"), entry["bid"])
        self.assertEqual(Decimal("102"), entry["ask"])
        self.assertEqual(Decimal("101"), entry["mid"])

    async def test_name(self):
        self.assertEqual("delta_perpetual", DeltaPerpetualRateSource().name)

    def test_build_exchange_returns_connector(self):
        from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative
        self.assertIsInstance(DeltaPerpetualRateSource()._build_exchange(), DeltaPerpetualDerivative)
