from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.wazirx_perpetual_rate_source import WazirxPerpetualRateSource

# Shape of the live all-symbols /fapi/v1/ticker/24hr.
TICKERS = [
    {"E": 1, "T": 1, "symbol": "BTCINR", "lastPrice": "8029292", "volume": "1174735761066"},
    {"E": 1, "T": 1, "symbol": "BTCUSDT", "lastPrice": "83952.1", "volume": "12303198936.2"},
    {"E": 1, "T": 1, "symbol": "DEADINR", "lastPrice": "0", "volume": "0"},
    {"E": 1, "T": 1, "symbol": "UNLISTEDINR", "lastPrice": "5", "volume": "1"},
]
SYMBOL_MAP = {"BTCINR": "BTC-INR", "BTCUSDT": "BTC-USDT", "DEADINR": "DEAD-INR"}


class WazirxPerpetualRateSourceTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        # async_ttl_cache keys on str(args), which embeds the instance address;
        # addresses are reused across tests, so clear for determinism.
        WazirxPerpetualRateSource.get_prices.cache_clear()
        WazirxPerpetualRateSource.get_bid_ask_prices.cache_clear()

    def _source(self, tickers=TICKERS):
        exchange = MagicMock()
        exchange.get_all_pairs_prices = AsyncMock(return_value=tickers)

        async def _resolve(symbol):
            return SYMBOL_MAP[symbol]

        exchange.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_resolve)
        source = WazirxPerpetualRateSource()
        source._exchange = exchange
        return source

    def test_registered(self):
        self.assertIs(WazirxPerpetualRateSource, RATE_ORACLE_SOURCES["wazirx_perpetual"])
        self.assertEqual("wazirx_perpetual", WazirxPerpetualRateSource().name)

    async def test_prices_skip_zero_and_unlisted(self):
        prices = await self._source().get_prices()
        self.assertEqual({"BTC-INR": Decimal("8029292"), "BTC-USDT": Decimal("83952.1")}, prices)

    async def test_quote_filter(self):
        prices = await self._source().get_prices(quote_token="USDT")
        self.assertEqual(["BTC-USDT"], list(prices))

    async def test_bid_ask_uses_last_price_with_zero_spread(self):
        entry = (await self._source().get_bid_ask_prices())["BTC-INR"]
        self.assertEqual(Decimal("8029292"), entry["bid"])
        self.assertEqual(Decimal("8029292"), entry["ask"])
        self.assertEqual(Decimal("0"), entry["spread"])

    async def test_fetch_error_returns_empty(self):
        source = self._source()
        source._exchange.get_all_pairs_prices = AsyncMock(side_effect=IOError("boom"))
        self.assertEqual({}, await source.get_prices())

    def test_builds_a_keyless_non_trading_connector(self):
        from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import (
            WazirxPerpetualDerivative,
        )

        exchange = WazirxPerpetualRateSource._build_exchange()
        self.assertIsInstance(exchange, WazirxPerpetualDerivative)
        self.assertFalse(exchange.is_trading_required)
