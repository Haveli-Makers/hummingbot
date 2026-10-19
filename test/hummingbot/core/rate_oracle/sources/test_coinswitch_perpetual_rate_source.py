from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.coinswitch_perpetual_rate_source import CoinswitchPerpetualRateSource

# All-pairs ticker rows as the connector returns them (symbol added from the key).
TICKERS = [
    {"symbol": "BTCUSDT", "last_price": "95136.60", "best_bid_price": "95136.60", "best_ask_price": "95136.70"},
    {"symbol": "DOGEUSDT", "last_price": "0.38135", "best_bid_price": "", "best_ask_price": ""},
    {"symbol": "DEADUSDT", "last_price": "0", "best_bid_price": "0", "best_ask_price": "0"},
    {"symbol": "UNLISTEDUSDT", "last_price": "5"},
]
SYMBOL_MAP = {"BTCUSDT": "BTC-USDT", "DOGEUSDT": "DOGE-USDT", "DEADUSDT": "DEAD-USDT"}


class CoinswitchPerpetualRateSourceTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        # async_ttl_cache keys on str(args), which embeds the instance address;
        # addresses are reused across tests, so clear for determinism.
        CoinswitchPerpetualRateSource.get_prices.cache_clear()
        CoinswitchPerpetualRateSource.get_bid_ask_prices.cache_clear()

    def _source(self, tickers=TICKERS):
        exchange = MagicMock()
        exchange.get_all_pairs_prices = AsyncMock(return_value=tickers)
        exchange.trading_pair_symbol_map = AsyncMock(return_value=dict(SYMBOL_MAP))
        source = CoinswitchPerpetualRateSource()
        source._exchange = exchange
        return source

    def test_registered(self):
        self.assertIs(CoinswitchPerpetualRateSource, RATE_ORACLE_SOURCES["coinswitch_perpetual"])
        self.assertEqual("coinswitch_perpetual", CoinswitchPerpetualRateSource().name)

    async def test_prices_use_the_mid_and_fall_back_to_last(self):
        prices = await self._source().get_prices()
        self.assertEqual({"BTC-USDT": Decimal("95136.65"), "DOGE-USDT": Decimal("0.38135")}, prices)

    async def test_quote_filter(self):
        self.assertEqual({}, await self._source().get_prices(quote_token="INR"))

    async def test_bid_ask_has_a_real_spread(self):
        entry = (await self._source().get_bid_ask_prices())["BTC-USDT"]
        self.assertEqual(Decimal("95136.60"), entry["bid"])
        self.assertEqual(Decimal("95136.70"), entry["ask"])
        self.assertEqual((Decimal("0.10") / Decimal("95136.65")) * 100, entry["spread"])

    async def test_symbol_map_is_read_once(self):
        source = self._source()
        await source.get_prices()
        source._exchange.trading_pair_symbol_map.assert_awaited_once()

    async def test_empty_symbol_map_gives_nothing(self):
        source = self._source()
        source._exchange.trading_pair_symbol_map = AsyncMock(return_value={})
        self.assertEqual({}, await source.get_prices())

    async def test_fetch_error_returns_empty(self):
        source = self._source()
        source._exchange.get_all_pairs_prices = AsyncMock(side_effect=IOError("boom"))
        self.assertEqual({}, await source.get_prices())

    def test_decimal_parsing(self):
        self.assertEqual(Decimal("0"), CoinswitchPerpetualRateSource._to_decimal("abc"))
        self.assertEqual(Decimal("0"), CoinswitchPerpetualRateSource._to_decimal(None))

    def test_builds_a_keyless_non_trading_connector(self):
        from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
            CoinswitchPerpetualDerivative,
        )

        exchange = CoinswitchPerpetualRateSource._build_exchange()
        self.assertIsInstance(exchange, CoinswitchPerpetualDerivative)
        self.assertFalse(exchange.is_trading_required)
