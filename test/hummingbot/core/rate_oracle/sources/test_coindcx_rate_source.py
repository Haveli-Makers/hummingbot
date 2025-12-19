from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource


class CoindcxRateSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.target_token = "BTC"
        cls.global_token = "USDT"
        cls.trading_pair = combine_to_hb_trading_pair(base=cls.target_token, quote=cls.global_token)
        cls.ignored_trading_pair = combine_to_hb_trading_pair(base="SOME", quote="PAIR")

    def setup_coindcx_responses(self, mock_tickers):
        exchange = CoindcxRateSource._build_coindcx_connector_without_private_keys()
        mapping = bidict({mock_tickers[0].get("market") if mock_tickers and mock_tickers[0].get("market") else mock_tickers[0].get("symbol"): self.trading_pair})
        exchange._set_trading_pair_symbol_map(mapping)

        exchange.get_all_pairs_prices = AsyncMock(return_value=mock_tickers)

        async def _resolve_symbol(symbol: str):
            if symbol in mapping:
                return mapping[symbol]
            if symbol.endswith("USDT"):
                return f"{symbol[:-4]}-USDT"
            if symbol.endswith("INR"):
                return f"{symbol[:-3]}-INR"
            raise Exception("Unknown symbol")

        exchange.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_resolve_symbol)
        return exchange

    async def test_get_coindcx_prices(self):
        expected_rate = Decimal("10")
        symbol = f"{self.target_token}{self.global_token}"
        tickers = [{"market": symbol, "bid": str(expected_rate - Decimal("0.1")), "ask": str(expected_rate + Decimal("0.1"))}]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            prices = await rate_source.get_prices()

        self.assertIn(self.trading_pair, prices)
        self.assertEqual(expected_rate, prices[self.trading_pair])

    async def test_get_coindcx_bid_ask_prices(self):
        expected_rate = Decimal("10")
        symbol = f"{self.target_token}{self.global_token}"
        tickers = [{"market": symbol, "bid": str(expected_rate - Decimal("0.1")), "ask": str(expected_rate + Decimal("0.1"))}]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            bid_ask_prices = await rate_source.get_bid_ask_prices()

        self.assertIn(self.trading_pair, bid_ask_prices)
        price_data = bid_ask_prices[self.trading_pair]
        self.assertEqual(expected_rate - Decimal("0.1"), price_data["bid"])
        self.assertEqual(expected_rate + Decimal("0.1"), price_data["ask"])
        self.assertEqual(expected_rate, price_data["mid"])
        self.assertEqual(Decimal("2.0"), price_data["spread"])
        self.assertNotIn(self.ignored_trading_pair, bid_ask_prices)

    async def test_get_prices_filters_by_quote_token(self):
        tickers = [
            {"market": "BTCUSDT", "bid": "100", "ask": "102"},
            {"market": "BTCINR", "bid": "200", "ask": "202"},
        ]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            prices = await rate_source.get_prices(quote_token="USDT")
            self.assertIn("BTC-USDT", prices)
            self.assertNotIn("BTC-INR", prices)

    async def test_get_prices_with_none_bid_ask(self):
        tickers = [{"market": "BTCUSDT", "bid": None, "ask": "102"}]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            prices = await rate_source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_prices_with_invalid_decimal(self):
        tickers = [{"market": "BTCUSDT", "bid": "not-a-number", "ask": "102"}]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            prices = await rate_source.get_prices()
            self.assertEqual(prices, {})

    async def test_get_bid_ask_prices_with_bid_greater_than_ask(self):
        tickers = [{"market": "BTCUSDT", "bid": "102", "ask": "100"}]
        fake_ex = self.setup_coindcx_responses(tickers)

        rate_source = CoindcxRateSource()
        with patch.object(rate_source, "_build_coindcx_connector_without_private_keys", return_value=fake_ex):
            bid_asks = await rate_source.get_bid_ask_prices()
            self.assertEqual(bid_asks, {})

    async def test_static_helpers_direct_call(self):
        tickers = [{"market": "BTCUSDT", "bid": "100", "ask": "102"}]

        class CoinDCXExchange:
            def __init__(self, tickers):
                self._tickers = tickers

            async def get_all_pairs_prices(self):
                return self._tickers

            async def trading_pair_associated_to_exchange_symbol(self, symbol: str):
                if symbol == "BTCUSDT":
                    return "BTC-USDT"
                raise Exception("Unknown symbol")

        stub_ex = CoinDCXExchange(tickers)

        prices = await CoindcxRateSource._get_coindcx_prices(exchange=stub_ex)
        assert prices.get("BTC-USDT") == Decimal("101")

        bid_asks = await CoindcxRateSource._get_coindcx_bid_ask_prices(exchange=stub_ex)
        entry = bid_asks.get("BTC-USDT")
        assert entry["bid"] == Decimal("100")
        assert entry["ask"] == Decimal("102")
        assert entry["mid"] == Decimal("101")
