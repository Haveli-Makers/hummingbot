import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.binance import binance_constants as CONSTANTS, binance_web_utils as web_utils
from hummingbot.core.volume_oracle.sources.binance_volume_source import BinanceVolumeSource


class BinanceVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.exchange_symbol = f"{cls.base}{cls.quote}"
        cls.other_pair = "ETH-USDT"
        cls.other_symbol = "ETHUSDT"

    def _ticker(self, symbol: str, vol: str, last: str) -> dict:
        return {
            "symbol": symbol,
            "volume": vol,
            "quoteVolume": str(Decimal(vol) * Decimal(last)),
            "lastPrice": last,
        }

    def _ticker_url(self) -> str:
        return web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL)

    def _per_pair_url(self, symbol: str) -> str:
        return f"{self._ticker_url()}?symbol={symbol}"

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        tickers = [
            self._ticker(self.exchange_symbol, "100", "50000"),
            self._ticker(self.other_symbol, "500", "3000"),
        ]
        mock_api.get(self._ticker_url(), body=json.dumps(tickers))

        source = BinanceVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.exchange_symbol, result)
        self.assertIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.exchange_symbol]["last_price"])
        self.assertEqual("binance", result[self.exchange_symbol]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter_calls_per_pair(self, mock_api):
        ticker = self._ticker(self.exchange_symbol, "100", "50000")
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(ticker))

        source = BinanceVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.exchange_symbol, result)
        self.assertNotIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])

    @aioresponses()
    async def test_unknown_symbol_skipped_with_warning(self, mock_api):
        mock_api.get(self._per_pair_url("FAKEPAIR"), status=400, body=json.dumps({"code": -1121, "msg": "Invalid symbol"}))

        source = BinanceVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["FAKE-PAIR"])

        self.assertEqual({}, result)
        self.assertTrue(any("FAKE-PAIR" in line for line in cm.output))

    @aioresponses()
    async def test_network_error_propagates(self, mock_api):
        mock_api.get(self._per_pair_url(self.exchange_symbol), status=503, body="Service Unavailable")

        source = BinanceVolumeSource()
        with self.assertRaises(IOError):
            await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

    @aioresponses()
    async def test_quote_volume_included_when_present(self, mock_api):
        ticker = self._ticker(self.exchange_symbol, "100", "50000")
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(ticker))

        source = BinanceVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn("quote_volume", result[self.exchange_symbol])
