import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.gate_io import gate_io_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.gate_io_volume_source import GateIoVolumeSource


class GateIoVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.exchange_symbol = f"{cls.base}_{cls.quote}"
        cls.other_pair = "ETH-USDT"
        cls.other_symbol = "ETH_USDT"

    def _ticker_url(self) -> str:
        return f"{CONSTANTS.REST_URL}/{CONSTANTS.TICKER_PATH_URL}"

    def _per_pair_url(self, currency_pair: str) -> str:
        return f"{self._ticker_url()}?currency_pair={currency_pair}"

    def _ticker_entry(self, currency_pair: str, vol: str, last: str) -> dict:
        return {
            "currency_pair": currency_pair,
            "base_volume": vol,
            "quote_volume": str(Decimal(vol) * Decimal(last)),
            "last": last,
        }

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        tickers = [
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
            self._ticker_entry(self.other_symbol, "500", "3000"),
        ]
        mock_api.get(self._ticker_url(), body=json.dumps(tickers))

        source = GateIoVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.exchange_symbol, result)
        self.assertIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.exchange_symbol]["last_price"])
        self.assertEqual("gate_io", result[self.exchange_symbol]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter(self, mock_api):
        tickers = [self._ticker_entry(self.exchange_symbol, "100", "50000")]
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(tickers))

        source = GateIoVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.exchange_symbol, result)
        self.assertNotIn(self.other_symbol, result)

    @aioresponses()
    async def test_unknown_symbol_skipped_with_warning(self, mock_api):
        mock_api.get(self._per_pair_url("BTC_FAKE"), status=400, body=json.dumps({"label": "INVALID_CURRENCY", "message": "invalid"}))

        source = GateIoVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["BTC-FAKE"])

        self.assertEqual({}, result)
        self.assertTrue(any("BTC-FAKE" in line for line in cm.output))

    @aioresponses()
    async def test_network_error_propagates(self, mock_api):
        mock_api.get(self._per_pair_url(self.exchange_symbol), status=503, body="Service Unavailable")

        source = GateIoVolumeSource()
        with self.assertRaises(IOError):
            await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

    @aioresponses()
    async def test_quote_volume_included(self, mock_api):
        tickers = [self._ticker_entry(self.exchange_symbol, "100", "50000")]
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(tickers))

        source = GateIoVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn("quote_volume", result[self.exchange_symbol])
