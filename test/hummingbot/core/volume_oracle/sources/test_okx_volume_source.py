import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS, okx_web_utils as web_utils
from hummingbot.core.volume_oracle.sources.okx_volume_source import OkxVolumeSource


class OkxVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.exchange_symbol = f"{cls.base}-{cls.quote}"   # OKX uses same BASE-QUOTE format
        cls.other_pair = "ETH-USDT"
        cls.other_symbol = "ETH-USDT"

    def _tickers_url(self) -> str:
        return web_utils.public_rest_url(CONSTANTS.OKX_TICKERS_PATH)

    def _ticker_url(self) -> str:
        return web_utils.public_rest_url(CONSTANTS.OKX_TICKER_PATH)

    def _ticker_entry(self, inst_id: str, vol: str, last: str) -> dict:
        return {
            "instId": inst_id,
            "instType": "SPOT",
            "vol24h": vol,
            "volCcy24h": str(Decimal(vol) * Decimal(last)),
            "last": last,
        }

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        response = {
            "code": "0",
            "data": [
                self._ticker_entry(self.exchange_symbol, "100", "50000"),
                self._ticker_entry(self.other_symbol, "500", "3000"),
            ],
        }
        mock_api.get(f"{self._tickers_url()}?instType=SPOT", body=json.dumps(response))

        source = OkxVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.exchange_symbol, result)
        self.assertIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.exchange_symbol]["last_price"])
        self.assertEqual("okx", result[self.exchange_symbol]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter_calls_per_pair(self, mock_api):
        response = {
            "code": "0",
            "data": [self._ticker_entry(self.exchange_symbol, "100", "50000")],
        }
        mock_api.get(f"{self._ticker_url()}?instId={self.exchange_symbol}", body=json.dumps(response))

        source = OkxVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.exchange_symbol, result)
        self.assertNotIn(self.other_symbol, result)

    @aioresponses()
    async def test_unknown_symbol_skipped_with_warning(self, mock_api):
        mock_api.get(f"{self._ticker_url()}?instId=FAKE-PAIR", status=400, body=json.dumps({"code": "51001", "msg": "Instrument ID does not exist"}))

        source = OkxVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["FAKE-PAIR"])

        self.assertEqual({}, result)
        self.assertTrue(any("FAKE-PAIR" in line for line in cm.output))

    @aioresponses()
    async def test_network_error_propagates(self, mock_api):
        mock_api.get(f"{self._ticker_url()}?instId={self.exchange_symbol}", status=503, body="Service Unavailable")

        source = OkxVolumeSource()
        with self.assertRaises(IOError):
            await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

    @aioresponses()
    async def test_quote_volume_included(self, mock_api):
        response = {
            "code": "0",
            "data": [self._ticker_entry(self.exchange_symbol, "100", "50000")],
        }
        mock_api.get(f"{self._ticker_url()}?instId={self.exchange_symbol}", body=json.dumps(response))

        source = OkxVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn("quote_volume", result[self.exchange_symbol])
