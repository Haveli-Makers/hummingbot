import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.ascend_ex import ascend_ex_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.ascend_ex_volume_source import AscendExVolumeSource


class AscendExVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.exchange_symbol = f"{cls.base}/{cls.quote}"
        cls.other_pair = "ETH-USDT"
        cls.other_symbol = "ETH/USDT"

    def _ticker_url(self) -> str:
        return f"{CONSTANTS.PUBLIC_REST_URL}{CONSTANTS.TICKER_PATH_URL}"

    def _per_pair_url(self, symbol: str) -> str:
        from urllib.parse import quote
        return f"{self._ticker_url()}?symbol={quote(symbol, safe='')}"

    def _ticker_entry(self, symbol: str, vol: str, last: str) -> dict:
        return {
            "symbol": symbol,
            "volume": vol,
            "close": last,
        }

    def _bulk_response(self, entries: list) -> dict:
        return {"code": 0, "data": entries}

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        response = self._bulk_response([
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
            self._ticker_entry(self.other_symbol, "500", "3000"),
        ])
        mock_api.get(self._ticker_url(), body=json.dumps(response))

        source = AscendExVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.exchange_symbol, result)
        self.assertIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.exchange_symbol]["last_price"])
        self.assertEqual("ascend_ex", result[self.exchange_symbol]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter_calls_per_pair(self, mock_api):
        per_pair_response = self._bulk_response([
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
        ])
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(per_pair_response))

        source = AscendExVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.exchange_symbol, result)
        self.assertNotIn(self.other_symbol, result)

    @aioresponses()
    async def test_unknown_symbol_skipped_with_warning(self, mock_api):
        mock_api.get(self._per_pair_url("BTC%2FFAKE"), status=400, body=json.dumps({"code": 100001, "msg": "invalid symbol"}))

        source = AscendExVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["BTC-FAKE"])

        self.assertEqual({}, result)
        self.assertTrue(any("BTC-FAKE" in line for line in cm.output))

    @aioresponses()
    async def test_network_error_propagates(self, mock_api):
        mock_api.get(self._per_pair_url(self.exchange_symbol), status=503, body="Service Unavailable")

        source = AscendExVolumeSource()
        with self.assertRaises(IOError):
            await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])
