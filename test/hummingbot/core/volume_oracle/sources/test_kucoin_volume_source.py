import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.kucoin import kucoin_constants as CONSTANTS, kucoin_web_utils as web_utils
from hummingbot.core.volume_oracle.sources.kucoin_volume_source import KucoinVolumeSource


class KucoinVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.other_pair = "ETH-USDT"

    def _tickers_url(self) -> str:
        return web_utils.public_rest_url(path_url=CONSTANTS.ALL_TICKERS_PATH_URL)

    def _tickers_response(self, pairs: list) -> dict:
        return {
            "code": "200000",
            "data": {
                "time": 1700000000000,
                "ticker": pairs,
            },
        }

    def _ticker_entry(self, symbol: str, vol: str, last: str, vol_value: str = None) -> dict:
        entry = {"symbol": symbol, "vol": vol, "last": last}
        if vol_value is not None:
            entry["volValue"] = vol_value
        return entry

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        response = self._tickers_response([
            self._ticker_entry(self.trading_pair, "100", "50000", "5000000"),
            self._ticker_entry(self.other_pair, "500", "3000"),
        ])
        mock_api.get(self._tickers_url(), body=json.dumps(response))

        source = KucoinVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.trading_pair, result)
        self.assertIn(self.other_pair, result)
        self.assertEqual(Decimal("100"), result[self.trading_pair]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.trading_pair]["last_price"])
        self.assertEqual(Decimal("5000000"), result[self.trading_pair]["quote_volume"])
        self.assertEqual("kucoin", result[self.trading_pair]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter(self, mock_api):
        response = self._tickers_response([
            self._ticker_entry(self.trading_pair, "100", "50000"),
            self._ticker_entry(self.other_pair, "500", "3000"),
        ])
        mock_api.get(self._tickers_url(), body=json.dumps(response))

        source = KucoinVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.trading_pair, result)
        self.assertNotIn(self.other_pair, result)

    @aioresponses()
    async def test_unknown_pair_produces_warning(self, mock_api):
        response = self._tickers_response([
            self._ticker_entry(self.trading_pair, "100", "50000"),
        ])
        mock_api.get(self._tickers_url(), body=json.dumps(response))

        source = KucoinVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["FAKE-PAIR"])

        self.assertEqual({}, result)
        self.assertTrue(any("FAKE-PAIR" in line for line in cm.output))

    @aioresponses()
    async def test_null_vol_or_last_skipped(self, mock_api):
        response = self._tickers_response([
            {"symbol": self.trading_pair, "vol": None, "last": "50000"},
            self._ticker_entry(self.other_pair, "500", "3000"),
        ])
        mock_api.get(self._tickers_url(), body=json.dumps(response))

        source = KucoinVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertNotIn(self.trading_pair, result)
        self.assertIn(self.other_pair, result)
