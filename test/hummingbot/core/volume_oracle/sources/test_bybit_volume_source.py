import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.bybit import bybit_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.bybit_volume_source import BybitVolumeSource


class BybitVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USDT"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.exchange_symbol = f"{cls.base}{cls.quote}"
        cls.other_pair = "ETH-USDT"
        cls.other_symbol = "ETHUSDT"

    def _ticker_url(self) -> str:
        return f"{CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN]}{CONSTANTS.LAST_TRADED_PRICE_PATH}"

    def _bulk_url(self) -> str:
        return f"{self._ticker_url()}?category={CONSTANTS.TRADE_CATEGORY}"

    def _per_pair_url(self, symbol: str) -> str:
        return f"{self._ticker_url()}?category={CONSTANTS.TRADE_CATEGORY}&symbol={symbol}"

    def _ticker_entry(self, symbol: str, vol: str, last: str) -> dict:
        return {
            "symbol": symbol,
            "volume24h": vol,
            "lastPrice": last,
            "turnover24h": str(Decimal(vol) * Decimal(last)),
        }

    def _bulk_response(self, entries: list) -> dict:
        return {"retCode": 0, "result": {"list": entries}}

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        response = self._bulk_response([
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
            self._ticker_entry(self.other_symbol, "500", "3000"),
        ])
        mock_api.get(self._bulk_url(), body=json.dumps(response))

        source = BybitVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.exchange_symbol, result)
        self.assertIn(self.other_symbol, result)
        self.assertEqual(Decimal("100"), result[self.exchange_symbol]["base_volume"])
        self.assertEqual(Decimal("50000"), result[self.exchange_symbol]["last_price"])
        self.assertEqual("bybit", result[self.exchange_symbol]["exchange"])

    @aioresponses()
    async def test_get_all_volumes_with_filter(self, mock_api):
        response = self._bulk_response([
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
        ])
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(response))

        source = BybitVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.exchange_symbol, result)
        self.assertNotIn(self.other_symbol, result)

    @aioresponses()
    async def test_unknown_symbol_skipped_with_warning(self, mock_api):
        mock_api.get(self._per_pair_url("FAKEPAIR"), status=400, body=json.dumps({"retCode": 10001, "retMsg": "invalid symbol"}))

        source = BybitVolumeSource()
        with self.assertLogs(level="WARNING") as cm:
            result = await source.get_all_24h_volumes(trading_pairs=["FAKE-PAIR"])

        self.assertEqual({}, result)
        self.assertTrue(any("FAKE-PAIR" in line for line in cm.output))

    @aioresponses()
    async def test_network_error_propagates(self, mock_api):
        mock_api.get(self._per_pair_url(self.exchange_symbol), status=503, body="Service Unavailable")

        source = BybitVolumeSource()
        with self.assertRaises(IOError):
            await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

    @aioresponses()
    async def test_quote_volume_included(self, mock_api):
        response = self._bulk_response([
            self._ticker_entry(self.exchange_symbol, "100", "50000"),
        ])
        mock_api.get(self._per_pair_url(self.exchange_symbol), body=json.dumps(response))

        source = BybitVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn("quote_volume", result[self.exchange_symbol])
        self.assertEqual(Decimal("5000000"), result[self.exchange_symbol]["quote_volume"])
