import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.connector.exchange.coinswitch import coinswitch_web_utils as web_utils
from hummingbot.core.volume_oracle.sources.coinswitch_volume_source import CoinswitchVolumeSource


class CoinswitchVolumeSourceTest(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "INR"
        cls.trading_pair = f"{cls.base}-{cls.quote}"
        cls.other_pair = "ETH-INR"

    def _all_tickers_url(self) -> str:
        base = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_ALL_PATH_URL)
        return f"{base}?exchange={CONSTANTS.DEFAULT_EXCHANGE}"

    def _single_ticker_url(self, symbol: str = None) -> str:
        base = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_PATH_URL)
        url = f"{base}?exchange={CONSTANTS.DEFAULT_EXCHANGE}"
        if symbol:
            url += f"&symbol={symbol}"
        return url

    def _all_tickers_response(self, entries: dict) -> dict:
        """CoinSwitch returns a dict keyed by 'BASE/QUOTE' symbol."""
        return {"data": entries}

    def _ticker_entry(
        self,
        base_volume: str,
        last_price: str,
        quote_volume: str = None,
    ) -> dict:
        entry = {"baseVolume": base_volume, "lastPrice": last_price}
        if quote_volume is not None:
            entry["quoteVolume"] = quote_volume
        return entry

    # ------------------------------------------------------------------ tests

    @aioresponses()
    async def test_get_all_volumes_no_filter(self, mock_api):
        response = self._all_tickers_response({
            "BTC/INR": self._ticker_entry("10", "5000000", "50000000"),
            "ETH/INR": self._ticker_entry("200", "300000"),
        })
        mock_api.get(self._all_tickers_url(), body=json.dumps(response))

        source = CoinswitchVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.trading_pair, result)
        self.assertIn(self.other_pair, result)
        self.assertEqual(Decimal("10"), result[self.trading_pair]["base_volume"])
        self.assertEqual(Decimal("5000000"), result[self.trading_pair]["last_price"])
        self.assertEqual(Decimal("50000000"), result[self.trading_pair]["quote_volume"])
        self.assertEqual("coinswitch", result[self.trading_pair]["exchange"])
        self.assertEqual(self.trading_pair, result[self.trading_pair]["symbol"])

    @aioresponses()
    async def test_get_all_volumes_no_quote_volume(self, mock_api):
        """Tickers without quoteVolume should still be included."""
        response = self._all_tickers_response({
            "BTC/INR": self._ticker_entry("10", "5000000"),
        })
        mock_api.get(self._all_tickers_url(), body=json.dumps(response))

        source = CoinswitchVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn(self.trading_pair, result)
        self.assertNotIn("quote_volume", result[self.trading_pair])

    @aioresponses()
    async def test_get_all_volumes_with_filter(self, mock_api):
        """When trading_pairs is given each pair is fetched via the single-pair endpoint."""
        response = self._all_tickers_response({
            "BTC/INR": self._ticker_entry("10", "5000000"),
        })
        mock_api.get(self._single_ticker_url(symbol="BTC/INR"), body=json.dumps(response))

        source = CoinswitchVolumeSource()
        result = await source.get_all_24h_volumes(trading_pairs=[self.trading_pair])

        self.assertIn(self.trading_pair, result)
        self.assertEqual(Decimal("10"), result[self.trading_pair]["base_volume"])

    @aioresponses()
    async def test_empty_data_returns_empty_dict(self, mock_api):
        mock_api.get(self._all_tickers_url(), body=json.dumps({"data": {}}))

        source = CoinswitchVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertEqual({}, result)

    @aioresponses()
    async def test_symbol_converted_from_slash_to_hyphen(self, mock_api):
        """API uses 'BASE/QUOTE'; the source must produce 'BASE-QUOTE'."""
        response = self._all_tickers_response({
            "DOGE/INR": self._ticker_entry("1000000", "8"),
        })
        mock_api.get(self._all_tickers_url(), body=json.dumps(response))

        source = CoinswitchVolumeSource()
        result = await source.get_all_24h_volumes()

        self.assertIn("DOGE-INR", result)
        self.assertNotIn("DOGE/INR", result)

    @aioresponses()
    async def test_source_name_is_coinswitch(self, mock_api):
        mock_api.get(self._all_tickers_url(), body=json.dumps({"data": {}}))

        source = CoinswitchVolumeSource()
        self.assertEqual("coinswitch", source.name)
