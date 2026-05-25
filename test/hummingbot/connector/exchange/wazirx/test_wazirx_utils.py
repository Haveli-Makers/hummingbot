import unittest

from hummingbot.connector.exchange.wazirx import wazirx_utils as utils


class WazirxUtilTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.hb_trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"

    def test_wazirx_pair_to_hb_pair(self):

        self.assertEqual("BTC-USDT", utils.wazirx_pair_to_hb_pair("btcusdt"))
        self.assertEqual("ETH-INR", utils.wazirx_pair_to_hb_pair("ethinr"))
        self.assertEqual("BTC-BTC", utils.wazirx_pair_to_hb_pair("btcbtc"))

        self.assertEqual("BTC-USDT", utils.wazirx_pair_to_hb_pair("btc_usdt"))

        self.assertEqual("UNKNOWN", utils.wazirx_pair_to_hb_pair("unknown"))

    def test_hb_pair_to_wazirx_symbol(self):
        self.assertEqual("BTCUSDT", utils.hb_pair_to_wazirx_symbol("BTC-USDT"))
        self.assertEqual("ETHINR", utils.hb_pair_to_wazirx_symbol("ETH-INR"))
