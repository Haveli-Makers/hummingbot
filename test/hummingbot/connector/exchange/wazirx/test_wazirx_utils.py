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

    def test_parse_api_credentials_single_key(self):
        self.assertEqual([("key", "secret")], utils.parse_api_credentials("key", "secret"))

    def test_parse_api_credentials_empty_key_for_public_data(self):
        self.assertEqual([("", "")], utils.parse_api_credentials("", ""))

    def test_parse_api_credentials_pairs_keys_with_secrets_by_position(self):
        self.assertEqual(
            [("k1", "s1"), ("k2", "s2"), ("k3", "s3")],
            utils.parse_api_credentials("k1, k2 ,k3", " s1,s2, s3 "),
        )

    def test_parse_api_credentials_accepts_five_keys(self):
        keys = ",".join(f"k{i}" for i in range(5))
        secrets = ",".join(f"s{i}" for i in range(5))
        self.assertEqual(5, len(utils.parse_api_credentials(keys, secrets)))

    def test_parse_api_credentials_rejects_count_mismatch(self):
        with self.assertRaisesRegex(ValueError, "got 2 keys and 1 secrets"):
            utils.parse_api_credentials("k1,k2", "s1")

    def test_parse_api_credentials_rejects_more_than_five_keys(self):
        keys = ",".join(f"k{i}" for i in range(6))
        secrets = ",".join(f"s{i}" for i in range(6))
        with self.assertRaisesRegex(ValueError, "at most 5 API keys"):
            utils.parse_api_credentials(keys, secrets)

    def test_parse_api_credentials_rejects_blank_entries(self):
        with self.assertRaisesRegex(ValueError, "blank entries"):
            utils.parse_api_credentials("k1,,k3", "s1,s2,s3")
        with self.assertRaisesRegex(ValueError, "blank entries"):
            utils.parse_api_credentials("k1,k2", "s1, ")

    def test_parse_api_credentials_rejects_repeated_key(self):
        with self.assertRaisesRegex(ValueError, "only once"):
            utils.parse_api_credentials("k1,k1", "s1,s2")
