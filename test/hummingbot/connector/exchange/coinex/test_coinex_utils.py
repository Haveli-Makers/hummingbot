from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.exchange.coinex import coinex_utils as utils


class CoinexUtilsTests(TestCase):
    def test_centralized_and_example_pair(self):
        self.assertTrue(utils.CENTRALIZED)
        self.assertEqual("BTC-USDT", utils.EXAMPLE_PAIR)

    def test_default_fees(self):
        self.assertEqual(Decimal("0.002"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.002"), utils.DEFAULT_FEES.taker_percent_fee_decimal)

    def test_config_map_keys_are_connect_keys(self):
        cfg = utils.CoinexConfigMap.model_construct()
        self.assertEqual("coinex", cfg.connector)
        fields = utils.CoinexConfigMap.model_fields
        for name in ("coinex_api_key", "coinex_api_secret"):
            self.assertIn(name, fields)
            self.assertTrue(fields[name].json_schema_extra["is_connect_key"])

    def test_keys_exported(self):
        self.assertIsInstance(utils.KEYS, utils.CoinexConfigMap)
