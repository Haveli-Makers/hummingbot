from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.exchange.valr import valr_utils as utils


class ValrUtilsTests(TestCase):
    def test_centralized_and_example_pair(self):
        self.assertTrue(utils.CENTRALIZED)
        self.assertEqual("BTC-ZAR", utils.EXAMPLE_PAIR)

    def test_default_fees(self):
        self.assertEqual(Decimal("0.0"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.001"), utils.DEFAULT_FEES.taker_percent_fee_decimal)

    def test_config_map_keys_are_connect_keys(self):
        cfg = utils.ValrConfigMap.model_construct()
        self.assertEqual("valr", cfg.connector)
        fields = utils.ValrConfigMap.model_fields
        for name in ("valr_api_key", "valr_api_secret"):
            self.assertIn(name, fields)
            self.assertTrue(fields[name].json_schema_extra["is_connect_key"])

    def test_keys_exported(self):
        self.assertIsInstance(utils.KEYS, utils.ValrConfigMap)
