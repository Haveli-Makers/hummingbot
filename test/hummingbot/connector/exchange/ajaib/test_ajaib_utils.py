import uuid
from unittest import TestCase

from hummingbot.connector.exchange.ajaib import ajaib_utils


class AjaibUtilsTests(TestCase):
    def test_symbol_conversions(self):
        self.assertEqual("BTC-IDR", ajaib_utils.ajaib_symbol_to_hb_pair("BTC_IDR"))
        self.assertEqual("BTC_IDR", ajaib_utils.hb_pair_to_ajaib_symbol("BTC-IDR"))

    def test_generate_client_order_id_is_uuid4(self):
        order_id = ajaib_utils.generate_client_order_id()
        parsed = uuid.UUID(order_id)
        self.assertEqual(4, parsed.version)
        self.assertEqual(order_id, str(parsed))

    def test_is_exchange_information_valid_accepts_spot_market(self):
        info = {
            "symbol": "BTC_IDR",
            "isSpotTradingAllowed": True,
            "filters": [{"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "100"}],
        }
        self.assertTrue(ajaib_utils.is_exchange_information_valid(info))

    def test_is_exchange_information_valid_rejects_non_spot(self):
        info = {"symbol": "BTC_IDR", "isSpotTradingAllowed": False}
        self.assertFalse(ajaib_utils.is_exchange_information_valid(info))

    def test_is_exchange_information_valid_rejects_missing_symbol(self):
        info = {"isSpotTradingAllowed": True, "filters": []}
        self.assertFalse(ajaib_utils.is_exchange_information_valid(info))

    def test_config_map_has_proxy_field(self):
        fields = ajaib_utils.AjaibConfigMap.model_fields
        self.assertIn("ajaib_api_key", fields)
        self.assertIn("ajaib_api_secret", fields)
        self.assertIn("ajaib_proxy_url", fields)

    def test_proxy_field_is_not_a_connect_key(self):
        extra = ajaib_utils.AjaibConfigMap.model_fields["ajaib_proxy_url"].json_schema_extra
        self.assertFalse(extra["is_connect_key"])
