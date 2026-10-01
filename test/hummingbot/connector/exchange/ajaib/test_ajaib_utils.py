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

    def test_testnet_is_registered_as_its_own_domain(self):
        self.assertEqual(["ajaib_testnet"], ajaib_utils.OTHER_DOMAINS)
        for table in (ajaib_utils.OTHER_DOMAINS_PARAMETER, ajaib_utils.OTHER_DOMAINS_EXAMPLE_PAIR,
                      ajaib_utils.OTHER_DOMAINS_DEFAULT_FEES, ajaib_utils.OTHER_DOMAINS_KEYS):
            self.assertIn("ajaib_testnet", table)
        self.assertEqual("ajaib_testnet", ajaib_utils.OTHER_DOMAINS_PARAMETER["ajaib_testnet"])

    def test_testnet_keys_map_onto_the_constructor_parameters(self):
        """
        The client builds the constructor kwargs with
        k.replace("ajaib_testnet", "ajaib"); every testnet connect key must
        therefore land on a real AjaibExchange parameter.
        """
        import inspect

        from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange
        init_params = inspect.signature(AjaibExchange.__init__).parameters
        for name, field in ajaib_utils.AjaibTestnetConfigMap.model_fields.items():
            if (field.json_schema_extra or {}).get("is_connect_key"):
                mapped = name.replace("ajaib_testnet", "ajaib")
                self.assertIn(mapped, init_params, f"{name} maps to {mapped}, not a constructor parameter")

    def test_every_prompted_field_reaches_the_connector(self):
        """
        Connect keys are the only config fields forwarded to the connector
        constructor. A prompted field flagged False is stored but never reaches
        it -- for the proxy that means `connect` validates over a direct
        connection and Ajaib rejects it 403, since only the proxy IP is
        allowlisted. Checked for mainnet and testnet alike.
        """
        for config_map in (ajaib_utils.AjaibConfigMap, ajaib_utils.AjaibTestnetConfigMap):
            for name, field in config_map.model_fields.items():
                extra = field.json_schema_extra or {}
                if extra.get("prompt_on_new"):
                    self.assertTrue(
                        extra.get("is_connect_key"),
                        f"{config_map.__name__}.{name} is prompted but not a connect key, "
                        f"so it is silently discarded")
