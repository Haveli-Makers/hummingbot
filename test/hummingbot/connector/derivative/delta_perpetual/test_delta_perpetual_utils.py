from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.derivative.delta_perpetual import delta_perpetual_utils as utils


class DeltaPerpetualUtilsTests(TestCase):
    def test_centralized_and_example_pair(self):
        self.assertTrue(utils.CENTRALIZED)
        self.assertEqual("BTC-USD", utils.EXAMPLE_PAIR)

    def test_default_fees(self):
        self.assertEqual(Decimal("0.0002"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.0005"), utils.DEFAULT_FEES.taker_percent_fee_decimal)

    def test_config_map_keys(self):
        cfg = utils.DeltaPerpetualConfigMap.model_construct()
        self.assertEqual("delta_perpetual", cfg.connector)
        fields = utils.DeltaPerpetualConfigMap.model_fields
        self.assertIn("delta_perpetual_api_key", fields)
        self.assertIn("delta_perpetual_api_secret", fields)
        # Both credentials must be flagged as connect keys so the CLI prompts for them.
        for name in ("delta_perpetual_api_key", "delta_perpetual_api_secret"):
            self.assertTrue(fields[name].json_schema_extra["is_connect_key"])

    def test_proxy_url_is_connect_key(self):
        # The proxy URL MUST be a connect key, otherwise it is dropped before reaching
        # the connector constructor and traffic goes out directly (ip_not_whitelisted).
        fields = utils.DeltaPerpetualConfigMap.model_fields
        self.assertIn("delta_perpetual_proxy_url", fields)
        self.assertTrue(fields["delta_perpetual_proxy_url"].json_schema_extra["is_connect_key"])

    def test_proxy_url_reaches_connector_via_config_map(self):
        # End-to-end through the real CLI path: config map → api_keys → constructor.
        from pydantic import SecretStr

        from hummingbot.client.config.config_helpers import ClientConfigAdapter, api_keys_from_connector_config_map
        cm = ClientConfigAdapter(utils.DeltaPerpetualConfigMap(
            delta_perpetual_api_key=SecretStr("k"),
            delta_perpetual_api_secret=SecretStr("s"),
            delta_perpetual_proxy_url=SecretStr("socks5://user:pass@host:1080"),
        ))
        keys = api_keys_from_connector_config_map(cm)
        self.assertEqual("socks5://user:pass@host:1080", keys.get("delta_perpetual_proxy_url"))

    def test_keys_exported(self):
        self.assertIsInstance(utils.KEYS, utils.DeltaPerpetualConfigMap)
