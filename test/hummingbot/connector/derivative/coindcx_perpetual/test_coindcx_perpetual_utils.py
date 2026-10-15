from unittest import TestCase

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_utils as utils,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


def _symbol(**overrides):
    symbol = {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "contractType": "PERPETUAL",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
            {"filterType": "MIN_NOTIONAL", "notional": "50.0"},
        ],
    }
    symbol.update(overrides)
    return symbol


class CoinDCXPerpetualUtilsTests(TestCase):
    def test_legacy_pair_conversion(self):
        # Still used by the legacy private code to read positions.
        self.assertEqual("BTC-USDT", utils.coindcx_pair_to_hb_pair("B-BTC_USDT"))

    def test_is_exchange_information_valid(self):
        self.assertTrue(utils.is_exchange_information_valid(_symbol()))
        self.assertFalse(utils.is_exchange_information_valid(_symbol(status="SETTLING")))
        self.assertFalse(utils.is_exchange_information_valid(_symbol(contractType="CURRENT_QUARTER")))
        self.assertFalse(utils.is_exchange_information_valid(_symbol(symbol="")))
        self.assertFalse(utils.is_exchange_information_valid(_symbol(baseAsset=None)))

    def test_symbol_filter(self):
        self.assertEqual("0.1", utils.symbol_filter(_symbol(), "PRICE_FILTER")["tickSize"])
        self.assertEqual({}, utils.symbol_filter(_symbol(), "LOT_SIZE"))
        self.assertEqual({}, utils.symbol_filter(_symbol(filters=None), "PRICE_FILTER"))

    def test_normalize_margin_currency_accepts_usdt_and_inr(self):
        self.assertEqual("USDT", utils.normalize_margin_currency("usdt"))
        self.assertEqual("INR", utils.normalize_margin_currency(" inr "))
        self.assertEqual("USDT", utils.normalize_margin_currency(None))

    def test_normalize_margin_currency_rejects_others(self):
        for bad in ("BTC", "USD", "eth"):
            with self.assertRaises(ValueError):
                utils.normalize_margin_currency(bad)

    def test_config_map_fields(self):
        fields = utils.CoinDCXPerpetualConfigMap.model_fields
        self.assertIn("coindcx_perpetual_api_key", fields)
        self.assertIn("coindcx_perpetual_api_secret", fields)
        self.assertIn("coindcx_perpetual_proxy_url", fields)
        self.assertIn("coindcx_perpetual_margin_currency", fields)

    def test_margin_currency_config_defaults_to_usdt_and_is_a_connect_key(self):
        field = utils.CoinDCXPerpetualConfigMap.model_fields["coindcx_perpetual_margin_currency"]
        self.assertEqual(CONSTANTS.DEFAULT_MARGIN_CURRENCY, field.default)
        # Must reach the connector constructor, like the proxy field.
        self.assertTrue(field.json_schema_extra["is_connect_key"])

    def test_proxy_url_is_a_connect_key(self):
        # Fields flagged False are dropped before reaching the connector, which
        # would silently ignore the configured proxy.
        extra = utils.CoinDCXPerpetualConfigMap.model_fields["coindcx_perpetual_proxy_url"].json_schema_extra
        self.assertTrue(extra["is_connect_key"])


class CoinDCXPerpetualWebUtilsTests(TestCase):
    def test_build_connections_factory_default(self):
        self.assertIsInstance(web_utils._build_connections_factory(None), ConnectionsFactory)

    def test_build_connections_factory_with_proxy(self):
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        factory = web_utils._build_connections_factory("socks5://user:pass@localhost:1080")
        self.assertIsInstance(factory, ProxyConnectionsFactory)

    def test_build_api_factory_wires_proxy(self):
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        api_factory = web_utils.build_api_factory(proxy_url="socks5://user:pass@localhost:1080")
        self.assertIsInstance(api_factory._connections_factory, ProxyConnectionsFactory)
