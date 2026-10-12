from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_utils as utils,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


def _instrument(**overrides):
    instrument = {
        "pair": "B-BTC_USDT",
        "status": "active",
        "kind": "perpetual",
        "is_inverse": False,
        "exit_only": False,
        "min_quantity": 0.001,
        "max_quantity": 950.0,
    }
    instrument.update(overrides)
    return instrument


class CoinDCXPerpetualUtilsTests(TestCase):
    def test_pair_conversions_round_trip(self):
        self.assertEqual("BTC-USDT", utils.coindcx_pair_to_hb_pair("B-BTC_USDT"))
        self.assertEqual("B-BTC_USDT", utils.hb_pair_to_coindcx_pair("BTC-USDT"))

    def test_market_symbol_conversion(self):
        # depth-snapshot frames identify instruments by this market symbol.
        self.assertEqual("BTCUSDT", utils.coindcx_pair_to_market_symbol("B-BTC_USDT"))
        self.assertEqual("BTCUSDT", utils.hb_pair_to_market_symbol("BTC-USDT"))

    def test_normalize_margin_currency_accepts_usdt_and_inr(self):
        self.assertEqual("USDT", utils.normalize_margin_currency("usdt"))
        self.assertEqual("INR", utils.normalize_margin_currency(" inr "))
        self.assertEqual("USDT", utils.normalize_margin_currency(None))

    def test_normalize_margin_currency_rejects_others(self):
        for bad in ("BTC", "USD", "eth"):
            with self.assertRaises(ValueError):
                utils.normalize_margin_currency(bad)

    def test_percent_fee_converted_to_decimal_fraction(self):
        # CoinDCX reports 0.0236 meaning 0.0236 %.
        self.assertEqual(Decimal("0.000236"), utils.percent_fee_to_decimal(0.0236))
        self.assertEqual(Decimal("0"), utils.percent_fee_to_decimal(None))

    def test_is_exchange_information_valid(self):
        self.assertTrue(utils.is_exchange_information_valid(_instrument()))
        self.assertFalse(utils.is_exchange_information_valid(_instrument(status="delisted")))
        self.assertFalse(utils.is_exchange_information_valid(_instrument(is_inverse=True)))
        self.assertFalse(utils.is_exchange_information_valid(_instrument(exit_only=True)))
        self.assertFalse(utils.is_exchange_information_valid(_instrument(max_quantity=0)))
        self.assertFalse(utils.is_exchange_information_valid(_instrument(pair="")))

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
    def test_private_rest_url(self):
        self.assertEqual(
            "https://api.coindcx.com/exchange/v1/derivatives/futures/orders/create",
            web_utils.private_rest_url(CONSTANTS.CREATE_ORDER_PATH_URL),
        )

    def test_public_market_data_url_uses_public_host(self):
        self.assertEqual(
            "https://public.coindcx.com/market_data/v3/current_prices/futures/rt",
            web_utils.public_market_data_url(CONSTANTS.CURRENT_PRICES_PATH_URL),
        )

    def test_order_book_url_includes_futures_suffix(self):
        # Without "-futures" the same path returns the SPOT book.
        url = web_utils.order_book_url("B-BTC_USDT", depth=20)
        self.assertEqual("https://public.coindcx.com/market_data/v3/orderbook/B-BTC_USDT-futures/20", url)
        self.assertIn("-futures", url)

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
