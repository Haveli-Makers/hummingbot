from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.derivative.wazirx_perpetual import (
    wazirx_perpetual_constants as CONSTANTS,
    wazirx_perpetual_utils as utils,
    wazirx_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.in_flight_order import OrderState

# Trimmed from the live /fapi/v1/exchangeInfo.
BTCINR = {
    "symbol": "BTCINR", "contractType": "PERPETUAL", "baseAsset": "BTC", "quoteAsset": "INR",
    "marginAsset": "INR", "orderTypes": ["MARKET", "LIMIT"], "maxLeverage": "150",
    "pricePrecision": "0", "quantityPrecision": "3",
    "filters": [
        {"maxQty": "50", "minQty": "0.001", "filterType": "limit_qty_size"},
        {"maxQty": "25", "minQty": "0.001", "filterType": "market_qty_size"},
        {"filterType": "max_num_orders", "limit": "200"},
        {"filterType": "min_notional", "notional": "10999.75"},
    ],
}


class WazirxPerpetualUtilsTests(TestCase):
    def test_valid_perpetual(self):
        self.assertTrue(utils.is_exchange_information_valid(BTCINR))

    def test_rejects_non_perpetual_and_broken_filters(self):
        self.assertFalse(utils.is_exchange_information_valid({**BTCINR, "contractType": "QUARTERLY"}))
        self.assertFalse(utils.is_exchange_information_valid({**BTCINR, "filters": []}))
        self.assertFalse(utils.is_exchange_information_valid({**BTCINR, "baseAsset": ""}))
        inverted = {**BTCINR, "filters": [{"filterType": "limit_qty_size", "minQty": "5", "maxQty": "1"}]}
        self.assertFalse(utils.is_exchange_information_valid(inverted))

    def test_get_filter(self):
        self.assertEqual("10999.75", utils.get_filter(BTCINR, "min_notional")["notional"])
        self.assertIsNone(utils.get_filter(BTCINR, "price_filter"))

    def test_precision_is_a_decimal_count_not_a_tick(self):
        self.assertEqual(Decimal("1"), utils.precision_to_increment("0"))
        self.assertEqual(Decimal("0.001"), utils.precision_to_increment("3"))
        self.assertEqual(Decimal("0.0000001"), utils.precision_to_increment(7))
        self.assertEqual(Decimal("1"), utils.precision_to_increment("garbage"))

    def test_ws_symbol(self):
        self.assertEqual("btcinr", utils.hb_pair_to_ws_symbol("BTC-INR"))
        self.assertEqual("1000pepeusdt", utils.hb_pair_to_ws_symbol("1000PEPE-USDT"))

    def test_to_decimal(self):
        self.assertEqual(Decimal("1.5"), utils.to_decimal("1.5"))
        self.assertEqual(Decimal("0"), utils.to_decimal(""))
        self.assertEqual(Decimal("7"), utils.to_decimal(None, Decimal("7")))
        self.assertEqual(Decimal("0"), utils.to_decimal("abc"))

    def test_default_fees_match_the_published_schedule(self):
        self.assertEqual(Decimal("0.0002"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.0004"), utils.DEFAULT_FEES.taker_percent_fee_decimal)

    def test_config_keys(self):
        fields = utils.WazirxPerpetualConfigMap.model_fields
        self.assertEqual("wazirx_perpetual", fields["connector"].default)
        for key in ("wazirx_perpetual_api_key", "wazirx_perpetual_api_secret", "wazirx_perpetual_proxy_url"):
            self.assertIn(key, fields)
        # False would silently drop the proxy before it reaches the connector.
        self.assertTrue(fields["wazirx_perpetual_proxy_url"].json_schema_extra["is_connect_key"])


class WazirxPerpetualConstantsTests(TestCase):
    def test_order_id_prefix_is_alphanumeric(self):
        # requestId must be alphanumeric (error 3017), and it IS the client order id.
        self.assertTrue(CONSTANTS.HBOT_ORDER_ID_PREFIX.isalnum())
        self.assertLessEqual(CONSTANTS.MAX_ORDER_ID_LEN, 64)

    def test_status_mapping(self):
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["init"])
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["wait"])
        self.assertEqual(OrderState.FILLED, CONSTANTS.ORDER_STATE["done"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["cancel"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["expire"])
        self.assertEqual(OrderState.FAILED, CONSTANTS.ORDER_STATE["reject"])

    def test_every_request_path_is_rate_limited(self):
        ids = {limit.limit_id for limit in CONSTANTS.RATE_LIMITS}
        for limit_id in (CONSTANTS.CREATE_ORDER_LIMIT_ID, CONSTANTS.CANCEL_ORDER_LIMIT_ID,
                         CONSTANTS.QUERY_ORDER_LIMIT_ID, CONSTANTS.DEPTH_PATH_URL, CONSTANTS.FUNDS_PATH_URL,
                         CONSTANTS.POSITION_RISK_PATH_URL, CONSTANTS.USER_TRADES_PATH_URL,
                         CONSTANTS.INCOME_PATH_URL, CONSTANTS.CREATE_AUTH_TOKEN_PATH_URL,
                         CONSTANTS.SERVER_TIME_PATH_URL, CONSTANTS.EXCHANGE_INFO_PATH_URL):
            self.assertIn(limit_id, ids)
        limits = {limit.limit_id: limit.limit for limit in CONSTANTS.RATE_LIMITS}
        self.assertEqual(10, limits[CONSTANTS.CREATE_ORDER_LIMIT_ID])
        self.assertEqual(10, limits[CONSTANTS.CANCEL_ORDER_LIMIT_ID])


class WazirxPerpetualWebUtilsTests(TestCase):
    def test_urls(self):
        self.assertEqual("https://api.wazirx.com/fapi/v1/depth", web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL))
        self.assertEqual("https://api.wazirx.com/sapi/v2/funds", web_utils.private_rest_url(CONSTANTS.FUNDS_PATH_URL))
        self.assertEqual("wss://fstreamx.wazirx.com/stream", web_utils.wss_url())

    def test_proxy_selects_a_dedicated_factory(self):
        from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory

        self.assertIsInstance(web_utils._build_connections_factory(None), ConnectionsFactory)
        self.assertIsInstance(web_utils._build_connections_factory("socks5://u:p@host:1080"), ProxyConnectionsFactory)
