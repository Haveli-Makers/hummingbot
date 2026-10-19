import uuid
from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.derivative.coinswitch_perpetual import (
    coinswitch_perpetual_constants as CONSTANTS,
    coinswitch_perpetual_utils as utils,
)

# From the Get Instrument Info docs.
BTCUSDT = {
    "symbol": "btc", "base_asset": "btc", "quote_asset": "usdt", "status": "TRADING", "type": "PERPETUAL_FUTURES",
    "min_leverage": "1", "max_leverage": "25", "leverage_step": 1, "min_base_quantity": "0.001",
    "base_quantity_step_size": "0.001", "lot_size": "0.001", "quantity_precision": 3, "price_precision": 2,
    "tick_size": 1, "max_market_base_quantity": "119", "max_base_quantity": "952", "risk_limit": "1000000",
    "maint_margin_rate": "0.56", "taker_fee_rate": "0.00065", "maker_fee_rate": "0.00024", "liq_fee_rate": "0.0075",
    "quote_asset_precision": 1,
}


class CoinswitchPerpetualUtilsTests(TestCase):
    def test_to_decimal(self):
        self.assertEqual(Decimal("0.28"), utils.to_decimal("0.28"))
        self.assertEqual(Decimal("133"), utils.to_decimal(133))
        self.assertEqual(Decimal("0"), utils.to_decimal(""))
        self.assertEqual(Decimal("0"), utils.to_decimal(None))
        self.assertEqual(Decimal("7"), utils.to_decimal("n/a", Decimal("7")))

    def test_unwrap_and_socket_payloads(self):
        self.assertEqual({"a": 1}, utils.unwrap_data({"data": {"a": 1}}))
        self.assertEqual([1], utils.unwrap_data([1]))
        self.assertEqual({"a": 1}, utils.parse_socket_payload('{"a": 1}'))
        self.assertEqual({"a": 1}, utils.parse_socket_payload(b'{"a": 1}'))
        self.assertIsNone(utils.parse_socket_payload("not json"))
        self.assertEqual({"a": 1}, utils.parse_socket_payload({"a": 1}))

    def test_subscription_ack(self):
        ack = {"success": "true", "message": "Subscribed to FETCH_ORDER_BOOK_CS_PRO_EXCHANGE_2_BTCUSDT"}
        self.assertTrue(utils.is_subscription_ack(ack))
        self.assertTrue(utils.is_subscription_ack({"success": "false", "message": "Invalid pair"}))
        self.assertFalse(utils.is_subscription_ack({"symbol": "BTCUSDT", "bids": [], "asks": []}))
        self.assertFalse(utils.is_subscription_ack([ack]))

    def test_instrument_validity(self):
        self.assertTrue(utils.is_exchange_information_valid("BTCUSDT", BTCUSDT))
        self.assertFalse(utils.is_exchange_information_valid("BTCUSDT", {**BTCUSDT, "status": "SETTLING"}))
        self.assertFalse(utils.is_exchange_information_valid("BTCUSDT", {**BTCUSDT, "type": "QUARTERLY"}))
        self.assertFalse(utils.is_exchange_information_valid("BTCINR", BTCUSDT))
        self.assertFalse(utils.is_exchange_information_valid("", BTCUSDT))
        self.assertFalse(utils.is_exchange_information_valid("USDT", BTCUSDT))

    def test_base_comes_from_the_symbol_key(self):
        self.assertEqual(("BTC", "USDT"), utils.base_and_quote("BTCUSDT", BTCUSDT))
        pepe = {**BTCUSDT, "symbol": "pepe", "base_asset": "pepe"}
        self.assertEqual(("1000PEPE", "USDT"), utils.base_and_quote("1000PEPEUSDT", pepe))

    def test_price_increment_is_tick_size_at_price_precision(self):
        self.assertEqual(Decimal("0.01"), utils.price_increment(BTCUSDT))
        self.assertEqual(Decimal("0.05"), utils.price_increment({"price_precision": 2, "tick_size": 5}))
        self.assertEqual(Decimal("0.00001"), utils.price_increment({"price_precision": 5, "tick_size": 1}))
        self.assertEqual(Decimal("1"), utils.price_increment({}))

    def test_quantity_increment(self):
        self.assertEqual(Decimal("0.001"), utils.quantity_increment(BTCUSDT))
        self.assertEqual(Decimal("1E+2"), utils.quantity_increment({"quantity_precision": -2}))
        self.assertEqual(Decimal("0.1"), utils.quantity_increment({"lot_size": "0.1"}))

    def test_client_order_uuid_is_a_stable_uuid(self):
        first = utils.client_order_uuid("x-CSPBBTUT6081a2b3c4d5e6f7")
        self.assertEqual(first, utils.client_order_uuid("x-CSPBBTUT6081a2b3c4d5e6f7"))
        self.assertNotEqual(first, utils.client_order_uuid("x-CSPSBTUT6081a2b3c4d5e6f7"))
        self.assertEqual(5, uuid.UUID(first).version)
        self.assertEqual(str(uuid.uuid5(CONSTANTS.CLIENT_ORDER_ID_NAMESPACE, "abc")), utils.client_order_uuid("abc"))

    def test_symbol_conversion(self):
        self.assertEqual("BTCUSDT", utils.hb_pair_to_symbol("BTC-USDT"))
        self.assertEqual("1000PEPEUSDT", utils.hb_pair_to_symbol("1000pepe-usdt"))

    def test_config_map(self):
        config = utils.CoinswitchPerpetualConfigMap.model_construct()
        self.assertEqual("coinswitch_perpetual", config.connector)
        fields = utils.CoinswitchPerpetualConfigMap.model_fields
        for field in ("coinswitch_perpetual_api_key", "coinswitch_perpetual_api_secret",
                      "coinswitch_perpetual_proxy_url"):
            self.assertTrue(fields[field].json_schema_extra["is_connect_key"])
            self.assertTrue(fields[field].json_schema_extra["is_secure"])
        self.assertEqual("", config.coinswitch_perpetual_proxy_url.get_secret_value())
        self.assertEqual("BTC-USDT", utils.EXAMPLE_PAIR)
        self.assertEqual(Decimal("0.00024"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.00065"), utils.DEFAULT_FEES.taker_percent_fee_decimal)
