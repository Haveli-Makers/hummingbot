from unittest import TestCase

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState


class ValrConstantsTests(TestCase):
    def test_base_urls(self):
        self.assertTrue(CONSTANTS.REST_URL.startswith("https://"))
        self.assertTrue(CONSTANTS.WSS_TRADE_URL.startswith("wss://"))
        self.assertTrue(CONSTANTS.WSS_ACCOUNT_URL.startswith("wss://"))

    def test_order_id_prefix(self):
        self.assertTrue(CONSTANTS.HBOT_ORDER_ID_PREFIX.isalnum())
        self.assertLessEqual(len(CONSTANTS.HBOT_ORDER_ID_PREFIX), CONSTANTS.MAX_ORDER_ID_LEN)

    def test_order_state_mapping(self):
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["Placed"])
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["Active"])
        self.assertEqual(OrderState.PARTIALLY_FILLED, CONSTANTS.ORDER_STATE["Partially Filled"])
        self.assertEqual(OrderState.FILLED, CONSTANTS.ORDER_STATE["Filled"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["Cancelled"])
        self.assertEqual(OrderState.FAILED, CONSTANTS.ORDER_STATE["Failed"])

    def test_rate_limits_cover_key_endpoints(self):
        ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        for ep in (CONSTANTS.PAIRS_PATH_URL, CONSTANTS.PLACE_LIMIT_ORDER_PATH_URL,
                   CONSTANTS.CANCEL_ORDER_PATH_URL, CONSTANTS.BALANCES_PATH_URL):
            self.assertIn(ep, ids)
