from unittest import TestCase

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState


class CoinexConstantsTests(TestCase):
    def test_base_urls(self):
        self.assertTrue(CONSTANTS.REST_URL.startswith("https://"))
        self.assertTrue(CONSTANTS.WSS_URL.startswith("wss://"))

    def test_order_id_prefix_alphanumeric(self):
        self.assertTrue(CONSTANTS.HBOT_ORDER_ID_PREFIX.isalnum())
        self.assertLessEqual(len(CONSTANTS.HBOT_ORDER_ID_PREFIX), CONSTANTS.MAX_ORDER_ID_LEN)

    def test_order_state_mapping(self):
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["open"])
        self.assertEqual(OrderState.PARTIALLY_FILLED, CONSTANTS.ORDER_STATE["part_filled"])
        self.assertEqual(OrderState.FILLED, CONSTANTS.ORDER_STATE["filled"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["canceled"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["part_canceled"])

    def test_rate_limits_cover_key_endpoints(self):
        ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        for ep in (CONSTANTS.MARKETS_PATH_URL, CONSTANTS.ORDER_PATH_URL,
                   CONSTANTS.CANCEL_ORDER_PATH_URL, CONSTANTS.BALANCE_PATH_URL):
            self.assertIn(ep, ids)
