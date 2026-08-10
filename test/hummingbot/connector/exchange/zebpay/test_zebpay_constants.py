import unittest

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS


class ZebpayConstantsTests(unittest.TestCase):

    def test_rest_url_is_https(self):
        self.assertTrue(CONSTANTS.REST_URL.startswith("https://"))

    def test_required_order_states_mapped(self):
        required = {"OPEN", "FILLED", "CANCELLED", "COMPLETED"}
        self.assertTrue(required <= set(CONSTANTS.ORDER_STATE.keys()),
                        f"missing: {required - set(CONSTANTS.ORDER_STATE.keys())}")

    def test_rate_limits_present(self):
        self.assertGreater(len(CONSTANTS.RATE_LIMITS), 0)
        ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        self.assertIn(CONSTANTS.CREATE_ORDER_PATH_URL, ids)
        self.assertIn(CONSTANTS.BALANCE_PATH_URL, ids)
        self.assertIn(CONSTANTS.ORDERBOOK_PATH_URL, ids)
        self.assertIn(CONSTANTS.PUBLIC_LIMIT_ID, ids)
        self.assertIn(CONSTANTS.PRIVATE_LIMIT_ID, ids)

    def test_prefix_is_alphanumeric(self):
        self.assertTrue(CONSTANTS.HBOT_ORDER_ID_PREFIX.isalnum())

    def test_sides_uppercase(self):
        self.assertEqual("BUY", CONSTANTS.SIDE_BUY)
        self.assertEqual("SELL", CONSTANTS.SIDE_SELL)


if __name__ == "__main__":
    unittest.main()
