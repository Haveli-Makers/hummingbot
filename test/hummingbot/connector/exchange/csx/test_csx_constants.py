import unittest

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS


class CsxConstantsTests(unittest.TestCase):

    def test_rest_url_is_set(self):
        self.assertTrue(CONSTANTS.REST_URL.startswith("https://"))

    def test_all_order_states_are_mapped(self):
        required = {"OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"}
        self.assertEqual(required, set(CONSTANTS.ORDER_STATE.keys()))

    def test_rate_limits_list_not_empty(self):
        self.assertGreater(len(CONSTANTS.RATE_LIMITS), 0)

    def test_rate_limit_ids_include_key_endpoints(self):
        ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        self.assertIn(CONSTANTS.CREATE_ORDER_PATH_URL, ids)
        self.assertIn(CONSTANTS.BALANCE_V2_PATH_URL, ids)
        self.assertIn(CONSTANTS.TICKER_V2_PATH_URL, ids)

    def test_hbot_prefix(self):
        self.assertTrue(CONSTANTS.HBOT_ORDER_ID_PREFIX.startswith("x-"))

    def test_max_order_id_len(self):
        self.assertGreaterEqual(CONSTANTS.MAX_ORDER_ID_LEN, 36)


if __name__ == "__main__":
    unittest.main()
