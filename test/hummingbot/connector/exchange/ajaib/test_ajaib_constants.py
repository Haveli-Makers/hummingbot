from unittest import TestCase

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState


class AjaibConstantsTests(TestCase):
    def test_hosts(self):
        self.assertEqual("https://api.kripto.ajaib.co.id", CONSTANTS.REST_URL)
        self.assertEqual("wss://stream.kripto.ajaib.co.id", CONSTANTS.WSS_URL)

    def test_endpoint_paths(self):
        self.assertEqual("/v1/portfolio", CONSTANTS.PORTFOLIO_PATH_URL)
        self.assertEqual("/v1/order/open", CONSTANTS.CANCEL_ALL_ORDERS_PATH_URL)
        self.assertEqual("/auth/v1/listen-key", CONSTANTS.LISTEN_KEY_PATH_URL)

    def test_order_state_mapping(self):
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["NEW"])
        self.assertEqual(OrderState.FILLED, CONSTANTS.ORDER_STATE["FILLED"])
        self.assertEqual(OrderState.PARTIALLY_FILLED, CONSTANTS.ORDER_STATE["PARTIALLY_FILLED"])
        self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE["CANCELLED"])
        self.assertEqual(OrderState.FAILED, CONSTANTS.ORDER_STATE["REJECTED"])

    def test_rate_limits_cover_all_paths(self):
        limit_ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        for path in [
            CONSTANTS.SERVER_TIME_PATH_URL, CONSTANTS.EXCHANGE_INFO_PATH_URL, CONSTANTS.KLINES_PATH_URL,
            CONSTANTS.CREATE_ORDER_PATH_URL, CONSTANTS.OPEN_ORDERS_PATH_URL, CONSTANTS.TRADES_PATH_URL,
            CONSTANTS.PORTFOLIO_PATH_URL, CONSTANTS.LISTEN_KEY_PATH_URL,
        ]:
            self.assertIn(path, limit_ids)
