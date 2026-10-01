from unittest import TestCase

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState


class AjaibConstantsTests(TestCase):
    def test_hosts(self):
        # Mainnet base endpoints per the Open API docs. "kripto" is a DIFFERENT
        # service sitting behind Cloudflare and 403s every request.
        self.assertEqual("https://api.crypto.ajaib.co.id", CONSTANTS.REST_URL)
        self.assertEqual("wss://stream.crypto.ajaib.co.id", CONSTANTS.WSS_URL)

    def test_endpoint_paths(self):
        # Verified live: "/v1/portfolio" 404s, "/v1/ticker/bookTicker" 404s.
        self.assertEqual("/v1/account", CONSTANTS.ACCOUNT_PATH_URL)
        self.assertEqual("/v1/ticker/book-ticker", CONSTANTS.BOOK_TICKER_PATH_URL)
        self.assertEqual("/v1/depth", CONSTANTS.DEPTH_PATH_URL)
        self.assertEqual("/v1/order/open", CONSTANTS.CANCEL_ALL_ORDERS_PATH_URL)
        self.assertEqual("/auth/v1/listen-key", CONSTANTS.LISTEN_KEY_PATH_URL)

    def test_recv_window_is_within_the_server_cap(self):
        """5000 is a hard cap: 6000+ is rejected with -1021, despite the docs
        documenting no maximum."""
        self.assertLessEqual(CONSTANTS.RECV_WINDOW, CONSTANTS.MAX_RECV_WINDOW)
        self.assertEqual(5000, CONSTANTS.MAX_RECV_WINDOW)

    def test_every_terminal_status_is_mapped(self):
        """
        An unmapped status falls back to the order's CURRENT state, and the REST
        poll keeps returning that same status -- so a terminal order would never
        settle. PARTIALLY_EXPIRED was missing and would have stranded orders.
        """
        for status in CONSTANTS.TERMINAL_ORDER_STATUSES:
            self.assertIn(status, CONSTANTS.ORDER_STATE, f"{status} is unmapped")
            self.assertIn(CONSTANTS.ORDER_STATE[status],
                          {OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED},
                          f"{status} is terminal but maps to a non-terminal state")

    def test_self_trade_prevention_outcomes_are_terminal(self):
        for status in ("EXPIRED_IN_MATCH", "PARTIALLY_EXPIRED_IN_MATCH", "PARTIALLY_EXPIRED"):
            self.assertEqual(OrderState.CANCELED, CONSTANTS.ORDER_STATE[status])

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
            CONSTANTS.ACCOUNT_PATH_URL, CONSTANTS.LISTEN_KEY_PATH_URL,
            CONSTANTS.DEPTH_PATH_URL, CONSTANTS.BOOK_TICKER_PATH_URL,
        ]:
            self.assertIn(path, limit_ids)
