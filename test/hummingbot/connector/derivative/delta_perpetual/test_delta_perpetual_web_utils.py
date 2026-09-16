from unittest import TestCase

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_web_utils as web_utils


class DeltaPerpetualWebUtilsTests(TestCase):
    def test_public_rest_url(self):
        self.assertEqual(
            f"{CONSTANTS.REST_URL}{CONSTANTS.PRODUCTS_PATH_URL}",
            web_utils.public_rest_url(CONSTANTS.PRODUCTS_PATH_URL),
        )

    def test_private_rest_url(self):
        self.assertEqual(
            f"{CONSTANTS.REST_URL}{CONSTANTS.ORDERS_PATH_URL}",
            web_utils.private_rest_url(CONSTANTS.ORDERS_PATH_URL),
        )

    def test_build_api_factory_has_time_synchronizer_preprocessor(self):
        factory = web_utils.build_api_factory()
        self.assertEqual(1, len(factory._rest_pre_processors))

    def test_get_current_server_time_returns_milliseconds(self):
        import asyncio

        ms = asyncio.get_event_loop().run_until_complete(web_utils.get_current_server_time())
        # A millisecond epoch is > 1e12 (seconds epoch is ~1.7e9).
        self.assertGreater(ms, 1e12)
