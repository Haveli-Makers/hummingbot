import asyncio
from unittest import TestCase

import hummingbot.connector.exchange.valr.valr_constants as CONSTANTS
import hummingbot.connector.exchange.valr.valr_web_utils as web_utils


class ValrWebUtilsTests(TestCase):
    def test_public_and_private_rest_url(self):
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.PAIRS_PATH_URL}",
                         web_utils.public_rest_url(CONSTANTS.PAIRS_PATH_URL))
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.BALANCES_PATH_URL}",
                         web_utils.private_rest_url(CONSTANTS.BALANCES_PATH_URL))

    def test_build_api_factory_has_time_sync_preprocessor(self):
        factory = web_utils.build_api_factory()
        self.assertEqual(1, len(factory._rest_pre_processors))

    def test_get_current_server_time_returns_ms(self):
        ms = asyncio.get_event_loop().run_until_complete(web_utils.get_current_server_time())
        self.assertGreater(ms, 1e12)
