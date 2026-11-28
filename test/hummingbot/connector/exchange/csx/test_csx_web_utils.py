import unittest

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS, csx_web_utils as web_utils


class CsxWebUtilsTests(unittest.TestCase):

    def test_public_rest_url(self):
        url = web_utils.public_rest_url(CONSTANTS.TICKER_V2_PATH_URL)
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_V2_PATH_URL}", url)

    def test_private_rest_url(self):
        url = web_utils.private_rest_url(CONSTANTS.BALANCE_V2_PATH_URL)
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.BALANCE_V2_PATH_URL}", url)

    def test_create_throttler_returns_throttler(self):
        from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
        throttler = web_utils.create_throttler()
        self.assertIsInstance(throttler, AsyncThrottler)

    def test_build_api_factory_returns_factory(self):
        from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
        factory = web_utils.build_api_factory()
        self.assertIsInstance(factory, WebAssistantsFactory)


if __name__ == "__main__":
    unittest.main()
