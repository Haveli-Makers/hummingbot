import unittest

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS, zebpay_web_utils as web_utils


class ZebpayWebUtilsTests(unittest.IsolatedAsyncioTestCase):

    def test_public_rest_url(self):
        url = web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL)
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_PATH_URL}", url)

    def test_private_rest_url(self):
        url = web_utils.private_rest_url(CONSTANTS.BALANCE_PATH_URL)
        self.assertEqual(f"{CONSTANTS.REST_URL}{CONSTANTS.BALANCE_PATH_URL}", url)

    def test_create_throttler(self):
        from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
        self.assertIsInstance(web_utils.create_throttler(), AsyncThrottler)

    def test_build_api_factory(self):
        from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
        self.assertIsInstance(web_utils.build_api_factory(), WebAssistantsFactory)

    async def test_server_time_is_milliseconds(self):
        t = await web_utils.get_current_server_time()
        # Milliseconds since epoch are ~1.7e12; seconds would be ~1.7e9.
        self.assertGreater(t, 1e12)


if __name__ == "__main__":
    unittest.main()
