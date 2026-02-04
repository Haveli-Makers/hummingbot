import unittest

import hummingbot.connector.exchange.coindcx.coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx import coindcx_web_utils as web_utils


class CoindcxUtilTestCases(unittest.TestCase):

    def test_public_rest_url_market_data(self):
        path_url = "/market_data/TEST_PATH"
        expected_url = CONSTANTS.BASE_URL.format(CONSTANTS.PUBLIC_DOMAIN) + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url))

    def test_public_rest_url_other(self):
        path_url = "/TEST_PATH"
        domain = "custom.domain"
        expected_url = CONSTANTS.BASE_URL.format(domain) + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = "custom.domain"
        expected_url = CONSTANTS.BASE_URL.format(domain) + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))
