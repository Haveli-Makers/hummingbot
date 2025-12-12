import unittest

import hummingbot.connector.exchange.wazirx.wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx import wazirx_web_utils as web_utils


class WazirxWebUtilTestCases(unittest.TestCase):

    def test_public_rest_url(self):
        path_url = "/TEST_PATH"
        domain = ""
        expected_url = CONSTANTS.REST_URL + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = ""
        expected_url = CONSTANTS.REST_URL + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))