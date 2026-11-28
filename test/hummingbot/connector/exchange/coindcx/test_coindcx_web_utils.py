import unittest
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import aiohttp
from aiohttp_socks import ProxyConnector

import hummingbot.connector.exchange.coindcx.coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx import coindcx_web_utils as web_utils
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory


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

    def test_build_api_factory_without_proxy_uses_the_shared_connections_factory(self):
        api_factory = web_utils.build_api_factory()
        self.assertIsInstance(api_factory._connections_factory, ConnectionsFactory)

    def test_build_api_factory_with_proxy_uses_a_proxy_connections_factory(self):
        api_factory = web_utils.build_api_factory(proxy_url="socks5://user:pass@localhost:1080")
        self.assertIsInstance(api_factory._connections_factory, ProxyConnectionsFactory)

    def test_build_proxy_session_without_proxy_returns_none(self):
        self.assertIsNone(web_utils.build_proxy_session(None))
        self.assertIsNone(web_utils.build_proxy_session(""))


class CoindcxProxySessionTests(IsolatedAsyncioWrapperTestCase):

    async def test_build_proxy_session_connects_through_the_proxy(self):
        session = web_utils.build_proxy_session("http://user:pass@localhost:3128")
        try:
            self.assertIsInstance(session, aiohttp.ClientSession)
            self.assertIsInstance(session.connector, ProxyConnector)
        finally:
            await session.close()

    async def test_build_proxy_session_makes_a_new_session_each_time(self):
        first = web_utils.build_proxy_session("socks5://localhost:1080")
        second = web_utils.build_proxy_session("socks5://localhost:1080")
        try:
            self.assertIsNot(first, second)
        finally:
            await first.close()
            await second.close()
