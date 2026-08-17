from unittest import TestCase

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_web_utils as web_utils
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


class AjaibWebUtilsTests(TestCase):
    def test_public_rest_url_uses_kripto_host(self):
        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        self.assertEqual("https://api.crypto.ajaib.co.id/v1/exchange-info", url)

    def test_private_rest_url_matches_public(self):
        path = CONSTANTS.CREATE_ORDER_PATH_URL
        self.assertEqual(web_utils.public_rest_url(path), web_utils.private_rest_url(path))

    def test_build_connections_factory_default(self):
        factory = web_utils._build_connections_factory(proxy_url=None)
        self.assertIsInstance(factory, ConnectionsFactory)

    def test_build_connections_factory_with_proxy(self):
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        factory = web_utils._build_connections_factory(proxy_url="socks5://user:pass@localhost:1080")
        self.assertIsInstance(factory, ProxyConnectionsFactory)

    def test_build_api_factory_with_proxy_uses_proxy_connections(self):
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        api_factory = web_utils.build_api_factory(proxy_url="socks5://user:pass@localhost:1080")
        self.assertIsInstance(api_factory._connections_factory, ProxyConnectionsFactory)

    def test_build_api_factory_without_proxy_uses_default_connections(self):
        api_factory = web_utils.build_api_factory()
        self.assertIsInstance(api_factory._connections_factory, ConnectionsFactory)
