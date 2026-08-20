from unittest import TestCase

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_web_utils as web_utils
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


class AjaibWebUtilsTests(TestCase):
    def test_public_rest_url_uses_mainnet_host(self):
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


class AjaibDomainRoutingTests(TestCase):
    def test_mainnet_is_the_default(self):
        self.assertEqual(
            'https://api.crypto.ajaib.co.id/v1/account',
            web_utils.public_rest_url('/v1/account'))
        self.assertEqual('wss://stream.crypto.ajaib.co.id', web_utils.wss_url())

    def test_testnet_domain_selects_testnet_hosts(self):
        self.assertEqual(
            'https://testnet.api.crypto.ajaib.co.id/v1/account',
            web_utils.public_rest_url('/v1/account', domain=CONSTANTS.TESTNET_DOMAIN))
        self.assertEqual(
            'wss://testnet.stream.crypto.ajaib.co.id',
            web_utils.wss_url(domain=CONSTANTS.TESTNET_DOMAIN))

    def test_unknown_domain_falls_back_to_mainnet(self):
        self.assertEqual(
            'https://api.crypto.ajaib.co.id/v1/time',
            web_utils.public_rest_url('/v1/time', domain='nonsense'))


class AjaibTlsScopingTests(TestCase):
    """
    Ajaib's TESTNET certificate is expired, so testnet traffic must skip
    verification. Mainnet must never do so: without it, anyone able to
    intercept the connection could read the API key and alter live orders.
    """

    PROXY = "http://user:pass@host:3128"

    def test_mainnet_always_verifies_tls(self):
        factory = web_utils._build_connections_factory(self.PROXY, CONSTANTS.DEFAULT_DOMAIN)
        self.assertTrue(factory._verify_ssl, "mainnet must verify TLS")

    def test_mainnet_verifies_tls_by_default(self):
        # No domain argument at all must still be safe.
        factory = web_utils._build_connections_factory(self.PROXY)
        self.assertTrue(factory._verify_ssl)

    def test_unknown_domain_verifies_tls(self):
        # A typo in the domain must fail SAFE, not fall through to unverified.
        factory = web_utils._build_connections_factory(self.PROXY, "ajaib_testnetX")
        self.assertTrue(factory._verify_ssl)

    def test_testnet_skips_tls_verification(self):
        factory = web_utils._build_connections_factory(self.PROXY, CONSTANTS.TESTNET_DOMAIN)
        self.assertFalse(factory._verify_ssl)

    def test_only_the_testnet_domain_relaxes_tls(self):
        relaxed = [d for d in (CONSTANTS.DEFAULT_DOMAIN, CONSTANTS.TESTNET_DOMAIN, "", "prod")
                   if not web_utils._build_connections_factory(self.PROXY, d)._verify_ssl]
        self.assertEqual([CONSTANTS.TESTNET_DOMAIN], relaxed)

    def test_build_api_factory_propagates_the_domain(self):
        factory = web_utils.build_api_factory(
            domain=CONSTANTS.TESTNET_DOMAIN, proxy_url=self.PROXY)
        self.assertFalse(factory._connections_factory._verify_ssl)

        factory = web_utils.build_api_factory(
            domain=CONSTANTS.DEFAULT_DOMAIN, proxy_url=self.PROXY)
        self.assertTrue(factory._connections_factory._verify_ssl)
