from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory


class FakeSession:
    def __init__(self, on_close=None):
        self.closed = False
        self._on_close = on_close

    async def close(self):
        if self._on_close is not None:
            await self._on_close()
        self.closed = True


class ProxyConnectionsFactoryTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.factory = ProxyConnectionsFactory(proxy_url="http://user:pass@proxy.example:3128")

    def test_empty_proxy_url_is_rejected(self):
        with self.assertRaises(ValueError):
            ProxyConnectionsFactory(proxy_url="")

    async def test_rest_and_websocket_share_one_proxied_session(self):
        rest = await self.factory.get_rest_connection()
        ws = await self.factory.get_ws_connection()

        self.assertIs(rest._client_session, ws._client_session)
        await self.factory.close()
        self.assertTrue(rest._client_session.closed)

    async def test_a_new_session_is_made_after_close(self):
        first = await self.factory._get_shared_client()
        await self.factory.close()

        second = await self.factory._get_shared_client()

        self.assertIsNot(first, second)
        self.assertFalse(second.closed)
        await self.factory.close()

    async def test_session_requested_while_closing_is_kept_not_leaked(self):
        replacement = FakeSession()
        first = FakeSession(on_close=self.factory._get_shared_client)  # another task asks for a session mid-close
        self.factory._make_session = MagicMock(side_effect=[first, replacement])
        await self.factory._get_shared_client()

        await self.factory.close()

        self.assertTrue(first.closed)
        self.assertIs(replacement, self.factory._shared_client)
        await self.factory.close()
        self.assertTrue(replacement.closed)
