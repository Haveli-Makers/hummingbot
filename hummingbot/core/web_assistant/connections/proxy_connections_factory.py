from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector

from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection


class ProxyConnectionsFactory:
    """
    A non-singleton connections factory that routes all traffic through a proxy.

    Supports HTTP, HTTPS, SOCKS4, and SOCKS5 proxies via a single URL string:
      - socks5://username:password@proxy-host:1080
      - socks4://proxy-host:1080
      - http://username:password@proxy-host:8080

    Usage:
        factory = ProxyConnectionsFactory(proxy_url="socks5://user:pass@host:1080")
        web_factory = WebAssistantsFactory(..., connections_factory=factory)

    One ProxyConnectionsFactory instance = one dedicated aiohttp.ClientSession that
    routes through the configured proxy. It is intentionally NOT a singleton so each
    connector can have its own independently configured session.
    """

    def __init__(self, proxy_url: str, verify_ssl: bool = True):
        """
        :param verify_ssl: set False ONLY to reach a sandbox/testnet whose TLS
            certificate is invalid. Traffic stays encrypted, but the server's
            identity is no longer checked, so anyone able to intercept the
            connection could read the API key and alter orders in flight. Never
            use it against a production host or with credentials that control
            real funds. Callers are expected to gate this on the environment.
        """
        if not proxy_url:
            raise ValueError("proxy_url must not be empty")
        self._proxy_url = proxy_url
        self._verify_ssl = verify_ssl
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._ws_independent_session: Optional[aiohttp.ClientSession] = None

    def _make_session(self) -> aiohttp.ClientSession:
        kwargs = {"rdns": True}
        if not self._verify_ssl:
            kwargs["ssl"] = False
        connector = ProxyConnector.from_url(self._proxy_url, **kwargs)
        return aiohttp.ClientSession(connector=connector)

    async def _get_shared_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None or self._shared_client.closed:
            self._shared_client = self._make_session()
        return self._shared_client

    async def get_rest_connection(self) -> RESTConnection:
        client = await self._get_shared_client()
        return RESTConnection(aiohttp_client_session=client)

    async def get_ws_connection(self) -> WSConnection:
        client = self._ws_independent_session or await self._get_shared_client()
        return WSConnection(aiohttp_client_session=client)

    async def close(self) -> None:
        if self._shared_client is not None and not self._shared_client.closed:
            await self._shared_client.close()
            self._shared_client = None
        if self._ws_independent_session is not None and not self._ws_independent_session.closed:
            await self._ws_independent_session.close()
            self._ws_independent_session = None

    async def __aenter__(self) -> "ProxyConnectionsFactory":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
