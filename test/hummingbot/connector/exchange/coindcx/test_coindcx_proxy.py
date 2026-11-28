"""
Proxy routing for the CoinDCX spot connector.

REST goes through the web assistants factory, as on other connectors. The two streams (order book and user stream)
use python-socketio, which opens its own connections, so each Socket.IO client gets its own aiohttp session over the
proxy instead. These tests check the wiring and the life of those sessions; nothing here opens a connection.
"""
import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aiohttp_socks import ProxyConnector

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory

PROXY_URL = "http://user:pass@proxy.local:3128"
ORDER_BOOK_CLIENT = "hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source.socketio.AsyncClient"
USER_STREAM_CLIENT = "hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.socketio.AsyncClient"


def fake_client(connect_error: Optional[BaseException] = None) -> MagicMock:
    """
    A socketio.AsyncClient stand-in. Its connect raises connect_error if one is given; otherwise the connection
    ends with CancelledError, as when the connector stops.
    """
    client = MagicMock()
    client.event = MagicMock(side_effect=lambda func: func)
    client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
    client.connect = AsyncMock(side_effect=connect_error)
    client.emit = AsyncMock()
    client.wait = AsyncMock(side_effect=asyncio.CancelledError)
    client.disconnect = AsyncMock()
    client.connected = True
    return client


class ClientRecorder:
    """
    Stands in for socketio.AsyncClient and notes the session each client was given. It also notes the session's
    connector at that moment, because aiohttp drops the connector when the session closes.
    """

    def __init__(self, *clients: MagicMock):
        self._clients = list(clients)
        self.sessions: List[Optional[aiohttp.ClientSession]] = []
        self.connectors: List[Optional[aiohttp.BaseConnector]] = []

    def __call__(self, **kwargs) -> MagicMock:
        session = kwargs["http_session"]
        self.sessions.append(session)
        self.connectors.append(None if session is None else session.connector)
        return self._clients.pop(0)


async def no_wait(delay: float):
    await asyncio.sleep(0)


class CoindcxExchangeProxyTests(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def exchange(proxy_url: str = "") -> CoindcxExchange:
        return CoindcxExchange(
            coindcx_api_key="key",
            coindcx_api_secret="secret",
            trading_pairs=["ETH-INR"],
            trading_required=False,
            coindcx_proxy_url=proxy_url,
        )

    def test_with_proxy_rest_and_both_streams_use_it(self):
        exchange = self.exchange(PROXY_URL)

        self.assertIsInstance(exchange._web_assistants_factory._connections_factory, ProxyConnectionsFactory)
        self.assertEqual(PROXY_URL, exchange._orderbook_ds._proxy_url)
        self.assertEqual(PROXY_URL, exchange._user_stream_tracker.data_source._proxy_url)

    def test_without_proxy_everything_connects_directly(self):
        exchange = self.exchange()

        self.assertIsInstance(exchange._web_assistants_factory._connections_factory, ConnectionsFactory)
        self.assertIsNone(exchange._orderbook_ds._proxy_url)
        self.assertIsNone(exchange._user_stream_tracker.data_source._proxy_url)

    def test_connectors_built_without_the_argument_connect_directly(self):
        # The rate and volume oracles build keyless connectors without a proxy argument.
        exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="", trading_pairs=[], trading_required=False)

        self.assertIsInstance(exchange._web_assistants_factory._connections_factory, ConnectionsFactory)

    async def test_stop_network_closes_the_proxy_session(self):
        exchange = self.exchange(PROXY_URL)

        with patch.object(exchange._web_assistants_factory, "close", new_callable=AsyncMock) as close:
            await exchange.stop_network()

        close.assert_awaited_once()

    async def test_stop_network_leaves_the_shared_session_alone_without_proxy(self):
        exchange = self.exchange()

        with patch.object(exchange._web_assistants_factory, "close", new_callable=AsyncMock) as close:
            await exchange.stop_network()

        close.assert_not_awaited()


class CoinDCXStreamProxyTests(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def order_book_source(proxy_url: Optional[str] = None) -> CoinDCXAPIOrderBookDataSource:
        connector = MagicMock()
        connector.exchange_pair_code = AsyncMock(return_value="I-ETH_INR")  # names the channels it joins
        return CoinDCXAPIOrderBookDataSource(
            trading_pairs=["ETH-INR"], connector=connector, api_factory=MagicMock(), proxy_url=proxy_url)

    @staticmethod
    def user_stream_source(proxy_url: Optional[str] = None) -> CoinDCXAPIUserStreamDataSource:
        auth = MagicMock()
        auth.generate_ws_auth_payload.return_value = {"channelName": "coindcx"}
        source = CoinDCXAPIUserStreamDataSource(
            auth=auth, trading_pairs=["ETH-INR"], connector=MagicMock(), api_factory=MagicMock(), proxy_url=proxy_url)
        source._sleep = no_wait
        return source

    def assert_closed_proxy_sessions(self, recorder: ClientRecorder, count: int):
        self.assertEqual(count, len(recorder.sessions))
        self.assertEqual(count, len({id(session) for session in recorder.sessions}), "a session was reused")
        for session, connector in zip(recorder.sessions, recorder.connectors):
            self.assertIsInstance(session, aiohttp.ClientSession)
            self.assertIsInstance(connector, ProxyConnector)
            self.assertTrue(session.closed)

    async def test_order_book_stream_connects_through_the_proxy(self):
        source = self.order_book_source(PROXY_URL)
        recorder = ClientRecorder(fake_client())

        with patch(ORDER_BOOK_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_subscriptions()

        self.assert_closed_proxy_sessions(recorder, count=1)
        self.assertIsNone(source._http_session)

    async def test_order_book_stream_connects_directly_without_proxy(self):
        source = self.order_book_source()
        recorder = ClientRecorder(fake_client())

        with patch(ORDER_BOOK_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_subscriptions()

        self.assertEqual([None], recorder.sessions)

    async def test_order_book_stream_closes_the_proxy_session_when_the_client_fails_to_disconnect(self):
        source = self.order_book_source(PROXY_URL)
        client = fake_client()
        client.disconnect = AsyncMock(side_effect=RuntimeError("already gone"))
        recorder = ClientRecorder(client)

        with patch(ORDER_BOOK_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_subscriptions()

        self.assert_closed_proxy_sessions(recorder, count=1)
        self.assertIsNone(source._http_session)
        self.assertIsNone(source._client)

    async def test_user_stream_connects_through_the_proxy(self):
        source = self.user_stream_source(PROXY_URL)
        recorder = ClientRecorder(fake_client())

        with patch(USER_STREAM_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_user_stream(asyncio.Queue())

        self.assert_closed_proxy_sessions(recorder, count=1)
        self.assertIsNone(source._http_session)

    async def test_user_stream_connects_directly_without_proxy(self):
        source = self.user_stream_source()
        recorder = ClientRecorder(fake_client())

        with patch(USER_STREAM_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_user_stream(asyncio.Queue())

        self.assertEqual([None], recorder.sessions)

    async def test_user_stream_reconnects_with_a_new_proxy_session(self):
        # engine.io silently swaps a closed session for a direct one, so a reconnect must never reuse the old one.
        source = self.user_stream_source(PROXY_URL)
        recorder = ClientRecorder(fake_client(ConnectionError("proxy refused")),
                                  fake_client(ConnectionError("proxy reset")),
                                  fake_client())

        with patch(USER_STREAM_CLIENT, new=recorder):
            with self.assertRaises(asyncio.CancelledError):
                await source.listen_for_user_stream(asyncio.Queue())

        self.assert_closed_proxy_sessions(recorder, count=3)

    async def test_user_stream_stop_closes_the_proxy_session(self):
        source = self.user_stream_source(PROXY_URL)
        recorder = ClientRecorder(fake_client())

        with patch(USER_STREAM_CLIENT, new=recorder):
            source._client = source._build_client(asyncio.Queue())
        await source.stop()

        self.assert_closed_proxy_sessions(recorder, count=1)
        self.assertIsNone(source._http_session)
