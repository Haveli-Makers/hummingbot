import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.exchange.valr.valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_api_user_stream_data_source import ValrAPIUserStreamDataSource


class ValrUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-ZAR"

    def setUp(self):
        super().setUp()
        self.auth = MagicMock()
        self.auth.ws_auth_headers = MagicMock(return_value={"X-VALR-API-KEY": "k", "X-VALR-SIGNATURE": "sig",
                                                            "X-VALR-TIMESTAMP": "1"})
        self.connector = MagicMock()
        self.data_source = ValrAPIUserStreamDataSource(
            auth=self.auth, trading_pairs=[self.trading_pair], connector=self.connector, api_factory=MagicMock())

    async def test_forwards_account_events(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"type": CONSTANTS.WS_ORDER_STATUS_UPDATE, "data": {}}, queue)
        await self.data_source._process_event_message({"type": CONSTANTS.WS_BALANCE_UPDATE, "data": {}}, queue)
        await self.data_source._process_event_message({"type": CONSTANTS.WS_NEW_ACCOUNT_TRADE, "data": {}}, queue)
        self.assertEqual(3, queue.qsize())

    async def test_skips_non_account_events(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"type": CONSTANTS.WS_AUTHENTICATED}, queue)
        await self.data_source._process_event_message({"type": "PONG"}, queue)
        self.assertEqual(0, queue.qsize())

    async def test_connect_uses_auth_headers(self):
        ws = AsyncMock()
        factory = MagicMock()
        factory.get_ws_assistant = AsyncMock(return_value=ws)
        self.data_source._api_factory = factory
        result = await self.data_source._connected_websocket_assistant()
        self.assertIs(ws, result)
        ws.connect.assert_awaited_once()
        # auth headers were generated for the /ws/account path and passed to connect
        self.auth.ws_auth_headers.assert_called_once_with("/ws/account")
        _, kwargs = ws.connect.call_args
        self.assertEqual({"X-VALR-API-KEY": "k", "X-VALR-SIGNATURE": "sig", "X-VALR-TIMESTAMP": "1"},
                         kwargs.get("ws_headers"))
