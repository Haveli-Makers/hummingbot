import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.exchange.coinex.coinex_constants as CONSTANTS
from hummingbot.connector.exchange.coinex.coinex_api_user_stream_data_source import CoinexAPIUserStreamDataSource


class CoinexUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-USDT"
        cls.symbol = "BTCUSDT"

    def setUp(self):
        super().setUp()
        self.auth = MagicMock()
        self.auth.ws_auth_payload = MagicMock(return_value={"method": "server.sign", "params": {}, "id": 1})
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=self.symbol)
        self.data_source = CoinexAPIUserStreamDataSource(
            auth=self.auth, trading_pairs=[self.trading_pair], connector=self.connector, api_factory=MagicMock())

    async def test_forwards_order_and_balance_updates(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"method": CONSTANTS.WS_ORDER_UPDATE, "data": {}}, queue)
        await self.data_source._process_event_message({"method": CONSTANTS.WS_BALANCE_UPDATE, "data": {}}, queue)
        self.assertEqual(2, queue.qsize())

    async def test_skips_acks(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"id": 1, "code": 0, "message": "OK"}, queue)
        await self.data_source._process_event_message({"method": "server.sign", "id": 1}, queue)
        self.assertEqual(0, queue.qsize())

    async def test_connect_sends_auth(self):
        ws = AsyncMock()
        factory = MagicMock()
        factory.get_ws_assistant = AsyncMock(return_value=ws)
        self.data_source._api_factory = factory
        result = await self.data_source._connected_websocket_assistant()
        self.assertIs(ws, result)
        ws.connect.assert_awaited_once()
        ws.send.assert_awaited_once()  # the server.sign auth message
        self.auth.ws_auth_payload.assert_called_once()
