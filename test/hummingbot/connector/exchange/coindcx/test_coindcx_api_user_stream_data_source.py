import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth


class CoinDCXAPIUserStreamDataSourceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.auth = MagicMock(spec=CoinDCXAuth)
        self.auth.generate_ws_auth_payload.return_value = {"channelName": "coindcx", "authSignature": "sig", "apiKey": "key"}
        self.trading_pairs = ["BTC-USDT"]
        self.connector = MagicMock()
        self.api_factory = MagicMock()
        self.ws_assistant = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=self.ws_assistant)
        self.data_source = CoinDCXAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=self.trading_pairs,
            connector=self.connector,
            api_factory=self.api_factory
        )

    async def test_get_ws_assistant(self):
        ws = await self.data_source._get_ws_assistant()
        self.api_factory.get_ws_assistant.assert_awaited_once()
        self.assertEqual(ws, self.ws_assistant)

    async def test_connected_websocket_assistant(self):
        self.ws_assistant.connect = AsyncMock()
        self.ws_assistant.send = AsyncMock()
        self.data_source.logger = MagicMock()
        ws = await self.data_source._connected_websocket_assistant()
        self.ws_assistant.connect.assert_awaited_once()
        self.ws_assistant.send.assert_awaited_once()
        self.data_source.logger().info.assert_called_with("Authenticated and joined CoinDCX private channel")
        self.assertEqual(ws, self.ws_assistant)

    async def test_subscribe_channels(self):
        self.data_source.logger = MagicMock()
        await self.data_source._subscribe_channels(self.ws_assistant)
        self.data_source.logger().info.assert_called_with("Subscribed to CoinDCX user stream channels (balance, order, trade updates)")

    async def test_process_websocket_messages_puts_expected_events(self):
        queue = asyncio.Queue()
        ws_response = MagicMock()
        ws_response.data = {"event": "balance-update", "foo": "bar"}
        self.ws_assistant.iter_messages = AsyncMock(return_value=iter([ws_response]))
        put_nowait = AsyncMock()
        queue.put_nowait = put_nowait
        # Patch async for

        async def fake_iter():
            yield ws_response
        self.ws_assistant.iter_messages = fake_iter
        await self.data_source._process_websocket_messages(self.ws_assistant, queue)
        put_nowait.assert_called_with({"event": "balance-update", "foo": "bar"})

    async def test_on_user_stream_interruption_resets_ws(self):
        self.data_source._ws_assistant = self.ws_assistant
        await self.data_source._on_user_stream_interruption(self.ws_assistant)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_listen_for_user_stream_handles_exception(self):
        # Simulate _connected_websocket_assistant raising an exception
        self.data_source._connected_websocket_assistant = AsyncMock(side_effect=Exception("fail"))
        self.data_source._subscribe_channels = AsyncMock()
        self.data_source._process_websocket_messages = AsyncMock()
        self.data_source._on_user_stream_interruption = AsyncMock()
        self.data_source._sleep = AsyncMock(side_effect=Exception("stop"))
        self.data_source.logger = MagicMock()
        with self.assertRaises(Exception):
            await self.data_source.listen_for_user_stream(asyncio.Queue())
        self.data_source.logger().error.assert_called()
        self.data_source._on_user_stream_interruption.assert_awaited()


if __name__ == "__main__":
    unittest.main()
