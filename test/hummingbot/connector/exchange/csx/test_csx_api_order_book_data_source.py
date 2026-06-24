import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.csx.csx_api_order_book_data_source import CsxAPIOrderBookDataSource
from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

_VALID_SECRET = "aa" * 32


def _make_connector() -> CsxExchange:
    ex = CsxExchange(
        csx_api_key="key",
        csx_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC/INR": "BTC-INR"}))
    return ex


class CsxOrderBookDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = CsxAPIOrderBookDataSource(
            trading_pairs=["BTC-INR"],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    async def test_request_order_book_snapshot_returns_data(self):
        depth_response = {
            "buy": [["3000000", "0.001"], ["2999000", "0.01"]],
            "sell": [["3001000", "0.002"], ["3002000", "0.005"]],
            "timestamp": "2024-01-01T00:00:00Z",
        }
        with patch.object(self.connector, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = depth_response
            result = await self.source._request_order_book_snapshot("BTC-INR")
        self.assertEqual(depth_response, result)

    async def test_order_book_snapshot_builds_correct_message(self):
        depth_response = {
            "buy": [["3000000", "0.001"]],
            "sell": [["3001000", "0.002"]],
        }
        with patch.object(self.connector, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = depth_response
            msg = await self.source._order_book_snapshot("BTC-INR")
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-INR", msg.content["trading_pair"])
        self.assertEqual([["3000000", "0.001"]], msg.content["bids"])
        self.assertEqual([["3001000", "0.002"]], msg.content["asks"])

    async def test_parse_order_book_snapshot_message(self):
        queue = asyncio.Queue()
        raw = {
            "trading_pair": "BTC-INR",
            "bids": [["3000000", "0.001"]],
            "asks": [["3001000", "0.002"]],
            "timestamp": 1_725_010_288_000,
        }
        await self.source._parse_order_book_snapshot_message(raw, queue)
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)

    async def test_parse_order_book_snapshot_message_missing_pair_ignored(self):
        queue = asyncio.Queue()
        await self.source._parse_order_book_snapshot_message({"bids": [], "asks": []}, queue)
        self.assertTrue(queue.empty())

    async def test_parse_trade_message(self):
        queue = asyncio.Queue()
        raw = {
            "_trading_pair": "BTC-INR",
            "price": "3000000",
            "quantity": "0.001",
            "id": "trade-1",
            "isBuyerMaker": False,
            "timestamp": 1_725_010_288_000,
        }
        await self.source._parse_trade_message(raw, queue)
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)

    async def test_connected_websocket_assistant_raises(self):
        with self.assertRaises(NotImplementedError):
            await self.source._connected_websocket_assistant()

    async def test_get_last_traded_prices(self):
        # Must call the PLURAL connector method; the singular one is not
        # overridden and raises NotImplementedError.
        with patch.object(self.connector, "_get_last_traded_prices", new_callable=AsyncMock) as mock_price:
            mock_price.return_value = {"BTC-INR": 3_000_000.0}
            prices = await self.source.get_last_traded_prices(["BTC-INR"])
        self.assertEqual({"BTC-INR": 3_000_000.0}, prices)
        mock_price.assert_awaited_once_with(trading_pairs=["BTC-INR"])


if __name__ == "__main__":
    unittest.main()
