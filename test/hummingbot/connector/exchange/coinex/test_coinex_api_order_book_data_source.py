import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.exchange.coinex.coinex_constants as CONSTANTS
from hummingbot.connector.exchange.coinex.coinex_api_order_book_data_source import CoinexAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class CoinexOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-USDT"
        cls.symbol = "BTCUSDT"

    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=self.symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        self.data_source = CoinexAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair], connector=self.connector, api_factory=MagicMock())

    def test_channel_routing(self):
        self.assertEqual(self.data_source._snapshot_messages_queue_key,
                         self.data_source._channel_originating_message({"method": CONSTANTS.WS_DEPTH_UPDATE}))
        self.assertEqual(self.data_source._trade_messages_queue_key,
                         self.data_source._channel_originating_message({"method": CONSTANTS.WS_DEALS_UPDATE}))
        self.assertEqual("", self.data_source._channel_originating_message({"id": 1, "code": 0}))

    async def test_parse_depth_snapshot(self):
        queue = asyncio.Queue()
        raw = {
            "method": CONSTANTS.WS_DEPTH_UPDATE,
            "data": {
                "market": self.symbol, "is_full": True,
                "depth": {
                    "asks": [["62762", "0.9"]], "bids": [["62756", "0.1"]],
                    "last": "62760", "updated_at": 1700000000000,
                },
            },
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])
        self.assertEqual([["62756", "0.1"]], msg.content["bids"])
        self.assertEqual([["62762", "0.9"]], msg.content["asks"])

    async def test_parse_deals_trade(self):
        queue = asyncio.Queue()
        raw = {
            "method": CONSTANTS.WS_DEALS_UPDATE,
            "data": {"market": self.symbol, "deal_list": [
                {"deal_id": 1, "created_at": 1700000000000, "side": "sell", "price": "62750", "amount": "0.02"},
            ]},
        }
        await self.data_source._parse_trade_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual("62750", msg.content["price"])
        self.assertEqual("0.02", msg.content["amount"])

    def test_levels_filters_bad_rows(self):
        levels = self.data_source._levels([["1", "2"], ["bad"], None, ["3", "4", "x"]])
        self.assertEqual([["1", "2"], ["3", "4"]], levels)
