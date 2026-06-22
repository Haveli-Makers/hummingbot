import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.exchange.valr.valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_api_order_book_data_source import ValrAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class ValrOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-ZAR"
        cls.symbol = "BTCZAR"

    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=self.symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        self.data_source = ValrAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair], connector=self.connector, api_factory=MagicMock())

    def test_channel_routing(self):
        self.assertEqual(self.data_source._snapshot_messages_queue_key,
                         self.data_source._channel_originating_message({"type": CONSTANTS.WS_AGGREGATED_ORDERBOOK_UPDATE}))
        self.assertEqual(self.data_source._trade_messages_queue_key,
                         self.data_source._channel_originating_message({"type": CONSTANTS.WS_NEW_TRADE}))
        self.assertEqual("", self.data_source._channel_originating_message({"type": "SUBSCRIBED"}))

    def test_levels_from_objects(self):
        levels = self.data_source._levels([{"price": "100", "quantity": "1.5"}, {"side": "x"}])
        self.assertEqual([["100", "1.5"]], levels)

    async def test_parse_orderbook_snapshot(self):
        queue = asyncio.Queue()
        raw = {
            "type": CONSTANTS.WS_AGGREGATED_ORDERBOOK_UPDATE,
            "currencyPairSymbol": self.symbol,
            "data": {
                "Asks": [{"side": "sell", "quantity": "0.1", "price": "1060000"}],
                "Bids": [{"side": "buy", "quantity": "0.2", "price": "1059000"}],
                "SequenceNumber": 42,
            },
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])
        self.assertEqual(42, msg.content["update_id"])
        self.assertEqual([["1059000", "0.2"]], msg.content["bids"])
        self.assertEqual([["1060000", "0.1"]], msg.content["asks"])

    async def test_parse_trade_sell(self):
        queue = asyncio.Queue()
        raw = {
            "type": CONSTANTS.WS_NEW_TRADE,
            "currencyPairSymbol": self.symbol,
            "data": {"price": "1059500", "quantity": "0.05", "takerSide": "sell", "id": "t1"},
        }
        await self.data_source._parse_trade_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual("1059500", msg.content["price"])
        self.assertEqual("0.05", msg.content["amount"])
