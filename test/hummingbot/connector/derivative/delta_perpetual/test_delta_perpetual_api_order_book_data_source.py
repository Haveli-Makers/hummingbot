import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_api_order_book_data_source import (
    DeltaPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class DeltaPerpetualOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-USDT"
        cls.symbol = "BTCUSD"

    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=self.symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=self.trading_pair)
        # 1 contract == 0.001 base.
        self.connector._format_size_to_amount = lambda tp, size: Decimal(str(size)) * Decimal("0.001")
        self.data_source = DeltaPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    def test_channel_routing(self):
        self.assertEqual(
            self.data_source._snapshot_messages_queue_key,
            self.data_source._channel_originating_message({"type": CONSTANTS.WS_ORDERBOOK_CHANNEL}),
        )
        self.assertEqual(
            self.data_source._trade_messages_queue_key,
            self.data_source._channel_originating_message({"type": CONSTANTS.WS_TRADES_CHANNEL}),
        )
        self.assertEqual(
            self.data_source._funding_info_messages_queue_key,
            self.data_source._channel_originating_message({"type": CONSTANTS.WS_FUNDING_CHANNEL}),
        )
        self.assertEqual("", self.data_source._channel_originating_message({"type": "subscriptions"}))

    def test_levels_converts_contracts_to_base(self):
        levels = self.data_source._levels(self.trading_pair, [{"limit_price": "50000", "size": 10}])
        self.assertEqual([["50000", "0.010"]], levels)

    async def test_parse_order_book_snapshot_message(self):
        queue = asyncio.Queue()
        raw = {
            "type": CONSTANTS.WS_ORDERBOOK_CHANNEL,
            "symbol": self.symbol,
            "buy": [{"limit_price": "49900", "size": 5}],
            "sell": [{"limit_price": "50100", "size": 3}],
            "timestamp": 1700000000000000,
            "last_sequence_no": 42,
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])
        self.assertEqual(42, msg.content["update_id"])
        self.assertEqual([["49900", "0.005"]], msg.content["bids"])
        self.assertEqual([["50100", "0.003"]], msg.content["asks"])

    async def test_parse_trade_message_sell(self):
        queue = asyncio.Queue()
        raw = {
            "type": CONSTANTS.WS_TRADES_CHANNEL,
            "symbol": self.symbol,
            "price": "50000",
            "size": 4,
            "side": "sell",
            "timestamp": 1700000000000000,
            "trade_id": "t1",
        }
        await self.data_source._parse_trade_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual("50000", msg.content["price"])
        self.assertEqual("0.004", msg.content["amount"])

    async def test_get_funding_info(self):
        self.data_source._request_complete_funding_info = AsyncMock(return_value={
            "mark_price": "50010", "spot_price": "50000", "funding_rate": "0.0001",
            "next_funding_realization": 1700000000000000,
        })
        info = await self.data_source.get_funding_info(self.trading_pair)
        self.assertEqual(Decimal("50010"), info.mark_price)
        self.assertEqual(Decimal("50000"), info.index_price)
        self.assertEqual(Decimal("0.0001"), info.rate)

    async def test_parse_funding_info_message(self):
        queue = asyncio.Queue()
        raw = {
            "type": CONSTANTS.WS_FUNDING_CHANNEL, "symbol": self.symbol,
            "mark_price": "50010", "spot_price": "50000", "funding_rate": "0.0002",
        }
        await self.data_source._parse_funding_info_message(raw, queue)
        update = queue.get_nowait()
        self.assertEqual(self.trading_pair, update.trading_pair)
        self.assertEqual(Decimal("50010"), update.mark_price)
        self.assertEqual(Decimal("0.0002"), update.rate)

    def test_parse_ts_normalization(self):
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000000000))
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000000))
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000))
        self.assertEqual(0.0, self.data_source._parse_ts(None))
