import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_api_order_book_data_source import (
    DeltaPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class DeltaPerpetualOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = "BTC-USD"
        cls.symbol = "BTCUSD"

    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(side_effect=self._symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=self._pair)
        # 1 contract == 0.001 base.
        self.connector._format_size_to_amount = lambda tp, size: Decimal(str(size)) * Decimal("0.001")
        self.data_source = DeltaPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    def _symbol(self, trading_pair):
        if trading_pair != self.trading_pair:
            raise KeyError(trading_pair)
        return self.symbol

    def _pair(self, symbol):
        if symbol != self.symbol:
            raise KeyError(symbol)
        return self.trading_pair

    def test_channel_routing(self):
        routing = {
            CONSTANTS.WS_ORDERBOOK_CHANNEL: self.data_source._snapshot_messages_queue_key,
            CONSTANTS.WS_TRADES_CHANNEL: self.data_source._trade_messages_queue_key,
            CONSTANTS.WS_TICKER_CHANNEL: self.data_source._funding_info_messages_queue_key,
            CONSTANTS.WS_FUNDING_CHANNEL: self.data_source._funding_info_messages_queue_key,
            "subscriptions": "",
        }
        for message_type, queue_key in routing.items():
            self.assertEqual(queue_key, self.data_source._channel_originating_message({"type": message_type}))

    async def test_connects_to_the_public_endpoint(self):
        ws = AsyncMock()
        self.data_source._api_factory.get_ws_assistant = AsyncMock(return_value=ws)

        await self.data_source._connected_websocket_assistant()

        self.assertEqual("wss://public-socket.india.delta.exchange", ws.connect.call_args.kwargs["ws_url"])

    async def test_subscribes_to_the_new_channels_and_skips_unlisted_pairs(self):
        self.data_source._trading_pairs = [self.trading_pair, "DOGE-USD"]
        ws = AsyncMock()

        await self.data_source._subscribe_channels(ws)

        channels = ws.send.call_args.args[0].payload["payload"]["channels"]
        self.assertEqual(
            [{"name": name, "symbols": [self.symbol]} for name in ("ob_l2", "trades", "ticker", "funding_rate")],
            channels,
        )

    async def test_rejected_subscription_is_logged(self):
        with self.assertLogs(self.data_source.logger(), level="ERROR") as logs:
            await self.data_source._process_message_for_unknown_channel(
                {"type": "subscriptions", "channels": [{"name": "ob_l2", "error": "invalid symbol"}]}, AsyncMock())

        self.assertIn("invalid symbol", logs.output[0])

    def test_levels_converts_contracts_to_base(self):
        self.assertEqual([["50000", "0.010"]], self.data_source._levels(self.trading_pair, [["50000", "10"]]))
        self.assertEqual([["50000", "0.010"]], self.data_source._levels(self.trading_pair, [{"price": "50000", "size": 10}]))

    async def test_parse_order_book_snapshot_message(self):
        queue = asyncio.Queue()
        await self.data_source._parse_order_book_snapshot_message({
            "type": "ob_l2", "sy": self.symbol, "lts": 1700000000123456, "ts": 1700000000623456,
            "b": [["49900", "5"]], "a": [["50100", "3"]],
        }, queue)

        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])
        self.assertEqual(1700000000123456, msg.content["update_id"])
        self.assertEqual([["49900", "0.005"]], msg.content["bids"])
        self.assertEqual([["50100", "0.003"]], msg.content["asks"])

    async def test_parse_trade_message_side_comes_from_the_buyer_role(self):
        queue = asyncio.Queue()
        trade = {"type": "trades", "sy": self.symbol, "p": "50000", "s": 4, "t": 1700000000000001}

        await self.data_source._parse_trade_message({**trade, "r": "m"}, queue)
        await self.data_source._parse_trade_message({**trade, "r": "t"}, queue)

        maker_buyer, taker_buyer = queue.get_nowait(), queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, maker_buyer.type)
        self.assertEqual(float(TradeType.SELL.value), maker_buyer.content["trade_type"])
        self.assertEqual(float(TradeType.BUY.value), taker_buyer.content["trade_type"])
        self.assertEqual("50000", maker_buyer.content["price"])
        self.assertEqual("0.004", maker_buyer.content["amount"])

    async def test_messages_for_unknown_symbols_are_ignored(self):
        queue = asyncio.Queue()

        await self.data_source._parse_order_book_snapshot_message({"sy": "DOGEUSD", "lts": 1}, queue)
        await self.data_source._parse_trade_message({"sy": "DOGEUSD", "t": 1}, queue)

        self.assertTrue(queue.empty())

    async def test_get_funding_info_converts_the_percent_rate(self):
        self.data_source._request_complete_funding_info = AsyncMock(return_value={
            "mark_price": "50010", "spot_price": "50000", "funding_rate": "0.01",
            "next_funding_realization": 1700000000000000,
        })
        info = await self.data_source.get_funding_info(self.trading_pair)
        self.assertEqual(Decimal("50010"), info.mark_price)
        self.assertEqual(Decimal("50000"), info.index_price)
        self.assertEqual(Decimal("0.0001"), info.rate)

    async def test_funding_rate_message(self):
        queue = asyncio.Queue()
        await self.data_source._parse_funding_info_message({
            "type": "funding_rate", "sy": self.symbol, "fr": 0.010000000000000002, "nfr": 1700028800000000,
            "fi": 28800, "ts": 1700000000000000,
        }, queue)

        update = queue.get_nowait()
        self.assertEqual(self.trading_pair, update.trading_pair)
        self.assertAlmostEqual(Decimal("0.0001"), update.rate)
        self.assertEqual(1700028800, update.next_funding_utc_timestamp)

    async def test_ticker_message_updates_mark_and_index_price(self):
        queue = asyncio.Queue()
        await self.data_source._parse_funding_info_message({
            "type": "ticker", "sy": self.symbol, "sp": "50000.5",
            "d": [{"s": self.symbol, "m": "50010.2"}, {"s": "DOGEUSD", "m": "0.1"}],
        }, queue)

        update = queue.get_nowait()
        self.assertTrue(queue.empty())
        self.assertEqual(Decimal("50010.2"), update.mark_price)
        self.assertEqual(Decimal("50000.5"), update.index_price)
        self.assertIsNone(update.rate)

    async def test_rest_snapshot_and_ticker_requests(self):
        rest = MagicMock()
        rest.execute_request = AsyncMock(side_effect=[
            {"success": True, "result": {"buy": [{"price": "49900", "size": 5}], "sell": [{"price": "50100", "size": 3}],
                                         "last_updated_at": 1700000000000000}},
            {"success": True, "result": {"mark_price": "50010", "spot_price": "50000", "funding_rate": "0.02"}},
        ])
        self.data_source._api_factory.get_rest_assistant = AsyncMock(return_value=rest)

        snapshot = await self.data_source._order_book_snapshot(self.trading_pair)
        info = await self.data_source.get_funding_info(self.trading_pair)

        self.assertEqual([["49900", "0.005"]], snapshot.content["bids"])
        self.assertEqual(1700000000000000, snapshot.update_id)
        self.assertEqual(Decimal("0.0002"), info.rate)
        urls = [call.kwargs["url"] for call in rest.execute_request.call_args_list]
        self.assertEqual(["https://api.india.delta.exchange/v2/l2orderbook/BTCUSD",
                          "https://api.india.delta.exchange/v2/tickers/BTCUSD"], urls)

    async def test_last_traded_prices_skip_pairs_without_a_price(self):
        self.connector._get_last_traded_price = AsyncMock(side_effect=[50000.0, 0.0])

        prices = await self.data_source.get_last_traded_prices([self.trading_pair, "ETH-USD"])

        self.assertEqual({self.trading_pair: 50000.0}, prices)

    def test_parse_ts_normalization(self):
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000000000))
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000000))
        self.assertAlmostEqual(1700000000.0, self.data_source._parse_ts(1700000000))
        self.assertEqual(0.0, self.data_source._parse_ts(None))
