import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_api_order_book_data_source import (
    DeltaOptionsAPIOrderBookDataSource,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class DeltaOptionsAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    symbol = "C-BTC-100000-251226"
    trading_pair = "C_BTC_100000_251226-USD"

    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(side_effect=self._symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=self._pair)
        self.connector.exchange_symbols_for_live_pairs = AsyncMock(return_value=[self.symbol])
        self.connector.size_to_amount = lambda trading_pair, size: size * Decimal("0.001")
        self.rest_assistant = MagicMock()
        self.rest_assistant.execute_request = AsyncMock()
        self.api_factory = MagicMock()
        self.api_factory.get_rest_assistant = AsyncMock(return_value=self.rest_assistant)
        self.data_source = DeltaOptionsAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair], connector=self.connector, api_factory=self.api_factory)

    def _symbol(self, trading_pair):
        if not trading_pair.startswith(("C_", "P_")):
            raise KeyError(trading_pair)
        base = trading_pair.split("-")[0]
        return base.replace("_", "-")

    def _pair(self, symbol):
        if symbol not in (self.symbol, "P-BTC-90000-251226"):
            raise KeyError(symbol)
        return f"{symbol.replace('-', '_')}-USD"

    def test_channel_routing(self):
        routing = {
            "ob_l2": self.data_source._snapshot_messages_queue_key,
            "trades": self.data_source._trade_messages_queue_key,
            "ticker": self.data_source._option_info_messages_queue_key,
            "subscriptions": "",
            "heartbeat": "",
        }
        for message_type, queue_key in routing.items():
            self.assertEqual(queue_key, self.data_source._channel_originating_message({"type": message_type}))

    async def test_subscribe_uses_only_live_symbols(self):
        ws = AsyncMock()

        await self.data_source._subscribe_channels(ws)

        payload = ws.send.call_args.args[0].payload
        self.assertEqual("subscribe", payload["type"])
        self.assertEqual(
            [{"name": name, "symbols": [self.symbol]} for name in ("ob_l2", "trades", "ticker")],
            payload["payload"]["channels"],
        )
        self.connector.exchange_symbols_for_live_pairs.assert_awaited_once_with([self.trading_pair])

    async def test_subscribe_sends_nothing_when_every_option_expired(self):
        self.connector.exchange_symbols_for_live_pairs.return_value = []
        ws = AsyncMock()

        await self.data_source._subscribe_channels(ws)

        ws.send.assert_not_awaited()

    async def test_rejected_subscription_is_logged(self):
        self.data_source.logger().setLevel(1)
        with self.assertLogs(self.data_source.logger(), level="ERROR") as logs:
            await self.data_source._process_message_for_unknown_channel(
                {"type": "subscriptions", "channels": [{"name": "ob_l2", "error": "invalid symbol"}]}, AsyncMock())

        self.assertIn("invalid symbol", logs.output[0])

    async def test_order_book_snapshot_converts_contracts(self):
        queue = asyncio.Queue()
        await self.data_source._parse_order_book_snapshot_message({
            "type": "ob_l2", "sy": self.symbol, "lts": 1766664000123456, "ts": 1766664000623456,
            "a": [["510.5", "20"], ["512", "5"]], "b": [["505", "10"]],
        }, queue)

        message = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(self.trading_pair, message.content["trading_pair"])
        self.assertEqual(1766664000123456, message.update_id)
        self.assertEqual([["505", "0.010"]], message.content["bids"])
        self.assertEqual([["510.5", "0.020"], ["512", "0.005"]], message.content["asks"])
        self.assertAlmostEqual(1766664000.123456, message.timestamp)

    async def test_messages_for_unknown_symbols_are_ignored(self):
        queue = asyncio.Queue()

        await self.data_source._parse_order_book_snapshot_message({"sy": "C-BTC-1-010101", "lts": 1}, queue)
        await self.data_source._parse_trade_message({"sy": "C-BTC-1-010101", "t": 1}, queue)

        self.assertTrue(queue.empty())

    async def test_trade_side_comes_from_the_buyer_role(self):
        queue = asyncio.Queue()
        base = {"type": "trades", "sy": self.symbol, "p": "507", "s": 3, "t": 1766664000000001}

        await self.data_source._parse_trade_message({**base, "r": "m"}, queue)
        await self.data_source._parse_trade_message({**base, "r": "t", "t": 1766664000000002}, queue)

        maker_buyer, taker_buyer = queue.get_nowait(), queue.get_nowait()
        self.assertEqual(float(TradeType.SELL.value), maker_buyer.content["trade_type"])
        self.assertEqual(float(TradeType.BUY.value), taker_buyer.content["trade_type"])
        self.assertEqual("0.003", maker_buyer.content["amount"])
        self.assertEqual("507", maker_buyer.content["price"])

    async def test_ticker_message_becomes_option_info_updates(self):
        queue = asyncio.Queue()
        await self.data_source._parse_option_info_message({
            "type": "ticker", "sy": "BTC-251226", "sp": "98000.5", "ts": 1766664000000000,
            "d": [
                {"s": self.symbol, "m": "512.3", "g": ["0.42", "0.00002", "0.5", "-80.1", "35.2"],
                 "qiv": ["0.58", "0.55", "0.56"], "oi": ["1500", "1000"], "pb": ["300", "800"]},
                {"s": "C-BTC-1-010101", "m": "1"},
            ],
        }, queue)

        update = queue.get_nowait()
        self.assertTrue(queue.empty())
        self.assertEqual(self.trading_pair, update.trading_pair)
        self.assertEqual(Decimal("512.3"), update.mark_price)
        self.assertEqual(Decimal("98000.5"), update.underlying_price)
        self.assertEqual((Decimal("0.42"), Decimal("0.00002"), Decimal("0.5"), Decimal("-80.1"), Decimal("35.2")),
                         (update.delta, update.gamma, update.rho, update.theta, update.vega))
        self.assertEqual((Decimal("0.58"), Decimal("0.55"), Decimal("0.56")), (update.ask_iv, update.bid_iv, update.mark_iv))
        self.assertEqual(Decimal("1.5"), update.open_interest)
        self.assertEqual((Decimal("300"), Decimal("800")), (update.price_band_lower, update.price_band_upper))
        self.assertEqual(1766664000.0, update.timestamp)

    async def test_option_info_listener_reads_ticker_messages(self):
        output = asyncio.Queue()
        self.data_source._message_queue["option_info"].put_nowait(
            {"type": "ticker", "sp": "98000", "d": [{"s": self.symbol, "m": "512"}]})

        listener = asyncio.ensure_future(self.data_source.listen_for_option_info(output))
        update = await asyncio.wait_for(output.get(), timeout=1)
        listener.cancel()

        self.assertEqual(self.trading_pair, update.trading_pair)
        self.assertEqual(Decimal("98000"), update.underlying_price)

    async def test_ticker_with_missing_greeks_leaves_them_unset(self):
        queue = asyncio.Queue()

        await self.data_source._parse_option_info_message({"type": "ticker", "d": [{"s": self.symbol, "m": "512"}]}, queue)

        update = queue.get_nowait()
        self.assertEqual(Decimal("512"), update.mark_price)
        self.assertIsNone(update.delta)
        self.assertIsNone(update.underlying_price)

    async def test_rest_snapshot(self):
        self.rest_assistant.execute_request.return_value = {"success": True, "result": {
            "buy": [{"price": "505", "size": 10, "depth": "10"}], "sell": [{"price": "510", "size": 4, "depth": "4"}],
            "last_updated_at": 1766664000000000, "symbol": self.symbol}}

        message = await self.data_source._order_book_snapshot(self.trading_pair)

        self.assertEqual([["505", "0.010"]], message.content["bids"])
        self.assertEqual([["510", "0.004"]], message.content["asks"])
        self.assertEqual(1766664000000000, message.update_id)
        url = self.rest_assistant.execute_request.call_args.kwargs["url"]
        self.assertTrue(url.endswith(f"/v2/l2orderbook/{self.symbol}"))

    async def test_get_option_info_from_rest_ticker(self):
        self.rest_assistant.execute_request.return_value = {"success": True, "result": {
            "symbol": self.symbol, "mark_price": "512.3", "spot_price": "98000", "oi": "2000",
            "greeks": {"delta": "0.42", "gamma": "0.00002", "rho": "0.5", "theta": "-80.1", "vega": "35.2"},
            "quotes": {"bid_iv": "0.55", "ask_iv": "0.58", "mark_iv": "0.56"},
            "price_band": {"lower_limit": "300", "upper_limit": "800"}, "timestamp": 1766664000000000}}

        info = await self.data_source.get_option_info(self.trading_pair)

        self.assertEqual(Decimal("512.3"), info.mark_price)
        self.assertEqual(Decimal("98000"), info.underlying_price)
        self.assertEqual(Decimal("0.42"), info.delta)
        self.assertEqual(Decimal("0.56"), info.mark_iv)
        self.assertEqual(Decimal("2"), info.open_interest)
        self.assertEqual(Decimal("800"), info.price_band_upper)
        self.assertEqual(1766664000.0, info.timestamp)

    async def test_tickers_are_requested_ten_symbols_at_a_time(self):
        pairs = [f"C_BTC_{strike}_251226-USD" for strike in range(100, 112)]
        self.rest_assistant.execute_request.side_effect = [
            {"success": True, "result": [{"symbol": "C-BTC-100-251226", "close": None, "mark_price": "12"}]},
            {"success": True, "result": {"symbol": "C-BTC-111-251226", "close": "7.5", "mark_price": "8"}},
        ]

        prices = await self.data_source.get_last_traded_prices(pairs)

        urls = [call.kwargs["url"] for call in self.rest_assistant.execute_request.call_args_list]
        self.assertEqual(2, len(urls))
        self.assertEqual(10, urls[0].rsplit("/", 1)[1].count("C-BTC"))
        self.assertTrue(urls[1].endswith("/v2/tickers/C-BTC-110-251226,C-BTC-111-251226"))
        self.assertEqual({"C_BTC_100_251226-USD": 12.0, "C_BTC_111_251226-USD": 7.5}, prices)

    async def test_websocket_connects_to_the_public_endpoint(self):
        ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=ws)

        await self.data_source._connected_websocket_assistant()

        self.assertEqual(CONSTANTS.WSS_PUBLIC_URL, ws.connect.call_args.kwargs["ws_url"])
