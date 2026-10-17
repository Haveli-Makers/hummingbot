import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.coinswitch_perpetual import coinswitch_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_api_order_book_data_source import (
    CoinswitchPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

BOOK = {"timestamp": 1732685131699, "asks": [["95136.70", "3.963"]], "bids": [["95136.60", "3.189"]],
        "symbol": "BTCUSDT"}
# Ticker WebSocket docs payload, keyed by symbol.
TICKER_FRAME = {
    "BTCUSDT": {
        "E": 1732801135779, "s": "BTCUSDT", "o": "93905.00", "h": "97276.00", "l": "93803.00", "c": "95357.00",
        "e": "EXCHANGE_2", "bv": "124666.7500", "qv": "11922532766.1146", "P": "1.546200", "b": "95357.00",
        "a": "95357.10", "T": 1732809600000, "p": 95347.19, "i": 95280.65, "r": 0.00024185, "oi": "", "oiv": "",
        "bs": "", "as": "",
    }
}
TRADES_FRAME = {"data": [{"E": 1732691693128, "p": 95136.6, "q": 0.01, "e": "EXCHANGE_2", "s": "BTCUSDT",
                          "m": True}]}


class CoinswitchPerpetualAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="BTCUSDT")

        async def _pair(symbol):
            if symbol == "BTCUSDT":
                return "BTC-USDT"
            raise KeyError(symbol)

        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=_pair)
        self.connector._api_get = AsyncMock(return_value={"data": BOOK})
        self.data_source = CoinswitchPerpetualAPIOrderBookDataSource(
            trading_pairs=["BTC-USDT"], connector=self.connector, api_factory=MagicMock())
        self.data_source._time = lambda: 1732685132.0

    async def test_rest_snapshot_is_signed_l2(self):
        message = await self.data_source._order_book_snapshot("BTC-USDT")
        kwargs = self.connector._api_get.call_args.kwargs
        self.assertEqual(CONSTANTS.ORDER_BOOK_PATH_URL, kwargs["path_url"])
        self.assertEqual({"exchange": "EXCHANGE_2", "symbol": "BTCUSDT", "l2Orderbook": "true"}, kwargs["params"])
        self.assertTrue(kwargs["is_auth_required"])
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(1732685131699, message.update_id)
        self.assertEqual(95136.6, message.bids[0].price)

    async def test_rest_snapshot_failure_falls_back_to_the_socket_snapshot(self):
        self.connector._api_get = AsyncMock(side_effect=IOError("HTTP status is 401. Invalid Access"))
        await self.data_source._parse_order_book_snapshot_message(BOOK, asyncio.Queue())
        message = await self.data_source._order_book_snapshot("BTC-USDT")
        self.assertEqual(1732685131699, message.update_id)

    async def test_rest_snapshot_failure_without_socket_frames_raises(self):
        self.connector._api_get = AsyncMock(return_value={"message": "weird"})
        with patch.object(CONSTANTS, "WS_FIRST_FRAME_TIMEOUT", 0.01):
            with self.assertRaises(IOError):
                await self.data_source._order_book_snapshot("BTC-USDT")

    async def test_funding_info_from_rest_then_socket_fallback(self):
        rest_info = FundingInfo("BTC-USDT", Decimal("1"), Decimal("2"), 3, Decimal("0.0001"))
        self.connector.build_funding_info = AsyncMock(return_value=rest_info)
        self.assertIs(rest_info, await self.data_source.get_funding_info("BTC-USDT"))

        self.connector.build_funding_info = AsyncMock(side_effect=IOError("refused"))
        await self.data_source._parse_funding_info_message(TICKER_FRAME["BTCUSDT"], asyncio.Queue())
        info = await self.data_source.get_funding_info("BTC-USDT")
        self.assertEqual(Decimal("95347.19"), info.mark_price)
        self.assertEqual(Decimal("95280.65"), info.index_price)
        self.assertEqual(Decimal("0.00024185"), info.rate)
        self.assertEqual(1732809600, info.next_funding_utc_timestamp)

    async def test_funding_info_failure_without_socket_frames_raises(self):
        self.connector.build_funding_info = AsyncMock(side_effect=IOError("refused"))
        with patch.object(CONSTANTS, "WS_FIRST_FRAME_TIMEOUT", 0.01):
            with self.assertRaises(IOError):
                await self.data_source.get_funding_info("BTC-USDT")

    async def test_last_traded_prices_delegate_to_the_connector(self):
        self.connector.get_last_traded_prices = AsyncMock(return_value={"BTC-USDT": 1.0})
        self.assertEqual({"BTC-USDT": 1.0}, await self.data_source.get_last_traded_prices(["BTC-USDT"]))

    async def test_socket_handlers_route_frames_to_their_queues(self):
        await self.data_source._on_order_book({"data": BOOK})
        await self.data_source._on_order_book('{"symbol": "BTCUSDT", "bids": [], "asks": []}')
        await self.data_source._on_order_book({"data": {"unrelated": True}})
        await self.data_source._on_trades(TRADES_FRAME)
        await self.data_source._on_trades({"E": 1, "p": 1, "q": 1, "s": "BTCUSDT", "m": False})
        await self.data_source._on_ticker(TICKER_FRAME)
        await self.data_source._on_ticker(TICKER_FRAME["BTCUSDT"])
        await self.data_source._on_ticker([TICKER_FRAME["BTCUSDT"], "junk"])
        await self.data_source._on_ticker("junk")

        queues = self.data_source._message_queue
        self.assertEqual(2, queues[self.data_source._snapshot_messages_queue_key].qsize())
        self.assertEqual(2, queues[self.data_source._trade_messages_queue_key].qsize())
        self.assertEqual(3, queues[self.data_source._funding_info_messages_queue_key].qsize())
        self.assertEqual(1732685132.0, self.data_source._last_frame_time)

    async def test_parsers(self):
        output = asyncio.Queue()
        await self.data_source._parse_order_book_snapshot_message(BOOK, output)
        await self.data_source._parse_order_book_snapshot_message({**BOOK, "symbol": "NOPEUSDT"}, output)
        await self.data_source._parse_trade_message(TRADES_FRAME["data"][0], output)
        await self.data_source._parse_trade_message({**TRADES_FRAME["data"][0], "s": ""}, output)
        await self.data_source._parse_order_book_diff_message(BOOK, output)
        snapshot, trade = output.get_nowait(), output.get_nowait()
        self.assertTrue(output.empty())
        self.assertEqual("BTC-USDT", snapshot.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade.type)
        self.assertEqual("BTC-USDT", trade.trading_pair)

    async def test_funding_update_skips_empty_fields(self):
        output = asyncio.Queue()
        await self.data_source._parse_funding_info_message(TICKER_FRAME["BTCUSDT"], output)
        update: FundingInfoUpdate = output.get_nowait()
        self.assertEqual(Decimal("95347.19"), update.mark_price)
        self.assertEqual(1732809600, update.next_funding_utc_timestamp)

        await self.data_source._parse_funding_info_message({"s": "BTCUSDT", "p": "", "i": "", "r": "", "T": ""}, output)
        self.assertTrue(output.empty())
        await self.data_source._parse_funding_info_message({"s": "BTCUSDT", "p": "95000", "r": ""}, output)
        partial = output.get_nowait()
        self.assertEqual(Decimal("95000"), partial.mark_price)
        self.assertIsNone(partial.rate)
        self.assertIsNone(partial.next_funding_utc_timestamp)

    async def test_subscribes_every_event_per_pair(self):
        client = MagicMock()
        client.emit = AsyncMock()
        await self.data_source._subscribe_channels(client)
        emitted = [(call.args[0], call.args[1], call.kwargs["namespace"]) for call in client.emit.call_args_list]
        payload = {"event": "subscribe", "pair": "BTCUSDT"}
        self.assertEqual([
            ("FETCH_ORDER_BOOK_CS_PRO", payload, "/exchange_2"),
            ("FETCH_TRADES_CS_PRO", payload, "/exchange_2"),
            ("FETCH_TICKER_INFO_CS_PRO", payload, "/exchange_2"),
        ], emitted)

    async def test_subscribe_failure_raises(self):
        client = MagicMock()
        client.emit = AsyncMock(side_effect=ConnectionError("gone"))
        with self.assertRaises(ConnectionError):
            await self.data_source._subscribe_channels(client)

    def test_client_registers_the_handlers_on_the_namespace(self):
        client = self.data_source._build_client()
        handlers = client.handlers["/exchange_2"]
        for event in ("connect", "disconnect", CONSTANTS.ORDER_BOOK_EVENT, CONSTANTS.TRADES_EVENT,
                      CONSTANTS.TICKER_EVENT):
            self.assertIn(event, handlers)

    async def test_watchdog_returns_on_silence_and_on_disconnect(self):
        now = [1000.0]
        self.data_source._time = lambda: now[0]

        async def _advance(_):
            now[0] += 61

        self.data_source._sleep = AsyncMock(side_effect=_advance)
        client = MagicMock()
        client.connected = True
        await self.data_source._watch_connection(client)
        self.assertEqual(2, self.data_source._sleep.call_count)

        client.connected = False
        await self.data_source._watch_connection(client)
        self.assertEqual(2, self.data_source._sleep.call_count)

    async def test_listen_connects_with_the_documented_handshake_and_reconnects(self):
        client = MagicMock()
        client.connect = AsyncMock()
        client.emit = AsyncMock()
        client.disconnect = AsyncMock()
        client.connected = False
        self.data_source._build_client = MagicMock(return_value=client)
        self.data_source._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError])
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

        args, kwargs = client.connect.call_args
        self.assertEqual("https://ws.coinswitch.co", args[0])
        self.assertEqual(["/exchange_2"], kwargs["namespaces"])
        self.assertEqual("/pro/realtime-rates-socket/futures/exchange_2", kwargs["socketio_path"])
        self.assertEqual(["websocket"], kwargs["transports"])
        self.assertEqual(2, client.connect.call_count)
        self.assertEqual(2, client.disconnect.call_count)

    async def test_listen_survives_a_failed_connect(self):
        client = MagicMock()
        client.connect = AsyncMock(side_effect=ConnectionError("refused"))
        client.disconnect = AsyncMock(side_effect=RuntimeError("not connected"))
        self.data_source._build_client = MagicMock(return_value=client)
        self.data_source._sleep = AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()
        self.assertIsNone(self.data_source._client)

    async def test_connect_and_disconnect_callbacks(self):
        await self.data_source._on_connect()
        await self.data_source._on_disconnect("transport close")
