"""
CoinDCX stream messages as CoinDCX's docs (Spot Sockets) describe them, and the data the connector takes from its
streams instead of REST:
- trade-update fills, with their short keys ("c", "o", "t", "p", "q", "f", "T");
- order-update times, which the docs give as milliseconds in one place and as ISO text in another;
- each market's pair code ("I-ETH_INR", "B-BTC_USDT"), which names the order book and trade channels;
- last prices from the ticker, since the markets list has none;
- a connected private stream counting as alive, so REST polling stays slow;
- one failed network check not restarting the connector's networking.
"""
import asyncio
import json
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.network_iterator import NetworkStatus

USER_STREAM_CLIENT = "hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source.socketio.AsyncClient"


def market(pair_code: str, base: str, quote: str, symbol: str = "") -> dict:
    """A markets_details row with the fields the connector reads ("pair" is e.g. "B-BTC_USDT")."""
    return {"coindcx_name": symbol or f"{base}{quote}", "symbol": symbol or f"{base}{quote}", "pair": pair_code,
            "ecode": pair_code.split("-")[0], "status": "active", "target_currency_short_name": base,
            "base_currency_short_name": quote, "min_quantity": 0.0001, "max_quantity": 1000, "min_notional": 100,
            "base_currency_precision": 1, "target_currency_precision": 4, "step": 0.0001}


MARKETS = [
    market("I-ETH_INR", "ETH", "INR"),
    market("B-BTC_USDT", "BTC", "USDT"),
    market("KC-XYZ_USDT", "XYZ", "USDT"),
    market("G-ASR_USDT", "ASR", "USDT", symbol="ASR_USDT"),  # a pair on two routes: the plain symbol wins
    market("B-ASR_USDT", "ASR", "USDT"),
]


class CoindcxStreamFillTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="key", coindcx_api_secret="secret",
                                        trading_pairs=["ETH-INR"], trading_required=False)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(MARKETS)
        self.exchange._set_current_timestamp(1795675600)
        self.fills = EventLogger()
        self.cancels = EventLogger()
        self.exchange.add_listener(MarketEvent.OrderFilled, self.fills)
        self.exchange.add_listener(MarketEvent.OrderCancelled, self.cancels)
        self.exchange.start_tracking_order(order_id="haveli-B1", exchange_order_id="4075000764", trading_pair="ETH-INR",
                                           trade_type=TradeType.BUY, price=Decimal("170000"),
                                           amount=Decimal("0.0007"), order_type=OrderType.LIMIT)
        self.order = self.exchange.in_flight_orders["haveli-B1"]

    async def listen(self, *events):
        """Runs the connector's user stream listener over these events, as they come off the stream."""
        queue = AsyncMock()
        queue.get.side_effect = list(events) + [asyncio.CancelledError()]
        self.exchange._user_stream_tracker._user_stream = queue
        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._user_stream_event_listener()
        for _ in range(5):  # the order tracker applies order updates in tasks of their own
            await asyncio.sleep(0)

    @staticmethod
    def trade_update(**fields) -> dict:
        """A trade-update as the docs show it: short keys, "data" a JSON list in text."""
        trade = {"o": 4075000764, "c": "haveli-B1", "t": "17105", "s": "ETHINR", "p": "170000.5", "q": "0.0003",
                 "T": 1795675620118.92, "m": False, "f": "0.051", "e": "I", "x": "partially_filled"}
        trade.update(fields)
        return {"event": CONSTANTS.TRADE_UPDATE_EVENT_TYPE, "data": json.dumps([trade])}

    async def test_a_documented_trade_update_becomes_a_fill(self):
        await self.listen(self.trade_update())

        self.assertEqual(1, len(self.fills.event_log))
        fill = self.order.order_fills["17105"]
        self.assertEqual(Decimal("170000.5"), fill.fill_price)
        self.assertEqual(Decimal("0.0003"), fill.fill_base_amount)
        self.assertEqual(Decimal("0.0003") * Decimal("170000.5"), fill.fill_quote_amount)
        self.assertEqual([TokenAmount("INR", Decimal("0.051"))], fill.fee.flat_fees)
        self.assertAlmostEqual(1795675620.11892, fill.fill_timestamp, places=5)
        self.assertEqual("4075000764", fill.exchange_order_id)  # "o" is a number on the stream
        self.assertEqual(Decimal("0.0003"), self.order.executed_amount_base)

    async def test_a_fill_without_the_client_order_id_is_matched_by_the_exchange_order_id(self):
        await self.listen(self.trade_update(c=""))

        self.assertEqual(["17105"], list(self.order.order_fills))

    async def test_fills_of_orders_it_does_not_track_are_ignored(self):
        await self.listen(self.trade_update(c="someone-else", o=999))

        self.assertEqual([], self.fills.event_log)

    async def test_the_same_trade_from_the_stream_and_from_rest_counts_once(self):
        await self.listen(self.trade_update())
        # The REST trade history's names for the same trade, as the backup poll could bring it.
        await self.exchange._process_trade_update({"id": 17105, "order_id": "4075000764", "price": 170000.5,
                                                   "quantity": 0.0003, "fee_amount": 0.051,
                                                   "timestamp": 1795675620118.92})

        self.assertEqual(1, len(self.fills.event_log))
        self.assertEqual(Decimal("0.0003"), self.order.executed_amount_base)

    async def test_long_rest_names_are_still_read(self):
        await self.exchange._process_trade_update({"id": "9", "client_order_id": "haveli-B1", "order_id": "4075000764",
                                                   "price": "170000", "quantity": "0.0007", "fee_amount": "0.1",
                                                   "timestamp": 1795675620000})

        self.assertEqual(Decimal("0.0007"), self.order.order_fills["9"].fill_base_amount)

    async def test_order_update_times_in_iso_text_or_milliseconds_are_both_read(self):
        opened = {"event": CONSTANTS.ORDER_UPDATE_EVENT_TYPE, "data": json.dumps([{
            "id": 4075000764, "client_order_id": "haveli-B1", "status": "open", "updated_at": 1795675620000}])}
        cancelled = {"event": CONSTANTS.ORDER_UPDATE_EVENT_TYPE, "data": json.dumps([{
            "id": 4075000764, "client_order_id": "haveli-B1", "status": "cancelled",
            "updated_at": "2026-11-26T06:47:00.118Z"}])}

        await self.listen(opened, cancelled)

        self.assertEqual(OrderState.CANCELED, self.order.current_state)
        self.assertAlmostEqual(1795675620.118, self.order.last_update_timestamp, places=3)
        self.assertEqual(1, len(self.cancels.event_log))

    async def test_an_order_update_without_the_client_order_id_is_matched_by_the_exchange_order_id(self):
        await self.listen({"event": CONSTANTS.ORDER_UPDATE_EVENT_TYPE, "data": json.dumps([{
            "id": 4075000764, "client_order_id": "", "status": "open", "updated_at": 1795675620000}])})

        self.assertEqual(OrderState.OPEN, self.order.current_state)

    def test_untriggered_stop_orders_count_as_open(self):
        self.assertEqual(OrderState.OPEN, CONSTANTS.ORDER_STATE["untriggered"])


class CoindcxStreamFillFeeTests(IsolatedAsyncioWrapperTestCase):
    """
    Live, CoinDCX's trade-update had no fee (28 Nov 2026: only T, c, m, o, p, q, s, t). Such a fill is recorded from
    REST, which has the fee, so that the fill is recorded once and with the right fee.
    """
    level = 0

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="key", coindcx_api_secret="secret",
                                        trading_pairs=["ETH-INR"], trading_required=False)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(MARKETS)
        self.exchange._set_current_timestamp(1795866290)
        self.exchange._sleep = AsyncMock()
        self.fills = EventLogger()
        self.exchange.add_listener(MarketEvent.OrderFilled, self.fills)
        self.exchange.start_tracking_order(order_id="haveli-B2", exchange_order_id="4125327912", trading_pair="ETH-INR",
                                           trade_type=TradeType.BUY, price=Decimal("174000"),
                                           amount=Decimal("0.0007"), order_type=OrderType.LIMIT)
        self.order = self.exchange.in_flight_orders["haveli-B2"]
        self.log_records = []
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    def tearDown(self):
        self.exchange.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    @staticmethod
    def live_trade_update(trade_id="352917974", quantity="0.0007") -> dict:
        """A trade-update as CoinDCX sent it live on 28 Nov 2026: no fee."""
        return {"event": CONSTANTS.TRADE_UPDATE_EVENT_TYPE, "data": json.dumps([{
            "o": 4125327912, "c": "haveli-B2", "t": trade_id, "s": "ETHINR", "p": "173506.1", "q": quantity,
            "T": 1795866299000, "m": False}])}

    @staticmethod
    def rest_trade(trade_id=352917974, quantity=0.0007, fee=0.060727135) -> dict:
        """The same fill in the REST trade history, as it came back live: with the fee, in INR."""
        return {"id": trade_id, "order_id": "4125327912", "side": "buy", "fee_amount": fee, "ecode": "I",
                "quantity": quantity, "price": 173506.1, "symbol": "ETHINR", "timestamp": 1795866299000}

    async def listen(self, *events):
        queue = AsyncMock()
        queue.get.side_effect = list(events) + [asyncio.CancelledError()]
        self.exchange._user_stream_tracker._user_stream = queue
        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._user_stream_event_listener()
        while self.exchange._stream_fill_tasks:  # the REST reads run in a task of their own
            await asyncio.gather(*self.exchange._stream_fill_tasks.values())

    async def test_a_live_fill_without_a_fee_is_recorded_from_rest_with_its_fee(self):
        self.exchange._api_post = AsyncMock(return_value=[self.rest_trade()])

        await self.listen(self.live_trade_update())

        fill = self.order.order_fills["352917974"]
        self.assertEqual([TokenAmount("INR", Decimal("0.060727135"))], fill.fee.flat_fees)
        self.assertEqual(Decimal("173506.1"), fill.fill_price)
        self.assertEqual(1, len(self.fills.event_log))
        request = self.exchange._api_post.call_args.kwargs
        self.assertEqual(CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL, request["path_url"])
        self.assertEqual({"symbol": "ETHINR", "limit": 100, "sort": "desc", "from_timestamp": 1795866230000},
                         request["data"])  # newest first, from a minute before the order was created
        self.exchange._sleep.assert_not_awaited()

    async def test_rest_is_read_again_until_it_has_the_fill(self):
        self.exchange._api_post = AsyncMock(side_effect=[[], [self.rest_trade()]])

        await self.listen(self.live_trade_update())

        self.assertEqual([TokenAmount("INR", Decimal("0.060727135"))], self.order.order_fills["352917974"].fee.flat_fees)
        self.assertEqual(2, self.exchange._api_post.await_count)
        self.exchange._sleep.assert_awaited_once_with(0.5)

    async def test_a_fill_rest_never_shows_is_recorded_from_the_stream_with_the_estimated_fee(self):
        self.exchange._api_post = AsyncMock(return_value=[])

        await self.listen(self.live_trade_update())

        fill = self.order.order_fills["352917974"]
        self.assertEqual([], fill.fee.flat_fees)
        self.assertGreater(fill.fee.percent, 0)
        self.assertEqual(3, self.exchange._api_post.await_count)
        self.assertEqual([0.5, 1.0], [call.args[0] for call in self.exchange._sleep.await_args_list])
        self.assertTrue(any(record.levelname == "WARNING" and "with an estimated fee" in record.getMessage()
                            for record in self.log_records))

    async def test_fills_reported_together_take_one_read(self):
        self.exchange._api_post = AsyncMock(return_value=[
            self.rest_trade(352917974, 0.0003, 0.026), self.rest_trade(352917975, 0.0004, 0.034)])

        await self.listen(self.live_trade_update("352917974", "0.0003"), self.live_trade_update("352917975", "0.0004"))

        self.assertEqual({"352917974", "352917975"}, set(self.order.order_fills))
        self.assertEqual(Decimal("0.0007"), self.order.executed_amount_base)
        self.assertEqual(1, self.exchange._api_post.await_count)

    async def test_a_fill_already_recorded_is_not_read_again(self):
        self.exchange._api_post = AsyncMock(return_value=[self.rest_trade()])
        await self.listen(self.live_trade_update())

        await self.listen(self.live_trade_update())

        self.assertEqual(1, self.exchange._api_post.await_count)
        self.assertEqual(1, len(self.fills.event_log))

    async def test_the_backup_poll_reads_newest_first_and_takes_every_fill_of_an_order(self):
        self.exchange._last_poll_timestamp = 0
        self.exchange._set_current_timestamp(1795866300)
        self.exchange._api_post = AsyncMock(return_value=[
            self.rest_trade(352917975, 0.0004, 0.034), self.rest_trade(352917974, 0.0003, 0.026)])

        await self.exchange._update_order_fills_from_trades()

        self.assertEqual({"352917974", "352917975"}, set(self.order.order_fills))  # it kept only one per order
        self.assertEqual({"limit": 100, "sort": "desc", "from_timestamp": 1795866230000},
                         self.exchange._api_post.call_args.kwargs["data"])

    async def test_the_backup_poll_skips_rest_without_orders(self):
        exchange = CoindcxExchange(coindcx_api_key="key", coindcx_api_secret="secret", trading_pairs=["ETH-INR"],
                                   trading_required=False)
        exchange._set_current_timestamp(1795866300)
        exchange._api_post = AsyncMock(return_value=[])

        await exchange._update_order_fills_from_trades()

        exchange._api_post.assert_not_awaited()  # there are no fills of its own to look for

    async def test_after_a_stream_gap_balances_and_orders_are_read_over_rest(self):
        self.exchange._update_balances = AsyncMock()
        self.exchange._update_order_status = AsyncMock()

        await self.exchange._sync_after_user_stream_gap()

        self.exchange._update_balances.assert_awaited_once()
        self.exchange._update_order_status.assert_awaited_once()

    async def test_a_failed_read_after_a_stream_gap_is_logged(self):
        self.exchange._update_balances = AsyncMock(side_effect=IOError("proxy refused"))

        await self.exchange._sync_after_user_stream_gap()

        self.assertTrue(any(record.levelname == "WARNING" and record.getMessage() ==
                            "Could not read the account over REST after the user stream reconnected: proxy refused"
                            for record in self.log_records))


class CoindcxPairCodeTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="",
                                        trading_pairs=["ETH-INR", "BTC-USDT"], trading_required=False)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(MARKETS)

    async def test_each_pair_gets_its_markets_own_code(self):
        self.assertEqual("I-ETH_INR", await self.exchange.exchange_pair_code("ETH-INR"))
        self.assertEqual("B-BTC_USDT", await self.exchange.exchange_pair_code("BTC-USDT"))  # was "I-BTC_USDT"
        self.assertEqual("KC-XYZ_USDT", await self.exchange.exchange_pair_code("XYZ-USDT"))
        self.assertEqual("B-ASR_USDT", await self.exchange.exchange_pair_code("ASR-USDT"))  # the mapped market's

    async def test_a_market_without_a_code_gets_the_usual_one(self):
        rows = [dict(market("I-SOL_INR", "SOL", "INR"), pair=""), dict(market("B-DOT_USDT", "DOT", "USDT"), pair="")]
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(rows)

        self.assertEqual("I-SOL_INR", await self.exchange.exchange_pair_code("SOL-INR"))
        self.assertEqual("B-DOT_USDT", await self.exchange.exchange_pair_code("DOT-USDT"))

    async def test_codes_are_read_with_the_markets_when_the_map_is_not_ready(self):
        exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="", trading_pairs=["BTC-USDT"],
                                   trading_required=False)
        exchange._make_trading_pairs_request = AsyncMock(return_value=MARKETS)

        self.assertEqual("B-BTC_USDT", await exchange.exchange_pair_code("BTC-USDT"))
        exchange._make_trading_pairs_request.assert_awaited_once()

    async def test_channels_and_the_rest_order_book_use_the_codes(self):
        rest = AsyncMock()
        rest.execute_request = AsyncMock(return_value={"timestamp": 1795675620118, "bids": {}, "asks": {}})
        factory = MagicMock()
        factory.get_rest_assistant = AsyncMock(return_value=rest)
        source = CoinDCXAPIOrderBookDataSource(trading_pairs=["ETH-INR", "BTC-USDT"], connector=self.exchange,
                                               api_factory=factory)
        client = MagicMock()
        client.emit = AsyncMock()

        await source._subscribe_channels(client)
        await source._request_order_book_snapshot("BTC-USDT")

        joined = [call.args[1]["channelName"] for call in client.emit.call_args_list]
        self.assertEqual(["I-ETH_INR@orderbook@50", "I-ETH_INR@trades", "B-BTC_USDT@orderbook@50",
                          "B-BTC_USDT@trades"], joined)
        self.assertEqual({"pair": "B-BTC_USDT"}, rest.execute_request.call_args.kwargs["params"])

    async def test_trades_are_mapped_by_pair_code_before_and_after_joining(self):
        source = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=self.exchange,
                                               api_factory=MagicMock())
        trade = {"T": 1795675620118, "p": "97000.1", "q": "0.01", "m": 1, "s": "B-BTC_USDT", "pr": "spot"}
        queue = asyncio.Queue()

        await source._parse_trade_message(trade, queue)  # no channel joined yet
        client = MagicMock()
        client.emit = AsyncMock()
        await source._subscribe_channels(client)
        await source._parse_trade_message(trade, queue)

        self.assertEqual(["BTC-USDT", "BTC-USDT"], [queue.get_nowait().trading_pair for _ in range(2)])


class CoindcxLastPriceTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="",
                                        trading_pairs=["ETH-INR", "BTC-USDT"], trading_required=False)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(MARKETS)
        self.exchange.get_all_pairs_prices = AsyncMock(return_value=[
            {"market": "ETHINR", "last_price": "174012.3", "bid": "174000.0", "ask": "174020.0"},
            {"market": "BTCUSDT", "last_price": 97001.5},
            {"market": "XYZUSDT", "last_price": None},
        ])

    async def test_last_prices_come_from_one_ticker_read(self):
        prices = await self.exchange.get_last_traded_prices(["ETH-INR", "BTC-USDT", "XYZ-USDT"])

        self.assertEqual({"ETH-INR": 174012.3, "BTC-USDT": 97001.5}, prices)  # XYZ has no price: left out, not 0
        self.exchange.get_all_pairs_prices.assert_awaited_once()

    async def test_one_pairs_price_raises_when_the_ticker_has_none(self):
        self.assertEqual(174012.3, await self.exchange._get_last_traded_price("ETH-INR"))
        with self.assertRaises(KeyError):
            await self.exchange._get_last_traded_price("XYZ-USDT")


class CoindcxUserStreamLivenessTests(IsolatedAsyncioWrapperTestCase):
    level = 0  # this test case is also the log handler

    def setUp(self):
        super().setUp()
        auth = MagicMock()
        auth.generate_ws_auth_payload.return_value = {"channelName": "coindcx"}
        self.source = CoinDCXAPIUserStreamDataSource(auth=auth, trading_pairs=["ETH-INR"], connector=MagicMock(),
                                                     api_factory=MagicMock())
        self.log_records = []
        self.source.logger().setLevel(1)
        self.source.logger().addHandler(self)

    def tearDown(self):
        self.source.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def stop_after_one_wait(self):
        waits = []

        async def sleep(delay):
            waits.append(delay)
            if len(waits) > 1:
                raise asyncio.CancelledError
        self.source._sleep = sleep

    async def test_a_ping_on_a_connected_stream_counts_as_hearing_from_it(self):
        self.source._client = MagicMock(connected=True, emit=AsyncMock())
        self.source._time = MagicMock(return_value=1795675620.0)
        self.stop_after_one_wait()

        await self.source._ping_task()

        self.source._client.emit.assert_awaited_once_with("ping", {"data": "Ping message"})
        self.assertEqual(1795675620.0, self.source.last_recv_time)

    async def test_a_disconnected_stream_does_not(self):
        self.source._client = MagicMock(connected=False, emit=AsyncMock())
        self.source._time = MagicMock(return_value=1795675620.0)
        self.stop_after_one_wait()

        await self.source._ping_task()

        self.assertEqual(0.0, self.source.last_recv_time)

    def test_rest_polling_stays_slow_while_the_stream_is_heard_from(self):
        exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="", trading_pairs=["ETH-INR"])
        stream = exchange._user_stream_tracker.data_source
        now = 1795675620.0

        stream._last_recv_time = now - CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL  # the last ping
        self.assertEqual(exchange.LONG_POLL_INTERVAL, exchange._get_poll_interval(now))
        stream._last_recv_time = now - 90  # nothing for a minute and a half: the stream may be gone
        self.assertEqual(exchange.SHORT_POLL_INTERVAL, exchange._get_poll_interval(now))

    async def test_a_reconnect_has_the_connector_catch_up_over_rest(self):
        connector = MagicMock()
        connector._sync_after_user_stream_gap = AsyncMock()
        self.source._connector = connector
        client = MagicMock()
        client.event = MagicMock(side_effect=lambda func: func)
        client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
        client.emit = AsyncMock()
        with patch(USER_STREAM_CLIENT, return_value=client):
            self.source._build_client(asyncio.Queue())
        connect = next(call.args[0] for call in client.event.call_args_list if call.args[0].__name__ == "connect")

        await connect()  # the first join: nothing missed
        await asyncio.sleep(0)
        connector._sync_after_user_stream_gap.assert_not_awaited()
        await connect()  # a reconnect
        await asyncio.sleep(0)
        connector._sync_after_user_stream_gap.assert_awaited_once()

    async def test_a_failed_connection_is_logged(self):
        client = MagicMock()
        client.event = MagicMock(side_effect=lambda func: func)
        client.on = MagicMock(side_effect=lambda event_type: lambda func: func)
        client.connect = AsyncMock(side_effect=ConnectionError("403 Forbidden"))
        client.disconnect = AsyncMock()

        async def stop(delay):
            raise asyncio.CancelledError
        self.source._sleep = stop

        with patch(USER_STREAM_CLIENT, return_value=client):
            with self.assertRaises(asyncio.CancelledError):
                await self.source.listen_for_user_stream(asyncio.Queue())

        self.assertTrue(any(record.levelname == "WARNING" and record.getMessage() ==
                            "CoinDCX user stream connection failed: 403 Forbidden. Retrying in 5 s."
                            for record in self.log_records))


class CoindcxOrderBookSilenceTests(IsolatedAsyncioWrapperTestCase):
    """A stuck connection (seen through a proxy: quiet for about 15 s, then dropped) is dropped so it reconnects."""
    level = 0

    def setUp(self):
        super().setUp()
        self.source = CoinDCXAPIOrderBookDataSource(trading_pairs=["ETH-INR"], connector=MagicMock(),
                                                    api_factory=MagicMock())
        self.client = MagicMock(disconnect=AsyncMock())
        self.log_records = []
        self.source.logger().setLevel(1)
        self.source.logger().addHandler(self)

    def tearDown(self):
        self.source.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    async def test_a_silent_connection_is_dropped(self):
        self.source._last_message_time = time.time() - 20
        self.source._sleep = AsyncMock()

        await self.source._watch_for_silence(self.client)

        self.client.disconnect.assert_awaited_once()
        self.assertTrue(any(record.levelname == "WARNING" and "sent nothing for 20 s; reconnecting" in
                            record.getMessage() for record in self.log_records))

    async def test_a_connection_that_sends_is_kept(self):
        self.source._queue_message({"event": "depth-snapshot", "data": "{}"}, CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE)
        self.source._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with self.assertRaises(asyncio.CancelledError):
            await self.source._watch_for_silence(self.client)

        self.client.disconnect.assert_not_awaited()


class CoindcxNetworkCheckTests(IsolatedAsyncioWrapperTestCase):
    """
    A failed network check makes hummingbot stop and restart all of the connector's networking. Through a proxy a
    single check can fail now and then, so while connected, only two failures in a row are reported.
    """
    level = 0

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="", coindcx_api_secret="", trading_pairs=["ETH-INR"],
                                        trading_required=False)
        self.log_records = []
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    def tearDown(self):
        self.exchange.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def connected(self, status=NetworkStatus.CONNECTED):
        return patch.object(CoindcxExchange, "network_status", new_callable=PropertyMock, return_value=status)

    async def checks(self, *outcomes):
        """The check's answers, one per call; each outcome is None (the call works) or the error it raises."""
        self.exchange._make_network_check_request = AsyncMock(side_effect=list(outcomes))
        return [await self.exchange.check_network() for _ in outcomes]

    async def test_one_failure_while_connected_is_not_reported_but_two_in_a_row_are(self):
        with self.connected():
            answers = await self.checks(IOError("Connection reset by peer"), IOError("Connection reset by peer"))

        self.assertEqual([NetworkStatus.CONNECTED, NetworkStatus.NOT_CONNECTED], answers)
        self.assertTrue(any(record.levelname == "INFO" and record.getMessage() ==
                            "CoinDCX network check failed (Connection reset by peer); staying connected unless the "
                            "next check fails too." for record in self.log_records))

    async def test_a_working_check_starts_the_count_again(self):
        with self.connected():
            answers = await self.checks(IOError("reset"), None, IOError("reset"), None)

        self.assertEqual([NetworkStatus.CONNECTED] * 4, answers)

    async def test_while_not_connected_a_failure_is_reported_at_once(self):
        with self.connected(NetworkStatus.NOT_CONNECTED):
            answers = await self.checks(IOError("Couldn't connect to proxy"))

        self.assertEqual([NetworkStatus.NOT_CONNECTED], answers)

    async def test_a_slow_check_counts_as_a_failure(self):
        self.exchange.check_network_timeout = 1.5  # the call gets 1 s

        async def slow():
            await asyncio.sleep(5)
        self.exchange._make_network_check_request = slow
        with self.connected():
            first = await self.exchange.check_network()
            self.assertEqual(1, self.exchange._failed_network_checks)

        self.assertEqual(NetworkStatus.CONNECTED, first)
