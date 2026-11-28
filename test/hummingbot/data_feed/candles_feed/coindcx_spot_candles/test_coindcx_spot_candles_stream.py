"""
CoinDCX candles from its Socket.IO "candlestick" stream (docs: Spot Sockets, Get Candlestick Info), with REST as the
seed, the backfill and the fallback.
"""
import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.data_feed.candles_feed.coindcx_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.coindcx_spot_candles.coindcx_spot_candles import CoinDCXSpotCandles

CLIENT = "hummingbot.data_feed.candles_feed.coindcx_spot_candles.coindcx_spot_candles.socketio.AsyncClient"
T0 = 1795675620  # a minute's start, in seconds


def stream_candle(start_s: float = T0, close: str = "97010.5", **fields) -> dict:
    """A "candlestick" message as the docs show it: the candle in "data", here as JSON text."""
    candle = {"t": int(start_s * 1000), "T": int(start_s * 1000) + 59999, "eT": int(start_s * 1000) + 1234,
              "s": "BTCUSDT", "i": "1m", "f": 101, "L": 140, "o": "97000.0", "c": close, "h": "97020.0",
              "l": "96990.0", "v": "1.25", "n": 40, "x": False, "q": "121262.5", "V": "0.75", "Q": "72757.5",
              "B": 101, "ecode": "B", "channel": "B-BTC_USDT_1m", "pr": "spot"}
    candle.update(fields)
    return {"event": "candlestick", "data": json.dumps(candle)}


class CoinDCXCandlesStreamTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    def setUp(self):
        super().setUp()
        self.feed = CoinDCXSpotCandles(trading_pair="BTC-USDT", interval="1m", max_records=150)
        self.feed._ex_trading_pair = "B-BTC_USDT"
        self.log_records = []
        self.feed.logger().setLevel(1)
        self.feed.logger().addHandler(self)

    def tearDown(self):
        self.feed.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def logged(self, level: str, text: str) -> bool:
        return any(record.levelname == level and text in record.getMessage() for record in self.log_records)

    def test_only_the_intervals_coindcx_streams_use_the_stream(self):
        self.assertTrue(self.feed.uses_socket)
        self.assertEqual("B-BTC_USDT_1m", self.feed.socket_channel)
        self.assertTrue(CoinDCXSpotCandles("BTC-USDT", "1h").uses_socket)
        self.assertFalse(CoinDCXSpotCandles("BTC-USDT", "5m").uses_socket)  # built from 1m REST candles

    def test_a_documented_candle_becomes_a_row(self):
        self.assertEqual([[T0, 97000.0, 97020.0, 96990.0, 97010.5, 1.25, 121262.5, 40.0, 0.75, 72757.5]],
                         self.feed._parse_socket_candles(stream_candle()))

    def test_candle_data_as_an_object_or_a_list_is_read_too(self):
        body = json.loads(stream_candle()["data"])

        self.assertEqual(1, len(self.feed._parse_socket_candles({"event": "candlestick", "data": body})))
        self.assertEqual(2, len(self.feed._parse_socket_candles({"event": "candlestick", "data": [body, body]})))

    def test_candles_of_another_interval_or_channel_are_skipped(self):
        self.assertEqual([], self.feed._parse_socket_candles(stream_candle(i="15m")))
        self.assertEqual([], self.feed._parse_socket_candles(stream_candle(channel="B-ETH_USDT_1m")))

    def test_a_candle_it_cannot_read_is_skipped_with_a_warning(self):
        message = json.loads(stream_candle()["data"])
        del message["o"]

        self.assertEqual([], self.feed._parse_socket_candles({"data": message}))
        self.assertTrue(self.logged("WARNING", "CoinDCX: ignoring a stream candle it can't read"))

    def test_the_stream_waits_for_rest_to_seed_the_feed(self):
        self.feed._apply_socket_candle(self.feed._parse_socket_candles(stream_candle())[0])

        self.assertEqual(0, len(self.feed._candles))
        self.assertFalse(self.feed._socket_live)

    def test_the_current_candle_is_replaced_and_a_new_one_appended(self):
        self.feed._candles.append([T0, 97000.0, 97000.0, 97000.0, 97000.0, 0.1, 0.0, 0.0, 0.0, 0.0])

        self.feed._apply_socket_candle(self.feed._parse_socket_candles(stream_candle(close="97011.0"))[0])
        self.assertEqual(1, len(self.feed._candles))
        self.assertEqual(97011.0, self.feed._candles[-1][4])

        self.feed._apply_socket_candle(self.feed._parse_socket_candles(stream_candle(T0 + 120))[0])
        self.assertEqual([T0, T0 + 60, T0 + 120], [candle[0] for candle in self.feed._candles])  # a heartbeat between
        self.assertEqual(97011.0, self.feed._candles[1][4])

        self.feed._apply_socket_candle(self.feed._parse_socket_candles(stream_candle(T0 - 60))[0])  # older: ignored
        self.assertEqual(3, len(self.feed._candles))
        self.assertTrue(self.feed._socket_live)

    def test_rest_is_read_often_only_while_the_stream_is_not_delivering(self):
        self.feed._time = MagicMock(return_value=T0 + 100)
        self.feed._last_rest_poll = T0 + 90

        self.assertTrue(self.feed._rest_poll_due())  # stream not delivering
        self.feed._socket_live = True
        self.assertFalse(self.feed._rest_poll_due())
        self.feed._last_rest_poll = T0 + 100 - CONSTANTS.SOCKET_REST_CHECK_INTERVAL
        self.assertTrue(self.feed._rest_poll_due())  # the minute's check

    async def run_polling_loop_once(self):
        self.feed._is_running = True
        self.feed._initialize_candles = AsyncMock()
        self.feed._poll_and_update = AsyncMock(side_effect=lambda: self.feed._shutdown_event.set())
        self.feed._ensure_heartbeats_to_current_time = MagicMock(side_effect=lambda: self.feed._shutdown_event.set())
        await self.feed._polling_loop()

    async def test_the_polling_loop_skips_rest_while_the_stream_delivers(self):
        self.feed._socket_live = True

        await self.run_polling_loop_once()

        self.feed._poll_and_update.assert_not_awaited()
        self.feed._ensure_heartbeats_to_current_time.assert_called_once()

    async def test_the_polling_loop_reads_rest_without_the_stream(self):
        await self.run_polling_loop_once()

        self.feed._poll_and_update.assert_awaited_once()

    def fake_client(self, connect_error=None):
        client = MagicMock()
        client.connect = AsyncMock(side_effect=connect_error)
        client.emit = AsyncMock()
        client.disconnect = AsyncMock()
        client.connected = True

        async def wait():  # the connection ends, and the feed is stopping
            self.feed._shutdown_event.set()
        client.wait = AsyncMock(side_effect=wait)
        return client

    async def test_the_stream_joins_the_candle_channel_and_hands_candles_over(self):
        client = self.fake_client()
        self.feed._is_running = True

        with patch(CLIENT, return_value=client):
            await self.feed._socket_loop()

        client.connect.assert_awaited_once()
        client.emit.assert_any_await("join", {"channelName": "B-BTC_USDT_1m"})
        client.on.assert_called_once_with("candlestick", self.feed._on_socket_candle)
        client.disconnect.assert_awaited_once()
        self.assertFalse(self.feed._socket_live)

    async def test_a_failed_stream_is_logged_and_rest_takes_over(self):
        client = self.fake_client(connect_error=ConnectionError("refused"))
        self.feed._is_running = True

        async def stop_soon():
            await asyncio.sleep(0)
            self.feed._shutdown_event.set()

        with patch(CLIENT, return_value=client), patch.object(CONSTANTS, "SOCKET_RETRY_DELAY", 0.01):
            await asyncio.gather(self.feed._socket_loop(), stop_soon())

        self.assertTrue(self.logged("WARNING", "CoinDCX candles stream B-BTC_USDT_1m failed: refused. "
                                               "Polling REST until it is back."))
        self.assertFalse(self.feed._socket_live)

    async def test_stream_candles_reach_the_feed(self):
        self.feed._candles.append([T0, 97000.0, 97000.0, 97000.0, 97000.0, 0.1, 0.0, 0.0, 0.0, 0.0])

        await self.feed._on_socket_candle(stream_candle(close="97005.0"))

        self.assertEqual(97005.0, self.feed._candles[-1][4])
