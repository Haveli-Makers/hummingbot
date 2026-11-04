import asyncio
import inspect
import json
import os
import re
import threading
from test.hummingbot.data_feed.candles_feed.test_candles_base import TestCandlesBase
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
from aioresponses import aioresponses

from hummingbot.data_feed.candles_feed.ajaib_spot_candles import (
    ajaib_spot_candles as ajaib_module,
    constants as CONSTANTS,
)
from hummingbot.data_feed.candles_feed.ajaib_spot_candles.ajaib_spot_candles import (
    AjaibSpotCandles,
    _AjaibSigningPreProcessor,
    _SharedConnectionsFactory,
)
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig, CandlesFactory

BASE_TS = 1_700_000_040.0  # a whole minute


class TestAjaibSpotCandles(TestCandlesBase):
    __test__ = True
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "IDR"
        cls.interval = "1m"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = "BTC_IDR"
        cls.max_records = 150

    def setUp(self) -> None:
        super().setUp()
        self._signing_patch = patch.object(
            _AjaibSigningPreProcessor, "pre_process", new=AsyncMock(side_effect=lambda request: request))
        self._signing_patch.start()
        self.data_feed = self._make_feed(max_records=self.max_records)
        self.log_records = []
        self.data_feed.logger().setLevel(1)
        self.data_feed.logger().addHandler(self)

    def tearDown(self) -> None:
        self.data_feed.logger().removeHandler(self)
        self._signing_patch.stop()
        super().tearDown()

    async def asyncTearDown(self):
        if self.data_feed._polling_task:
            await self.data_feed.stop_network()
        await super().asyncTearDown()

    def _make_feed(self, max_records: int, interval: str = "1m") -> AjaibSpotCandles:
        with patch.dict(os.environ, {CONSTANTS.PROXY_URL_ENV_VAR: ""}):
            return AjaibSpotCandles(trading_pair=self.trading_pair, interval=interval, max_records=max_records)

    @staticmethod
    def _candle(ts: float, close: float = None, volume: float = 10.0):
        close = ts if close is None else close
        return [ts, close, close, close, close, volume, 0.0, 0.0, 0.0, 0.0]

    @staticmethod
    def _page_fetcher(feed: AjaibSpotCandles, has_trade=lambda ts: True, calls: list = None, during_fetch=None):
        """
        Stand-in for fetch_candles that answers like the real one: the page ends at
        end_time and spans at most candles_max_result_per_rest_request buckets. Every
        candle closes at its own timestamp, so a close later than the candle's
        timestamp would mean a price was taken from the future.
        """
        async def fetch(start_time=None, end_time=None, limit=None):
            if calls is not None:
                calls.append(limit)
            if during_fetch is not None:
                during_fetch()
            interval = feed.interval_in_seconds
            size = min(limit or feed.candles_max_result_per_rest_request, feed.candles_max_result_per_rest_request)
            start = feed._round_timestamp_to_interval_multiple(end_time - size * interval)
            rows = [TestAjaibSpotCandles._candle(float(ts))
                    for ts in range(start, int(end_time) + 1, interval) if has_trade(ts)]
            return np.array(rows, dtype=float)
        return fetch

    def _assert_equidistant(self, feed: AjaibSpotCandles):
        timestamps = [c[0] for c in feed._candles]
        diffs = set(np.diff(timestamps))
        self.assertEqual({float(feed.interval_in_seconds)}, diffs, f"Uneven spacing: {sorted(diffs)[:5]}")

    def _assert_no_look_ahead(self, feed: AjaibSpotCandles):
        for candle in feed._candles:
            self.assertLessEqual(candle[4], candle[0], f"Candle at {candle[0]} uses a later close {candle[4]}")

    @staticmethod
    def get_candles_rest_data_mock():
        """Ajaib rows: [openTime, open, high, low, close, volume, closeTime], ms timestamps, oldest first."""
        return [
            [1672992000000, "16810.18", "16823.63", "16792.12", "16810.18", "6230.44034", 1672992059999],
            [1672992060000, "16786.86", "16816.45", "16779.96", "16786.86", "6529.22759", 1672992119999],
            [1672992120000, "16794.06", "16802.87", "16780.15", "16794.06", "5763.44917", 1672992179999],
            [1672992180000, "16802.11", "16812.22", "16791.47", "16802.11", "5475.13940", 1672992239999],
        ]

    def get_fetch_candles_data_mock(self):
        return [
            [1672992000.0, 16810.18, 16823.63, 16792.12, 16810.18, 6230.44034, 0.0, 0.0, 0.0, 0.0],
            [1672992060.0, 16786.86, 16816.45, 16779.96, 16786.86, 6529.22759, 0.0, 0.0, 0.0, 0.0],
            [1672992120.0, 16794.06, 16802.87, 16780.15, 16794.06, 5763.44917, 0.0, 0.0, 0.0, 0.0],
            [1672992180.0, 16802.11, 16812.22, 16791.47, 16802.11, 5475.13940, 0.0, 0.0, 0.0, 0.0],
        ]

    @staticmethod
    def get_candles_ws_data_mock_1():
        return {}

    @staticmethod
    def get_candles_ws_data_mock_2():
        return {}

    @staticmethod
    def _success_subscription_mock():
        return {}

    # Ajaib has no websocket feed; the shared websocket tests do not apply.

    async def test_listen_for_subscriptions_subscribes_to_klines(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_process_websocket_messages_empty_candle(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_process_websocket_messages_duplicated_candle_not_included(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_process_websocket_messages_with_two_valid_messages(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_subscribe_channels_raises_cancel_exception(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_listen_for_subscriptions_raises_cancel_exception(self):
        self.data_feed._is_running = True
        self.data_feed._polling_task = asyncio.get_event_loop().create_task(asyncio.Event().wait())
        task = asyncio.get_event_loop().create_task(self.data_feed.listen_for_subscriptions())
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.data_feed._polling_task.cancel()
        try:
            await self.data_feed._polling_task
        except asyncio.CancelledError:
            pass
        self.data_feed._polling_task = None

    async def test_listen_for_subscriptions_logs_exception_details(self):
        polls = 0

        async def failing_poll():
            nonlocal polls
            polls += 1
            if polls == 2:
                self.data_feed._shutdown_event.set()
            raise Exception("TEST ERROR.")

        self.data_feed._is_running = True
        with patch.object(self.data_feed, "_poll_and_update", new=failing_poll), \
                patch.object(self.data_feed, "_poll_delay", return_value=0.001):
            await self.data_feed._polling_loop()

        errors = [r for r in self.log_records if r.levelname == "ERROR" and "TEST ERROR." in r.getMessage()]
        self.assertEqual(2, len(errors))
        self.assertIsNotNone(errors[0].exc_info)

    # Properties and factory registration

    def test_properties(self):
        self.assertEqual(f"ajaib_{self.trading_pair}", self.data_feed.name)
        self.assertEqual(CONSTANTS.REST_URL, self.data_feed.rest_url)
        self.assertIsNone(self.data_feed.wss_url)
        self.assertEqual(CONSTANTS.REST_URL + CONSTANTS.CANDLES_ENDPOINT, self.data_feed.candles_url)
        self.assertEqual(CONSTANTS.RATE_LIMITS, self.data_feed.rate_limits)
        self.assertEqual(CONSTANTS.INTERVALS, self.data_feed.intervals)

    def test_get_exchange_trading_pair_is_upper_case_with_underscore(self):
        self.assertEqual("ETH_IDR", self.data_feed.get_exchange_trading_pair("eth-idr"))

    def test_candles_factory_builds_ajaib_feed(self):
        with patch.dict(os.environ, {CONSTANTS.PROXY_URL_ENV_VAR: ""}):
            feed = CandlesFactory.get_candle(CandlesConfig(connector="ajaib", trading_pair="BTC-IDR", interval="5m"))
        self.assertIsInstance(feed, AjaibSpotCandles)

    # Native interval resolution and the per-request cap

    def test_native_interval_resolution(self):
        expected = {
            "1m": ("1m", 1), "3m": ("1m", 3), "5m": ("1m", 5), "15m": ("15m", 1), "30m": ("15m", 2),
            "1h": ("1h", 1), "2h": ("1h", 2), "4h": ("4h", 1), "6h": ("1h", 6), "8h": ("4h", 2),
            "12h": ("4h", 3), "1d": ("1d", 1), "3d": ("1d", 3),
        }
        self.assertEqual(set(CONSTANTS.INTERVALS), set(expected))
        for interval, (native, ratio) in expected.items():
            feed = self._make_feed(max_records=10, interval=interval)
            self.assertEqual(native, feed._native_interval, interval)
            self.assertEqual(ratio, feed._resample_ratio, interval)

    def test_native_interval_resolution_fails_when_nothing_divides(self):
        feed = self._make_feed(max_records=10)
        with patch.object(type(feed), "interval_in_seconds", new=property(lambda _: 90)):
            with self.assertRaises(ValueError):
                feed._resolve_native_interval()

    def test_candles_max_result_per_rest_request(self):
        self.assertEqual(998, self._make_feed(10, "1m").candles_max_result_per_rest_request)
        self.assertEqual(331, self._make_feed(10, "3m").candles_max_result_per_rest_request)
        self.assertEqual(164, self._make_feed(10, "6h").candles_max_result_per_rest_request)

    def test_full_page_never_exceeds_server_row_cap(self):
        """A full page, widened to whole buckets at both ends, stays within 1000 native rows."""
        unaligned_end = 1_700_000_000 + 37
        for interval in CONSTANTS.INTERVALS:
            feed = self._make_feed(max_records=10, interval=interval)
            page = feed.candles_max_result_per_rest_request
            params = feed._get_rest_candles_params(
                start_time=unaligned_end - feed.interval_in_seconds * page, end_time=unaligned_end)
            native_seconds = feed.get_seconds_from_interval(feed._native_interval)
            rows = (params["end_time"] - params["start_time"]) // 1000 // native_seconds + 1
            self.assertLessEqual(rows, CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST, interval)

    # Request params and parsing

    def test_get_rest_candles_params_widens_window_to_whole_buckets(self):
        feed = self._make_feed(max_records=10, interval="3m")
        start, end = 1_700_000_100, 1_700_000_400  # neither is on a 3m boundary
        params = feed._get_rest_candles_params(start_time=start, end_time=end)
        self.assertEqual("1m", params["interval"])
        self.assertEqual("BTC_IDR", params["symbol"])
        self.assertEqual(feed._round_timestamp_to_interval_multiple(start) * 1000, params["start_time"])
        # The last 1m candle of the 3m bucket that contains end_time.
        self.assertEqual((feed._round_timestamp_to_interval_multiple(end) + 120) * 1000, params["end_time"])
        self.assertEqual(CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST, params["limit"])

    def test_get_rest_candles_params_defaults_to_one_page_back_from_now(self):
        with patch.object(self.data_feed, "_time", return_value=BASE_TS):
            params = self.data_feed._get_rest_candles_params()
        self.assertEqual(int(BASE_TS) * 1000, params["end_time"])
        self.assertEqual(int(BASE_TS - 60 * 998) * 1000, params["start_time"])

    def test_resample_candles_aggregates_native_rows(self):
        feed = self._make_feed(max_records=10, interval="3m")
        bucket = feed._round_timestamp_to_interval_multiple(BASE_TS)
        native = [
            [bucket, 10.0, 12.0, 9.0, 11.0, 1.0, 0, 0, 0, 0],
            [bucket + 60, 11.0, 15.0, 10.0, 14.0, 2.0, 0, 0, 0, 0],
            [bucket + 120, 14.0, 14.5, 8.0, 13.0, 3.0, 0, 0, 0, 0],
            [bucket + 180, 13.0, 13.0, 12.0, 12.5, 4.0, 0, 0, 0, 0],
        ]
        result = feed._resample_candles(native)
        self.assertEqual([bucket, 10.0, 15.0, 8.0, 13.0, 6.0, 0.0, 0.0, 0.0, 0.0], result[0])
        self.assertEqual([bucket + 180, 13.0, 13.0, 12.0, 12.5, 4.0, 0.0, 0.0, 0.0, 0.0], result[1])

    def test_resample_candles_is_identity_for_native_interval(self):
        native = [self._candle(BASE_TS)]
        self.assertIs(native, self.data_feed._resample_candles(native))

    def test_parse_rest_candles(self):
        result = self.data_feed._parse_rest_candles(list(reversed(self.get_candles_rest_data_mock())))
        self.assertEqual(self.get_fetch_candles_data_mock(), result)

    def test_parse_rest_candles_filters_by_end_time(self):
        result = self.data_feed._parse_rest_candles(self.get_candles_rest_data_mock(), end_time=1672992060)
        self.assertEqual([1672992000.0, 1672992060.0], [c[0] for c in result])

    def test_parse_rest_candles_raises_on_error_payload(self):
        with self.assertRaises(ValueError) as ctx:
            self.data_feed._parse_rest_candles({"err_code": "EC0009007", "err_message": "Data Invalid: coin not found"})
        self.assertIn("coin not found", str(ctx.exception))

    def test_parse_rest_candles_skips_malformed_row(self):
        rows = self.get_candles_rest_data_mock()
        rows[1] = [1672992060000, "BAD", "1", "1", "1", "1", 0]
        result = self.data_feed._parse_rest_candles(rows)
        self.assertEqual(3, len(result))
        self.assertTrue(any("error parsing candle row" in r.getMessage() for r in self.log_records))

    @aioresponses()
    async def test_fetch_candles(self, mock_api):
        mock_api.get(url=re.compile(f"^{re.escape(self.data_feed.candles_url)}"),
                     body=json.dumps(self.get_candles_rest_data_mock()))

        resp = await self.data_feed.fetch_candles(start_time=1672992000, end_time=1672992180)

        self.assertEqual((4, 10), resp.shape)

    async def test_fetch_candles_retries_connection_reset(self):
        ok = np.array(self.get_fetch_candles_data_mock())
        with patch("hummingbot.data_feed.candles_feed.candles_base.CandlesBase.fetch_candles",
                   new=AsyncMock(side_effect=[ConnectionError("reset"), ok])), \
                patch.object(self.data_feed, "_sleep", new=AsyncMock()):
            resp = await self.data_feed.fetch_candles(end_time=1672992180)
        self.assertIs(ok, resp)

    # Heartbeats

    def test_heartbeat(self):
        self.assertEqual([BASE_TS, 5.0, 5.0, 5.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                         AjaibSpotCandles._heartbeat(BASE_TS, 5.0))

    def test_fill_gaps_and_append_inserts_heartbeats_with_previous_close(self):
        self.data_feed._fill_gaps_and_append(self._candle(BASE_TS, close=99.5))
        self.data_feed._fill_gaps_and_append(self._candle(BASE_TS + 180, close=101.0))
        self.assertEqual([BASE_TS, BASE_TS + 60, BASE_TS + 120, BASE_TS + 180], [c[0] for c in self.data_feed._candles])
        self.assertEqual(AjaibSpotCandles._heartbeat(BASE_TS + 60, 99.5), self.data_feed._candles[1])
        self.assertEqual(AjaibSpotCandles._heartbeat(BASE_TS + 120, 99.5), self.data_feed._candles[2])

    def test_ensure_heartbeats_to_current_time_stops_before_current_interval(self):
        self.data_feed._candles.append(self._candle(BASE_TS, close=105.0))
        with patch.object(self.data_feed, "_time", return_value=BASE_TS + 210):
            self.data_feed._ensure_heartbeats_to_current_time()
        self.assertEqual([BASE_TS, BASE_TS + 60, BASE_TS + 120], [c[0] for c in self.data_feed._candles])
        self.assertEqual(105.0, self.data_feed._candles[-1][4])

    def test_ensure_heartbeats_to_current_time_no_op_when_empty(self):
        self.data_feed._ensure_heartbeats_to_current_time()
        self.assertEqual(0, len(self.data_feed._candles))

    def test_fill_historical_gaps_starts_at_first_real_candle(self):
        real = [self._candle(BASE_TS - 240, close=82.0), self._candle(BASE_TS - 120, close=93.0)]
        result = self.data_feed._fill_historical_gaps_with_heartbeats(real, end_timestamp=BASE_TS - 60)
        self.assertEqual([BASE_TS - 240, BASE_TS - 180, BASE_TS - 120, BASE_TS - 60], [c[0] for c in result])
        self.assertEqual(AjaibSpotCandles._heartbeat(BASE_TS - 180, 82.0), result[1])
        self.assertEqual(AjaibSpotCandles._heartbeat(BASE_TS - 60, 93.0), result[3])

    def test_fill_historical_gaps_returns_nothing_without_real_candles(self):
        self.assertEqual([], self.data_feed._fill_historical_gaps_with_heartbeats([], BASE_TS))
        self.assertEqual([], self.data_feed._fill_historical_gaps_with_heartbeats([self._candle(BASE_TS)], BASE_TS - 60))

    # Historical backfill

    async def test_fill_historical_candles_beyond_one_page(self):
        """max_records above one page: walks back page by page, all real, no gaps."""
        feed = self._make_feed(max_records=1500)
        feed._candles.append(self._candle(BASE_TS))
        feed._ws_candle_available.set()
        calls = []
        with patch.object(feed, "fetch_candles", new=self._page_fetcher(feed, calls=calls)), \
                patch.object(feed, "_sleep", new=AsyncMock()):
            await feed.fill_historical_candles()

        self.assertTrue(feed.ready)
        self.assertEqual(BASE_TS, feed._candles[-1][0])
        self._assert_equidistant(feed)
        self.assertTrue(all(c[5] > 0 for c in feed._candles), "No heartbeats expected when every bucket traded")
        self.assertTrue(all(limit <= feed.candles_max_result_per_rest_request for limit in calls))
        self.assertEqual(2, len(calls))

    async def test_fill_historical_candles_sparse_trades_has_no_look_ahead(self):
        feed = self._make_feed(max_records=1500)
        feed._candles.append(self._candle(BASE_TS))
        feed._ws_candle_available.set()
        with patch.object(feed, "fetch_candles", new=self._page_fetcher(feed, has_trade=lambda ts: ts % 420 == 0)), \
                patch.object(feed, "_sleep", new=AsyncMock()):
            await feed.fill_historical_candles()

        self.assertTrue(feed.ready)
        self._assert_equidistant(feed)
        self._assert_no_look_ahead(feed)

    async def test_fill_historical_candles_steps_over_pages_without_trades(self):
        """Empty pages widen the search; the gap is filled with the older close, not a later one."""
        feed = self._make_feed(max_records=50)
        feed._candles.append(self._candle(BASE_TS))
        feed._ws_candle_available.set()
        last_trade = BASE_TS - 2001 * 60
        with patch.object(feed, "fetch_candles", new=self._page_fetcher(feed, has_trade=lambda ts: ts <= last_trade)), \
                patch.object(feed, "_sleep", new=AsyncMock()):
            await feed.fill_historical_candles()

        self.assertTrue(feed.ready)
        self._assert_equidistant(feed)
        self.assertTrue(all(c[4] == last_trade for c in list(feed._candles)[:-1]))

    async def test_fill_historical_candles_gives_up_when_no_history(self):
        feed = self._make_feed(max_records=50)
        feed._candles.append(self._candle(BASE_TS))
        feed._ws_candle_available.set()
        calls = []
        with patch.object(feed, "fetch_candles", new=self._page_fetcher(feed, has_trade=lambda ts: False, calls=calls)), \
                patch.object(feed, "_sleep", new=AsyncMock()):
            await feed.fill_historical_candles()

        self.assertEqual(1, len(feed._candles), "Nothing is made up when there is no older trade")
        self.assertEqual(CONSTANTS.HISTORICAL_FILL_MAX_PAGES, len(calls))
        self.assertTrue(any(r.levelname == "WARNING" for r in self.log_records))

    async def test_fill_historical_candles_keeps_newest_candle_when_polling_races(self):
        feed = self._make_feed(max_records=10)
        feed._candles.append(self._candle(BASE_TS))
        feed._ws_candle_available.set()

        def poll_appends_while_fetching():
            while len(feed._candles) < 9:
                feed._fill_gaps_and_append(self._candle(feed._candles[-1][0] + 60))

        with patch.object(feed, "fetch_candles", new=self._page_fetcher(feed, during_fetch=poll_appends_while_fetching)), \
                patch.object(feed, "_sleep", new=AsyncMock()):
            await feed.fill_historical_candles()

        self.assertTrue(feed.ready)
        self.assertEqual(BASE_TS + 8 * 60, feed._candles[-1][0])
        self._assert_equidistant(feed)

    async def test_fill_historical_candles_guard_prevents_reentrance(self):
        self.data_feed._historical_fill_in_progress = True
        self.data_feed._ws_candle_available.set()
        with patch.object(self.data_feed, "fetch_candles", new=AsyncMock()) as mock_fetch:
            await self.data_feed.fill_historical_candles()
        mock_fetch.assert_not_called()

    # Seeding and polling

    @patch.object(ajaib_module, "safe_ensure_future")
    async def test_seed_uses_older_close_when_recent_window_is_empty(self, mock_ensure_future):
        feed = self._make_feed(max_records=500)
        rows = np.array([self._candle(BASE_TS - 300 * 60), self._candle(BASE_TS - 298 * 60)])
        fetch = AsyncMock(side_effect=[np.array([]), rows])
        with patch.object(feed, "fetch_candles", new=fetch), patch.object(feed, "_time", return_value=BASE_TS + 30):
            self.assertTrue(await feed._seed_candles())

        self.assertEqual(BASE_TS - 300 * 60, feed._candles[0][0])
        self.assertEqual(BASE_TS - 60, feed._candles[-1][0])
        self._assert_equidistant(feed)
        self._assert_no_look_ahead(feed)
        self.assertTrue(feed._ws_candle_available.is_set())
        coroutine = mock_ensure_future.call_args[0][0]
        self.assertTrue(inspect.iscoroutine(coroutine))
        coroutine.close()

    @patch.object(ajaib_module, "safe_ensure_future")
    async def test_seed_fills_intervals_missing_from_rest_rows(self, mock_ensure_future):
        rows = np.array([self._candle(BASE_TS - 600), self._candle(BASE_TS - 360), self._candle(BASE_TS - 60)])
        with patch.object(self.data_feed, "fetch_candles", new=AsyncMock(return_value=rows)), \
                patch.object(self.data_feed, "_time", return_value=BASE_TS + 5):
            await self.data_feed._seed_candles()
        self.assertEqual(10, len(self.data_feed._candles))
        self._assert_equidistant(self.data_feed)
        mock_ensure_future.call_args[0][0].close()

    async def test_seed_returns_false_when_no_trades_found(self):
        fetch = AsyncMock(return_value=np.array([]))
        with patch.object(self.data_feed, "fetch_candles", new=fetch):
            self.assertFalse(await self.data_feed._seed_candles())
        self.assertEqual(0, len(self.data_feed._candles))
        self.assertEqual(1 + CONSTANTS.SEED_LOOKBACK_PAGES, fetch.call_count)

    async def test_poll_and_update_seeds_when_empty(self):
        with patch.object(self.data_feed, "_seed_candles", new=AsyncMock()) as seed:
            await self.data_feed._poll_and_update()
        seed.assert_awaited_once()

    async def test_poll_and_update_appends_and_fills_gaps(self):
        self.data_feed._candles.append(self._candle(BASE_TS))
        rest = MagicMock()
        rest.execute_request = AsyncMock(return_value=[
            [int((BASE_TS + 180) * 1000), "1", "1", "1", "1", "3", 0],
            [int(BASE_TS * 1000), "2", "2", "2", "2", "5", 0],
        ])
        with patch.object(self.data_feed, "_time", return_value=BASE_TS + 250), \
                patch.object(self.data_feed._api_factory, "get_rest_assistant", new=AsyncMock(return_value=rest)):
            await self.data_feed._poll_and_update()

        self.assertEqual([BASE_TS, BASE_TS + 60, BASE_TS + 120, BASE_TS + 180], [c[0] for c in self.data_feed._candles])
        self.assertEqual(5.0, self.data_feed._candles[0][5], "Same-bucket row replaces the live candle")
        self.assertEqual(2.0, self.data_feed._candles[1][4])

    async def test_poll_and_update_propagates_errors(self):
        self.data_feed._candles.append(self._candle(BASE_TS))
        rest = MagicMock()
        rest.execute_request = AsyncMock(side_effect=RuntimeError("boom"))
        with patch.object(self.data_feed._api_factory, "get_rest_assistant", new=AsyncMock(return_value=rest)):
            with self.assertRaises(RuntimeError):
                await self.data_feed._poll_and_update()

    async def test_polling_loop_logs_config_error_once_without_traceback(self):
        polls = 0

        async def missing_credentials():
            nonlocal polls
            polls += 1
            if polls == 3:
                self.data_feed._shutdown_event.set()
            raise ValueError("Ajaib candles require Ajaib API credentials.")

        delays = []
        self.data_feed._is_running = True
        with patch.object(self.data_feed, "_poll_and_update", new=missing_credentials), \
                patch.object(self.data_feed, "_poll_delay", side_effect=lambda n: delays.append(n) or 0.001):
            await self.data_feed._polling_loop()

        errors = [r for r in self.log_records if r.levelname == "ERROR"]
        self.assertEqual(1, len(errors))
        self.assertIn("credentials", errors[0].getMessage())
        self.assertIsNone(errors[0].exc_info)
        self.assertEqual([1, 2, 3], delays)

    def test_poll_delay_backs_off_exponentially(self):
        self.assertEqual(CONSTANTS.POLL_INTERVAL, AjaibSpotCandles._poll_delay(0))
        self.assertEqual(CONSTANTS.POLL_INTERVAL, AjaibSpotCandles._poll_delay(1))
        self.assertEqual(CONSTANTS.POLL_INTERVAL * 4, AjaibSpotCandles._poll_delay(3))
        self.assertEqual(CONSTANTS.POLL_ERROR_BACKOFF_MAX, AjaibSpotCandles._poll_delay(20))

    # Shared connections factory

    async def test_shared_connections_factory_is_per_event_loop(self):
        built = []

        def build(proxy_url):
            built.append(proxy_url)
            return MagicMock()

        with patch.object(ajaib_module.ajaib_web_utils, "_build_connections_factory", side_effect=build), \
                patch.dict(ajaib_module._CONNECTIONS_FACTORIES, clear=True):
            shared = _SharedConnectionsFactory("socks5://proxy:1080")
            here = shared._factory()
            self.assertIs(here, _SharedConnectionsFactory("socks5://proxy:1080")._factory())

            other = {}
            thread = threading.Thread(target=lambda: other.update(factory=asyncio.run(self._call_factory(shared))))
            thread.start()
            thread.join()

            self.assertIsNot(here, other["factory"])
            self.assertEqual(2, len(built))
            # The other loop has closed; its entry is dropped on the next lookup.
            shared._factory()
            self.assertEqual(1, len(ajaib_module._CONNECTIONS_FACTORIES))

    @staticmethod
    async def _call_factory(shared):
        return shared._factory()
