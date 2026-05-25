import asyncio
import json
import re
from test.hummingbot.data_feed.candles_feed.test_candles_base import TestCandlesBase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses

from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.coindcx_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.coindcx_spot_candles.coindcx_spot_candles import CoinDCXSpotCandles


class TestCoinDCXSpotCandles(TestCandlesBase):
    __test__ = True
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.interval = "1m"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = "B-BTC_USDT"
        cls.max_records = 150

    def setUp(self) -> None:
        super().setUp()
        self.data_feed = CoinDCXSpotCandles(
            trading_pair=self.trading_pair,
            interval=self.interval,
            max_records=self.max_records,
        )
        self.data_feed._ex_trading_pair = self.ex_trading_pair
        self.log_records = []
        self.data_feed.logger().setLevel(1)
        self.data_feed.logger().addHandler(self)
        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        super().tearDown()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.mocking_assistant = NetworkMockingAssistant()

    async def asyncTearDown(self):
        if hasattr(self.data_feed, "_polling_task") and self.data_feed._polling_task:
            await self.data_feed.stop_network()
        await super().asyncTearDown()

    @staticmethod
    def get_candles_rest_data_mock():
        """
        CoinDCX REST API response format: list of dicts, newest first,
        timestamps in milliseconds.
        """
        return [
            {"time": 1672992180000, "open": "16802.11", "high": "16812.22",
             "low": "16791.47", "close": "16802.11", "volume": "5475.13940"},
            {"time": 1672992120000, "open": "16794.06", "high": "16802.87",
             "low": "16780.15", "close": "16794.06", "volume": "5763.44917"},
            {"time": 1672992060000, "open": "16786.86", "high": "16816.45",
             "low": "16779.96", "close": "16786.86", "volume": "6529.22759"},
            {"time": 1672992000000, "open": "16810.18", "high": "16823.63",
             "low": "16792.12", "close": "16810.18", "volume": "6230.44034"},
        ]

    def get_fetch_candles_data_mock(self):
        """Processed 10-field float lists, oldest first (ascending timestamps)."""
        return [
            [1672992000.0, 16810.18, 16823.63, 16792.12, 16810.18, 6230.44034, 0.0, 0.0, 0.0, 0.0],
            [1672992060.0, 16786.86, 16816.45, 16779.96, 16786.86, 6529.22759, 0.0, 0.0, 0.0, 0.0],
            [1672992120.0, 16794.06, 16802.87, 16780.15, 16794.06, 5763.44917, 0.0, 0.0, 0.0, 0.0],
            [1672992180.0, 16802.11, 16812.22, 16791.47, 16802.11, 5475.13940, 0.0, 0.0, 0.0, 0.0],
        ]

    @staticmethod
    def get_candles_ws_data_mock_1():
        """WebSocket not supported for CoinDCX."""
        return {}

    @staticmethod
    def get_candles_ws_data_mock_2():
        """WebSocket not supported for CoinDCX."""
        return {}

    @staticmethod
    def _success_subscription_mock():
        """WebSocket not supported for CoinDCX."""
        return {}

    async def test_listen_for_subscriptions_subscribes_to_klines(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_process_websocket_messages_empty_candle(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_process_websocket_messages_duplicated_candle_not_included(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_process_websocket_messages_with_two_valid_messages(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed._parse_websocket_message({})

    async def test_subscribe_channels_raises_cancel_exception(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_listen_for_subscriptions_raises_cancel_exception(self):
        """CoinDCX uses polling; WebSocket is not supported."""
        with self.assertRaises(NotImplementedError):
            self.data_feed.ws_subscription_payload()

    async def test_listen_for_subscriptions_logs_exception_details(self):
        """CoinDCX uses polling; error logging tested via _poll_and_update."""
        mock_rest = MagicMock()
        mock_rest.execute_request = AsyncMock(side_effect=Exception("TEST ERROR."))

        with patch.object(self.data_feed._api_factory, "get_rest_assistant",
                          return_value=mock_rest):
            await self.data_feed._poll_and_update()

        self.assertTrue(
            any("TEST ERROR." in str(r.getMessage()) for r in self.log_records
                if r.levelname == "ERROR")
        )

    def test_name_property(self):
        self.assertEqual(self.data_feed.name, f"coindcx_{self.trading_pair}")

    def test_rest_url_property(self):
        self.assertEqual(self.data_feed.rest_url, CONSTANTS.PUBLIC_REST_URL)

    def test_wss_url_property(self):
        self.assertIsNone(self.data_feed.wss_url)

    def test_health_check_url_property(self):
        expected = CONSTANTS.REST_URL + CONSTANTS.HEALTH_CHECK_ENDPOINT
        self.assertEqual(self.data_feed.health_check_url, expected)

    def test_candles_url_property(self):
        expected = CONSTANTS.PUBLIC_REST_URL + CONSTANTS.CANDLES_ENDPOINT
        self.assertEqual(self.data_feed.candles_url, expected)

    def test_rate_limits_property(self):
        self.assertEqual(self.data_feed.rate_limits, CONSTANTS.RATE_LIMITS)

    def test_intervals_property(self):
        self.assertEqual(self.data_feed.intervals, CONSTANTS.INTERVALS)

    def test_candles_max_result_per_rest_request_property(self):
        self.assertEqual(
            self.data_feed.candles_max_result_per_rest_request,
            CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
        )

    # ── get_exchange_trading_pair ─────────────────────────────────────────

    def test_get_exchange_trading_pair_btc_usdt(self):
        self.assertEqual(
            self.data_feed.get_exchange_trading_pair("BTC-USDT"), "B-BTC_USDT"
        )

    def test_get_exchange_trading_pair_eth_usdt(self):
        self.assertEqual(
            self.data_feed.get_exchange_trading_pair("ETH-USDT"), "B-ETH_USDT"
        )

    def test_get_rest_candles_params_no_times(self):
        params = self.data_feed._get_rest_candles_params()
        self.assertEqual(params["pair"], self.ex_trading_pair)
        self.assertEqual(params["interval"], self.interval)
        self.assertIn("limit", params)
        self.assertNotIn("startTime", params)
        self.assertNotIn("endTime", params)

    def test_get_rest_candles_params_with_start_time(self):
        start_time = 1672992000
        params = self.data_feed._get_rest_candles_params(start_time=start_time)
        self.assertEqual(params["startTime"], start_time * 1000)

    def test_get_rest_candles_params_with_end_time(self):
        end_time = 1672995600
        params = self.data_feed._get_rest_candles_params(end_time=end_time)
        self.assertEqual(params["endTime"], end_time * 1000)

    def test_get_rest_candles_params_with_limit(self):
        params = self.data_feed._get_rest_candles_params(limit=50)
        self.assertEqual(params["limit"], 50)

    def test_parse_rest_candles_success(self):
        raw = self.get_candles_rest_data_mock()
        result = self.data_feed._parse_rest_candles(raw)

        self.assertEqual(len(result), 4)
        # Should be sorted oldest-first
        for i in range(1, len(result)):
            self.assertLess(result[i - 1][0], result[i][0])

        first = result[0]
        self.assertEqual(len(first), 10)
        self.assertAlmostEqual(first[0], 1672992000.0)
        self.assertAlmostEqual(first[1], 16810.18)   # open
        self.assertAlmostEqual(first[2], 16823.63)   # high
        self.assertAlmostEqual(first[3], 16792.12)   # low
        self.assertAlmostEqual(first[4], 16810.18)   # close
        self.assertAlmostEqual(first[5], 6230.44034) # volume
        self.assertEqual(first[6], 0.0)
        self.assertEqual(first[7], 0.0)
        self.assertEqual(first[8], 0.0)
        self.assertEqual(first[9], 0.0)

    def test_parse_rest_candles_empty(self):
        self.assertEqual(self.data_feed._parse_rest_candles([]), [])

    def test_parse_rest_candles_filters_by_end_time(self):
        raw = self.get_candles_rest_data_mock()
        # Only candles with timestamp <= 1672992060 should be kept
        result = self.data_feed._parse_rest_candles(raw, end_time=1672992060)
        self.assertEqual(len(result), 2)
        self.assertLessEqual(result[-1][0], 1672992060.0)

    def test_parse_rest_candles_converts_ms_to_s(self):
        raw = [{"time": 1672992000000, "open": "100", "high": "110",
                "low": "90", "close": "105", "volume": "1000"}]
        result = self.data_feed._parse_rest_candles(raw)
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0][0], 1672992000.0)

    def test_fill_gaps_and_append_first_candle(self):
        candle = [1672992000.0, 100, 110, 90, 105, 1000, 0, 0, 0, 0]
        self.data_feed._fill_gaps_and_append(candle)
        self.assertEqual(len(self.data_feed._candles), 1)
        self.assertEqual(list(self.data_feed._candles[0]), candle)

    def test_fill_gaps_and_append_no_gap(self):
        first = [1672992000.0, 100, 110, 90, 105, 1000, 0, 0, 0, 0]
        second = [1672992060.0, 105, 115, 100, 110, 1500, 0, 0, 0, 0]
        self.data_feed._candles.append(first)
        self.data_feed._fill_gaps_and_append(second)
        self.assertEqual(len(self.data_feed._candles), 2)

    def test_fill_gaps_and_append_with_gap_inserts_heartbeats(self):
        first = [1672992000.0, 100, 110, 90, 105, 1000, 0, 0, 0, 0]
        new_candle = [1672992180.0, 106, 120, 104, 115, 2000, 0, 0, 0, 0]  
        self.data_feed._candles.append(first)
        self.data_feed._fill_gaps_and_append(new_candle)

        self.assertEqual(len(self.data_feed._candles), 4)

        self.assertEqual(self.data_feed._candles[1][5], 0.0)
        self.assertEqual(self.data_feed._candles[2][5], 0.0)

        self.assertAlmostEqual(
            self.data_feed._candles[1][0], 1672992060.0
        )
        self.assertAlmostEqual(
            self.data_feed._candles[2][0], 1672992120.0
        )

        self.assertEqual(self.data_feed._candles[1][4], 105)

        self.assertEqual(list(self.data_feed._candles[-1]), new_candle)

    def test_fill_gaps_and_append_heartbeat_carries_close(self):
        first = [1672992000.0, 100, 110, 90, 99.5, 1000, 0, 0, 0, 0]
        next_candle = [1672992120.0, 99.5, 100, 98, 99, 500, 0, 0, 0, 0]
        self.data_feed._candles.append(first)
        self.data_feed._fill_gaps_and_append(next_candle)

        heartbeat = self.data_feed._candles[1]
        self.assertEqual(heartbeat[1], 99.5) 
        self.assertEqual(heartbeat[2], 99.5)  
        self.assertEqual(heartbeat[3], 99.5)  
        self.assertEqual(heartbeat[4], 99.5)  

    async def test_check_network_success(self):
        mock_rest = MagicMock()
        mock_rest.execute_request = AsyncMock()

        with patch.object(self.data_feed._api_factory, "get_rest_assistant",
                          return_value=mock_rest):
            status = await self.data_feed.check_network()

        self.assertEqual(status, NetworkStatus.CONNECTED)
        mock_rest.execute_request.assert_called_once()

    @aioresponses()
    async def test_fetch_candles(self, mock_api):
        regex_url = re.compile(
            f"^{re.escape(self.data_feed.candles_url)}"
        )
        mock_api.get(url=regex_url, body=json.dumps(self.get_candles_rest_data_mock()))

        resp = await self.data_feed.fetch_candles(
            start_time=int(self.start_time),
            end_time=int(self.end_time),
        )

        self.assertEqual(resp.shape[0], len(self.get_fetch_candles_data_mock()))
        self.assertEqual(resp.shape[1], 10)

    def test_rate_limits_has_root_raw_entry(self):
        root_ids = {r.limit_id for r in CONSTANTS.RATE_LIMITS}
        self.assertIn("raw", root_ids)

    def test_rate_limits_linked_to_raw(self):
        non_root = [r for r in CONSTANTS.RATE_LIMITS if r.limit_id != "raw"]
        for rate_limit in non_root:
            linked_ids = [ll.limit_id for ll in rate_limit.linked_limits]
            self.assertIn("raw", linked_ids,
                          f"{rate_limit.limit_id} should be linked to 'raw'")

    def test_intervals_contains_standard_values(self):
        for iv in ["1m", "5m", "15m", "1h", "4h", "1d"]:
            self.assertIn(iv, CONSTANTS.INTERVALS)

    def test_interval_in_seconds_for_1m(self):
        self.assertEqual(self.data_feed.interval_in_seconds, 60)
