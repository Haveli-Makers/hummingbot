import asyncio
import logging
import time
from typing import List, Optional

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.coinex_perpetual_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class CoinexPerpetualCandles(CandlesBase):

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)
        self._polling_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._historical_fill_in_progress = False

    @property
    def name(self) -> str:
        return f"coinex_perpetual_{self._trading_pair}"

    @property
    def rest_url(self) -> str:
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return None

    @property
    def health_check_url(self) -> str:
        return CONSTANTS.REST_URL + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self) -> str:
        return CONSTANTS.REST_URL + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self) -> str:
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self) -> int:
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    @property
    def _api_period(self) -> str:
        """CoinEx's own 'period' string for self.interval (e.g. '1m' -> '1min')."""
        return CONSTANTS.INTERVALS[self.interval]

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT,
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair: str) -> str:
        return trading_pair.replace("-", "").upper()

    async def fill_historical_candles(self):
        if self._historical_fill_in_progress:
            return
        self._historical_fill_in_progress = True
        try:
            await self._ws_candle_available.wait()
            iteration = 0
            max_iterations = 20
            while not self.ready and len(self._candles) > 0 and iteration < max_iterations:
                iteration += 1
                try:
                    oldest_timestamp = self._candles[0][0]
                    missing_records = self._candles.maxlen - len(self._candles)
                    if missing_records <= 0:
                        break
                    end_timestamp = oldest_timestamp - self.interval_in_seconds
                    start_timestamp = end_timestamp - (missing_records * self.interval_in_seconds)
                    candles_np = await self.fetch_candles(
                        start_time=start_timestamp,
                        end_time=end_timestamp + self.interval_in_seconds,
                    )
                    real_candles = candles_np.tolist() if candles_np.size > 0 else []
                    filled = self._fill_historical_gaps_with_heartbeats(
                        real_candles, start_timestamp, end_timestamp
                    )
                    if not filled:
                        break
                    candles_to_add = (
                        filled[-missing_records:]
                        if len(filled) > missing_records
                        else filled
                    )
                    for candle in reversed(candles_to_add):
                        self._candles.appendleft(candle)
                    await self._sleep(0.1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().exception(
                        f"Error during historical fill iteration {iteration}: {e}"
                    )
                    await self._sleep(1.0)
        finally:
            self._historical_fill_in_progress = False

    def _fill_historical_gaps_with_heartbeats(
        self, candles: List[List[float]], start_timestamp: float, end_timestamp: float
    ) -> List[List[float]]:
        fallback_close = self._candles[0][4] if self._candles else 0.0

        candle_map = {
            self._round_timestamp_to_interval_multiple(c[0]): c
            for c in candles
        }

        result: List[List[float]] = []
        current_ts = float(self._round_timestamp_to_interval_multiple(start_timestamp))
        prev_close = fallback_close
        interval_count = 0

        while current_ts <= end_timestamp and interval_count < 1000:
            if current_ts in candle_map:
                candle = candle_map[current_ts]
                result.append(candle)
                prev_close = candle[4]
            else:
                result.append(
                    [current_ts, prev_close, prev_close, prev_close, prev_close,
                     0.0, 0.0, 0.0, 0.0, 0.0]
                )
            current_ts += self.interval_in_seconds
            interval_count += 1

        return result

    async def start_network(self):
        await self.stop_network()
        await self.initialize_exchange_data()
        self._is_running = True
        self._shutdown_event.clear()
        self._polling_task = asyncio.create_task(self._polling_loop())

    async def stop_network(self):
        if self._polling_task and not self._polling_task.done():
            self._is_running = False
            self._shutdown_event.set()
            try:
                await asyncio.wait_for(self._polling_task, timeout=10.0)
            except asyncio.TimeoutError:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
        self._polling_task = None
        self._is_running = False

    def _get_rest_candles_params(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        params: dict = {
            "market": self._ex_trading_pair,
            "period": self._api_period,
            "limit": min(limit or CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
                         CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST),
        }
        if start_time:
            params["start_time"] = int(start_time * 1000)
        if end_time:
            params["end_time"] = int(end_time * 1000)
        return params

    def _parse_rest_candles(
        self, data, end_time: Optional[int] = None
    ) -> List[List[float]]:
        if not data:
            return []

        if isinstance(data, dict):
            code = data.get("code")
            if code not in (0, None):
                self.logger().error(f"CoinEx Perpetual: kline request failed (code={code}): {data.get('message')}")
                return []
            rows = data.get("data") or []
        else:
            rows = data

        candles = []
        for row in rows:
            try:
                timestamp = self.ensure_timestamp_in_seconds(row["created_at"])
                if end_time and timestamp > end_time:
                    continue
                candles.append([
                    timestamp,
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["volume"]),
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ])
            except Exception as e:
                self.logger().error(f"CoinEx Perpetual: error parsing candle row {row}: {e}")
        candles.sort(key=lambda x: x[0])
        return candles

    async def listen_for_subscriptions(self):
        if not self._is_running:
            await self.start_network()
        if self._polling_task:
            try:
                await self._polling_task
            except asyncio.CancelledError:
                self.logger().info("CoinEx Perpetual candles subscription cancelled.")
                raise

    def ws_subscription_payload(self):
        raise NotImplementedError("WebSocket not supported for CoinEx Perpetual candles; polling is used instead.")

    def _parse_websocket_message(self, data: dict):
        raise NotImplementedError("WebSocket not supported for CoinEx Perpetual candles; polling is used instead.")

    async def _polling_loop(self):
        try:
            self.logger().info(
                f"Starting CoinEx Perpetual candles polling for {self._trading_pair} [{self.interval}]"
            )
            await self._initialize_candles()

            while self._is_running and not self._shutdown_event.is_set():
                try:
                    await self._poll_and_update()
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=CONSTANTS.POLL_INTERVAL,
                        )
                        break
                    except asyncio.TimeoutError:
                        continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().exception(f"CoinEx Perpetual candles polling error: {e}")
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                        break
                    except asyncio.TimeoutError:
                        continue
        finally:
            self._is_running = False
            self.logger().info("CoinEx Perpetual candles polling loop stopped.")

    async def _initialize_candles(self):
        try:
            candles = await self.fetch_candles(
                end_time=int(time.time()),
                limit=10,
            )
            if candles.size > 0:
                self._candles.extend(candles)
                self._ws_candle_available.set()
                safe_ensure_future(self.fill_historical_candles())
                self.logger().info(
                    f"CoinEx Perpetual candles seeded with {len(self._candles)} recent candles "
                    f"for {self._trading_pair} [{self.interval}]; backfill scheduled."
                )
        except Exception as e:
            self.logger().error(
                f"Error initialising CoinEx Perpetual candles for {self._trading_pair}: {e}",
                exc_info=True,
            )

    def _fill_gaps_and_append(self, new_candle: List[float]) -> None:
        """Insert heartbeat candles for any skipped intervals then append new_candle."""
        if not self._candles:
            self._candles.append(new_candle)
            return

        last_ts = self._candles[-1][0]
        new_ts = new_candle[0]
        next_ts = last_ts + self.interval_in_seconds

        while next_ts < new_ts:
            prev = self._candles[-1]
            close_price = prev[4]
            heartbeat = [next_ts, close_price, close_price, close_price, close_price, 0.0, 0.0, 0.0, 0.0, 0.0]
            self._candles.append(heartbeat)
            self.logger().debug(f"CoinEx Perpetual: inserted heartbeat candle at {next_ts}")
            next_ts += self.interval_in_seconds

        self._candles.append(new_candle)

    def _ensure_heartbeats_to_current_time(self) -> None:
        if not self._candles:
            return
        current_interval_ts = self._round_timestamp_to_interval_multiple(self._time())
        next_ts = self._candles[-1][0] + self.interval_in_seconds
        while next_ts < current_interval_ts:
            prev_close = self._candles[-1][4]
            heartbeat = [next_ts, prev_close, prev_close, prev_close, prev_close,
                         0.0, 0.0, 0.0, 0.0, 0.0]
            self._candles.append(heartbeat)
            self.logger().debug(f"CoinEx Perpetual: heartbeat candle inserted at {next_ts}")
            next_ts += self.interval_in_seconds

    async def _poll_and_update(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=self.candles_url,
                throttler_limit_id=self._rest_throttler_limit_id,
                params={
                    "market": self._ex_trading_pair,
                    "period": self._api_period,
                    "limit": 10,
                },
                method=self._rest_method,
            )

            candles = self._parse_rest_candles(data)

            if not self._candles:
                if candles:
                    self._candles.append(candles[-1])
                    self._ws_candle_available.set()
                    safe_ensure_future(self.fill_historical_candles())
                return

            for candle in candles:
                candle_ts = candle[0]
                last_ts = self._candles[-1][0]
                if candle_ts > last_ts:
                    self._fill_gaps_and_append(candle)
                elif candle_ts == last_ts:
                    self._candles[-1] = candle

            self._ensure_heartbeats_to_current_time()

        except Exception as e:
            self.logger().error(f"CoinEx Perpetual candles poll error: {e}", exc_info=True)
