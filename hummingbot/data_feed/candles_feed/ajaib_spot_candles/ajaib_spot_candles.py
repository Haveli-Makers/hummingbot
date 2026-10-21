import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from hummingbot.connector.exchange.ajaib import ajaib_web_utils
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.candles_feed.ajaib_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger


class _AjaibSigningPreProcessor(RESTPreProcessorBase):
    """
    Signs every request this feed sends. Ajaib has no anonymous endpoints, and
    CandlesBase.fetch_candles never asks for authentication, so signing is done
    here instead of through ``is_auth_required``.

    The auth is built on first use so the feed can still be constructed (and
    its intervals listed) on a server that has no Ajaib credentials configured.
    """

    def __init__(self):
        self._auth: Optional[AjaibAuth] = None

    def _get_auth(self) -> AjaibAuth:
        if self._auth is None:
            api_key = os.environ.get(CONSTANTS.API_KEY_ENV_VAR, "")
            api_secret = os.environ.get(CONSTANTS.API_SECRET_ENV_VAR, "")
            if not api_key or not api_secret:
                raise ValueError(
                    f"Ajaib candles require Ajaib API credentials. Set the "
                    f"{CONSTANTS.API_KEY_ENV_VAR} and {CONSTANTS.API_SECRET_ENV_VAR} "
                    f"environment variables (and {CONSTANTS.PROXY_URL_ENV_VAR} if the "
                    f"server's IP is not allowlisted) in the api-server's .env file and "
                    f"restart the backend."
                )
            self._auth = AjaibAuth(api_key=api_key, secret_key=api_secret, time_provider=TimeSynchronizer())
        return self._auth

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        return await self._get_auth().rest_authenticate(request)


_CONNECTIONS_FACTORIES: Dict[Tuple[asyncio.AbstractEventLoop, Optional[str]], Any] = {}


class _SharedConnectionsFactory:
    """
    Ajaib's API is geo-blocked, so the proxy URL is forwarded to every connection.
    The factory is shared across all Ajaib feeds in the same event loop so the proxy
    is not reconnected for every feed. The factory is keyed by (loop, proxy_url) so
    multiple loops can each have their own factory.
    """

    def __init__(self, proxy_url: Optional[str]):
        self._proxy_url = proxy_url

    def _factory(self):
        loop = asyncio.get_running_loop()
        for key in [key for key in _CONNECTIONS_FACTORIES if key[0].is_closed()]:
            del _CONNECTIONS_FACTORIES[key]
        key = (loop, self._proxy_url)
        if key not in _CONNECTIONS_FACTORIES:
            _CONNECTIONS_FACTORIES[key] = ajaib_web_utils._build_connections_factory(self._proxy_url)
        return _CONNECTIONS_FACTORIES[key]

    async def get_rest_connection(self):
        return await self._factory().get_rest_connection()

    async def get_ws_connection(self):
        return await self._factory().get_ws_connection()

    async def close(self):
        pass


class AjaibSpotCandles(CandlesBase):
    """
    Polls Ajaib's signed GET /v1/klines. Rows are
    [openTime, open, high, low, close, volume, closeTime] with ms timestamps;
    the time window is ``start_time``/``end_time`` (both inclusive, ms) --
    the Binance-style ``startTime``/``endTime`` are silently ignored.
    """

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)
        proxy_url = os.environ.get(CONSTANTS.PROXY_URL_ENV_VAR, "") or None
        self._api_factory = WebAssistantsFactory(
            throttler=AsyncThrottler(rate_limits=self.rate_limits),
            connections_factory=_SharedConnectionsFactory(proxy_url),
            rest_pre_processors=[_AjaibSigningPreProcessor()],
        )
        self._polling_task: Optional[asyncio.Task] = None
        self._historical_fill_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._historical_fill_in_progress = False

    @property
    def name(self) -> str:
        return f"ajaib_{self._trading_pair}"

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
        # _get_rest_candles_params widens the window by up to two buckets so
        # the first and last buckets are complete; leave room for them so the
        # native request never exceeds the server's 1000-row cap.
        return max(1, CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST // self._resample_ratio - 2)

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    def _resolve_native_interval(self) -> str:
        """
        Resolve the largest Ajaib-native interval (CONSTANTS.NATIVE_INTERVALS) that
        evenly divides self.interval, so it can be fetched and resampled client-side.
        """
        target_seconds = self.interval_in_seconds
        for native in CONSTANTS.NATIVE_INTERVALS:
            native_seconds = self.get_seconds_from_interval(native)
            if native_seconds <= target_seconds and target_seconds % native_seconds == 0:
                return native
        raise ValueError(
            f"Ajaib cannot provide '{self.interval}' candles: its API only supports "
            f"{CONSTANTS.NATIVE_INTERVALS} as base intervals, and none of them evenly "
            f"divide '{self.interval}'."
        )

    @property
    def _native_interval(self) -> str:
        return self._resolve_native_interval()

    @property
    def _resample_ratio(self) -> int:
        return self.interval_in_seconds // self.get_seconds_from_interval(self._native_interval)

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT,
            params={"symbol": self._ex_trading_pair, "interval": "1m", "limit": 1},
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair: str) -> str:
        # Symbols are case-sensitive: BTC_IDR works, btc_idr does not.
        return trading_pair.replace("-", "_").upper()

    async def fetch_candles(self,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None,
                            limit: Optional[int] = None):
        # The proxy occasionally resets a connection outright; one bad hop
        # should not abort a multi-page historical download.
        for attempt in range(CONSTANTS.CONNECTION_RETRIES + 1):
            try:
                return await super().fetch_candles(start_time=start_time, end_time=end_time, limit=limit)
            except (aiohttp.ClientConnectionError, ConnectionError) as e:
                if attempt == CONSTANTS.CONNECTION_RETRIES:
                    raise
                self.logger().warning(f"Ajaib kline request failed ({e}); retrying.")
                await self._sleep(1.0 * (attempt + 1))

    async def fill_historical_candles(self):
        if self._historical_fill_in_progress:
            return
        self._historical_fill_in_progress = True
        try:
            await self._ws_candle_available.wait()
            iteration = 0
            page_end: Optional[float] = None
            while (not self.ready and len(self._candles) > 0
                   and iteration < CONSTANTS.HISTORICAL_FILL_MAX_PAGES):
                iteration += 1
                try:
                    oldest_timestamp = self._candles[0][0]
                    end_timestamp = oldest_timestamp - self.interval_in_seconds
                    if page_end is None:
                        page_end = end_timestamp
                    page_size = self.candles_max_result_per_rest_request
                    candles_np = await self.fetch_candles(end_time=int(page_end), limit=page_size)
                    real_candles = candles_np.tolist() if candles_np.size > 0 else []

                    if not self._candles or self._candles[0][0] != oldest_timestamp:
                        page_end = None
                        continue
                    filled = self._fill_historical_gaps_with_heartbeats(real_candles, end_timestamp)
                    if not filled:
                        page_end -= page_size * self.interval_in_seconds
                        await self._sleep(0.1)
                        continue

                    room = self._candles.maxlen - len(self._candles)
                    if room <= 0:
                        break
                    for candle in reversed(filled[-room:]):
                        self._candles.appendleft(candle)
                    page_end = None
                    await self._sleep(0.1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().exception(
                        f"Error during historical fill iteration {iteration}: {e}"
                    )
                    await self._sleep(1.0)
            if not self.ready and self._candles:
                self.logger().warning(
                    f"Ajaib candles for {self._trading_pair} [{self.interval}] hold "
                    f"{len(self._candles)}/{self._candles.maxlen} records: no older trades were "
                    f"found within {CONSTANTS.HISTORICAL_FILL_MAX_PAGES} pages."
                )
        finally:
            self._historical_fill_in_progress = False

    def _fill_historical_gaps_with_heartbeats(
        self, candles: List[List[float]], end_timestamp: float
    ) -> List[List[float]]:
        candle_map = {
            self._round_timestamp_to_interval_multiple(c[0]): c
            for c in candles
            if c[0] <= end_timestamp
        }
        if not candle_map:
            return []

        result: List[List[float]] = []
        current_ts = float(min(candle_map))
        prev_close = candle_map[min(candle_map)][4]
        while current_ts <= end_timestamp:
            candle = candle_map.get(current_ts)
            if candle is not None:
                result.append(candle)
                prev_close = candle[4]
            else:
                result.append(self._heartbeat(current_ts, prev_close))
            current_ts += self.interval_in_seconds
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
        if self._historical_fill_task and not self._historical_fill_task.done():
            self._historical_fill_task.cancel()
            try:
                await self._historical_fill_task
            except asyncio.CancelledError:
                pass
        self._historical_fill_task = None
        self._historical_fill_in_progress = False

    def _get_rest_candles_params(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict:
        now = int(self._time())
        native_seconds = self.get_seconds_from_interval(self._native_interval)
        resolved_end_time = end_time if end_time else now
        resolved_start_time = (
            start_time if start_time
            else resolved_end_time - self.interval_in_seconds * self.candles_max_result_per_rest_request
        )
        # Widen the native window to whole target buckets. Without this the
        # first and last resampled bars are built from only part of their
        # native candles, and get_historical_candles keeps the partial copy of
        # the overlapping bar when it stitches pages together.
        native_start_time = self._round_timestamp_to_interval_multiple(resolved_start_time)
        native_end_time = (
            self._round_timestamp_to_interval_multiple(resolved_end_time)
            + self.interval_in_seconds - native_seconds
        )
        return {
            "symbol": self._ex_trading_pair,
            "interval": self._native_interval,
            "start_time": int(native_start_time * 1000),
            "end_time": int(native_end_time * 1000),
            "limit": CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
        }

    def _resample_candles(self, native_candles: List[List[float]]) -> List[List[float]]:
        """Aggregate Ajaib's native-interval candles into self.interval bars."""
        if not native_candles or self._native_interval == self.interval:
            return native_candles

        grouped: dict = {}
        for candle in native_candles:
            bucket_ts = self._round_timestamp_to_interval_multiple(candle[0])
            grouped.setdefault(bucket_ts, []).append(candle)

        resampled: List[List[float]] = []
        for bucket_ts in sorted(grouped.keys()):
            bucket = sorted(grouped[bucket_ts], key=lambda c: c[0])
            resampled.append([
                bucket_ts,
                bucket[0][1],
                max(c[2] for c in bucket),
                min(c[3] for c in bucket),
                bucket[-1][4],
                sum(c[5] for c in bucket),
                0.0,
                0.0,
                0.0,
                0.0,
            ])
        return resampled

    def _parse_rest_candles(
        self, data, end_time: Optional[int] = None
    ) -> List[List[float]]:
        if not data:
            return []
        if isinstance(data, dict):
            # Errors normally arrive as HTTP 400 (raised before parsing), e.g.
            # {"err_code": "EC0009007", "err_message": "Data Invalid: coin not found"}.
            raise ValueError(f"Ajaib kline request failed: {data.get('err_message') or data}")

        native_candles = []
        for row in data:
            try:
                native_candles.append([
                    self.ensure_timestamp_in_seconds(row[0]),
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                    float(row[5]),
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ])
            except Exception as e:
                self.logger().error(f"Ajaib: error parsing candle row {row}: {e}")
        native_candles.sort(key=lambda x: x[0])

        candles = self._resample_candles(native_candles)
        if end_time:
            candles = [c for c in candles if c[0] <= end_time]
        return candles

    async def listen_for_subscriptions(self):
        if not self._is_running:
            await self.start_network()
        if self._polling_task:
            try:
                await self._polling_task
            except asyncio.CancelledError:
                self.logger().info("Ajaib candles subscription cancelled.")
                raise

    def ws_subscription_payload(self):
        raise NotImplementedError("WebSocket not supported for Ajaib candles; polling is used instead.")

    def _parse_websocket_message(self, data: dict):
        raise NotImplementedError("WebSocket not supported for Ajaib candles; polling is used instead.")

    async def _polling_loop(self):
        consecutive_errors = 0
        last_config_error: Optional[str] = None
        try:
            self.logger().info(
                f"Starting Ajaib candles polling for {self._trading_pair} [{self.interval}]"
            )
            while self._is_running and not self._shutdown_event.is_set():
                try:
                    await self._poll_and_update()
                    consecutive_errors = 0
                    last_config_error = None
                except asyncio.CancelledError:
                    raise
                except ValueError as e:
                    consecutive_errors += 1
                    if str(e) != last_config_error:
                        last_config_error = str(e)
                        self.logger().error(
                            f"Ajaib candles misconfigured for {self._trading_pair}: {e} "
                            f"This feed will keep retrying but will stay empty until this is fixed."
                        )
                except Exception as e:
                    consecutive_errors += 1
                    self.logger().exception(f"Ajaib candles polling error: {e}")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self._poll_delay(consecutive_errors),
                    )
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            self._is_running = False
            self.logger().info("Ajaib candles polling loop stopped.")

    @staticmethod
    def _poll_delay(consecutive_errors: int) -> float:
        if consecutive_errors == 0:
            return CONSTANTS.POLL_INTERVAL
        return min(CONSTANTS.POLL_INTERVAL * 2 ** (consecutive_errors - 1), CONSTANTS.POLL_ERROR_BACKOFF_MAX)

    async def _seed_candles(self) -> bool:
        end_time = int(self._time())
        candles = await self.fetch_candles(end_time=end_time, limit=CONSTANTS.SEED_RECENT_INTERVALS)
        page = 0
        while candles.size == 0 and page < CONSTANTS.SEED_LOOKBACK_PAGES:
            candles = await self.fetch_candles(end_time=end_time)
            end_time -= self.interval_in_seconds * self.candles_max_result_per_rest_request
            page += 1
        if candles.size == 0 or self._candles:
            return False

        for candle in candles.tolist():
            self._fill_gaps_and_append(candle)
        self._ensure_heartbeats_to_current_time()
        self._ws_candle_available.set()
        self._historical_fill_task = safe_ensure_future(self.fill_historical_candles())
        self.logger().info(
            f"Ajaib candles seeded with {len(self._candles)} candles "
            f"for {self._trading_pair} [{self.interval}]; backfill scheduled."
        )
        return True

    @staticmethod
    def _heartbeat(timestamp: float, close: float) -> List[float]:
        """A zero-volume candle that carries ``close`` through an interval with no trades."""
        return [timestamp, close, close, close, close, 0.0, 0.0, 0.0, 0.0, 0.0]

    def _append_heartbeats_until(self, timestamp: float) -> None:
        """Append a heartbeat for every interval after the newest candle and before ``timestamp``."""
        next_ts = self._candles[-1][0] + self.interval_in_seconds
        while next_ts < timestamp:
            self._candles.append(self._heartbeat(next_ts, self._candles[-1][4]))
            self.logger().debug(f"Ajaib: inserted heartbeat candle at {next_ts}")
            next_ts += self.interval_in_seconds

    def _fill_gaps_and_append(self, new_candle: List[float]) -> None:
        """Insert heartbeat candles for any skipped intervals then append new_candle."""
        if self._candles:
            self._append_heartbeats_until(new_candle[0])
        self._candles.append(new_candle)

    def _ensure_heartbeats_to_current_time(self) -> None:
        if self._candles:
            self._append_heartbeats_until(self._round_timestamp_to_interval_multiple(self._time()))

    async def _poll_and_update(self):
        if not self._candles:
            await self._seed_candles()
            return

        rest_assistant = await self._api_factory.get_rest_assistant()
        now = int(self._time())
        data = await rest_assistant.execute_request(
            url=self.candles_url,
            throttler_limit_id=self._rest_throttler_limit_id,
            params=self._get_rest_candles_params(
                start_time=now - self.interval_in_seconds * CONSTANTS.SEED_RECENT_INTERVALS,
                end_time=now,
            ),
            method=self._rest_method,
        )
        if not self._candles:
            return

        for candle in self._parse_rest_candles(data):
            last_ts = self._candles[-1][0]
            if candle[0] > last_ts:
                self._fill_gaps_and_append(candle)
            elif candle[0] == last_ts:
                self._candles[-1] = candle

        self._ensure_heartbeats_to_current_time()
