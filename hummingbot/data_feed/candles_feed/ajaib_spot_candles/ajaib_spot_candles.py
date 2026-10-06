import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

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


_CONNECTIONS_FACTORIES: Dict[Optional[str], Any] = {}


def _shared_connections_factory(proxy_url: Optional[str]):
    """
    One connections factory (hence one aiohttp session) per proxy URL, shared by
    every Ajaib candles feed. A session per feed leaves a pile of idle proxy
    tunnels open, and the proxy then resets new connections with
    ``[WinError 64] The specified network name is no longer available``.
    """
    if proxy_url not in _CONNECTIONS_FACTORIES:
        _CONNECTIONS_FACTORIES[proxy_url] = ajaib_web_utils._build_connections_factory(proxy_url)
    return _CONNECTIONS_FACTORIES[proxy_url]


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
        # Ajaib allowlists API access by IP, so traffic goes through the same
        # proxy the connector and rate source use.
        proxy_url = os.environ.get(CONSTANTS.PROXY_URL_ENV_VAR, "") or None
        self._api_factory = WebAssistantsFactory(
            throttler=AsyncThrottler(rate_limits=self.rate_limits),
            connections_factory=_shared_connections_factory(proxy_url),
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
        try:
            self.logger().info(
                f"Starting Ajaib candles polling for {self._trading_pair} [{self.interval}]"
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
                    self.logger().exception(f"Ajaib candles polling error: {e}")
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                        break
                    except asyncio.TimeoutError:
                        continue
        finally:
            self._is_running = False
            self.logger().info("Ajaib candles polling loop stopped.")

    async def _initialize_candles(self):
        try:
            candles = await self.fetch_candles(
                end_time=int(time.time()),
                limit=10,
            )
            if candles.size > 0:
                self._candles.extend(candles)
                self._ws_candle_available.set()
                self._historical_fill_task = safe_ensure_future(self.fill_historical_candles())
                self.logger().info(
                    f"Ajaib candles seeded with {len(self._candles)} recent candles "
                    f"for {self._trading_pair} [{self.interval}]; backfill scheduled."
                )
        except ValueError as e:
            # Most commonly missing API credentials (see _AjaibSigningPreProcessor).
            self.logger().error(
                f"Ajaib candles misconfigured for {self._trading_pair}: {e} "
                f"This feed will keep retrying every {CONSTANTS.POLL_INTERVAL}s but will stay "
                f"empty until this is fixed.",
            )
        except Exception as e:
            self.logger().error(
                f"Error initialising Ajaib candles for {self._trading_pair}: {e}",
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
            self.logger().debug(f"Ajaib: inserted heartbeat candle at {next_ts}")
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
            self.logger().debug(f"Ajaib: heartbeat candle inserted at {next_ts}")
            next_ts += self.interval_in_seconds

    async def _poll_and_update(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            now = int(self._time())
            data = await rest_assistant.execute_request(
                url=self.candles_url,
                throttler_limit_id=self._rest_throttler_limit_id,
                params=self._get_rest_candles_params(
                    start_time=now - self.interval_in_seconds * 10,
                    end_time=now,
                ),
                method=self._rest_method,
            )

            candles = self._parse_rest_candles(data)

            if not self._candles:
                if candles:
                    self._candles.append(candles[-1])
                    self._ws_candle_available.set()
                    self._historical_fill_task = safe_ensure_future(self.fill_historical_candles())
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
            self.logger().error(f"Ajaib candles poll error: {e}", exc_info=True)
