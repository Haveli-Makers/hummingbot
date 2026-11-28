import asyncio
import json
import logging
import time
from typing import Any, List, Optional

import socketio

from hummingbot.connector.exchange.coindcx import coindcx_constants as EXCHANGE_CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_utils import hb_pair_to_coindcx_pair
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.coindcx_spot_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class CoinDCXSpotCandles(CandlesBase):
    """
    CoinDCX candles. REST seeds and backfills them. At the intervals CoinDCX streams (CONSTANTS.SOCKET_INTERVALS)
    the current candle then follows its Socket.IO "candlestick" stream, and REST is only read every
    CONSTANTS.SOCKET_REST_CHECK_INTERVAL seconds as a check. Other intervals, and any time the stream is down, are
    polled from REST every CONSTANTS.POLL_INTERVAL seconds.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)
        self._polling_task: Optional[asyncio.Task] = None
        self._historical_fill_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._historical_fill_in_progress = False
        self._socket_task: Optional[asyncio.Task] = None
        # True once the stream has delivered a candle on its current connection.
        self._socket_live = False
        self._last_rest_poll = 0.0

    @property
    def name(self) -> str:
        return f"coindcx_{self._trading_pair}"

    @property
    def rest_url(self) -> str:
        return CONSTANTS.PUBLIC_REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self) -> str:
        return CONSTANTS.REST_URL + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self) -> str:
        return CONSTANTS.PUBLIC_REST_URL + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self) -> str:
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self) -> int:
        return max(1, CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST // self._resample_ratio)

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    def _resolve_native_interval(self) -> str:
        """
        CoinDCX's public candles REST endpoint only accepts a handful of native intervals
        (CONSTANTS.NATIVE_INTERVALS). Resolve the largest one that evenly divides the
        requested self.interval so it can be fetched and then resampled client-side.
        """
        target_seconds = self.interval_in_seconds
        for native in CONSTANTS.NATIVE_INTERVALS:
            native_seconds = self.get_seconds_from_interval(native)
            if native_seconds <= target_seconds and target_seconds % native_seconds == 0:
                return native
        raise ValueError(
            f"CoinDCX cannot provide '{self.interval}' candles: its public API only supports "
            f"{CONSTANTS.NATIVE_INTERVALS} as base intervals, and none of them evenly divide "
            f"'{self.interval}'."
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
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair: str) -> str:
        _, quote = trading_pair.split("-")
        ecode = "I" if quote.upper() == "INR" else "B"
        return hb_pair_to_coindcx_pair(trading_pair, ecode=ecode)

    async def initialize_exchange_data(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            markets = await rest_assistant.execute_request(
                url=CONSTANTS.REST_URL + CONSTANTS.MARKETS_DETAILS_ENDPOINT,
                throttler_limit_id=CONSTANTS.MARKETS_DETAILS_ENDPOINT,
                method=RESTMethod.GET,
            )

            base, quote = self._trading_pair.split("-")
            expected_ecode = "I" if quote.upper() == "INR" else "B"
            ecode_prefix = f"{expected_ecode}-"
            for market in markets:
                if (market.get("target_currency_short_name") == base
                        and market.get("base_currency_short_name") == quote
                        and market.get("status") == "active"
                        and market.get("pair", "").startswith(ecode_prefix)):
                    self._ex_trading_pair = market["pair"]
                    self.logger().info(
                        f"CoinDCX candles pair resolved: {self._trading_pair} → {self._ex_trading_pair}"
                    )
                    return

            self.logger().warning(
                f"Could not resolve CoinDCX pair for {self._trading_pair} from markets details. "
                f"Using default: {self._ex_trading_pair}"
            )
        except Exception as e:
            self.logger().error(
                f"Error initialising CoinDCX exchange data for {self._trading_pair}: {e}",
                exc_info=True,
            )
            raise

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
        if self.uses_socket:
            self._socket_task = asyncio.create_task(self._socket_loop())

    @property
    def uses_socket(self) -> bool:
        """Whether this interval is one CoinDCX streams. Others are built from REST candles."""
        return self.interval in CONSTANTS.SOCKET_INTERVALS

    @property
    def socket_channel(self) -> str:
        return f"{self._ex_trading_pair}_{self.interval}"

    async def stop_network(self):
        if self._socket_task and not self._socket_task.done():
            self._socket_task.cancel()
            try:
                await self._socket_task
            except asyncio.CancelledError:
                pass
        self._socket_task = None
        self._socket_live = False
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
        limit: Optional[int] = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
    ) -> dict:
        native_limit = (limit or CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST) * self._resample_ratio
        params: dict = {
            "pair": self._ex_trading_pair,
            "interval": self._native_interval,
            "limit": min(native_limit, CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST),
        }
        if start_time:
            params["startTime"] = int(start_time * 1000)
        if end_time:
            params["endTime"] = int(end_time * 1000)
        return params

    def _resample_candles(self, native_candles: List[List[float]]) -> List[List[float]]:
        """Aggregate CoinDCX's native-interval candles into self.interval bars."""
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
        self, data: list, end_time: Optional[int] = None
    ) -> List[List[float]]:
        if not data:
            return []

        native_candles = []
        for row in data:
            try:
                timestamp = self.ensure_timestamp_in_seconds(row["time"])
                native_candles.append([
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
                self.logger().error(f"CoinDCX: error parsing candle row {row}: {e}")
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
                self.logger().info("CoinDCX candles subscription cancelled.")
                raise

    def ws_subscription_payload(self):
        raise NotImplementedError("CoinDCX streams candles over Socket.IO, not a plain websocket: see _socket_loop.")

    def _parse_websocket_message(self, data: dict):
        raise NotImplementedError("CoinDCX streams candles over Socket.IO, not a plain websocket: see _socket_loop.")

    async def _socket_loop(self):
        """
        Follows the current candle on CoinDCX's "candlestick" stream until the feed stops, reconnecting after
        CONSTANTS.SOCKET_RETRY_DELAY seconds when the connection fails or ends. REST polling covers the gaps.
        """
        while self._is_running and not self._shutdown_event.is_set():
            client = socketio.AsyncClient(logger=False, reconnection=False)
            client.on(CONSTANTS.CANDLESTICK_EVENT, self._on_socket_candle)
            ping_task = None
            try:
                await client.connect(EXCHANGE_CONSTANTS.WSS_URL, transports=["websocket"])
                await client.emit("join", {"channelName": self.socket_channel})
                self.logger().info(f"CoinDCX candles stream joined {self.socket_channel}.")
                ping_task = asyncio.create_task(self._socket_ping(client))
                await client.wait()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"CoinDCX candles stream {self.socket_channel} failed: "
                                      f"{str(e) or type(e).__name__}. Polling REST until it is back.")
            finally:
                self._socket_live = False
                if ping_task is not None:
                    ping_task.cancel()
                try:
                    await client.disconnect()
                except Exception:
                    self.logger().debug("CoinDCX candles stream disconnect failed.", exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=CONSTANTS.SOCKET_RETRY_DELAY)
            except asyncio.TimeoutError:
                pass

    async def _socket_ping(self, client: socketio.AsyncClient):
        """Pings the stream like the CoinDCX connector does, to keep the connection open."""
        while True:
            await asyncio.sleep(EXCHANGE_CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
            if client.connected:
                try:
                    await client.emit("ping", {"data": "Ping message"})
                except Exception as e:
                    self.logger().debug(f"CoinDCX candles stream ping failed: {e}")

    async def _on_socket_candle(self, message: Any):
        for candle in self._parse_socket_candles(message):
            self._apply_socket_candle(candle)

    def _parse_socket_candles(self, message: Any) -> List[List[float]]:
        """
        The candles in a "candlestick" message, as candle rows. CoinDCX sends {"event": ..., "data": ...} with "data"
        a candle (as JSON text or an object) whose fields are, per its docs: "t" start (ms), "o", "h", "l", "c",
        "v" base volume, "q" quote volume, "n" trades, "V" taker buy base volume, "Q" taker quote volume, "i" the
        interval, "x" whether the candle is complete, "channel". Candles of another interval or channel are skipped.
        """
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except ValueError:
                return []
        body = message.get("data", message) if isinstance(message, dict) else message
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except ValueError:
                return []
        items = [body] if isinstance(body, dict) else (body if isinstance(body, list) else [])
        candles = []
        for item in items:
            if not isinstance(item, dict) or item.get("i", self.interval) != self.interval:
                continue
            if item.get("channel", self.socket_channel) != self.socket_channel:
                continue
            try:
                candles.append([
                    self.ensure_timestamp_in_seconds(item["t"]),
                    float(item["o"]),
                    float(item["h"]),
                    float(item["l"]),
                    float(item["c"]),
                    float(item.get("v") or 0),
                    float(item.get("q") or 0),
                    float(item.get("n") or 0),
                    float(item.get("V") or 0),
                    float(item.get("Q") or 0),
                ])
            except (KeyError, TypeError, ValueError) as e:
                self.logger().warning(f"CoinDCX: ignoring a stream candle it can't read ({e}): {item}")
        return candles

    def _apply_socket_candle(self, candle: List[float]):
        """
        Puts a stream candle in place: the current candle is replaced, a newer one appended (after heartbeat candles
        for any interval in between). Older candles are left alone, and so is an empty feed, which REST seeds first.
        """
        if not self._candles:
            return
        last_ts = self._candles[-1][0]
        if candle[0] == last_ts:
            self._candles[-1] = candle
        elif candle[0] > last_ts:
            self._fill_gaps_and_append(candle)
        self._socket_live = True

    def _rest_poll_due(self) -> bool:
        """
        Whether to read REST now: every POLL_INTERVAL while the stream isn't delivering, and every
        SOCKET_REST_CHECK_INTERVAL while it is.
        """
        return not self._socket_live or self._time() - self._last_rest_poll >= CONSTANTS.SOCKET_REST_CHECK_INTERVAL

    async def _polling_loop(self):
        try:
            self.logger().info(
                f"Starting CoinDCX candles polling for {self._trading_pair} [{self.interval}]"
            )
            await self._initialize_candles()
            self._last_rest_poll = self._time()

            while self._is_running and not self._shutdown_event.is_set():
                try:
                    if self._rest_poll_due():
                        await self._poll_and_update()
                        self._last_rest_poll = self._time()
                    else:
                        self._ensure_heartbeats_to_current_time()
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
                    self.logger().exception(f"CoinDCX candles polling error: {e}")
                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                        break
                    except asyncio.TimeoutError:
                        continue
        finally:
            self._is_running = False
            self.logger().info("CoinDCX candles polling loop stopped.")

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
                    f"CoinDCX candles seeded with {len(self._candles)} recent candles "
                    f"for {self._trading_pair} [{self.interval}]; backfill scheduled."
                )
        except Exception as e:
            self.logger().error(
                f"Error initialising CoinDCX candles for {self._trading_pair}: {e}",
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
            self.logger().debug(f"CoinDCX: inserted heartbeat candle at {next_ts}")
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
            self.logger().debug(f"CoinDCX: heartbeat candle inserted at {next_ts}")
            next_ts += self.interval_in_seconds

    async def _poll_and_update(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            limit = min(10 * self._resample_ratio, CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST)
            data = await rest_assistant.execute_request(
                url=self.candles_url,
                throttler_limit_id=self._rest_throttler_limit_id,
                params={
                    "pair": self._ex_trading_pair,
                    "interval": self._native_interval,
                    "limit": limit,
                },
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
            self.logger().error(f"CoinDCX candles poll error: {e}", exc_info=True)
