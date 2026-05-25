import asyncio
import logging
import time
from typing import List, Optional

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.coindcx_spot_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class CoinDCXSpotCandles(CandlesBase):
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
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS
    
    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT,
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair: str) -> str:
        base, quote = trading_pair.split("-")
        return f"KC-{base}_{quote}"

    async def initialize_exchange_data(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            markets = await rest_assistant.execute_request(
                url=CONSTANTS.REST_URL + CONSTANTS.MARKETS_DETAILS_ENDPOINT,
                throttler_limit_id=CONSTANTS.MARKETS_DETAILS_ENDPOINT,
                method=RESTMethod.GET,
            )

            base, quote = self._trading_pair.split("-")
            for market in markets:
                if (market.get("target_currency_short_name") == base
                        and market.get("base_currency_short_name") == quote):
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
        limit: Optional[int] = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
    ) -> dict:
        params: dict = {
            "pair": self._ex_trading_pair,
            "interval": self.interval,
            "limit": limit or CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
        }
        if start_time:
            params["startTime"] = int(start_time * 1000)  
        if end_time:
            params["endTime"] = int(end_time * 1000)  
        return params

    def _parse_rest_candles(
        self, data: list, end_time: Optional[int] = None
    ) -> List[List[float]]:
        if not data:
            return []

        candles = []
        for row in data:
            timestamp = self.ensure_timestamp_in_seconds(row["time"])
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
        candles.sort(key=lambda x: x[0])
        return candles

    def ws_subscription_payload(self):  
        return {}

    def _parse_websocket_message(self, data: dict):  
        return None

    async def _polling_loop(self):
        try:
            self.logger().info(
                f"Starting CoinDCX candles polling for {self._trading_pair} [{self.interval}]"
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
            end_time = int(time.time())
            candles = await self.fetch_candles(
                end_time=end_time,
                limit=self._candles.maxlen,
            )
            if candles.size > 0:
                self._candles.extend(candles)
                self._ws_candle_available.set()
                self.logger().info(
                    f"CoinDCX candles initialised: {len(self._candles)} candles "
                    f"for {self._trading_pair} [{self.interval}]"
                )
        except Exception as e:
            self.logger().error(
                f"Error initialising CoinDCX candles for {self._trading_pair}: {e}",
                exc_info=True,
            )

    async def _poll_and_update(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=self.candles_url,
                throttler_limit_id=self._rest_throttler_limit_id,
                params={
                    "pair": self._ex_trading_pair,
                    "interval": self.interval,
                    "limit": 2,
                },
                method=self._rest_method,
            )

            candles = self._parse_rest_candles(data)
            if not candles:
                return

            latest = candles[-1]

            if not self._candles:
                self._candles.append(latest)
                self._ws_candle_available.set()
                safe_ensure_future(self.fill_historical_candles())
                return

            last_ts = self._candles[-1][0]
            latest_ts = latest[0]

            if latest_ts > last_ts:
                self._candles.append(latest)
            elif latest_ts == last_ts:
                self._candles[-1] = latest

        except Exception as e:
            self.logger().error(f"CoinDCX candles poll error: {e}", exc_info=True)
