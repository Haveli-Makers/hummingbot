import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.config import PMMSLAMonitorConfig
from hummingbot.monitoring.sla_sampler import OpenOrder, SampleResult, evaluate_sample

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore


class PMMSLAMonitor:
    """
    In-bot SLA monitor for market making (plan: Layer B/C measurement source).

    Every ``sample_interval_sec`` it reads the strategy's open orders and the live mid
    price from the connector, and evaluates whether both sides meet the SLA (orders
    within the spread band with at least the minimum depth). Phase 3 logs the results;
    Phase 4 attaches breach state machines, Phase 5 the daily uptime tally.

    Read-only by design: it never places, cancels or modifies anything.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_core: "TradingCore", config: PMMSLAMonitorConfig):
        self._trading_core = trading_core
        self._config = config
        self._monitor_task: Optional[asyncio.Task] = None
        # Session-local tally (Phase 5 replaces this with the persistent daily tracker)
        self._samples_total = 0
        self._samples_in_spec = 0
        self._last_in_spec: Optional[bool] = None
        self._last_heartbeat_ts = 0.0

    @property
    def uptime_pct(self) -> Decimal:
        if self._samples_total == 0:
            return Decimal("0")
        return Decimal(self._samples_in_spec) / Decimal(self._samples_total) * Decimal("100")

    def start(self):
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = safe_ensure_future(self.monitor_loop())

    def stop(self):
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_task = None

    async def monitor_loop(self):
        self.logger().info(
            f"SLA monitor started for {self._config.connector_name}:{self._config.trading_pair} "
            f"(band {self._config.spread_band_pct}%, min depth {self._config.min_depth_quote} quote, "
            f"target uptime {self._config.required_uptime_pct}%)."
        )
        while True:
            try:
                sample = self.take_sample()
                self._process_sample(sample)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"SLA monitor sampling failed: {e}", exc_info=True)
            await asyncio.sleep(self._config.sample_interval_sec)

    def take_sample(self) -> SampleResult:
        """Read orders + mid from the connector and evaluate one SLA sample."""
        connector = self._trading_core.markets.get(self._config.connector_name)
        mid: Optional[Decimal] = None
        orders = []
        if connector is not None:
            try:
                mid = connector.get_price_by_type(self._config.trading_pair, PriceType.MidPrice)
            except Exception:
                mid = None  # evaluated as orderbook_stale
            orders = [
                OpenOrder(
                    is_buy=order.trade_type.name == "BUY",
                    price=order.price,
                    amount_remaining=order.amount - order.executed_amount_base,
                )
                for order in connector.in_flight_orders.values()
                if order.trading_pair == self._config.trading_pair and order.is_open
            ]
        return evaluate_sample(
            mid=mid,
            orders=orders,
            band_pct=self._config.spread_band_pct,
            min_depth_quote=self._config.min_depth_quote,
        )

    def _process_sample(self, sample: SampleResult):
        self._samples_total += 1
        if sample.in_spec:
            self._samples_in_spec += 1
        self._log_transitions(sample)
        self._log_heartbeat()

    def _log_transitions(self, sample: SampleResult):
        if sample.in_spec == self._last_in_spec:
            return
        if sample.in_spec:
            self.logger().info(
                f"SLA back IN spec: bid depth {sample.bid_depth:.0f}, ask depth {sample.ask_depth:.0f} "
                f"(mid {sample.mid_price})."
            )
        else:
            self.logger().info(
                f"SLA OUT of spec ({', '.join(sample.reasons)}): bid depth {sample.bid_depth:.0f}, "
                f"ask depth {sample.ask_depth:.0f}, need {self._config.min_depth_quote} per side "
                f"(mid {sample.mid_price})."
            )
        self._last_in_spec = sample.in_spec

    def _log_heartbeat(self):
        now = time.time()
        if now - self._last_heartbeat_ts < self._config.heartbeat_log_interval_sec:
            return
        self._last_heartbeat_ts = now
        self.logger().info(
            f"SLA monitor heartbeat: uptime {self.uptime_pct:.2f}% "
            f"({self._samples_in_spec}/{self._samples_total} samples in spec)."
        )
