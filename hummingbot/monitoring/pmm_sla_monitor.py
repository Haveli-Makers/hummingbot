import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.alert import Severity
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher
from hummingbot.monitoring.breach_fsm import BreachStateMachine
from hummingbot.monitoring.config import PMMSLAMonitorConfig
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_recorder import SLARecorder
from hummingbot.monitoring.sla_sampler import (
    DEPTH_BELOW_MIN,
    ONE_SIDE_MISSING,
    ORDER_BOOK_STALE,
    SPREAD_TOO_WIDE,
    OpenOrder,
    SampleResult,
    evaluate_sample,
)

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore

# Severity and headline for each real-time SLA alert
CHECK_ALERTS = {
    ONE_SIDE_MISSING: (Severity.CRITICAL, "One side has no standing orders"),
    SPREAD_TOO_WIDE: (Severity.WARNING, "Standing orders are outside the spread band"),
    DEPTH_BELOW_MIN: (Severity.WARNING, "Depth below the SLA minimum"),
    ORDER_BOOK_STALE: (Severity.WARNING, "Order book stale or connector disconnected"),
}


class PMMSLAMonitor:
    """
    In-bot SLA monitor for market making.

    Every ``sample_interval_sec`` it reads the strategy's open orders and the live mid
    price from the connector and evaluates whether both sides meet the SLA (orders
    within the spread band with at least the minimum depth). Each sample:
    - is logged (state transitions and periodic heartbeats),
    - drives one breach state machine per check, which raises and resolves alerts
      through the dispatcher with a grace period,
    - is accumulated by the day tracker into a persistent per-day uptime figure; at
      day rollover the finished day is handed to the recorder (CSV row + breach alert).

    Read-only by design: it never places, cancels or modifies anything.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 trading_core: "TradingCore",
                 config: PMMSLAMonitorConfig,
                 dispatcher: Optional[AlertDispatcher] = None,
                 day_tracker: Optional[SLADayTracker] = None,
                 recorder: Optional[SLARecorder] = None):
        self._trading_core = trading_core
        self._config = config
        self._day_tracker = day_tracker
        self._recorder = recorder
        self._monitor_task: Optional[asyncio.Task] = None
        # Counters for the current run; the day tracker owns the persistent daily figure
        self._samples_total = 0
        self._samples_in_spec = 0
        self._last_in_spec: Optional[bool] = None
        self._last_reasons: Optional[list] = None
        self._last_heartbeat_ts = 0.0
        self._started_at = 0.0
        # One breach state machine per check; without a dispatcher the monitor is log-only
        self._fsms: Dict[str, BreachStateMachine] = {}
        if dispatcher is not None:
            source = f"pmm.{config.connector_name}.{config.trading_pair}"
            for check, (severity, title) in CHECK_ALERTS.items():
                self._fsms[check] = BreachStateMachine(
                    source=source,
                    check=check,
                    severity=severity,
                    title=title,
                    dispatcher=dispatcher,
                    grace_period_sec=config.grace_period_sec,
                )

    @property
    def uptime_pct(self) -> Decimal:
        if self._samples_total == 0:
            return Decimal("0")
        return Decimal(self._samples_in_spec) / Decimal(self._samples_total) * Decimal("100")

    def start(self):
        if self._monitor_task is None or self._monitor_task.done():
            self._record_interrupted_day()
            self._monitor_task = safe_ensure_future(self.monitor_loop())

    def _record_interrupted_day(self):
        """Record a day that ended while the bot was down (found in the state file)."""
        if self._day_tracker is None or self._recorder is None:
            return
        pending = self._day_tracker.pending_summary
        if pending is not None:
            self.logger().info(f"Recording interrupted SLA day {pending.day} from a previous run.")
            self._recorder.record(pending)
            self._day_tracker.pending_summary = None

    def stop(self):
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_task = None
        if self._day_tracker is not None:
            self._day_tracker.flush()

    async def monitor_loop(self):
        self._started_at = time.time()
        alerting = "alerts on" if self._fsms else "log-only"
        self.logger().info(
            f"SLA monitor started for {self._config.connector_name}:{self._config.trading_pair} "
            f"(band {self._config.spread_band_pct}%, min depth {self._config.min_depth_quote} quote, "
            f"target uptime {self._config.required_uptime_pct}%, {alerting})."
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
        # A disconnected connector serves a frozen local order book: the last-known mid
        # looks valid but proves nothing. Score those seconds as orderbook_stale.
        connected = (connector is not None
                     and getattr(connector, "network_status", NetworkStatus.CONNECTED) is NetworkStatus.CONNECTED)
        if connected:
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
        self._record_daily(sample)
        self._log_transitions(sample)
        self._log_heartbeat(sample)
        self._update_breach_alerts(sample)

    def _record_daily(self, sample: SampleResult):
        if self._day_tracker is None:
            return
        finished = self._day_tracker.record(sample)
        if finished is not None:
            self.logger().info(
                f"SLA day closed: {finished.day} uptime {finished.uptime_pct:.2f}% "
                f"({finished.in_spec_samples}/{finished.total_samples} samples in spec"
                f"{', main cause ' + finished.main_cause if finished.main_cause else ''})."
            )
            if self._recorder is not None:
                self._recorder.record(finished)

    def _update_breach_alerts(self, sample: SampleResult):
        if not self._fsms:
            return
        # Give the strategy a moment to place its first orders so every start does not
        # begin with a spurious one_side_missing alert.
        if time.time() - self._started_at < self._config.alert_warmup_sec:
            return
        detail = (f"Bid depth {sample.bid_depth:.0f}, ask depth {sample.ask_depth:.0f}, "
                  f"need {self._config.min_depth_quote} per side (mid {sample.mid_price}).")
        metrics = {
            "bid_depth": f"{sample.bid_depth:.0f}",
            "ask_depth": f"{sample.ask_depth:.0f}",
            "mid": str(sample.mid_price),
            "uptime_pct": f"{self.uptime_pct:.2f}",
        }
        for check, fsm in self._fsms.items():
            fsm.update(breached=check in sample.reasons, message=detail, metrics=metrics)

    def _log_transitions(self, sample: SampleResult):
        # Log when the in-spec state flips, and also when the reason set changes while
        # staying out of spec (e.g. one_side_missing -> spread_too_wide).
        if sample.in_spec == self._last_in_spec and sample.reasons == self._last_reasons:
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
        self._last_reasons = list(sample.reasons)

    def _log_heartbeat(self, sample: SampleResult):
        now = time.time()
        if now - self._last_heartbeat_ts < self._config.heartbeat_log_interval_sec:
            return
        self._last_heartbeat_ts = now
        state = "in spec" if sample.in_spec else f"OUT of spec ({', '.join(sample.reasons)})"
        day_part = ""
        if self._day_tracker is not None:
            day_part = (f" Day {self._day_tracker.current_day} uptime {self._day_tracker.uptime_pct:.2f}% "
                        f"({self._day_tracker.in_spec_samples}/{self._day_tracker.total_samples}).")
        self.logger().info(
            f"SLA monitor heartbeat: uptime {self.uptime_pct:.2f}% "
            f"({self._samples_in_spec}/{self._samples_total} samples in spec); currently {state}, "
            f"bid depth {sample.bid_depth:.0f}, ask depth {sample.ask_depth:.0f} "
            f"(mid {sample.mid_price}).{day_part}"
        )
