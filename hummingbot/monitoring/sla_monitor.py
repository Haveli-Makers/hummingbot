import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher
from hummingbot.monitoring.breach_fsm import BreachStateMachine
from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import SLASample, SLASamplerBase
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_recorder import SLARecorder


class SLAMonitor:
    """
    Strategy-agnostic SLA monitoring engine.

    Every ``sample_interval_sec`` it asks the strategy-specific sampler whether the
    SLA holds right now, and:
    - logs state transitions and periodic heartbeats,
    - drives one breach state machine per check, raising and resolving alerts
      through the dispatcher with a grace period,
    - accumulates samples in the day tracker into a persistent per-day uptime figure;
      at day rollover the finished day is handed to the recorder (CSV row + breach
      alert).

    All strategy knowledge lives in the sampler; this engine is reused unchanged by
    every monitored strategy. Read-only by design: nothing here places, cancels or
    modifies anything.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 sampler: SLASamplerBase,
                 config: MonitoringConfigBase,
                 dispatcher: Optional[AlertDispatcher] = None,
                 day_tracker: Optional[SLADayTracker] = None,
                 recorder: Optional[SLARecorder] = None):
        self._sampler = sampler
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
            for check, (severity, title) in sampler.check_alerts.items():
                self._fsms[check] = BreachStateMachine(
                    source=sampler.identity.source,
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

    def stop(self):
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_task = None
        if self._day_tracker is not None:
            self._day_tracker.flush()

    def _record_interrupted_day(self):
        """Record a day that ended while the bot was down (found in the state file)."""
        if self._day_tracker is None or self._recorder is None:
            return
        pending = self._day_tracker.pending_summary
        if pending is not None:
            self.logger().info(f"Recording interrupted SLA day {pending.day} from a previous run.")
            self._recorder.record(pending)
            self._day_tracker.pending_summary = None

    async def monitor_loop(self):
        self._started_at = time.time()
        identity = self._sampler.identity
        alerting = "alerts on" if self._fsms else "log-only"
        self.logger().info(
            f"SLA monitor started for {identity.connector_name}:{identity.trading_pair} "
            f"({self._sampler.config_summary()}, "
            f"target uptime {self._config.required_uptime_pct}%, {alerting})."
        )
        while True:
            try:
                sample = self._sampler.take_sample()
                self._process_sample(sample)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"SLA monitor sampling failed: {e}", exc_info=True)
            await asyncio.sleep(self._config.sample_interval_sec)

    def _process_sample(self, sample: SLASample):
        self._samples_total += 1
        if sample.in_spec:
            self._samples_in_spec += 1
        self._record_daily(sample)
        self._log_transitions(sample)
        self._log_heartbeat(sample)
        self._update_breach_alerts(sample)

    def _record_daily(self, sample: SLASample):
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

    def _update_breach_alerts(self, sample: SLASample):
        if not self._fsms:
            return
        # Give the strategy a moment to place its first orders so every start does not
        # begin with a spurious alert.
        if time.time() - self._started_at < self._config.alert_warmup_sec:
            return
        metrics = {"session uptime": f"{self.uptime_pct:.2f}%"}
        if self._day_tracker is not None:
            metrics["day uptime"] = f"{self._day_tracker.uptime_pct:.2f}%"
        for check, fsm in self._fsms.items():
            fsm.update(
                breached=check in sample.reasons,
                message=f"{self._sampler.describe_check(check, sample)}.",
                metrics=dict(metrics),
            )

    def _log_transitions(self, sample: SLASample):
        # Log when the in-spec state flips, and also when the reason set changes while
        # staying out of spec (e.g. one_side_missing -> spread_too_wide).
        if sample.in_spec == self._last_in_spec and sample.reasons == self._last_reasons:
            return
        if sample.in_spec:
            self.logger().info(f"SLA back IN spec: {self._sampler.describe(sample)}.")
        else:
            self.logger().info(
                f"SLA OUT of spec ({', '.join(sample.reasons)}): {self._sampler.describe(sample)}."
            )
        self._last_in_spec = sample.in_spec
        self._last_reasons = list(sample.reasons)

    def _log_heartbeat(self, sample: SLASample):
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
            f"{self._sampler.describe(sample)}.{day_part}"
        )
