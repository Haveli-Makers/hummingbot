import csv
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, Union

from hummingbot import data_path
from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.alert import Alert, AlertStatus, Severity
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher
from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import MonitorIdentity
from hummingbot.monitoring.sla_day_tracker import DaySummary

CSV_COLUMNS = [
    "date",
    "connector",
    "trading_pair",
    "slo",
    "uptime_pct",
    "in_spec_seconds",
    "total_seconds",
    "downtime_minutes",
    "main_cause",
    "sla_met",
    "complete_day",
]

OVERALL_SLO = "overall"


class SLARecorder:
    """
    Writes each finished SLA day to ``data/sla/<instance>_sla_daily.csv`` and raises a
    critical ``daily_sla_breach`` alert per objective that missed its uptime target.

    Single-objective monitors produce one row per day (slo = "overall"); multi-tier
    monitors additionally produce one row per tier, each judged against its own target
    from ``slo_targets``. Recording never raises: a reporting failure must not disturb
    monitoring or trading.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 config: MonitoringConfigBase,
                 identity: MonitorIdentity,
                 dispatcher: Optional[AlertDispatcher] = None,
                 output_dir: Optional[Union[str, Path]] = None,
                 slo_targets: Optional[Dict[str, Decimal]] = None):
        self._config = config
        self._identity = identity
        self._dispatcher = dispatcher
        self._slo_targets = dict(slo_targets or {})
        directory = Path(output_dir) if output_dir is not None else Path(data_path()) / "sla"
        directory.mkdir(parents=True, exist_ok=True)
        self._csv_path = directory / f"{identity.instance_id}_sla_daily.csv"

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    def record(self, summary: DaySummary):
        for slo_name, uptime_pct, in_spec_samples, target in self._rows_for(summary):
            try:
                self._append_row(summary, slo_name, uptime_pct, in_spec_samples, target)
            except Exception as e:
                self.logger().error(f"Failed to record SLA day {summary.day} ({slo_name}) to CSV: {e}")
            try:
                self._alert_if_breached(summary, slo_name, uptime_pct, target)
            except Exception as e:
                self.logger().error(f"Failed to dispatch the daily SLA alert for {summary.day} ({slo_name}): {e}")

    def _rows_for(self, summary: DaySummary):
        rows = [(OVERALL_SLO, summary.uptime_pct, summary.in_spec_samples,
                 self._config.required_uptime_pct)]
        for slo_name in summary.slo_in_spec:
            target = self._slo_targets.get(slo_name, self._config.required_uptime_pct)
            rows.append((slo_name, summary.slo_uptime_pct(slo_name),
                         summary.slo_in_spec[slo_name], target))
        return rows

    def _main_cause_for(self, summary: DaySummary, slo_name: str) -> str:
        if slo_name == OVERALL_SLO:
            return summary.main_cause
        slo_reasons = {reason: count for reason, count in summary.downtime_by_reason.items()
                       if reason.startswith(f"{slo_name}_")}
        if not slo_reasons:
            return ""
        return max(slo_reasons.items(), key=lambda kv: kv[1])[0]

    def _append_row(self, summary: DaySummary, slo_name: str,
                    uptime_pct: Decimal, in_spec_samples: int, target: Decimal):
        interval = self._config.sample_interval_sec
        in_spec_seconds = round(in_spec_samples * interval)
        total_seconds = round(summary.total_samples * interval)
        downtime_minutes = round((total_seconds - in_spec_seconds) / 60.0, 1)
        sla_met = uptime_pct >= target
        write_header = not self._csv_path.exists()
        with open(self._csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(CSV_COLUMNS)
            writer.writerow([
                summary.day,
                summary.connector_name,
                summary.trading_pair,
                slo_name,
                f"{uptime_pct:.2f}",
                in_spec_seconds,
                total_seconds,
                downtime_minutes,
                self._main_cause_for(summary, slo_name),
                "yes" if sla_met else "no",
                "yes" if summary.complete else "no",
            ])
        self.logger().info(
            f"SLA day recorded: {summary.day} [{slo_name}] uptime {uptime_pct:.2f}% "
            f"(target {target}%, {'met' if sla_met else 'BREACHED'}) -> {self._csv_path.name}"
        )

    def _alert_if_breached(self, summary: DaySummary, slo_name: str,
                           uptime_pct: Decimal, target: Decimal):
        if self._dispatcher is None or summary.total_samples == 0:
            return
        if uptime_pct >= target:
            return
        downtime_minutes = round(
            (summary.total_samples - summary.in_spec_samples) * self._config.sample_interval_sec / 60.0, 1)
        check = "daily_sla_breach" if slo_name == OVERALL_SLO else f"daily_sla_breach_{slo_name}"
        title = "Daily SLA breached" if slo_name == OVERALL_SLO else f"Daily SLA breached ({slo_name})"
        main_cause = self._main_cause_for(summary, slo_name)
        cause_note = f" Main cause: {main_cause}." if main_cause else ""
        partial_note = "" if summary.complete else " Partial day: the bot was down at rollover."
        self._dispatcher.dispatch(Alert(
            source=self._identity.source,
            check=check,
            severity=Severity.CRITICAL,
            title=title,
            message=(f"Uptime {uptime_pct:.2f}% on {summary.day} (target {target}%)."
                     f"{cause_note}{partial_note}"),
            metrics={
                "day": summary.day,
                "slo": slo_name,
                "uptime_pct": f"{uptime_pct:.2f}",
                "target_pct": str(target),
                "downtime_min": downtime_minutes,
            },
            status=AlertStatus.FIRING,
        ))
