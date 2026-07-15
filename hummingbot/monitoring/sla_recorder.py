import csv
import logging
from pathlib import Path
from typing import Optional, Union

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
    "uptime_pct",
    "in_spec_seconds",
    "total_seconds",
    "downtime_minutes",
    "main_cause",
    "sla_met",
    "complete_day",
]


class SLARecorder:
    """
    Writes each finished SLA day to ``data/sla/<connector>_<pair>_sla_daily.csv`` and
    raises a critical ``daily_sla_breach`` alert when the day's uptime is below the
    required percentage. Recording never raises: a reporting failure must not disturb
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
                 output_dir: Optional[Union[str, Path]] = None):
        self._config = config
        self._identity = identity
        self._dispatcher = dispatcher
        directory = Path(output_dir) if output_dir is not None else Path(data_path()) / "sla"
        directory.mkdir(parents=True, exist_ok=True)
        self._csv_path = directory / f"{identity.instance_id}_sla_daily.csv"

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    def record(self, summary: DaySummary):
        try:
            self._append_row(summary)
        except Exception as e:
            self.logger().error(f"Failed to record SLA day {summary.day} to CSV: {e}")
        try:
            self._alert_if_breached(summary)
        except Exception as e:
            self.logger().error(f"Failed to dispatch the daily SLA alert for {summary.day}: {e}")

    def _append_row(self, summary: DaySummary):
        interval = self._config.sample_interval_sec
        in_spec_seconds = round(summary.in_spec_samples * interval)
        total_seconds = round(summary.total_samples * interval)
        downtime_minutes = round((total_seconds - in_spec_seconds) / 60.0, 1)
        sla_met = summary.uptime_pct >= self._config.required_uptime_pct
        write_header = not self._csv_path.exists()
        with open(self._csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(CSV_COLUMNS)
            writer.writerow([
                summary.day,
                summary.connector_name,
                summary.trading_pair,
                f"{summary.uptime_pct:.2f}",
                in_spec_seconds,
                total_seconds,
                downtime_minutes,
                summary.main_cause,
                "yes" if sla_met else "no",
                "yes" if summary.complete else "no",
            ])
        self.logger().info(
            f"SLA day recorded: {summary.day} uptime {summary.uptime_pct:.2f}% "
            f"(SLA {'met' if sla_met else 'BREACHED'}) -> {self._csv_path.name}"
        )

    def _alert_if_breached(self, summary: DaySummary):
        if self._dispatcher is None or summary.total_samples == 0:
            return
        if summary.uptime_pct >= self._config.required_uptime_pct:
            return
        downtime_minutes = round(
            (summary.total_samples - summary.in_spec_samples) * self._config.sample_interval_sec / 60.0, 1)
        partial_note = "" if summary.complete else " Partial day: the bot was down at rollover."
        cause_note = f" Main cause: {summary.main_cause}." if summary.main_cause else ""
        self._dispatcher.dispatch(Alert(
            source=self._identity.source,
            check="daily_sla_breach",
            severity=Severity.CRITICAL,
            title="Daily SLA breached",
            message=(f"Uptime {summary.uptime_pct:.2f}% on {summary.day} "
                     f"(target {self._config.required_uptime_pct}%). "
                     f"Downtime {downtime_minutes} min.{cause_note}{partial_note}"),
            metrics={
                "day": summary.day,
                "uptime_pct": f"{summary.uptime_pct:.2f}",
                "downtime_min": downtime_minutes,
                "main_cause": summary.main_cause or "n/a",
            },
            status=AlertStatus.FIRING,
        ))
