import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Dict, Optional, Union
from zoneinfo import ZoneInfo

from hummingbot import data_path
from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample

STATE_VERSION = 1


@dataclass
class DaySummary:
    """The finished (or reconstructed) accounting of one SLA day."""
    day: str                 # ISO date in the configured timezone, e.g. "2026-07-13"
    connector_name: str
    trading_pair: str
    total_samples: int
    in_spec_samples: int
    downtime_by_reason: Dict[str, int] = field(default_factory=dict)
    # False when rebuilt from a stale state file (the bot was down at midnight, so the
    # tail of the day was never observed).
    complete: bool = True

    @property
    def uptime_pct(self) -> Decimal:
        if self.total_samples == 0:
            return Decimal("0")
        return Decimal(self.in_spec_samples) / Decimal(self.total_samples) * Decimal("100")

    @property
    def main_cause(self) -> str:
        if not self.downtime_by_reason:
            return ""
        return max(self.downtime_by_reason.items(), key=lambda kv: kv[1])[0]


class SLADayTracker:
    """
    Accumulates the per-second SLA samples into a per-day uptime figure.

    - Days follow the configured timezone (IST for the WazirX SLA); counters reset at
      local midnight and ``record`` returns the finished day's DaySummary exactly once
      at rollover so the recorder can write it out and alert on it.
    - State is persisted to ``data/sla/<connector>_<pair>_sla_state.json`` every few
      seconds, and restored on restart when it belongs to the current day, so an
      intraday restart does not reset the daily figure.
    - A stale state file from a previous day is surfaced as ``pending_summary``
      (complete=False) instead of being restored, so the recorder can still write out
      a day that ended while the bot was down.
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
                 state_dir: Optional[Union[str, Path]] = None,
                 time_fn: Callable[[], float] = time.time,
                 persist_interval_sec: float = 5.0):
        self._config = config
        self._identity = identity
        self._time = time_fn
        self._tz = ZoneInfo(config.day_reset_timezone)
        self._persist_interval_sec = persist_interval_sec
        directory = Path(state_dir) if state_dir is not None else Path(data_path()) / "sla"
        directory.mkdir(parents=True, exist_ok=True)
        self._state_path = directory / f"{identity.instance_id}_sla_state.json"

        self._current_day = self._day_key(self._time())
        self._total_samples = 0
        self._in_spec_samples = 0
        self._downtime_by_reason: Dict[str, int] = {}
        self._last_persist_ts = 0.0
        self.pending_summary: Optional[DaySummary] = None
        self._restore()

    @property
    def current_day(self) -> str:
        return self._current_day

    @property
    def total_samples(self) -> int:
        return self._total_samples

    @property
    def in_spec_samples(self) -> int:
        return self._in_spec_samples

    @property
    def uptime_pct(self) -> Decimal:
        if self._total_samples == 0:
            return Decimal("0")
        return Decimal(self._in_spec_samples) / Decimal(self._total_samples) * Decimal("100")

    def record(self, sample: SLASample) -> Optional[DaySummary]:
        """
        Record one sample against the current day.

        :return: the finished day's summary when this sample is the first of a new day,
                 otherwise None.
        """
        now = self._time()
        day = self._day_key(now)
        finished: Optional[DaySummary] = None
        if day != self._current_day:
            finished = self.summary()
            self._reset_for(day)
        self._total_samples += 1
        if sample.in_spec:
            self._in_spec_samples += 1
        else:
            for reason in sample.reasons:
                self._downtime_by_reason[reason] = self._downtime_by_reason.get(reason, 0) + 1
        if now - self._last_persist_ts >= self._persist_interval_sec:
            self.flush()
        return finished

    def summary(self) -> DaySummary:
        """The accounting of the current day so far."""
        return DaySummary(
            day=self._current_day,
            connector_name=self._identity.connector_name,
            trading_pair=self._identity.trading_pair,
            total_samples=self._total_samples,
            in_spec_samples=self._in_spec_samples,
            downtime_by_reason=dict(self._downtime_by_reason),
        )

    def flush(self):
        """Persist the current counters (atomic write; failures only log)."""
        try:
            payload = {
                "version": STATE_VERSION,
                "day": self._current_day,
                "connector_name": self._identity.connector_name,
                "trading_pair": self._identity.trading_pair,
                "total_samples": self._total_samples,
                "in_spec_samples": self._in_spec_samples,
                "downtime_by_reason": self._downtime_by_reason,
                "updated_at": self._time(),
            }
            tmp_path = self._state_path.with_suffix(".json.tmp")
            tmp_path.write_text(json.dumps(payload))
            tmp_path.replace(self._state_path)
            self._last_persist_ts = self._time()
        except Exception as e:
            self.logger().error(f"Failed to persist SLA day state: {e}")

    def _day_key(self, timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, tz=self._tz).date().isoformat()

    def _reset_for(self, day: str):
        self._current_day = day
        self._total_samples = 0
        self._in_spec_samples = 0
        self._downtime_by_reason = {}
        self.flush()

    def _restore(self):
        if not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text())
        except Exception as e:
            self.logger().warning(f"Ignoring unreadable SLA day state file {self._state_path}: {e}")
            return
        if data.get("day") == self._current_day:
            self._total_samples = int(data.get("total_samples", 0))
            self._in_spec_samples = int(data.get("in_spec_samples", 0))
            self._downtime_by_reason = {str(k): int(v) for k, v in data.get("downtime_by_reason", {}).items()}
            self.logger().info(
                f"Restored SLA day state for {self._current_day}: "
                f"{self._in_spec_samples}/{self._total_samples} samples in spec."
            )
        elif data.get("day"):
            # The bot was down over midnight: surface the interrupted day for recording.
            self.pending_summary = DaySummary(
                day=str(data["day"]),
                connector_name=str(data.get("connector_name", self._identity.connector_name)),
                trading_pair=str(data.get("trading_pair", self._identity.trading_pair)),
                total_samples=int(data.get("total_samples", 0)),
                in_spec_samples=int(data.get("in_spec_samples", 0)),
                downtime_by_reason={str(k): int(v) for k, v in data.get("downtime_by_reason", {}).items()},
                complete=False,
            )
