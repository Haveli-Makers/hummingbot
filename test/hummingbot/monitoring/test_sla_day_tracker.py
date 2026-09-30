import json
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from zoneinfo import ZoneInfo

from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_sampler import DEPTH_BELOW_MIN, ONE_SIDE_MISSING

IST = ZoneInfo("Asia/Kolkata")
IDENTITY = MonitorIdentity("pmm", "wazirx", "USDT-INR")


def ist_ts(year, month, day, hour=0, minute=0, second=0) -> float:
    return datetime(year, month, day, hour, minute, second, tzinfo=IST).timestamp()


def sample(in_spec: bool = True, reasons=None, slo_results=None) -> SLASample:
    return SLASample(
        in_spec=in_spec,
        reasons=list(reasons or []),
        metrics={"bid_depth": "290", "ask_depth": "298"},
        slo_results=slo_results,
    )


class FakeClock:
    def __init__(self, start: float):
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


class SLADayTrackerTests(TestCase):
    def setUp(self):
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self._tmp.name)
        self.config = MonitoringConfigBase()
        self.clock = FakeClock(ist_ts(2026, 7, 13, 12, 0, 0))

    def tearDown(self):
        self._tmp.cleanup()
        super().tearDown()

    def make_tracker(self, persist_interval: float = 0.0) -> SLADayTracker:
        return SLADayTracker(
            self.config,
            IDENTITY,
            state_dir=self.state_dir,
            time_fn=self.clock.time,
            persist_interval_sec=persist_interval,
        )

    @property
    def state_path(self) -> Path:
        return self.state_dir / "pmm_wazirx_USDT-INR_sla_state.json"

    def test_tally_and_downtime_by_reason(self):
        tracker = self.make_tracker()
        for _ in range(3):
            tracker.record(sample(in_spec=True))
            self.clock.advance(1)
        tracker.record(sample(in_spec=False, reasons=[DEPTH_BELOW_MIN]))
        self.clock.advance(1)
        tracker.record(sample(in_spec=False, reasons=[DEPTH_BELOW_MIN, ONE_SIDE_MISSING]))

        self.assertEqual(5, tracker.total_samples)
        self.assertEqual(3, tracker.in_spec_samples)
        self.assertEqual(Decimal("60"), tracker.uptime_pct)
        summary = tracker.summary()
        self.assertEqual({DEPTH_BELOW_MIN: 2, ONE_SIDE_MISSING: 1}, summary.downtime_by_reason)
        self.assertEqual(DEPTH_BELOW_MIN, summary.main_cause)
        self.assertTrue(summary.complete)

    def test_day_key_uses_configured_timezone_not_utc(self):
        # 00:30 IST on July 13 is still July 12 in UTC
        self.clock = FakeClock(ist_ts(2026, 7, 13, 0, 30))
        utc_date = datetime.fromtimestamp(self.clock.now, tz=timezone.utc).date().isoformat()
        self.assertEqual("2026-07-12", utc_date)

        tracker = self.make_tracker()

        self.assertEqual("2026-07-13", tracker.current_day)

    def test_rollover_at_ist_midnight_returns_finished_day(self):
        self.clock = FakeClock(ist_ts(2026, 7, 13, 23, 59, 58))
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True))
        self.clock.advance(1)   # 23:59:59
        tracker.record(sample(in_spec=False, reasons=[DEPTH_BELOW_MIN]))
        self.clock.advance(2)   # 00:00:01 on July 14

        finished = tracker.record(sample(in_spec=True))

        self.assertIsNotNone(finished)
        self.assertEqual("2026-07-13", finished.day)
        self.assertEqual(2, finished.total_samples)
        self.assertEqual(1, finished.in_spec_samples)
        self.assertEqual(Decimal("50"), finished.uptime_pct)
        # New day started fresh with the sample that triggered the rollover
        self.assertEqual("2026-07-14", tracker.current_day)
        self.assertEqual(1, tracker.total_samples)
        self.assertEqual(1, tracker.in_spec_samples)

    def test_same_day_restart_restores_counters(self):
        tracker = self.make_tracker()
        for _ in range(4):
            tracker.record(sample(in_spec=True))
            self.clock.advance(1)
        tracker.record(sample(in_spec=False, reasons=[ONE_SIDE_MISSING]))

        restarted = self.make_tracker()

        self.assertEqual(5, restarted.total_samples)
        self.assertEqual(4, restarted.in_spec_samples)
        self.assertEqual({ONE_SIDE_MISSING: 1}, restarted.summary().downtime_by_reason)
        self.assertIsNone(restarted.pending_summary)

    def test_restart_on_new_day_starts_fresh_and_surfaces_pending_summary(self):
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True))
        tracker.record(sample(in_spec=False, reasons=[DEPTH_BELOW_MIN]))
        tracker.flush()

        # Bot down over midnight; restarted the next morning
        self.clock.advance(24 * 3600)
        restarted = self.make_tracker()

        self.assertEqual(0, restarted.total_samples)
        self.assertEqual("2026-07-14", restarted.current_day)
        pending = restarted.pending_summary
        self.assertIsNotNone(pending)
        self.assertEqual("2026-07-13", pending.day)
        self.assertEqual(2, pending.total_samples)
        self.assertEqual(1, pending.in_spec_samples)
        self.assertFalse(pending.complete)

    def test_corrupt_state_file_is_ignored(self):
        self.state_path.write_text("{not valid json")

        tracker = self.make_tracker()

        self.assertEqual(0, tracker.total_samples)
        self.assertIsNone(tracker.pending_summary)

    def test_persisted_state_content(self):
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True))

        data = json.loads(self.state_path.read_text())
        self.assertEqual("2026-07-13", data["day"])
        self.assertEqual("wazirx", data["connector_name"])
        self.assertEqual("USDT-INR", data["trading_pair"])
        self.assertEqual(1, data["total_samples"])
        self.assertEqual(1, data["in_spec_samples"])

    def test_slo_counters_tallied_and_summarized(self):
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True, slo_results={"tier1": True, "tier2": True}))
        self.clock.advance(1)
        tracker.record(sample(in_spec=False, reasons=["tier2_depth_below_min"],
                              slo_results={"tier1": True, "tier2": False}))

        summary = tracker.summary()
        self.assertEqual({"tier1": 2, "tier2": 1}, summary.slo_in_spec)
        self.assertEqual(Decimal("100"), summary.slo_uptime_pct("tier1"))
        self.assertEqual(Decimal("50"), summary.slo_uptime_pct("tier2"))

    def test_slo_counters_survive_restart(self):
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True, slo_results={"tier1": True, "tier2": False}))

        restarted = self.make_tracker()

        self.assertEqual({"tier1": 1, "tier2": 0}, restarted.summary().slo_in_spec)

    def test_rollover_summary_carries_slo_counters(self):
        self.clock = FakeClock(ist_ts(2026, 7, 13, 23, 59, 59))
        tracker = self.make_tracker()
        tracker.record(sample(in_spec=True, slo_results={"tier1": True}))
        self.clock.advance(2)

        finished = tracker.record(sample(in_spec=True, slo_results={"tier1": True}))

        self.assertEqual({"tier1": 1}, finished.slo_in_spec)
        self.assertEqual({"tier1": 1}, tracker.summary().slo_in_spec)

    def test_persist_interval_throttles_writes(self):
        tracker = self.make_tracker(persist_interval=5.0)
        tracker.record(sample(in_spec=True))  # first record persists (last_persist == 0)
        first_mtime = self.state_path.stat().st_mtime_ns

        self.clock.advance(1)
        tracker.record(sample(in_spec=True))  # within interval -> no write

        self.assertEqual(first_mtime, self.state_path.stat().st_mtime_ns)
        data = json.loads(self.state_path.read_text())
        self.assertEqual(1, data["total_samples"])
