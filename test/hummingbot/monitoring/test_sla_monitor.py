import tempfile
from decimal import Decimal
from typing import Dict, Tuple
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.monitoring.alert import AlertStatus, Severity
from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample, SLASamplerBase
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_monitor import SLAMonitor

CHECK = "one_side_missing"


class FakeSampler(SLASamplerBase):
    def __init__(self):
        super().__init__(MonitorIdentity("pmm", "wazirx", "USDT-INR"))
        self.next_sample = SLASample(in_spec=True, metrics={"bid_depth": "290"})

    @property
    def check_alerts(self) -> Dict[str, Tuple[Severity, str]]:
        return {CHECK: (Severity.CRITICAL, "One side has no standing orders")}

    def take_sample(self) -> SLASample:
        return self.next_sample

    def config_summary(self) -> str:
        return "band 1.5%, min depth 250 quote"


class SLAMonitorTests(TestCase):
    def setUp(self):
        super().setUp()
        self.sampler = FakeSampler()
        self.config = MonitoringConfigBase(grace_period_sec=0.0, alert_warmup_sec=0.0)

    def make_monitor(self, **kwargs) -> SLAMonitor:
        return SLAMonitor(self.sampler, self.config, **kwargs)

    def in_spec(self) -> SLASample:
        return SLASample(in_spec=True, metrics={"bid_depth": "290"})

    def breached(self) -> SLASample:
        return SLASample(in_spec=False, reasons=[CHECK], metrics={"bid_depth": "0"})

    def test_uptime_tally(self):
        monitor = self.make_monitor()
        for sample in (self.in_spec(), self.in_spec(), self.breached()):
            self.sampler.next_sample = sample
            monitor._process_sample(self.sampler.take_sample())

        self.assertEqual(3, monitor._samples_total)
        self.assertEqual(2, monitor._samples_in_spec)
        self.assertAlmostEqual(Decimal("66.67"), monitor.uptime_pct.quantize(Decimal("0.01")))

    def test_breach_alerts_fire_and_resolve_through_dispatcher(self):
        dispatcher = MagicMock()
        dispatcher.dispatch.return_value = True
        monitor = self.make_monitor(dispatcher=dispatcher)

        monitor._process_sample(self.breached())
        fired = [call.args[0] for call in dispatcher.dispatch.call_args_list
                 if call.args[0].status == AlertStatus.FIRING]
        self.assertTrue(any(a.check == CHECK for a in fired))
        self.assertTrue(all(a.source == "pmm.wazirx.USDT-INR" for a in fired))

        dispatcher.dispatch.reset_mock()
        monitor._process_sample(self.in_spec())
        resolved = [call.args[0] for call in dispatcher.dispatch.call_args_list
                    if call.args[0].status == AlertStatus.RESOLVED]
        self.assertTrue(any(a.check == CHECK for a in resolved))

    def test_no_dispatcher_means_log_only(self):
        monitor = self.make_monitor()

        self.assertEqual({}, monitor._fsms)
        monitor._process_sample(self.breached())
        self.assertEqual(1, monitor._samples_total)

    def test_reason_changes_tracked(self):
        monitor = self.make_monitor()
        monitor._process_sample(self.breached())
        self.assertEqual([CHECK], monitor._last_reasons)

        monitor._process_sample(self.in_spec())
        self.assertEqual([], monitor._last_reasons)

    def test_day_tracker_records_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = SLADayTracker(self.config, self.sampler.identity, state_dir=tmp)
            monitor = self.make_monitor(day_tracker=tracker)
            monitor._process_sample(self.in_spec())
            monitor._process_sample(self.breached())

            self.assertEqual(2, tracker.total_samples)
            self.assertEqual(1, tracker.in_spec_samples)
            self.assertEqual({CHECK: 1}, tracker.summary().downtime_by_reason)

    def test_day_close_hands_summary_to_recorder(self):
        tracker = MagicMock()
        tracker.uptime_pct = Decimal("0")
        tracker.current_day = "2026-07-14"
        tracker.in_spec_samples = 0
        tracker.total_samples = 1
        finished_day = MagicMock()
        finished_day.uptime_pct = Decimal("94.20")
        finished_day.day = "2026-07-13"
        finished_day.in_spec_samples = 81389
        finished_day.total_samples = 86400
        finished_day.main_cause = "depth_below_min"
        tracker.record.return_value = finished_day
        recorder = MagicMock()
        monitor = self.make_monitor(day_tracker=tracker, recorder=recorder)

        monitor._process_sample(self.in_spec())

        recorder.record.assert_called_once_with(finished_day)

    def test_interrupted_day_recorded_on_start(self):
        tracker = MagicMock()
        pending = MagicMock()
        pending.day = "2026-07-12"
        tracker.pending_summary = pending
        recorder = MagicMock()
        monitor = self.make_monitor(day_tracker=tracker, recorder=recorder)

        monitor._record_interrupted_day()

        recorder.record.assert_called_once_with(pending)
        self.assertIsNone(tracker.pending_summary)
