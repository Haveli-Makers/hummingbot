import csv
import tempfile
from pathlib import Path
from unittest import TestCase

from hummingbot.monitoring.alert import AlertStatus, Severity
from hummingbot.monitoring.config import MonitoringConfigBase
from hummingbot.monitoring.sampler_base import MonitorIdentity
from hummingbot.monitoring.sla_day_tracker import DaySummary
from hummingbot.monitoring.sla_recorder import SLARecorder
from hummingbot.monitoring.sla_sampler import DEPTH_BELOW_MIN, ONE_SIDE_MISSING

IDENTITY = MonitorIdentity("pmm", "wazirx", "USDT-INR")


class FakeDispatcher:
    def __init__(self):
        self.alerts = []

    def dispatch(self, alert) -> bool:
        self.alerts.append(alert)
        return True


def make_summary(total: int = 86400,
                 in_spec: int = 84499,
                 complete: bool = True,
                 downtime_by_reason=None) -> DaySummary:
    return DaySummary(
        day="2026-07-13",
        connector_name="wazirx",
        trading_pair="USDT-INR",
        total_samples=total,
        in_spec_samples=in_spec,
        downtime_by_reason=downtime_by_reason if downtime_by_reason is not None
        else {DEPTH_BELOW_MIN: total - in_spec},
        complete=complete,
    )


class SLARecorderTests(TestCase):
    def setUp(self):
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.output_dir = Path(self._tmp.name)
        self.config = MonitoringConfigBase()
        self.dispatcher = FakeDispatcher()
        self.recorder = SLARecorder(self.config, IDENTITY,
                                    dispatcher=self.dispatcher, output_dir=self.output_dir)

    def tearDown(self):
        self._tmp.cleanup()
        super().tearDown()

    def read_rows(self):
        with open(self.recorder.csv_path, newline="") as f:
            return list(csv.reader(f))

    def test_creates_csv_with_header_and_row(self):
        self.recorder.record(make_summary())

        rows = self.read_rows()
        self.assertEqual(2, len(rows))
        self.assertEqual("date", rows[0][0])
        self.assertEqual(
            ["2026-07-13", "wazirx", "USDT-INR", "97.80", "84499", "86400", "31.7",
             DEPTH_BELOW_MIN, "yes", "yes"],
            rows[1],
        )

    def test_appends_without_duplicate_header(self):
        self.recorder.record(make_summary())
        self.recorder.record(make_summary(in_spec=80000))

        rows = self.read_rows()
        self.assertEqual(3, len(rows))
        self.assertEqual("date", rows[0][0])
        self.assertNotEqual("date", rows[2][0])

    def test_breached_day_marked_and_alerted(self):
        # 81389/86400 = 94.20% < 96%
        self.recorder.record(make_summary(in_spec=81389))

        rows = self.read_rows()
        self.assertEqual("no", rows[1][8])
        self.assertEqual(1, len(self.dispatcher.alerts))
        alert = self.dispatcher.alerts[0]
        self.assertEqual("daily_sla_breach", alert.check)
        self.assertEqual(Severity.CRITICAL, alert.severity)
        self.assertEqual(AlertStatus.FIRING, alert.status)
        self.assertEqual("pmm.wazirx.USDT-INR", alert.source)
        self.assertIn("94.20%", alert.message)
        self.assertIn("2026-07-13", alert.message)
        self.assertIn("96", alert.message)
        self.assertIn(DEPTH_BELOW_MIN, alert.message)

    def test_no_alert_when_target_met(self):
        self.recorder.record(make_summary())          # 97.80%
        self.recorder.record(make_summary(in_spec=82944))  # exactly 96.00%

        self.assertEqual([], self.dispatcher.alerts)

    def test_no_alert_for_empty_day(self):
        self.recorder.record(make_summary(total=0, in_spec=0, downtime_by_reason={}))

        self.assertEqual([], self.dispatcher.alerts)
        self.assertEqual(2, len(self.read_rows()))

    def test_incomplete_day_marked_and_noted_in_alert(self):
        self.recorder.record(make_summary(total=1000, in_spec=500, complete=False,
                                          downtime_by_reason={ONE_SIDE_MISSING: 500}))

        rows = self.read_rows()
        self.assertEqual("no", rows[1][9])
        self.assertIn("Partial day", self.dispatcher.alerts[0].message)

    def test_without_dispatcher_records_csv_only(self):
        recorder = SLARecorder(self.config, IDENTITY, output_dir=self.output_dir)
        recorder.record(make_summary(in_spec=1000))

        self.assertEqual(2, len(self.read_rows()))

    def test_record_never_raises_on_filesystem_error(self):
        self.recorder.csv_path.mkdir()  # a directory at the CSV path makes writes fail

        self.recorder.record(make_summary(in_spec=81389))  # must not raise

        # The alert still goes out even though the CSV write failed
        self.assertEqual(1, len(self.dispatcher.alerts))

    def test_downtime_uses_sample_interval(self):
        config = MonitoringConfigBase(sample_interval_sec=2.0)
        recorder = SLARecorder(config, IDENTITY, dispatcher=self.dispatcher, output_dir=self.output_dir)
        recorder.record(make_summary(total=43200, in_spec=43000))

        rows = self.read_rows()
        self.assertEqual("86400", rows[1][5])   # 43200 samples * 2s
        self.assertEqual("6.7", rows[1][6])     # 200 samples * 2s / 60
