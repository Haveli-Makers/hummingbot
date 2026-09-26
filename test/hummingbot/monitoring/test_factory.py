from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, patch

from hummingbot.monitoring.config import (
    MonitoringConfigBase,
    MultiLevelPMMSLAMonitorConfig,
    PMMSLAMonitorConfig,
    SLATierConfig,
)
from hummingbot.monitoring.factory import create_sla_monitor
from hummingbot.monitoring.sla_monitor import SLAMonitor


class CreateSLAMonitorTests(TestCase):
    def setUp(self):
        super().setUp()
        self.trading_core = MagicMock()
        self.trading_core.markets = {"wazirx": MagicMock()}
        self.config = PMMSLAMonitorConfig(connector_name="wazirx", trading_pair="USDT-INR")

    def test_builds_full_stack_for_registered_config(self):
        with patch("hummingbot.monitoring.factory.SLADayTracker") as tracker_cls, \
                patch("hummingbot.monitoring.factory.SLARecorder") as recorder_cls:
            monitor = create_sla_monitor(self.trading_core, self.config, dispatcher=MagicMock())

        self.assertIsInstance(monitor, SLAMonitor)
        tracker_cls.assert_called_once()
        recorder_cls.assert_called_once()

    def test_builds_multilevel_stack_with_tier_targets(self):
        config = MultiLevelPMMSLAMonitorConfig(
            connector_name="wazirx",
            trading_pair="USDT-INR",
            tiers=[SLATierConfig(name="tier1", spread_band_pct=Decimal("1.35"),
                                 min_depth_quote=Decimal("100"), required_uptime_pct=Decimal("99"))],
        )
        with patch("hummingbot.monitoring.factory.SLADayTracker"), \
                patch("hummingbot.monitoring.factory.SLARecorder") as recorder_cls:
            monitor = create_sla_monitor(self.trading_core, config, dispatcher=MagicMock())

        self.assertIsInstance(monitor, SLAMonitor)
        self.assertEqual({"tier1": Decimal("99")},
                         recorder_cls.call_args.kwargs["slo_targets"])

    def test_unknown_connector_returns_none(self):
        self.trading_core.markets = {}

        monitor = create_sla_monitor(self.trading_core, self.config)

        self.assertIsNone(monitor)

    def test_unregistered_config_type_returns_none(self):
        monitor = create_sla_monitor(self.trading_core, MonitoringConfigBase())

        self.assertIsNone(monitor)
