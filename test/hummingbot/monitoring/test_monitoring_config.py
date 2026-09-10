import tempfile
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from hummingbot.monitoring.config import (
    MultiLevelPMMSLAMonitorConfig,
    PMMSLAMonitorConfig,
    SLATierConfig,
    load_monitoring_config,
)


class LoadMonitoringConfigTests(TestCase):
    def setUp(self):
        super().setUp()
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self._tmp_dir.name) / "monitoring.yml"

    def tearDown(self):
        self._tmp_dir.cleanup()
        super().tearDown()

    def write(self, content: str):
        self.config_path.write_text(content)

    def test_loads_full_config(self):
        self.write(
            "pmm_sla_monitor:\n"
            "  connector_name: wazirx\n"
            "  trading_pair: USDT-INR\n"
            "  spread_band_pct: 1.5\n"
            "  min_depth_quote: 20000\n"
            "  required_uptime_pct: 96\n"
            "  sample_interval_sec: 1\n"
            "  grace_period_sec: 2\n"
        )

        config = load_monitoring_config(self.config_path)

        self.assertIsInstance(config, PMMSLAMonitorConfig)
        self.assertEqual("wazirx", config.connector_name)
        self.assertEqual("USDT-INR", config.trading_pair)
        self.assertEqual(Decimal("1.5"), config.spread_band_pct)
        self.assertEqual(Decimal("20000"), config.min_depth_quote)

    def test_defaults_applied_for_optional_fields(self):
        self.write(
            "pmm_sla_monitor:\n"
            "  connector_name: wazirx\n"
            "  trading_pair: USDT-INR\n"
        )

        config = load_monitoring_config(self.config_path)

        self.assertEqual(Decimal("1.5"), config.spread_band_pct)
        self.assertEqual(Decimal("20000"), config.min_depth_quote)
        self.assertEqual(Decimal("96"), config.required_uptime_pct)
        self.assertEqual(1.0, config.sample_interval_sec)
        self.assertEqual(2.0, config.grace_period_sec)
        self.assertEqual("Asia/Kolkata", config.day_reset_timezone)

    def test_missing_file_returns_none(self):
        self.assertIsNone(load_monitoring_config(self.config_path))

    def test_missing_section_returns_none(self):
        self.write("something_else:\n  foo: bar\n")

        self.assertIsNone(load_monitoring_config(self.config_path))

    def test_disabled_returns_none(self):
        self.write(
            "pmm_sla_monitor:\n"
            "  enabled: false\n"
            "  connector_name: wazirx\n"
            "  trading_pair: USDT-INR\n"
        )

        self.assertIsNone(load_monitoring_config(self.config_path))

    def test_loads_multilevel_config_with_tiers(self):
        self.write(
            "multilevel_pmm_sla_monitor:\n"
            "  connector_name: wazirx\n"
            "  trading_pair: USDT-INR\n"
            "  tiers:\n"
            "    - {name: tier1, spread_band_pct: 1.35, min_depth_quote: 10000, required_uptime_pct: 99}\n"
            "    - {name: tier2, spread_band_pct: 1.75, min_depth_quote: 40000, required_uptime_pct: 97}\n"
            "    - {name: tier3, spread_band_pct: 2.10, min_depth_quote: 90000, required_uptime_pct: 96}\n"
        )

        config = load_monitoring_config(self.config_path)

        self.assertIsInstance(config, MultiLevelPMMSLAMonitorConfig)
        self.assertEqual(3, len(config.tiers))
        self.assertEqual(Decimal("1.35"), config.tiers[0].spread_band_pct)
        self.assertEqual(
            {"tier1": Decimal("99"), "tier2": Decimal("97"), "tier3": Decimal("96")},
            config.slo_targets(),
        )

    def test_duplicate_tier_names_raise(self):
        with self.assertRaises(Exception):
            MultiLevelPMMSLAMonitorConfig(
                connector_name="wazirx",
                trading_pair="USDT-INR",
                tiers=[
                    SLATierConfig(name="t", spread_band_pct=Decimal("1"),
                                  min_depth_quote=Decimal("1"), required_uptime_pct=Decimal("99")),
                    SLATierConfig(name="t", spread_band_pct=Decimal("2"),
                                  min_depth_quote=Decimal("2"), required_uptime_pct=Decimal("97")),
                ],
            )

    def test_empty_tiers_raise(self):
        with self.assertRaises(Exception):
            MultiLevelPMMSLAMonitorConfig(connector_name="wazirx", trading_pair="USDT-INR", tiers=[])

    def test_single_slo_config_has_no_slo_targets(self):
        config = PMMSLAMonitorConfig(connector_name="wazirx", trading_pair="USDT-INR")
        self.assertEqual({}, config.slo_targets())

    def test_invalid_values_raise(self):
        self.write(
            "pmm_sla_monitor:\n"
            "  connector_name: wazirx\n"
            "  trading_pair: USDT-INR\n"
            "  spread_band_pct: -1\n"
        )

        with self.assertRaises(Exception):
            load_monitoring_config(self.config_path)
