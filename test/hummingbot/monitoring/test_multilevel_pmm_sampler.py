from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.alert import Severity
from hummingbot.monitoring.config import MultiLevelPMMSLAMonitorConfig, SLATierConfig
from hummingbot.monitoring.multilevel_pmm_sampler import MultiLevelPMMSampler
from hummingbot.monitoring.sla_sampler import ONE_SIDE_MISSING, ORDER_BOOK_STALE


def make_order(trade_type: TradeType, price: str, amount: str) -> Mock:
    order = Mock()
    order.trade_type = trade_type
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.executed_amount_base = Decimal("0")
    order.trading_pair = "USDT-INR"
    order.is_open = True
    return order


def three_levels(ask_l3_amount: str = "1.7"):
    """Bids at 1.25/1.65/2.0% below mid 100, asks mirrored above."""
    return [
        make_order(TradeType.BUY, "98.75", "1.1"),
        make_order(TradeType.BUY, "98.35", "1.4"),
        make_order(TradeType.BUY, "98.00", "1.7"),
        make_order(TradeType.SELL, "101.25", "1.1"),
        make_order(TradeType.SELL, "101.65", "1.4"),
        make_order(TradeType.SELL, "102.00", ask_l3_amount),
    ]


class MultiLevelPMMSamplerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.config = MultiLevelPMMSLAMonitorConfig(
            connector_name="wazirx",
            trading_pair="USDT-INR",
            tiers=[
                SLATierConfig(name="tier1", spread_band_pct=Decimal("1.35"),
                              min_depth_quote=Decimal("100"), required_uptime_pct=Decimal("99")),
                SLATierConfig(name="tier2", spread_band_pct=Decimal("1.75"),
                              min_depth_quote=Decimal("240"), required_uptime_pct=Decimal("97")),
                SLATierConfig(name="tier3", spread_band_pct=Decimal("2.2"),
                              min_depth_quote=Decimal("400"), required_uptime_pct=Decimal("96")),
            ],
        )
        self.connector = MagicMock()
        self.connector.get_price_by_type.return_value = Decimal("100")
        self.connector.network_status = NetworkStatus.CONNECTED
        self.trading_core = MagicMock()
        self.trading_core.markets = {"wazirx": self.connector}
        self.sampler = MultiLevelPMMSampler(self.trading_core, self.config)

    def set_orders(self, orders):
        self.connector.in_flight_orders = {f"order-{i}": o for i, o in enumerate(orders)}

    def test_identity_and_checks(self):
        self.assertEqual("pmm_ml.wazirx.USDT-INR", self.sampler.identity.source)
        self.assertEqual("pmm_ml_wazirx_USDT-INR", self.sampler.identity.instance_id)
        checks = self.sampler.check_alerts
        self.assertEqual(Severity.CRITICAL, checks[ONE_SIDE_MISSING][0])
        for tier in ("tier1", "tier2", "tier3"):
            self.assertIn(f"{tier}_depth_below_min", checks)

    def test_all_tiers_in_spec(self):
        # Nested cumulative depths: tier1 = L1 (109), tier2 = L1+L2 (246), tier3 = all (413)
        self.set_orders(three_levels())

        sample = self.sampler.take_sample()

        self.assertTrue(sample.in_spec)
        self.assertEqual([], sample.reasons)
        self.assertEqual({"tier1": True, "tier2": True, "tier3": True}, sample.slo_results)
        self.assertEqual("109", sample.metrics["tier1_bid"])
        self.assertEqual("246", sample.metrics["tier2_bid"])
        self.assertEqual("413", sample.metrics["tier3_bid"])

    def test_outer_tier_only_breach(self):
        # Shrink ask level 3: tier3 ask depth = 111+142+51 = 305 < 400; tiers 1-2 unaffected
        self.set_orders(three_levels(ask_l3_amount="0.5"))

        sample = self.sampler.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual(["tier3_depth_below_min"], sample.reasons)
        self.assertEqual([], sample.held_checks)   # both sides present -> tier owns its alert
        self.assertEqual({"tier1": True, "tier2": True, "tier3": False}, sample.slo_results)

    def test_missing_side_raises_critical_and_holds_tier_checks(self):
        self.set_orders([make_order(TradeType.BUY, "98.75", "1.1")])

        sample = self.sampler.take_sample()

        self.assertFalse(sample.in_spec)
        # The critical alert fires; the tier checks are failing but held, so they neither
        # fire redundantly nor resolve while the side is missing.
        self.assertEqual([ONE_SIDE_MISSING], sample.reasons)
        self.assertEqual(
            ["tier1_depth_below_min", "tier2_depth_below_min", "tier3_depth_below_min"],
            sample.held_checks,
        )
        self.assertEqual({"tier1": False, "tier2": False, "tier3": False}, sample.slo_results)

    def test_disconnected_is_stale_with_all_tiers_failing(self):
        self.set_orders(three_levels())
        self.connector.network_status = NetworkStatus.NOT_CONNECTED

        sample = self.sampler.take_sample()

        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)
        self.assertFalse(sample.data_available)
        self.assertEqual({"tier1": False, "tier2": False, "tier3": False}, sample.slo_results)

    def test_in_spec_sample_marks_data_available(self):
        self.set_orders(three_levels())

        self.assertTrue(self.sampler.take_sample().data_available)

    def test_describe_check_shows_only_the_failing_tier(self):
        self.set_orders(three_levels(ask_l3_amount="0.5"))

        detail = self.sampler.describe_check("tier3_depth_below_min", self.sampler.take_sample())

        self.assertIn("tier3: bid 413 / ask 305", detail)
        self.assertIn("need 400 within 2.2%", detail)
        self.assertNotIn("tier1", detail)

    def test_describe_lists_every_tier(self):
        self.set_orders(three_levels())

        detail = self.sampler.describe(self.sampler.take_sample())

        for fragment in ("tier1 bid/ask 109/111", "tier2 bid/ask 246/254",
                         "tier3 bid/ask 413/427", "need 400", "mid 100"):
            self.assertIn(fragment, detail)
