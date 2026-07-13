import tempfile
from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.alert import AlertStatus
from hummingbot.monitoring.config import PMMSLAMonitorConfig
from hummingbot.monitoring.pmm_sla_monitor import PMMSLAMonitor
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_sampler import DEPTH_BELOW_MIN, ONE_SIDE_MISSING, ORDER_BOOK_STALE


def make_order(trade_type: TradeType,
               price: str,
               amount: str,
               executed: str = "0",
               trading_pair: str = "USDT-INR",
               is_open: bool = True) -> Mock:
    order = Mock()
    order.trade_type = trade_type
    order.price = Decimal(price)
    order.amount = Decimal(amount)
    order.executed_amount_base = Decimal(executed)
    order.trading_pair = trading_pair
    order.is_open = is_open
    return order


class PMMSLAMonitorTests(TestCase):
    def setUp(self):
        super().setUp()
        self.config = PMMSLAMonitorConfig(
            connector_name="wazirx",
            trading_pair="USDT-INR",
            spread_band_pct=Decimal("1.5"),
            min_depth_quote=Decimal("20000"),
        )
        self.connector = MagicMock()
        self.connector.get_price_by_type.return_value = Decimal("100")
        self.connector.network_status = NetworkStatus.CONNECTED
        self.trading_core = MagicMock()
        self.trading_core.markets = {"wazirx": self.connector}
        self.monitor = PMMSLAMonitor(self.trading_core, self.config)

    def set_orders(self, *orders):
        self.connector.in_flight_orders = {f"order-{i}": o for i, o in enumerate(orders)}

    def test_take_sample_in_spec(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )

        sample = self.monitor.take_sample()

        self.assertTrue(sample.in_spec)
        self.assertEqual(Decimal("24750"), sample.bid_depth)
        self.assertEqual(Decimal("21210"), sample.ask_depth)

    def test_partial_fill_reduces_depth(self):
        # 250 - 100 filled = 150 remaining -> 99 * 150 = 14850 < 20000
        self.set_orders(
            make_order(TradeType.BUY, "99", "250", executed="100"),
            make_order(TradeType.SELL, "101", "210"),
        )

        sample = self.monitor.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual(Decimal("14850"), sample.bid_depth)
        self.assertIn(DEPTH_BELOW_MIN, sample.reasons)

    def test_closed_orders_and_other_pairs_excluded(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210", is_open=False),
            make_order(TradeType.SELL, "101", "500", trading_pair="BTC-INR"),
        )

        sample = self.monitor.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual(Decimal("0"), sample.ask_depth)

    def test_price_fetch_failure_is_stale_sample(self):
        self.set_orders(make_order(TradeType.BUY, "99", "250"))
        self.connector.get_price_by_type.side_effect = ValueError("no order book")

        sample = self.monitor.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_missing_connector_is_stale_sample(self):
        self.trading_core.markets = {}

        sample = self.monitor.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_disconnected_connector_is_stale_sample(self):
        # A frozen local order book still serves the last-known mid; a disconnected
        # connector must therefore score as stale even though the price call works.
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )
        self.connector.network_status = NetworkStatus.NOT_CONNECTED

        sample = self.monitor.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_reason_changes_tracked_while_out_of_spec(self):
        # out of spec: ask missing entirely
        self.set_orders(make_order(TradeType.BUY, "99", "250"))
        self.monitor._process_sample(self.monitor.take_sample())
        self.assertEqual(["one_side_missing"], self.monitor._last_reasons)

        # still out of spec, but for a different reason: ask outside the band
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "102", "300"),
        )
        self.monitor._process_sample(self.monitor.take_sample())
        self.assertEqual(["spread_too_wide"], self.monitor._last_reasons)

    def test_breach_alerts_fire_and_resolve_through_dispatcher(self):
        dispatcher = MagicMock()
        dispatcher.dispatch.return_value = True
        config = PMMSLAMonitorConfig(
            connector_name="wazirx",
            trading_pair="USDT-INR",
            min_depth_quote=Decimal("20000"),
            grace_period_sec=0.0,   # fire immediately for the test
            alert_warmup_sec=0.0,
        )
        monitor = PMMSLAMonitor(self.trading_core, config, dispatcher=dispatcher)

        # Ask missing entirely -> one_side_missing fires (grace 0)
        self.set_orders(make_order(TradeType.BUY, "99", "250"))
        monitor._process_sample(monitor.take_sample())

        fired = [call.args[0] for call in dispatcher.dispatch.call_args_list
                 if call.args[0].status == AlertStatus.FIRING]
        self.assertTrue(any(a.check == ONE_SIDE_MISSING for a in fired))
        self.assertTrue(all(a.source == "pmm.wazirx.USDT-INR" for a in fired))

        # Both sides now well above the minimum -> resolved alerts dispatched
        dispatcher.dispatch.reset_mock()
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "250"),
        )
        monitor._process_sample(monitor.take_sample())

        resolved = [call.args[0] for call in dispatcher.dispatch.call_args_list
                    if call.args[0].status == AlertStatus.RESOLVED]
        self.assertTrue(any(a.check == ONE_SIDE_MISSING for a in resolved))

    def test_day_tracker_records_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = SLADayTracker(self.config, state_dir=tmp)
            monitor = PMMSLAMonitor(self.trading_core, self.config, day_tracker=tracker)
            self.set_orders(
                make_order(TradeType.BUY, "99", "250"),
                make_order(TradeType.SELL, "101", "210"),
            )
            monitor._process_sample(monitor.take_sample())
            self.set_orders(make_order(TradeType.BUY, "99", "250"))
            monitor._process_sample(monitor.take_sample())

            self.assertEqual(2, tracker.total_samples)
            self.assertEqual(1, tracker.in_spec_samples)
            self.assertEqual({ONE_SIDE_MISSING: 1}, tracker.summary().downtime_by_reason)

    def test_no_dispatcher_means_log_only(self):
        self.set_orders(make_order(TradeType.BUY, "99", "250"))

        # self.monitor was built without a dispatcher: no FSMs, sampling still works
        self.assertEqual({}, self.monitor._fsms)
        self.monitor._process_sample(self.monitor.take_sample())
        self.assertEqual(1, self.monitor._samples_total)

    def test_uptime_tally(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )
        self.monitor._process_sample(self.monitor.take_sample())
        self.monitor._process_sample(self.monitor.take_sample())
        self.set_orders(make_order(TradeType.BUY, "99", "250"))
        self.monitor._process_sample(self.monitor.take_sample())

        self.assertEqual(3, self.monitor._samples_total)
        self.assertEqual(2, self.monitor._samples_in_spec)
        self.assertAlmostEqual(Decimal("66.67"), self.monitor.uptime_pct.quantize(Decimal("0.01")))
