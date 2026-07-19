from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.config import PMMSLAMonitorConfig
from hummingbot.monitoring.pmm_sampler import PMMDepthSampler
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


class PMMDepthSamplerTests(TestCase):
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
        self.sampler = PMMDepthSampler(self.trading_core, self.config)

    def set_orders(self, *orders):
        self.connector.in_flight_orders = {f"order-{i}": o for i, o in enumerate(orders)}

    def test_identity(self):
        self.assertEqual("pmm.wazirx.USDT-INR", self.sampler.identity.source)
        self.assertEqual("pmm_wazirx_USDT-INR", self.sampler.identity.instance_id)

    def test_in_spec_sample_with_metrics(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )

        sample = self.sampler.take_sample()

        self.assertTrue(sample.in_spec)
        self.assertEqual([], sample.reasons)
        self.assertEqual("24750", sample.metrics["bid_depth"])
        self.assertEqual("21210", sample.metrics["ask_depth"])
        self.assertEqual("100", sample.metrics["mid"])

    def test_partial_fill_reduces_depth(self):
        # 250 - 100 filled = 150 remaining -> 99 * 150 = 14850 < 20000
        self.set_orders(
            make_order(TradeType.BUY, "99", "250", executed="100"),
            make_order(TradeType.SELL, "101", "210"),
        )

        sample = self.sampler.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertIn(DEPTH_BELOW_MIN, sample.reasons)
        self.assertEqual("14850", sample.metrics["bid_depth"])

    def test_closed_orders_and_other_pairs_excluded(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210", is_open=False),
            make_order(TradeType.SELL, "101", "500", trading_pair="BTC-INR"),
        )

        sample = self.sampler.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertIn(ONE_SIDE_MISSING, sample.reasons)
        self.assertEqual("0", sample.metrics["ask_depth"])

    def test_price_fetch_failure_is_stale_sample(self):
        self.set_orders(make_order(TradeType.BUY, "99", "250"))
        self.connector.get_price_by_type.side_effect = ValueError("no order book")

        sample = self.sampler.take_sample()

        self.assertFalse(sample.in_spec)
        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_missing_connector_is_stale_sample(self):
        self.trading_core.markets = {}

        sample = self.sampler.take_sample()

        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_disconnected_connector_is_stale_sample(self):
        # A frozen local order book still serves the last-known mid; a disconnected
        # connector must therefore score as stale even though the price call works.
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )
        self.connector.network_status = NetworkStatus.NOT_CONNECTED

        sample = self.sampler.take_sample()

        self.assertEqual([ORDER_BOOK_STALE], sample.reasons)

    def test_describe_includes_depths_and_requirement(self):
        self.set_orders(
            make_order(TradeType.BUY, "99", "250"),
            make_order(TradeType.SELL, "101", "210"),
        )

        detail = self.sampler.describe(self.sampler.take_sample())

        self.assertIn("bid depth 24750", detail)
        self.assertIn("ask depth 21210", detail)
        self.assertIn("need 20000 per side", detail)
        self.assertIn("mid 100", detail)
