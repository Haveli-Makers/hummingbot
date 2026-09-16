from decimal import Decimal
from unittest import TestCase

from hummingbot.monitoring.sla_sampler import (
    DEPTH_BELOW_MIN,
    ONE_SIDE_MISSING,
    ORDER_BOOK_STALE,
    SPREAD_TOO_WIDE,
    OpenOrder,
    evaluate_sample,
)

MID = Decimal("100")
BAND_PCT = Decimal("1.5")
MIN_DEPTH = Decimal("20000")


def bid(price: str, amount: str) -> OpenOrder:
    return OpenOrder(is_buy=True, price=Decimal(price), amount_remaining=Decimal(amount))


def ask(price: str, amount: str) -> OpenOrder:
    return OpenOrder(is_buy=False, price=Decimal(price), amount_remaining=Decimal(amount))


class EvaluateSampleTests(TestCase):
    def evaluate(self, mid, orders):
        return evaluate_sample(mid=mid, orders=orders, band_pct=BAND_PCT, min_depth_quote=MIN_DEPTH)

    def test_both_sides_in_spec(self):
        result = self.evaluate(MID, [bid("99", "250"), ask("101", "210")])

        self.assertTrue(result.in_spec)
        self.assertEqual([], result.reasons)
        self.assertEqual(Decimal("24750"), result.bid_depth)   # 99 * 250
        self.assertEqual(Decimal("21210"), result.ask_depth)   # 101 * 210

    def test_one_side_missing(self):
        result = self.evaluate(MID, [bid("99", "250")])

        self.assertFalse(result.in_spec)
        self.assertEqual([ONE_SIDE_MISSING], result.reasons)
        self.assertEqual(Decimal("0"), result.ask_depth)

    def test_order_outside_band_is_spread_too_wide(self):
        # Ask at 102 is 2% from mid: present on the book but outside the 1.5% band
        result = self.evaluate(MID, [bid("99", "250"), ask("102", "300")])

        self.assertFalse(result.in_spec)
        self.assertEqual([SPREAD_TOO_WIDE], result.reasons)
        self.assertEqual(Decimal("0"), result.ask_depth)

    def test_depth_below_minimum(self):
        result = self.evaluate(MID, [bid("99", "250"), ask("101", "10")])

        self.assertFalse(result.in_spec)
        self.assertEqual([DEPTH_BELOW_MIN], result.reasons)
        self.assertEqual(Decimal("1010"), result.ask_depth)

    def test_cumulative_depth_across_multiple_orders(self):
        # 99 * 150 + 98.6 * 60 = 14850 + 5916 = 20766 >= 20000
        result = self.evaluate(MID, [bid("99", "150"), bid("98.6", "60"), ask("101", "210")])

        self.assertTrue(result.in_spec)
        self.assertEqual(Decimal("20766"), result.bid_depth)

    def test_price_exactly_on_band_edge_counts(self):
        # (100 - 98.5) / 100 == 1.5% exactly -> inside the band (inclusive)
        result = self.evaluate(MID, [bid("98.5", "250"), ask("101.5", "210")])

        self.assertTrue(result.in_spec)

    def test_depth_exactly_at_minimum_is_in_spec(self):
        # 100 * 200 = 20000 == minimum
        result = self.evaluate(MID, [bid("100", "200"), ask("100.5", "200")])

        self.assertTrue(result.in_spec)

    def test_bid_above_mid_still_counts_toward_depth(self):
        result = self.evaluate(MID, [bid("100.5", "250"), ask("101", "210")])

        self.assertTrue(result.in_spec)

    def test_missing_mid_is_orderbook_stale(self):
        for stale_mid in (None, Decimal("NaN"), Decimal("0")):
            result = self.evaluate(stale_mid, [bid("99", "250"), ask("101", "210")])

            self.assertFalse(result.in_spec)
            self.assertEqual([ORDER_BOOK_STALE], result.reasons)

    def test_both_sides_failing_reports_both_reasons(self):
        result = self.evaluate(MID, [ask("101", "10")])

        self.assertFalse(result.in_spec)
        self.assertIn(ONE_SIDE_MISSING, result.reasons)
        self.assertIn(DEPTH_BELOW_MIN, result.reasons)
