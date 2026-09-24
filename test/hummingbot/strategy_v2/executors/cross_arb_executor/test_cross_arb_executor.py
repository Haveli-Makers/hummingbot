"""
Cross-arb executor: what happens when the two legs do NOT behave.

Every test drives control_task() by hand against fake venues, so the whole life of an attempt is
covered without a network: both legs fill, one leg is refused, one leg times out, a partial fill,
a cancel the venue lied about, the leftover being flattened or held, and shutdown mid-flight.
"""
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.cross_arb_executor.fakes import FakeConnector, FakeStrategy
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy_v2.executors.cross_arb_executor.cross_arb_executor import CrossArbExecutor
from hummingbot.strategy_v2.executors.cross_arb_executor.data_types import (
    CrossArbExecutorConfig,
    CrossArbPhase,
    LegOrder,
    MismatchPolicy,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

D = Decimal


class CrossArbExecutorTests(IsolatedAsyncioWrapperTestCase):
    """Buy 1 SOL on csx at 100, sell it on wazirx at 102: a 2% gap."""

    def setUp(self):
        super().setUp()
        self.csx = FakeConnector("csx", bid=D("99"), ask=D("100"))
        self.wazirx = FakeConnector("wazirx", bid=D("102"), ask=D("103"))
        self.strategy = FakeStrategy({"csx": self.csx, "wazirx": self.wazirx})

    def make(self, **overrides) -> CrossArbExecutor:
        config = CrossArbExecutorConfig(
            timestamp=1_000.0,
            buying_market=ConnectorPair(connector_name="csx", trading_pair="SOL-INR"),
            selling_market=ConnectorPair(connector_name="wazirx", trading_pair="SOL-INR"),
            order_amount=D("1"), buy_price_cap=D("100"), sell_price_floor=D("102"),
            **overrides)
        executor = CrossArbExecutor(self.strategy, config)
        executor._status = RunnableStatus.RUNNING
        return executor

    async def tick(self, executor: CrossArbExecutor, seconds: float = 0.0):
        if seconds:
            self.strategy.advance(seconds)
        await executor.control_task()

    # ── the happy path ────────────────────────────────────────────────────────

    async def test_both_legs_fill_and_the_result_is_measured(self):
        executor = self.make(tds_pct=D("1"))
        await self.tick(executor)   # PLACING -> orders out
        self.assertEqual(executor.phase, CrossArbPhase.WAITING)
        self.assertEqual(len(self.strategy.sent), 2)
        buy, sell = self.strategy.sent[0], self.strategy.sent[1]
        self.assertEqual((buy[0], buy[2], buy[4]), ("csx", TradeType.BUY, D("100")))
        self.assertEqual((sell[0], sell[2], sell[4]), ("wazirx", TradeType.SELL, D("102")))

        self.strategy.order("buy-1").fill(fee_pct=D("0.1"))
        self.strategy.order("sell-2").fill(fee_pct=D("0"))
        await self.tick(executor)   # WAITING -> CLEANUP
        await self.tick(executor)   # CLEANUP -> RECONCILE
        await self.tick(executor)   # RECONCILE -> done

        self.assertEqual(executor.close_type, CloseType.COMPLETED)
        result = executor.result
        self.assertEqual(result.bought_quote, D("100"))
        self.assertEqual(result.sold_quote, D("102"))
        self.assertEqual(result.fees_quote, D("0.1"))       # 0.1% of the buy
        self.assertEqual(result.tds_quote, D("1.02"))       # 1% of the sale
        self.assertEqual(result.net_quote, D("0.88"))
        # Percentage is against what we SPENT, not against the coin amount.
        self.assertAlmostEqual(float(executor.get_net_pnl_pct()), 0.0088, places=6)

    async def test_orders_are_limits_capped_at_the_decided_price(self):
        executor = self.make(slippage_ticks=2)
        await self.tick(executor)
        _, _, _, _, buy_price = self.strategy.sent[0]
        _, _, _, _, sell_price = self.strategy.sent[1]
        self.assertEqual(buy_price, D("100.02"))    # two ticks through the ask, still capped
        self.assertEqual(sell_price, D("101.98"))   # two ticks below the bid

    # ── one leg misbehaves ────────────────────────────────────────────────────

    async def test_refused_leg_stops_the_attempt_and_is_not_resent(self):
        executor = self.make()
        await self.tick(executor)
        executor.process_order_failed_event(None, None, MarketOrderFailureEvent(
            timestamp=self.strategy.current_timestamp, order_id="buy-1", order_type=None))
        self.assertEqual(executor.phase, CrossArbPhase.CLEANUP)
        self.assertEqual(len(self.strategy.sent), 2)                   # nothing re-sent
        self.assertIn(("wazirx", "SOL-INR", "sell-2"), self.strategy.cancelled)

    async def test_filled_buy_with_refused_sell_is_flattened(self):
        executor = self.make()
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        executor.process_order_failed_event(None, None, MarketOrderFailureEvent(
            timestamp=self.strategy.current_timestamp, order_id="sell-2", order_type=None))
        await self.tick(executor, seconds=1)      # cleanup
        await self.tick(executor)                 # reconcile -> flatten
        flatten = self.strategy.sent[-1]
        self.assertEqual(flatten[2], TradeType.SELL)
        self.assertEqual(flatten[3], D("1"))
        self.assertEqual(flatten[0], "wazirx")    # the better bid of the two venues
        self.strategy.order("sell-3").fill()
        await self.tick(executor)
        self.assertEqual(executor.close_type, CloseType.COMPLETED)
        self.assertEqual(executor.result.imbalance_base, D("0"))

    async def test_nothing_fills_before_the_deadline(self):
        executor = self.make(fill_timeout=10)
        await self.tick(executor)
        await self.tick(executor, seconds=11)      # WAITING -> CLEANUP on the timeout
        self.assertEqual(executor.phase, CrossArbPhase.CLEANUP)
        self.assertEqual(len(self.strategy.cancelled), 2)
        self.strategy.order("buy-1").cancel()
        self.strategy.order("sell-2").cancel()
        await self.tick(executor, seconds=1)       # CLEANUP -> RECONCILE
        await self.tick(executor)
        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertEqual(executor.get_net_pnl_quote(), D("0"))

    async def test_partial_fills_keep_the_matched_part_and_flatten_the_rest(self):
        executor = self.make(fill_timeout=10)
        await self.tick(executor)
        self.strategy.order("buy-1").fill(amount=D("1"))
        self.strategy.order("sell-2").fill(amount=D("0.4"))
        await self.tick(executor, seconds=11)
        self.strategy.order("sell-2").cancel()
        await self.tick(executor, seconds=1)
        await self.tick(executor)
        flatten = self.strategy.sent[-1]
        self.assertEqual((flatten[2], flatten[3]), (TradeType.SELL, D("0.6")))

    # ── the cancel that lies ──────────────────────────────────────────────────

    async def test_fill_after_a_confirmed_cancel_is_counted_and_flattened(self):
        executor = self.make(fill_timeout=10)
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        await self.tick(executor, seconds=11)          # cleanup: cancel the unfilled sell
        self.strategy.order("sell-2").cancel()
        await self.tick(executor, seconds=1)           # reconcile
        await self.tick(executor)                      # flatten placed for 1 SOL
        self.assertEqual(self.strategy.sent[-1][3], D("1"))

        # The venue now fills the order it already said it had cancelled.
        self.strategy.order("sell-2").fill()
        await self.tick(executor)
        self.assertTrue(any(o.late_fill_logged for o in executor._orders))
        # We are now SHORT by the flatten order: it must not be left unnoticed.
        self.strategy.order("sell-3").fill()
        await self.tick(executor)
        self.assertEqual(executor.result.imbalance_base, D("-1"))
        await self.tick(executor)
        self.assertEqual(self.strategy.sent[-1][2], TradeType.BUY)   # buys the short back

    async def test_a_venue_reporting_filled_without_fills_is_believed(self):
        executor = self.make()
        await self.tick(executor)
        self.strategy.order("buy-1").mark_filled_without_fills()
        self.strategy.order("sell-2").fill()
        await self.tick(executor)
        await self.tick(executor)
        await self.tick(executor)
        self.assertEqual(executor.result.bought_base, D("1"))
        self.assertEqual(executor.close_type, CloseType.COMPLETED)

    # ── mismatch policy and dust ──────────────────────────────────────────────

    async def test_mismatch_can_be_held_instead_of_flattened(self):
        executor = self.make(fill_timeout=10, mismatch_policy=MismatchPolicy.HOLD)
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        await self.tick(executor, seconds=11)
        self.strategy.order("sell-2").cancel()
        await self.tick(executor, seconds=1)
        await self.tick(executor)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(executor.result.imbalance_base, D("1"))
        self.assertEqual(len([s for s in self.strategy.sent if s[3] == D("1")]), 2)  # no third order

    async def test_leftover_below_the_venue_minimum_is_reported_not_traded(self):
        # dust_threshold below the venue's own 10 INR minimum: the leftover is worth chasing by
        # our rules, but no venue would accept the order.
        executor = self.make(fill_timeout=10, dust_threshold_quote=D("1"))
        await self.tick(executor)
        self.strategy.order("buy-1").fill(amount=D("1"))
        self.strategy.order("sell-2").fill(amount=D("0.95"))   # 0.05 SOL = 5 INR, below the 10 INR minimum
        await self.tick(executor, seconds=11)
        self.strategy.order("sell-2").cancel()
        await self.tick(executor, seconds=1)
        await self.tick(executor)
        self.assertEqual(executor.close_type, CloseType.COMPLETED)
        self.assertEqual(len(self.strategy.sent), 2)           # nothing that would be rejected
        self.assertIn("below the venue minimum", executor.get_custom_info()["close_reason"])

    # ── balances, sizing and sequencing ───────────────────────────────────────

    async def test_missing_balance_stops_before_anything_is_sent(self):
        self.csx.balances["INR"] = D("50")     # needs 100
        executor = self.make()
        await executor.validate_sufficient_balance()
        self.assertEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(self.strategy.sent, [])

    async def test_sequential_mode_sizes_the_second_leg_to_the_first_fill(self):
        executor = self.make(leg_order=LegOrder.SELL_FIRST)
        await self.tick(executor)
        self.assertEqual(len(self.strategy.sent), 1)
        self.assertEqual(self.strategy.sent[0][2], TradeType.SELL)
        self.strategy.order("sell-1").fill(amount=D("0.5"))
        await self.tick(executor)
        self.assertEqual(len(self.strategy.sent), 2)
        self.assertEqual((self.strategy.sent[1][2], self.strategy.sent[1][3]), (TradeType.BUY, D("0.5")))

    # ── shutdown ──────────────────────────────────────────────────────────────

    async def test_early_stop_cancels_and_flattens_immediately(self):
        executor = self.make()
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        executor.early_stop()
        # Everything must already have been sent inside early_stop(), not on the next tick.
        self.assertEqual(len(self.strategy.cancelled), 1)
        self.assertEqual(self.strategy.sent[-1][2], TradeType.SELL)
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    async def test_early_stop_with_keep_position_does_not_trade(self):
        executor = self.make()
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        executor.early_stop(keep_position=True)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(len(self.strategy.sent), 2)

    async def test_shutdown_gives_up_loudly_rather_than_hanging(self):
        executor = self.make()
        await self.tick(executor)
        self.strategy.order("buy-1").fill()
        executor.early_stop()
        self.strategy.order("sell-3").fail()      # the flatten order is refused
        await self.tick(executor, seconds=executor.SHUTDOWN_SECONDS + 1)
        self.assertEqual(executor._status, RunnableStatus.TERMINATED)
        self.assertIn("unmatched", executor.get_custom_info()["close_reason"])

    # ── reporting ─────────────────────────────────────────────────────────────

    async def test_custom_info_carries_what_happened(self):
        executor = self.make(tds_pct=D("1"))
        await self.tick(executor)
        self.strategy.order("buy-1").fill(fee_pct=D("0.1"))
        self.strategy.order("sell-2").fill()
        for _ in range(3):
            await self.tick(executor)
        info = executor.get_custom_info()
        self.assertEqual(info["buy_connector"], "csx")
        self.assertEqual(info["sell_connector"], "wazirx")
        self.assertEqual(info["bought_base"], D("1"))
        self.assertEqual(info["sold_base"], D("1"))
        self.assertEqual(info["imbalance_base"], D("0"))
        self.assertEqual(info["expected_gross_pct"], D("2"))
        self.assertEqual(info["tds_quote"], D("1.02"))
        self.assertEqual(info["phase"], "done")
        self.assertTrue(executor.to_format_status()[0].strip().startswith("Cross-exchange arbitrage"))

    async def test_config_refuses_mismatched_markets(self):
        with self.assertRaises(ValueError):
            CrossArbExecutorConfig(
                timestamp=1_000.0,
                buying_market=ConnectorPair(connector_name="csx", trading_pair="SOL-INR"),
                selling_market=ConnectorPair(connector_name="wazirx", trading_pair="SOL-USDT"),
                order_amount=D("1"), buy_price_cap=D("100"), sell_price_floor=D("102"))
