"""
Cross-arb controller: the decision to trade, and every reason not to.

The executor is tested separately; here the question is only whether the controller reads the two
books correctly, sizes an order both venues would accept, and refuses in every case where trading
would be unsafe — each refusal named, because "why is it not trading?" is the question an operator
asks first.
"""
import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from unittest.mock import MagicMock

from controllers.generic.cross_arb import CrossArbConfig, CrossArbController, TriggerOn
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.models.executors import CloseType

D = Decimal
START_TS = 1_790_000_000
PAIR = "USDT-INR"


class FakeBook:
    """Only what the controller reads: the top of each side, and whether the venue is still talking."""

    def __init__(self, bid, bid_qty, ask, ask_qty, uid=1):
        self.bid, self.bid_qty, self.ask, self.ask_qty = D(bid), D(bid_qty), D(ask), D(ask_qty)
        self.snapshot_uid = uid

    def bid_entries(self):
        yield OrderBookRow(float(self.bid), float(self.bid_qty), self.snapshot_uid)

    def ask_entries(self):
        yield OrderBookRow(float(self.ask), float(self.ask_qty), self.snapshot_uid)


class CrossArbControllerTests(IsolatedAsyncioWrapperTestCase):
    """
    Default market: CSX sells at 99.00, WazirX buys at 100.00 — a 1.01% gap, buy CSX / sell WazirX.
    Fees are the measured ones: CSX 0.05% taker, WazirX 0%, 1% TDS on the sell.
    """

    def setUp(self):
        super().setUp()
        self.now = START_TS
        self.books = {
            ("csx", PAIR): FakeBook("98.90", "500", "99.00", "500"),
            ("wazirx", PAIR): FakeBook("100.00", "500", "100.10", "500"),
        }
        self.balances = {("csx", "INR"): D("100000"), ("csx", "USDT"): D("1000"),
                         ("wazirx", "INR"): D("100000"), ("wazirx", "USDT"): D("1000")}
        self.steps = {"csx": D("0.01"), "wazirx": D("0.01")}
        self.rules = {"csx": TradingRule(PAIR, min_order_size=D("0.01"), min_notional_size=D("60")),
                      "wazirx": TradingRule(PAIR, min_order_size=D("0.01"), min_notional_size=D("50"))}

        provider = MagicMock(spec=MarketDataProvider)
        provider.time.side_effect = lambda: self.now
        provider.get_order_book.side_effect = lambda ex, pair: self.books[(ex, pair)]
        provider.get_trading_rules.side_effect = lambda ex, pair: self.rules[ex]
        provider.quantize_order_amount.side_effect = self._quantize
        provider.get_connector.side_effect = self._connector
        self.provider = provider

    def _quantize(self, exchange, pair, amount):
        step = self.steps[exchange]
        return (D(str(amount)) // step) * step

    def _connector(self, exchange):
        connector = MagicMock()
        connector.get_available_balance.side_effect = lambda asset: self.balances.get((exchange, asset), D("0"))
        return connector

    def controller(self, **overrides) -> CrossArbController:
        params = dict(id="arb-1", exchange_a="csx", exchange_b="wazirx", trading_pairs=[PAIR],
                      min_profitability=D("0.01"), order_amount_quote=D("10000"),
                      min_order_amount_quote=D("100"),
                      taker_fee_pct={"csx": D("0.05"), "wazirx": D("0")}, gst_pct=D("18"), tds_pct=D("1"))
        params.update(overrides)
        return CrossArbController(CrossArbConfig(**params), self.provider, asyncio.Queue())

    def executor(self, active=True, pair=PAIR, close_type=None, pnl=D("0"), executor_id="e1"):
        return SimpleNamespace(
            id=executor_id, is_active=active, close_type=close_type, net_pnl_quote=pnl,
            config=SimpleNamespace(buying_market=SimpleNamespace(trading_pair=pair)))

    async def opportunities(self, controller):
        await controller.update_processed_data()
        return {(o.buy_exchange, o.sell_exchange): o for o in controller.processed_data["opportunities"]}

    # ── reading the market ───────────────────────────────────────────────────

    async def test_both_directions_are_priced(self):
        controller = self.controller()
        found = await self.opportunities(controller)
        good = found[("csx", "wazirx")]
        self.assertAlmostEqual(float(good.gross_pct), 1.0101, places=3)
        # net = [100 * (1 - 0 - 0.01)] - [99 * (1 + 0.0005 * 1.18)] = 99.0 - 99.05841, over 99.
        # A 1% gap does not survive 1% TDS: that is the whole economics of this strategy.
        self.assertAlmostEqual(float(good.net_pct), -0.059, places=3)
        self.assertIsNone(good.blocked_by)
        self.assertEqual(found[("wazirx", "csx")].blocked_by, "gap below threshold")

    async def test_trigger_can_use_the_net_number_instead(self):
        controller = self.controller(trigger_on=TriggerOn.NET, min_profitability=D("0.005"))
        found = await self.opportunities(controller)
        # gross is 1.01% but net is negative, so on NET it must not trade
        self.assertEqual(found[("csx", "wazirx")].blocked_by, "gap below threshold")

    async def test_a_book_that_stops_updating_is_refused(self):
        controller = self.controller()
        await controller.update_processed_data()          # first look: remembers the snapshot id
        self.now += 11                                    # max_book_age is 10s, id unchanged
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].blocked_by, "no fresh book")

    async def test_a_quiet_but_live_book_is_fine(self):
        """Prices repeating is normal here; a new snapshot id is what proves the venue is talking."""
        controller = self.controller()
        await controller.update_processed_data()
        self.now += 30
        for book in self.books.values():
            book.snapshot_uid += 1                        # same prices, fresh snapshot
        found = await self.opportunities(controller)
        self.assertIsNone(found[("csx", "wazirx")].blocked_by)

    # ── sizing ───────────────────────────────────────────────────────────────

    async def test_size_is_capped_by_the_thinner_side(self):
        self.books[("wazirx", PAIR)] = FakeBook("100.00", "3", "100.10", "500")   # only 3 on the bid
        controller = self.controller()
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].amount, D("3"))

    async def test_size_is_capped_by_the_operator_and_by_balances(self):
        controller = self.controller(order_amount_quote=D("990"))
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].amount, D("10"))                 # 990 / 99

        self.balances[("wazirx", "USDT")] = D("2")                                 # nothing to deliver
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].amount, D("2"))

    async def test_size_must_be_valid_on_both_venues(self):
        """A coarser step on one venue decides the amount, or the two legs would not match."""
        self.steps["wazirx"] = D("0.1")
        controller = self.controller(order_amount_quote=D("1000"))
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].amount, D("10.1"))               # 10.10 not 10.101

    async def test_below_the_operator_minimum_is_refused(self):
        self.books[("wazirx", PAIR)] = FakeBook("100.00", "0.5", "100.10", "500")  # 50 INR of depth
        controller = self.controller(min_order_amount_quote=D("100"))
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].blocked_by, "below min order value (100)")

    async def test_below_a_venue_minimum_is_refused(self):
        self.books[("wazirx", PAIR)] = FakeBook("100.00", "0.61", "100.10", "500")  # 60 INR
        controller = self.controller(min_order_amount_quote=D("10"))
        found = await self.opportunities(controller)
        # CSX needs 60 INR of notional at its own price (0.61 * 99 = 60.39 clears it), WazirX 50 -
        # so raise CSX's floor to show the check bites
        self.rules["csx"] = TradingRule(PAIR, min_order_size=D("0.01"), min_notional_size=D("100"))
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].blocked_by, "below a venue minimum")

    async def test_no_balance_is_named(self):
        self.balances[("wazirx", "USDT")] = D("0")
        controller = self.controller()
        found = await self.opportunities(controller)
        self.assertEqual(found[("csx", "wazirx")].blocked_by, "no balance")

    # ── deciding ─────────────────────────────────────────────────────────────

    async def test_action_carries_the_whole_plan(self):
        controller = self.controller(order_amount_quote=D("990"), fill_timeout=12.0, tds_pct=D("1"))
        await controller.update_processed_data()
        actions = controller.determine_executor_actions()
        self.assertEqual(len(actions), 1)
        config = actions[0].executor_config
        self.assertEqual(config.buying_market.connector_name, "csx")
        self.assertEqual(config.selling_market.connector_name, "wazirx")
        self.assertEqual((config.buy_price_cap, config.sell_price_floor), (D("99.00"), D("100.00")))
        self.assertEqual(config.order_amount, D("10"))
        self.assertEqual(config.fill_timeout, 12.0)
        self.assertEqual(config.tds_pct, D("1"))
        self.assertEqual(config.controller_id, "arb-1")

    async def test_the_better_direction_wins(self):
        self.books[("csx", PAIR)] = FakeBook("103.00", "500", "103.10", "500")   # CSX now the dear one
        controller = self.controller()
        await controller.update_processed_data()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.buying_market.connector_name, "wazirx")
        self.assertEqual(config.selling_market.connector_name, "csx")

    async def test_one_attempt_per_pair_at_a_time(self):
        controller = self.controller()
        controller.executors_info = [self.executor(active=True)]
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertEqual(controller.processed_data["skips"].get("executor already running", 0) +
                         controller._skips["executor already running"], 1)

    async def test_cooldown_between_attempts(self):
        controller = self.controller(cooldown=30)
        await controller.update_processed_data()
        self.assertEqual(len(controller.determine_executor_actions()), 1)
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])      # still inside the cooldown
        self.now += 31
        for book in self.books.values():
            book.snapshot_uid += 1      # 31s later the venues must have sent something, or it is stale
        await controller.update_processed_data()
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    # ── the brakes ───────────────────────────────────────────────────────────

    async def test_kill_switch(self):
        controller = self.controller(manual_kill_switch=True)
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertEqual(controller.halt_reason(), "kill switch on")

    async def test_daily_loss_limit(self):
        controller = self.controller(max_loss_quote=D("50"))
        controller.executors_info = [self.executor(active=False, close_type=CloseType.COMPLETED,
                                                   pnl=D("-60"), executor_id="e-loss")]
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertIn("daily loss limit", controller.halt_reason())

    async def test_failures_in_a_row_stop_it_and_a_win_clears_them(self):
        controller = self.controller(max_consecutive_failures=2)
        controller.executors_info = [
            self.executor(active=False, close_type=CloseType.FAILED, executor_id="f1"),
            self.executor(active=False, close_type=CloseType.INSUFFICIENT_BALANCE, executor_id="f2")]
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertIn("failed attempts in a row", controller.halt_reason())

        controller.executors_info.append(
            self.executor(active=False, close_type=CloseType.COMPLETED, pnl=D("5"), executor_id="w1"))
        controller.account_for_finished_executors()
        self.assertIsNone(controller.halt_reason())

    async def test_trades_per_hour(self):
        controller = self.controller(max_trades_per_hour=1, cooldown=0)
        await controller.update_processed_data()
        self.assertEqual(len(controller.determine_executor_actions()), 1)
        await controller.update_processed_data()
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertEqual(controller.halt_reason(), "trades per hour reached")
        self.now += 3601
        self.assertIsNone(controller.halt_reason())

    async def test_finished_attempts_are_counted_once(self):
        controller = self.controller()
        controller.executors_info = [self.executor(active=False, close_type=CloseType.COMPLETED,
                                                   pnl=D("7"), executor_id="e-done")]
        controller.account_for_finished_executors()
        controller.account_for_finished_executors()
        self.assertEqual(controller._realized_pnl, D("7"))

    # ── inventory and status ─────────────────────────────────────────────────

    async def test_running_low_raises_a_rebalance_note(self):
        self.balances[("wazirx", "USDT")] = D("1")        # about 100 INR of coin left to sell
        controller = self.controller(rebalance_below_quote=D("500"))
        await controller.update_processed_data()
        needs = controller.processed_data["rebalance_needs"]
        self.assertIn("wazirx:USDT", needs)
        self.assertIn("cannot sell", needs["wazirx:USDT"])

    async def test_status_shows_the_numbers_and_the_reasons(self):
        controller = self.controller()
        await controller.update_processed_data()
        text = "\n".join(controller.to_format_status())
        self.assertIn("Cross-exchange arbitrage", text)
        self.assertIn("csx", text)
        self.assertIn("gross %", text)
        self.assertIn("not trading because", text)       # the other direction was below threshold
