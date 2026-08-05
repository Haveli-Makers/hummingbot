import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from controllers.generic.simple_grid import SimpleGrid, SimpleGridConfig
from hummingbot.core.data_type.common import TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridBarriers,
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

START_TS = 1234567890


class TestSimpleGrid(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.now = START_TS
        self.market_data_provider = MagicMock(spec=MarketDataProvider)
        self.market_data_provider.get_price_by_type.return_value = Decimal("100")
        self.market_data_provider.time.side_effect = lambda: self.now
        self.actions_queue = asyncio.Queue()

    def controller(self, **overrides) -> SimpleGrid:
        params = dict(
            id="grid-1",
            connector_name="coindcx_perpetual",
            trading_pair="BTC-USDT",
            order_amount_quote=Decimal("100"),
            take_profit=Decimal("0.005"),
            stop_loss=Decimal("0.01"),
            cooldown_after_stop_loss=60,
        )
        params.update(overrides)
        return SimpleGrid(SimpleGridConfig(**params), self.market_data_provider, self.actions_queue)

    @staticmethod
    def closed_leg(executor_id, close_type, close_price="105", side=TradeType.BUY,
                   net_pnl_quote="1", close_timestamp=START_TS) -> ExecutorInfo:
        config = SimpleGridExecutorConfig(
            id=executor_id,
            timestamp=START_TS,
            connector_name="coindcx_perpetual",
            trading_pair="BTC-USDT",
            amount=Decimal("1"),
            barriers=SimpleGridBarriers(take_profit=Decimal("0.005"), stop_loss=Decimal("0.01")),
        )
        return ExecutorInfo(
            id=executor_id,
            timestamp=START_TS,
            type="simple_grid_executor",
            status=RunnableStatus.TERMINATED,
            config=config,
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal(net_pnl_quote),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("100"),
            is_active=False,
            is_trading=False,
            custom_info={"close_price": Decimal(close_price), "side": side},
            close_timestamp=close_timestamp,
            close_type=close_type,
            controller_id="grid-1",
        )

    @staticmethod
    def active_leg(executor_id="live-1") -> ExecutorInfo:
        info = TestSimpleGrid.closed_leg(executor_id, CloseType.TAKE_PROFIT)
        info.status = RunnableStatus.RUNNING
        info.is_active = True
        info.close_type = None
        return info

    # ------------------------------------------------------------------ first leg

    def test_first_leg_offers_both_sides(self):
        controller = self.controller()
        actions = controller.determine_executor_actions()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_config.entry_mode, SimpleGridEntryMode.BOTH_OCO)

    def test_leg_amount_is_derived_from_quote_size_and_price(self):
        controller = self.controller(order_amount_quote=Decimal("250"))
        self.market_data_provider.get_price_by_type.return_value = Decimal("50")
        actions = controller.determine_executor_actions()
        self.assertEqual(actions[0].executor_config.amount, Decimal("5"))

    def test_barriers_are_passed_to_the_leg(self):
        controller = self.controller(take_profit=Decimal("0.02"), stop_loss=Decimal("0.03"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.barriers.take_profit, Decimal("0.02"))
        self.assertEqual(config.barriers.stop_loss, Decimal("0.03"))

    # ------------------------------------------------------------------ re-anchoring

    def test_anchor_follows_the_close_price(self):
        controller = self.controller()
        controller.executors_info = [self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="105")]
        controller.determine_executor_actions()
        self.assertEqual(controller._anchor_price, Decimal("105"))

    def test_side_locks_to_the_side_that_filled(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.SELL)]
        actions = controller.determine_executor_actions()
        self.assertEqual(controller._locked_side, TradeType.SELL)
        self.assertEqual(actions[0].executor_config.entry_mode, SimpleGridEntryMode.SHORT_ONLY)

    def test_side_stays_locked_on_subsequent_legs(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.BUY)]
        controller.determine_executor_actions()
        controller.executors_info = [
            self.closed_leg("leg-2", CloseType.TAKE_PROFIT, side=TradeType.BUY)]
        actions = controller.determine_executor_actions()
        self.assertEqual(actions[0].executor_config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    def test_a_leg_that_never_opened_does_not_move_the_anchor(self):
        """An entry that timed out says nothing about where the grid should sit."""
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.EXPIRED, close_price="105", net_pnl_quote="0")]
        controller.determine_executor_actions()
        self.assertIsNone(controller._anchor_price)
        self.assertIsNone(controller._locked_side)
        self.assertEqual(controller._legs_closed, 0)

    # ------------------------------------------------------------------ pacing

    def test_only_one_leg_runs_at_a_time(self):
        controller = self.controller()
        controller.executors_info = [self.active_leg()]
        self.assertEqual(controller.determine_executor_actions(), [])

    def test_cooldown_after_stop_loss_blocks_then_releases(self):
        controller = self.controller(cooldown_after_stop_loss=60)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        self.assertEqual(controller.determine_executor_actions(), [])

        self.now = START_TS + 61
        controller.executors_info = []
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    def test_no_cooldown_after_take_profit_by_default(self):
        controller = self.controller(cooldown_after_take_profit=0)
        controller.executors_info = [self.closed_leg("leg-1", CloseType.TAKE_PROFIT)]
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    def test_manual_kill_switch_stops_new_legs(self):
        controller = self.controller(manual_kill_switch=True)
        self.assertEqual(controller.determine_executor_actions(), [])

    # ------------------------------------------------------------------ risk

    def test_drawdown_limit_halts_the_controller(self):
        controller = self.controller(max_drawdown_quote=Decimal("5"),
                                     cooldown_after_stop_loss=0)
        for i in range(3):
            controller.executors_info = [
                self.closed_leg(f"leg-{i}", CloseType.STOP_LOSS, net_pnl_quote="-2")]
            controller.determine_executor_actions()

        self.assertTrue(controller.is_halted)
        self.assertEqual(controller.current_drawdown_quote, Decimal("6"))
        self.assertEqual(controller.determine_executor_actions(), [])

    def test_drawdown_is_measured_from_the_peak_not_from_zero(self):
        """Giving back gains is a drawdown even while total PnL is still positive."""
        controller = self.controller(max_drawdown_quote=Decimal("5"), cooldown_after_stop_loss=0)
        controller.executors_info = [
            self.closed_leg("win", CloseType.TAKE_PROFIT, net_pnl_quote="10")]
        controller.determine_executor_actions()
        controller.executors_info = [
            self.closed_leg("loss", CloseType.STOP_LOSS, net_pnl_quote="-6")]
        controller.determine_executor_actions()

        self.assertEqual(controller._realized_pnl_quote, Decimal("4"))
        self.assertEqual(controller.current_drawdown_quote, Decimal("6"))
        self.assertTrue(controller.is_halted)

    def test_drawdown_pct_limit_uses_total_amount_quote(self):
        controller = self.controller(total_amount_quote=Decimal("1000"),
                                     max_drawdown_pct=Decimal("0.01"),
                                     cooldown_after_stop_loss=0)
        self.assertEqual(controller.drawdown_limit_quote, Decimal("10"))

    def test_a_closed_leg_is_only_counted_once(self):
        controller = self.controller(cooldown_after_stop_loss=0)
        leg = self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-2")
        controller.executors_info = [leg]
        controller.determine_executor_actions()
        controller.determine_executor_actions()
        self.assertEqual(controller._realized_pnl_quote, Decimal("-2"))
        self.assertEqual(controller._legs_closed, 1)

    # ------------------------------------------------------------------ reporting

    def test_break_even_win_rate(self):
        controller = self.controller(take_profit=Decimal("0.005"), stop_loss=Decimal("0.01"))
        self.assertAlmostEqual(float(controller.break_even_win_rate), 2 / 3, places=6)

    def test_win_rate_is_tracked(self):
        controller = self.controller(cooldown_after_stop_loss=0)
        controller.executors_info = [self.closed_leg("w1", CloseType.TAKE_PROFIT, net_pnl_quote="1")]
        controller.determine_executor_actions()
        controller.executors_info = [self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.assertEqual(controller._legs_closed, 2)
        self.assertEqual(controller.actual_win_rate, Decimal("0.5"))

    def test_format_status_renders(self):
        controller = self.controller()
        controller.executors_info = [self.closed_leg("leg-1", CloseType.TAKE_PROFIT)]
        controller.determine_executor_actions()
        status = "\n".join(controller.to_format_status())
        self.assertIn("Simple Grid", status)
        self.assertIn("Win rate needed", status)

    async def test_update_processed_data_exposes_state(self):
        controller = self.controller()
        controller.executors_info = [self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="105")]
        controller.determine_executor_actions()
        await controller.update_processed_data()
        self.assertEqual(controller.processed_data["anchor_price"], Decimal("105"))
        self.assertFalse(controller.processed_data["halted"])
