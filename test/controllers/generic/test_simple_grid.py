import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from controllers.generic.simple_grid import SimpleGrid, SimpleGridConfig
from hummingbot.core.data_type.common import OrderType, TradeType
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
            take_profit=Decimal("0.02"),
            stop_loss=Decimal("0.02"),
            cooldown_after_stop_loss=0,
        )
        params.update(overrides)
        return SimpleGrid(SimpleGridConfig(**params), self.market_data_provider, self.actions_queue)

    @staticmethod
    def closed_leg(executor_id, close_type, close_price="98", side=TradeType.BUY,
                   net_pnl_quote="1", close_timestamp=START_TS) -> ExecutorInfo:
        config = SimpleGridExecutorConfig(
            id=executor_id,
            timestamp=START_TS,
            connector_name="coindcx_perpetual",
            trading_pair="BTC-USDT",
            amount=Decimal("1"),
            barriers=SimpleGridBarriers(take_profit=Decimal("0.02"), stop_loss=Decimal("0.02")),
        )
        return ExecutorInfo(
            id=executor_id, timestamp=START_TS, type="simple_grid_executor",
            status=RunnableStatus.TERMINATED, config=config,
            net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal(net_pnl_quote),
            cum_fees_quote=Decimal("0"), filled_amount_quote=Decimal("100"),
            is_active=False, is_trading=False,
            custom_info={"close_price": Decimal(close_price), "side": side},
            close_timestamp=close_timestamp, close_type=close_type, controller_id="grid-1",
        )

    @staticmethod
    def active_leg(executor_id="live-1") -> ExecutorInfo:
        info = TestSimpleGrid.closed_leg(executor_id, CloseType.TAKE_PROFIT)
        info.status = RunnableStatus.RUNNING
        info.is_active = True
        info.close_type = None
        return info

    # ------------------------------------------------------------------ entry levels

    def test_perpetual_watches_both_directions(self):
        """The market decides: whichever level it reaches first sets the direction."""
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.BOTH_OCO)

    def test_spot_only_ever_goes_long(self):
        controller = self.controller(connector_name="binance")
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    def test_spot_opens_the_chain_with_a_passive_buy_at_the_touch(self):
        """
        Spot's first order goes in straight away, passively, just below the ask — it can
        only go long, so there is no direction to wait for. The chain proper starts from
        wherever that fills.
        """
        controller = self.controller(connector_name="binance")
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)
        self.assertEqual(config.entry_offset_pct, Decimal("0"))
        self.assertIsNone(config.entry_price)  # executor prices it off the live touch
        self.assertEqual(config.entry_order_type, OrderType.LIMIT)

    def test_futures_first_leg_waits_for_the_market_to_pick_a_side(self):
        """A perp can go either way, so the wait is what decides the direction."""
        controller = self.controller(take_profit=Decimal("0.02"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.BOTH_OCO)
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))
        self.assertEqual(config.entry_price, Decimal("100"))
        self.assertEqual(config.entry_order_type, OrderType.MARKET)

    def test_spot_second_leg_goes_back_to_waiting_for_the_step(self):
        """The passive opening applies only to the first leg."""
        controller = self.controller(connector_name="binance", take_profit=Decimal("0.02"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="102", net_pnl_quote="1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))
        self.assertEqual(config.entry_price, Decimal("102"))

    def test_entry_step_defaults_to_the_take_profit_distance(self):
        """So a winning leg lands exactly on the next entry level."""
        controller = self.controller(take_profit=Decimal("0.02"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))

    def test_entry_step_can_be_set_independently(self):
        controller = self.controller(take_profit=Decimal("0.02"), entry_step=Decimal("0.01"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_offset_pct, Decimal("0.01"))

    def test_take_profit_and_stop_loss_pass_through_separately(self):
        controller = self.controller(take_profit=Decimal("0.02"), stop_loss=Decimal("0.03"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.barriers.take_profit, Decimal("0.02"))
        self.assertEqual(config.barriers.stop_loss, Decimal("0.03"))

    def test_first_leg_anchors_on_the_current_price(self):
        self.market_data_provider.get_price_by_type.return_value = Decimal("100")
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_price, Decimal("100"))

    def test_next_leg_anchors_on_the_previous_exit(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="98")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_price, Decimal("98"))

    def test_a_leg_that_never_traded_does_not_move_the_anchor(self):
        """
        Spot in a falling market never reaches its up-trigger, so legs close having done
        nothing. If those moved the anchor the grid would walk down the market with no
        fills behind it.
        """
        controller = self.controller()
        leg = self.closed_leg("leg-1", CloseType.EARLY_STOP, close_price="85", net_pnl_quote="0")
        leg.filled_amount_quote = Decimal("0")
        controller.executors_info = [leg]
        controller.determine_executor_actions()
        self.assertIsNone(controller._anchor_price)
        self.assertEqual(controller._wins + controller._losses, 0)

    def test_a_leg_that_never_opened_does_not_move_the_anchor(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.EXPIRED, close_price="98", net_pnl_quote="0")]
        controller.determine_executor_actions()
        self.assertIsNone(controller._anchor_price)
        self.assertEqual(controller._wins + controller._losses, 0)

    # ------------------------------------------------------------------ spot re-entry

    def test_spot_buys_whichever_level_the_market_reaches(self):
        """
        Spot cannot short, so instead of only watching the level above it watches both and
        buys either one: a step up joins the rise, a step down buys the dip. Without this it
        would sit idle through every fall waiting for a rise to buy into.
        """
        controller = self.controller(connector_name="binance", take_profit=Decimal("0.02"),
                                     stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="98", net_pnl_quote="-1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertTrue(config.enter_on_either_level)
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    def test_futures_only_watches_its_own_side_of_each_level(self):
        """A perp does not need it: it can just go short when the lower level is reached."""
        controller = self.controller(stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertFalse(config.enter_on_either_level)

    def test_spot_waits_for_the_step_after_a_take_profit(self):
        controller = self.controller(connector_name="binance", take_profit=Decimal("0.02"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, net_pnl_quote="1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))

    def test_perpetual_still_waits_for_the_step_after_a_stop_loss(self):
        """A perp can flip direction, so it has no reason to re-enter at the anchor."""
        controller = self.controller(take_profit=Decimal("0.02"),
                                     stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_offset_pct, Decimal("0.02"))

    # ------------------------------------------------------------------ pacing

    def test_only_one_leg_runs_at_a_time(self):
        controller = self.controller()
        controller.executors_info = [self.active_leg()]
        self.assertEqual(controller.determine_executor_actions(), [])

    def test_cooldown_after_stop_loss_blocks_then_releases(self):
        controller = self.controller(cooldown_after_stop_loss=60,
                                     stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        self.assertEqual(controller.determine_executor_actions(), [])

        self.now = START_TS + 61
        controller.executors_info = []
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    def test_manual_kill_switch_stops_new_legs(self):
        controller = self.controller(manual_kill_switch=True)
        self.assertEqual(controller.determine_executor_actions(), [])

    # ------------------------------------------------------------------ halt conditions

    def test_halts_when_losses_reach_the_threshold(self):
        controller = self.controller(max_loss_quote=Decimal("5"))
        for i in range(3):
            controller.executors_info = [
                self.closed_leg(f"leg-{i}", CloseType.STOP_LOSS, net_pnl_quote="-2")]
            controller.determine_executor_actions()

        self.assertTrue(controller.is_halted)
        self.assertEqual(controller._realized_pnl_quote, Decimal("-6"))
        self.assertEqual(controller.determine_executor_actions(), [])

    def test_the_loss_threshold_is_the_only_active_stop_by_default(self):
        """
        Agreed with the team: for now the loss threshold is the single stop condition. The
        win/loss count rule stays in the code but is off unless switched on.
        """
        controller = self.controller(max_loss_quote=Decimal("100"))
        self.assertFalse(controller.config.stop_when_losses_outnumber_wins)

        # Four straight losses, well past any count rule, but nowhere near the money limit.
        for i in range(4):
            controller.executors_info = [
                self.closed_leg(f"l{i}", CloseType.STOP_LOSS, net_pnl_quote="-1")]
            controller.determine_executor_actions()

        self.assertEqual(controller._losses, 4)
        self.assertEqual(controller._wins, 0)
        self.assertFalse(controller.is_halted)
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    def test_max_loss_pct_is_measured_against_total_amount_quote(self):
        controller = self.controller(total_amount_quote=Decimal("1000"),
                                     max_loss_pct=Decimal("0.01"))
        self.assertEqual(controller.loss_limit_quote, Decimal("10"))

    def test_halts_when_losses_outnumber_wins_and_we_are_down(self):
        controller = self.controller(stop_when_losses_outnumber_wins=True,
                                     min_legs_before_count_check=2)
        controller.executors_info = [
            self.closed_leg("w1", CloseType.TAKE_PROFIT, net_pnl_quote="1")]
        controller.determine_executor_actions()
        controller.executors_info = [
            self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-3")]
        controller.determine_executor_actions()

        self.assertTrue(controller.is_halted)

    def test_does_not_halt_on_count_alone_while_still_in_profit(self):
        """Both halves of the rule must hold: behind on count AND down overall."""
        controller = self.controller(stop_when_losses_outnumber_wins=True)
        controller.executors_info = [
            self.closed_leg("w1", CloseType.TAKE_PROFIT, net_pnl_quote="10")]
        controller.determine_executor_actions()
        controller.executors_info = [
            self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        controller.determine_executor_actions()

        self.assertEqual(controller._losses, controller._wins)
        self.assertGreater(controller._realized_pnl_quote, Decimal("0"))
        self.assertFalse(controller.is_halted)

    def test_grace_period_protects_an_early_losing_streak(self):
        """
        Without it, one bad first leg already satisfies "losses >= wins and down" and the
        strategy would stop before it had a fair sample.
        """
        controller = self.controller(stop_when_losses_outnumber_wins=True,
                                     min_legs_before_count_check=3)
        for i in range(2):
            controller.executors_info = [
                self.closed_leg(f"l{i}", CloseType.STOP_LOSS, net_pnl_quote="-1")]
            controller.determine_executor_actions()
        self.assertFalse(controller.is_halted)

        controller.executors_info = [
            self.closed_leg("l3", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.assertTrue(controller.is_halted)

    def test_grace_period_does_not_delay_the_loss_threshold(self):
        """The money limit is absolute and must fire regardless of how few legs have run."""
        controller = self.controller(stop_when_losses_outnumber_wins=True,
                                     min_legs_before_count_check=50,
                                     max_loss_quote=Decimal("2"))
        controller.executors_info = [
            self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-3")]
        controller.determine_executor_actions()
        self.assertTrue(controller.is_halted)

    def test_count_rule_can_be_switched_off(self):
        controller = self.controller(stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.assertFalse(controller.is_halted)

    def test_a_closed_leg_is_only_counted_once(self):
        controller = self.controller(stop_when_losses_outnumber_wins=False)
        leg = self.closed_leg("leg-1", CloseType.STOP_LOSS, net_pnl_quote="-2")
        controller.executors_info = [leg]
        controller.determine_executor_actions()
        controller.determine_executor_actions()
        self.assertEqual(controller._realized_pnl_quote, Decimal("-2"))
        self.assertEqual(controller._losses, 1)

    # ------------------------------------------------------------------ reporting

    def test_wins_and_losses_are_tracked(self):
        controller = self.controller(stop_when_losses_outnumber_wins=False)
        controller.executors_info = [self.closed_leg("w1", CloseType.TAKE_PROFIT, net_pnl_quote="1")]
        controller.determine_executor_actions()
        controller.executors_info = [self.closed_leg("l1", CloseType.STOP_LOSS, net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.assertEqual(controller._wins, 1)
        self.assertEqual(controller._losses, 1)

    def test_format_status_renders(self):
        controller = self.controller()
        controller.executors_info = [self.closed_leg("leg-1", CloseType.TAKE_PROFIT)]
        controller.determine_executor_actions()
        status = "\n".join(controller.to_format_status())
        self.assertIn("Simple Grid", status)

    async def test_update_processed_data_exposes_state(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="98")]
        controller.determine_executor_actions()
        await controller.update_processed_data()
        self.assertEqual(controller.processed_data["anchor_price"], Decimal("98"))
        self.assertFalse(controller.processed_data["halted"])
