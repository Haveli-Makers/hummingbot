import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, PropertyMock

from controllers.generic.simple_grid import SimpleGrid, SimpleGridConfig
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import (
    OrderType,
    PositionAction,
    PositionSide,
    PriceType,
    TradeType,
)
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

    # ------------------------------------------------------------------ direction

    def test_perpetual_opening_leg_offers_both_sides(self):
        """A maker order on each side of the book; whichever gets hit sets the run's side."""
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.BOTH_OCO)

    def test_spot_only_ever_goes_long(self):
        controller = self.controller(connector_name="binance")
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    def test_the_side_locks_to_whichever_filled_first(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.SELL, net_pnl_quote="1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(controller._locked_side, TradeType.SELL)
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.SHORT_ONLY)

    def test_a_locked_long_run_stays_long_after_a_stop_loss(self):
        """The first fill decides the direction, not the outcome of each leg."""
        controller = self.controller(stop_when_losses_outnumber_wins=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, side=TradeType.BUY, net_pnl_quote="-1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    def test_a_locked_short_run_stays_short_after_a_take_profit(self):
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.SELL, net_pnl_quote="1")]
        controller.determine_executor_actions()
        controller.executors_info.append(
            self.closed_leg("leg-2", CloseType.TAKE_PROFIT, side=TradeType.SELL, net_pnl_quote="1"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.SHORT_ONLY)

    def test_the_lock_is_only_set_once(self):
        """A stray later leg on the other side must not be able to turn the run around."""
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.SELL, net_pnl_quote="1")]
        controller.determine_executor_actions()
        controller.executors_info.append(
            self.closed_leg("leg-2", CloseType.TAKE_PROFIT, side=TradeType.BUY, net_pnl_quote="1"))
        controller.determine_executor_actions()
        self.assertEqual(controller._locked_side, TradeType.SELL)

    def test_a_leg_that_never_traded_does_not_lock_the_side(self):
        controller = self.controller(cooldown_after_reanchor=0)
        leg = self.closed_leg("leg-1", CloseType.EXPIRED, side=TradeType.SELL, net_pnl_quote="0")
        leg.filled_amount_quote = Decimal("0")
        controller.executors_info = [leg]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertIsNone(controller._locked_side)
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.BOTH_OCO)

    def test_locking_can_be_switched_off(self):
        controller = self.controller(lock_side_after_first_fill=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.SELL, net_pnl_quote="1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertIsNone(controller._locked_side)
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.BOTH_OCO)

    def test_spot_never_offers_both_sides_even_once_locked(self):
        controller = self.controller(connector_name="binance")
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, side=TradeType.BUY, net_pnl_quote="1")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_mode, SimpleGridEntryMode.LONG_ONLY)

    # ------------------------------------------------------------------ the anchor

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
        A maker entry that is never filled closes having done nothing. Letting it move the
        anchor would walk the grid across the market with no fills behind it.
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

    # ------------------------------------------------------------------ settings passed through

    def test_take_profit_and_stop_loss_pass_through_separately(self):
        controller = self.controller(take_profit=Decimal("0.02"), stop_loss=Decimal("0.03"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.barriers.take_profit, Decimal("0.02"))
        self.assertEqual(config.barriers.stop_loss, Decimal("0.03"))

    def test_the_maker_entry_settings_reach_the_executor(self):
        controller = self.controller(entry_band_pct=Decimal("0.004"),
                                     entry_requote_pct=Decimal("0.0002"),
                                     entry_price_improvement_pct=Decimal("0.0001"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_band_pct, Decimal("0.004"))
        self.assertEqual(config.entry_requote_pct, Decimal("0.0002"))
        self.assertEqual(config.entry_price_improvement_pct, Decimal("0.0001"))

    def test_the_stop_loss_chase_settings_reach_the_executor(self):
        controller = self.controller(stop_loss_chase=True,
                                     stop_loss_requote_pct=Decimal("0.0003"),
                                     stop_loss_max_drift_pct=Decimal("0.008"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertTrue(config.barriers.stop_loss_chase)
        self.assertEqual(config.barriers.stop_loss_requote_pct, Decimal("0.0003"))
        self.assertEqual(config.barriers.stop_loss_max_drift_pct, Decimal("0.008"))

    def test_the_stop_loss_chase_can_be_switched_off(self):
        controller = self.controller(stop_loss_chase=False)
        config = controller.determine_executor_actions()[0].executor_config
        self.assertFalse(config.barriers.stop_loss_chase)

    def test_the_trigger_price_defaults_to_mid(self):
        """CoinDCX perpetuals never publish a last trade, so LastTrade would silently be mid."""
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.trigger_price_type, PriceType.MidPrice)

    def test_the_leg_amount_is_the_quote_size_over_the_anchor(self):
        controller = self.controller(order_amount_quote=Decimal("100"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="50")]
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.amount, Decimal("2"))

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

    def test_the_entry_band_defaults_to_a_fraction_of_the_step(self):
        """
        A band as wide as the step is a losing bracket before the leg starts: the entry can
        fill at the edge with its take profit already on top of it and its stop loss two
        steps away.
        """
        controller = self.controller(take_profit=Decimal("0.005"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_band_pct, Decimal("0.001"))

    def test_the_entry_band_can_be_set_outright(self):
        controller = self.controller(take_profit=Decimal("0.005"), entry_band_pct=Decimal("0.0004"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_band_pct, Decimal("0.0004"))

    def test_the_band_fraction_is_configurable(self):
        controller = self.controller(take_profit=Decimal("0.01"),
                                     entry_band_fraction_of_step=Decimal("0.5"))
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_band_pct, Decimal("0.005"))

    def test_status_reports_how_far_the_price_has_left_the_anchor_behind(self):
        """
        Nothing rests in the book while the price is outside the band, so without this number
        a strategy that a trend has left behind looks identical to one that is just waiting.
        """
        controller = self.controller(take_profit=Decimal("0.005"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="100", net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.market_data_provider.get_price_by_type.return_value = Decimal("130")

        status = "\n".join(controller.to_format_status())

        self.assertIn("30.00% from the anchor", status)

    def test_status_does_not_cry_stale_while_the_price_is_near_the_anchor(self):
        controller = self.controller(take_profit=Decimal("0.005"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="100", net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.market_data_provider.get_price_by_type.return_value = Decimal("100.05")

        self.assertNotIn("STALE", "\n".join(controller.to_format_status()))

    # ------------------------------------------------------------------ re-anchoring

    def test_an_entry_that_timed_out_re_anchors_the_next_leg_to_the_market(self):
        """
        Breaks the deadlock. The anchor only moves on a trade and no trade can happen while
        the price is outside the band around that anchor, so a trend would otherwise leave
        the strategy waiting for a price it will never see again.
        """
        controller = self.controller(cooldown_after_reanchor=0)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="130", net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.assertEqual(controller._anchor_price, Decimal("130"))

        # The price runs away and the maker entry never fills.
        expired = self.closed_leg("leg-2", CloseType.EXPIRED, close_price="173", net_pnl_quote="0")
        expired.filled_amount_quote = Decimal("0")
        controller.executors_info.append(expired)
        self.market_data_provider.get_price_by_type.return_value = Decimal("173")

        config = controller.determine_executor_actions()[0].executor_config

        self.assertEqual(controller._anchor_price, Decimal("173"))
        self.assertEqual(config.entry_price, Decimal("173"))
        self.assertEqual(controller._reanchors, 1)

    def test_the_re_anchor_uses_the_live_price_not_the_expired_legs_close(self):
        """An unfilled leg's close price is just the mid at the time; it is not a chain link."""
        controller = self.controller(cooldown_after_reanchor=0)
        expired = self.closed_leg("leg-1", CloseType.EXPIRED, close_price="150", net_pnl_quote="0")
        expired.filled_amount_quote = Decimal("0")
        controller.executors_info = [expired]
        self.market_data_provider.get_price_by_type.return_value = Decimal("173")

        config = controller.determine_executor_actions()[0].executor_config

        self.assertEqual(config.entry_price, Decimal("173"))

    def test_re_anchoring_waits_out_its_own_cooldown(self):
        """It deliberately allows an entry right after a big move, so it must not be instant."""
        controller = self.controller(cooldown_after_reanchor=90)
        expired = self.closed_leg("leg-1", CloseType.EXPIRED, net_pnl_quote="0")
        expired.filled_amount_quote = Decimal("0")
        controller.executors_info = [expired]

        self.assertEqual(controller.determine_executor_actions(), [])
        self.now = START_TS + 30
        self.assertEqual(controller.determine_executor_actions(), [])
        self.now = START_TS + 95
        self.assertEqual(len(controller.determine_executor_actions()), 1)

    def test_re_anchoring_can_be_switched_off(self):
        controller = self.controller(reanchor_on_entry_timeout=False, cooldown_after_reanchor=0)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="130", net_pnl_quote="-1")]
        controller.determine_executor_actions()

        expired = self.closed_leg("leg-2", CloseType.EXPIRED, net_pnl_quote="0")
        expired.filled_amount_quote = Decimal("0")
        controller.executors_info.append(expired)
        self.market_data_provider.get_price_by_type.return_value = Decimal("173")

        config = controller.determine_executor_actions()[0].executor_config

        self.assertEqual(controller._anchor_price, Decimal("130"))
        self.assertEqual(config.entry_price, Decimal("130"))
        self.assertEqual(controller._reanchors, 0)

    def test_a_leg_that_traded_still_anchors_on_where_it_closed(self):
        """Re-anchoring must not loosen the ordinary rule; the chain still follows the exits."""
        controller = self.controller()
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.TAKE_PROFIT, close_price="102", net_pnl_quote="1")]
        self.market_data_provider.get_price_by_type.return_value = Decimal("173")

        config = controller.determine_executor_actions()[0].executor_config

        self.assertEqual(config.entry_price, Decimal("102"))
        self.assertEqual(controller._reanchors, 0)

    def test_an_early_stop_does_not_re_anchor(self):
        """Only the entry timeout re-anchors; being stopped by hand is not a fresh start."""
        controller = self.controller(cooldown_after_reanchor=0)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="130", net_pnl_quote="-1")]
        controller.determine_executor_actions()

        stopped = self.closed_leg("leg-2", CloseType.EARLY_STOP, net_pnl_quote="0")
        stopped.filled_amount_quote = Decimal("0")
        controller.executors_info.append(stopped)
        self.market_data_provider.get_price_by_type.return_value = Decimal("173")

        config = controller.determine_executor_actions()[0].executor_config

        self.assertEqual(config.entry_price, Decimal("130"))
        self.assertEqual(controller._reanchors, 0)

    def test_the_entry_timeout_has_a_default_and_reaches_the_executor(self):
        """Without one the maker entry rests forever and no leg ever ends."""
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.entry_timeout, 300)

    def test_status_says_re_anchoring_rather_than_stale_when_it_is_armed(self):
        controller = self.controller(take_profit=Decimal("0.005"))
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="100", net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.market_data_provider.get_price_by_type.return_value = Decimal("130")

        status = "\n".join(controller.to_format_status())

        self.assertIn("Adrift", status)
        self.assertIn("re-anchoring in up to 300s", status)
        self.assertNotIn("STALE", status)

    def test_status_still_says_stale_when_re_anchoring_is_off(self):
        controller = self.controller(take_profit=Decimal("0.005"),
                                     reanchor_on_entry_timeout=False)
        controller.executors_info = [
            self.closed_leg("leg-1", CloseType.STOP_LOSS, close_price="100", net_pnl_quote="-1")]
        controller.determine_executor_actions()
        self.market_data_provider.get_price_by_type.return_value = Decimal("130")

        self.assertIn("STALE", "\n".join(controller.to_format_status()))

    # ------------------------------------------------------------------ failing legs

    def failed_leg(self, executor_id, close_type, filled="0"):
        leg = self.closed_leg(executor_id, close_type, net_pnl_quote="0")
        leg.filled_amount_quote = Decimal(filled)
        return leg

    def test_repeated_legs_that_never_trade_halt_instead_of_retrying_forever(self):
        """
        A live run logged 'Not enough budget' once a second for two minutes: a stranded
        position held the margin, so every new leg was refused the moment it was created.
        """
        controller = self.controller(max_consecutive_failed_legs=3, cooldown_after_reanchor=0)
        controller.executors_info = [
            self.failed_leg(f"leg-{i}", CloseType.INSUFFICIENT_BALANCE) for i in range(1, 4)]

        actions = controller.determine_executor_actions()

        self.assertEqual(actions, [])
        self.assertTrue(controller.is_halted)
        self.assertIn("failed before trading", controller._halt_reason)

    def test_a_leg_that_trades_clears_the_failure_streak(self):
        controller = self.controller(max_consecutive_failed_legs=3, cooldown_after_reanchor=0)
        controller.executors_info = [
            self.failed_leg("leg-1", CloseType.INSUFFICIENT_BALANCE),
            self.failed_leg("leg-2", CloseType.INSUFFICIENT_BALANCE),
            self.closed_leg("leg-3", CloseType.TAKE_PROFIT, net_pnl_quote="1"),
        ]

        controller.determine_executor_actions()

        self.assertEqual(controller._consecutive_failed_legs, 0)
        self.assertFalse(controller.is_halted)

    def test_a_leg_that_failed_after_trading_halts_at_once(self):
        """
        Its close order never landed, so a real position may still be open. Opening more legs
        on top of it is the worst thing the controller could do next.
        """
        controller = self.controller()
        controller.executors_info = [self.failed_leg("leg-1", CloseType.FAILED, filled="100")]

        actions = controller.determine_executor_actions()

        self.assertEqual(actions, [])
        self.assertTrue(controller.is_halted)
        self.assertIn("position may still be open", controller._halt_reason)

    def test_a_leg_that_failed_without_trading_does_not_halt_on_its_own(self):
        controller = self.controller(max_consecutive_failed_legs=5, cooldown_after_reanchor=0)
        controller.executors_info = [self.failed_leg("leg-1", CloseType.FAILED)]

        controller.determine_executor_actions()

        self.assertFalse(controller.is_halted)
        self.assertEqual(controller._consecutive_failed_legs, 1)

    def test_the_close_slippage_budget_reaches_the_executor(self):
        controller = self.controller(close_slippage_ticks=35)
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.barriers.close_slippage_ticks, 35)

    def test_the_urgent_exit_defaults_to_a_crossing_limit(self):
        """CoinDCX rejects reduce_only market orders, and a close must carry reduce_only."""
        controller = self.controller()
        config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(config.barriers.close_order_type, OrderType.LIMIT)

    # ------------------------------------------------------------------ venue reconciliation

    def set_venue(self, amount=None, side=PositionSide.LONG, pair="BTC-USDT"):
        """Point the controller at a connector reporting this position (or none)."""
        connector = MagicMock()
        connector.account_positions = {}
        if amount is not None:
            connector.account_positions = {pair: Position(
                trading_pair=pair, position_side=side, unrealized_pnl=Decimal("0"),
                entry_price=Decimal("100"), amount=Decimal(amount), leverage=Decimal("1"))}
        rule = MagicMock()
        rule.min_price_increment = Decimal("0.01")
        connector.trading_rules = {pair: rule}
        self.market_data_provider.connectors = {"coindcx_perpetual": connector}
        return connector

    def live_leg_holding(self, amount="1"):
        leg = self.active_leg("live-1")
        leg.custom_info = dict(leg.custom_info)
        leg.custom_info["filled_amount"] = Decimal(amount)
        return leg

    def past_the_grace(self, controller, seconds=11):
        self.now = START_TS + seconds
        return controller.determine_executor_actions()

    def test_a_flat_account_is_recorded_so_a_later_position_is_known_to_be_ours(self):
        controller = self.controller()
        self.set_venue(amount=None)

        controller.determine_executor_actions()

        self.assertTrue(controller._seen_flat_since_start)

    def test_a_position_a_live_leg_claims_is_left_alone(self):
        controller = self.controller()
        self.set_venue(amount="0.008")
        controller.executors_info = [self.live_leg_holding("0.008")]

        self.past_the_grace(controller)

        self.assertFalse(controller.is_halted)
        self.assertIsNone(controller._orphan_since)

    def test_an_unclaimed_position_is_given_a_grace_period_first(self):
        """A leg that has just filled takes a moment to say so; acting at once would be worse."""
        controller = self.controller()
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="0.008")

        controller.determine_executor_actions()

        self.assertFalse(controller.is_halted)
        self.assertEqual(controller._orphan_since, START_TS)

    def test_an_unclaimed_position_halts_the_controller(self):
        controller = self.controller()
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="0.008")
        controller.determine_executor_actions()

        self.past_the_grace(controller)

        self.assertTrue(controller.is_halted)
        self.assertIn("no leg claims", controller._halt_reason)

    def test_an_unclaimed_position_is_closed_with_a_crossing_limit(self):
        """The step that removes the manual cleanup."""
        controller = self.controller(close_slippage_ticks=20)
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="0.008")
        controller.determine_executor_actions()

        actions = self.past_the_grace(controller)

        self.assertEqual(len(actions), 1)
        config = actions[0].executor_config
        self.assertEqual(config.side, TradeType.SELL, "a long is closed by selling")
        self.assertEqual(config.amount, Decimal("0.008"))
        self.assertEqual(config.position_action, PositionAction.CLOSE)
        # mid 100 less 20 ticks of 0.01 — it crosses rather than resting.
        self.assertEqual(config.price, Decimal("99.80"))

    def test_a_short_orphan_is_closed_by_buying(self):
        controller = self.controller()
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="-0.008", side=PositionSide.SHORT)
        controller.determine_executor_actions()

        config = self.past_the_grace(controller)[0].executor_config

        self.assertEqual(config.side, TradeType.BUY)
        self.assertEqual(config.amount, Decimal("0.008"))
        self.assertEqual(config.price, Decimal("100.20"))

    def test_a_position_that_predates_the_run_is_never_touched(self):
        """
        The account is shared with hand-run tests. A position that was already there when we
        started is somebody else's, and closing it would be worse than leaving it.
        """
        controller = self.controller()
        self.set_venue(amount="0.008")   # already there on the very first tick
        controller.determine_executor_actions()

        actions = self.past_the_grace(controller)

        self.assertEqual(actions, [])
        self.assertTrue(controller.is_halted, "it still halts and says so")

    def test_the_flatten_is_only_sent_once(self):
        controller = self.controller()
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="0.008")
        controller.determine_executor_actions()
        self.assertEqual(len(self.past_the_grace(controller)), 1)

        self.assertEqual(self.past_the_grace(controller, seconds=12), [])

    def test_flattening_can_be_switched_off_while_the_halt_stays(self):
        controller = self.controller(flatten_orphan_positions=False)
        self.set_venue(amount=None)
        controller.determine_executor_actions()
        self.set_venue(amount="0.008")
        controller.determine_executor_actions()

        actions = self.past_the_grace(controller)

        self.assertEqual(actions, [])
        self.assertTrue(controller.is_halted)

    def test_reconciliation_can_be_switched_off_entirely(self):
        controller = self.controller(reconcile_positions=False)
        self.set_venue(amount="0.008")

        self.past_the_grace(controller)

        self.assertFalse(controller.is_halted)

    def test_a_connector_that_cannot_be_read_does_not_break_the_tick(self):
        """The safety net must never be the thing that takes the strategy down."""
        controller = self.controller()
        broken = MagicMock()
        type(broken).account_positions = PropertyMock(side_effect=RuntimeError("no connection"))
        self.market_data_provider.connectors = {"coindcx_perpetual": broken}

        actions = controller.determine_executor_actions()

        self.assertFalse(controller.is_halted)
        self.assertEqual(len(actions), 1, "it carries on opening legs")
