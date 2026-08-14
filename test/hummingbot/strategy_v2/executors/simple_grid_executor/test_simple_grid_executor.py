from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import BuyOrderCompletedEvent, OrderCancelledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridBarriers,
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.executors.simple_grid_executor.simple_grid_executor import SimpleGridExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

START_TS = 1234567890


class TestSimpleGridExecutor(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.strategy = self.create_mock_strategy
        # Event handlers re-read the order from the connector's tracker; stand in for it
        # so a test's InFlightOrder is what comes back.
        self._order_book = {}
        patcher = patch.object(
            SimpleGridExecutor, "get_in_flight_order",
            side_effect=lambda connector_name, order_id: self._order_book.get(order_id))
        patcher.start()
        self.addCleanup(patcher.stop)

    def register(self, order: InFlightOrder) -> InFlightOrder:
        """Make an order visible to the executor's event handlers."""
        self._order_book[order.client_order_id] = order
        return order

    @property
    def create_mock_strategy(self):
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).current_timestamp = PropertyMock(return_value=START_TS)
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {"coindcx_perpetual": MagicMock(spec=ExchangePyBase)}
        return strategy

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def trading_rules():
        rules = MagicMock(spec=TradingRule)
        rules.min_order_size = Decimal("0.1")
        rules.min_notional_size = Decimal("1")
        return rules

    @staticmethod
    def config(**overrides) -> SimpleGridExecutorConfig:
        params = dict(
            id="test-leg",
            timestamp=START_TS,
            connector_name="coindcx_perpetual",
            trading_pair="BTC-USDT",
            entry_mode=SimpleGridEntryMode.LONG_ONLY,
            amount=Decimal("1"),
            # Default to entering at the anchor, so barrier tests are not also testing
            # trigger arithmetic. The trigger tests set a step explicitly.
            entry_offset_pct=Decimal("0"),
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02")),
        )
        params.update(overrides)
        return SimpleGridExecutorConfig(**params)

    @staticmethod
    def price_feed(best_bid="99", best_ask="101", last_trade="100", mid="100"):
        """Stand-in for get_price that answers per PriceType."""
        prices = {
            PriceType.BestBid: Decimal(best_bid),
            PriceType.BestAsk: Decimal(best_ask),
            PriceType.LastTrade: Decimal(last_trade),
            PriceType.MidPrice: Decimal(mid),
        }

        def _get_price(_connector, _pair, price_type=PriceType.MidPrice):
            return prices[price_type]

        return _get_price

    def running_executor(self, config) -> SimpleGridExecutor:
        executor = SimpleGridExecutor(self.strategy, config)
        executor._status = RunnableStatus.RUNNING
        return executor

    @staticmethod
    def in_flight(order_id, side, amount="1", price="100", state=OrderState.OPEN) -> InFlightOrder:
        return InFlightOrder(
            client_order_id=order_id,
            exchange_order_id=f"E{order_id}",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=side,
            amount=Decimal(amount),
            price=Decimal(price),
            creation_timestamp=START_TS,
            initial_state=state,
        )

    @staticmethod
    def fill(order: InFlightOrder, amount, price, trade_id="1"):
        order.update_with_trade_update(TradeUpdate(
            trade_id=trade_id,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fill_price=Decimal(price),
            fill_base_amount=Decimal(amount),
            fill_quote_amount=Decimal(amount) * Decimal(price),
            fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))]),
            fill_timestamp=START_TS,
        ))
        return order

    def set_quantize(self, value):
        self.strategy.connectors["coindcx_perpetual"].quantize_order_amount.return_value = Decimal(value)

    # ------------------------------------------------------------------ properties

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_barrier_prices_long(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = SimpleGridExecutor(self.strategy, self.config(entry_price=Decimal("100")))
        self.assertEqual(executor.side, TradeType.BUY)
        self.assertEqual(executor.entry_price, Decimal("100"))
        self.assertEqual(executor.take_profit_price, Decimal("105.00"))
        self.assertEqual(executor.stop_loss_price, Decimal("98.00"))
        self.assertEqual(executor.close_order_side, TradeType.SELL)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_barrier_prices_short_are_mirrored(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = SimpleGridExecutor(self.strategy, self.config(
            entry_mode=SimpleGridEntryMode.SHORT_ONLY, entry_price=Decimal("100")))
        self.assertEqual(executor.side, TradeType.SELL)
        self.assertEqual(executor.take_profit_price, Decimal("95.00"))
        self.assertEqual(executor.stop_loss_price, Decimal("102.00"))
        self.assertEqual(executor.close_order_side, TradeType.BUY)

    # ------------------------------------------------------------------ entry triggers

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_levels_sit_above_and_below_the_anchor(self, mock_price, rules_mock):
        """
        The worked example: anchor 100, step 2% -> long level 102, short level 98.

        Note the direction. This strategy enters with the move, so the long level is ABOVE
        the anchor and the short level BELOW it.
        """
        mock_price.side_effect = self.price_feed(last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_price=Decimal("100"),
            entry_offset_pct=Decimal("0.02")))
        self.assertEqual(executor._entry_target_price(TradeType.BUY), Decimal("102.00"))
        self.assertEqual(executor._entry_target_price(TradeType.SELL), Decimal("98.00"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_nothing_is_placed_until_a_level_is_reached(self, mock_price, rules_mock):
        """No order rests in the book while we wait — the executor only watches."""
        mock_price.side_effect = self.price_feed(last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_price=Decimal("100"),
            entry_offset_pct=Decimal("0.02")))
        await executor.control_task()
        self.strategy.buy.assert_not_called()
        self.strategy.sell.assert_not_called()
        self.assertIsNone(executor._triggered_side)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_rising_price_takes_the_long_side(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(last_trade="102")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_price=Decimal("100"),
            entry_offset_pct=Decimal("0.02")))
        await executor.control_task()
        self.assertEqual(executor._triggered_side, TradeType.BUY)
        self.strategy.buy.assert_called_once()
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_falling_price_takes_the_short_side(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(last_trade="98")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_price=Decimal("100"),
            entry_offset_pct=Decimal("0.02")))
        await executor.control_task()
        self.assertEqual(executor._triggered_side, TradeType.SELL)
        self.strategy.sell.assert_called_once()
        self.strategy.buy.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_only_one_side_is_ever_taken(self, mock_price, rules_mock):
        """Once a level triggers the direction is settled, even if the other is reached."""
        mock_price.side_effect = self.price_feed(last_trade="102")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_price=Decimal("100"),
            entry_offset_pct=Decimal("0.02")))
        await executor.control_task()
        mock_price.side_effect = self.price_feed(last_trade="98")
        await executor.control_task()
        self.assertEqual(executor._triggered_side, TradeType.BUY)
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_zero_step_enters_immediately_at_the_anchor(self, mock_price, rules_mock):
        """How spot re-enters after a stop loss: no waiting for a move that may not come."""
        mock_price.side_effect = self.price_feed(last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_price=Decimal("100"), entry_offset_pct=Decimal("0")))
        await executor.control_task()
        self.assertEqual(executor._triggered_side, TradeType.BUY)
        self.strategy.buy.assert_called_once()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_passive_entry_crosses_the_spread_once_it_has_waited(self, mock_price, rules_mock):
        """
        A resting order only fills if the market comes back to it. In a market moving away
        it never does, so after the wait we take the price rather than miss the move.
        """
        mock_price.side_effect = self.price_feed(best_bid="100", last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_order_type=OrderType.LIMIT, entry_offset_pct=Decimal("0"),
            entry_cross_after=30))
        await executor.control_task()
        first_id = executor._entry_orders[TradeType.BUY].order_id
        executor._entry_orders[TradeType.BUY].order = self.in_flight(first_id, TradeType.BUY)

        # Still inside the wait: leave it alone.
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 10)
        await executor.control_task()
        self.assertEqual(executor._entry_orders[TradeType.BUY].order_id, first_id)

        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 31)
        await executor.control_task()

        self.strategy.cancel.assert_called_once()
        self.assertNotEqual(executor._entry_orders[TradeType.BUY].order_id, first_id)
        self.assertEqual(self.strategy.buy.call_args.args[3], OrderType.MARKET)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_passive_entry_waits_when_no_cross_time_is_set(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="100", last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_order_type=OrderType.LIMIT, entry_offset_pct=Decimal("0"),
            entry_cross_after=None))
        await executor.control_task()
        first_id = executor._entry_orders[TradeType.BUY].order_id
        executor._entry_orders[TradeType.BUY].order = self.in_flight(first_id, TradeType.BUY)

        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 9999)
        await executor.control_task()

        self.strategy.cancel.assert_not_called()
        self.assertEqual(executor._entry_orders[TradeType.BUY].order_id, first_id)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_nan_trigger_price_falls_back_to_mid(self, mock_price, rules_mock):
        """
        LastTrade is NaN until a trade prints into the book, and CoinDCX perpetuals never
        feed one. A NaN Decimal raises on comparison, so this used to kill the executor.
        """
        rules_mock.return_value = self.trading_rules()

        def feed(_c, _p, price_type=PriceType.MidPrice):
            if price_type == PriceType.LastTrade:
                return Decimal("NaN")
            return {PriceType.BestBid: Decimal("100"), PriceType.BestAsk: Decimal("100"),
                    PriceType.MidPrice: Decimal("103")}[price_type]

        mock_price.side_effect = feed
        executor = self.running_executor(self.config(
            entry_price=Decimal("100"), entry_offset_pct=Decimal("0.02"),
            trigger_price_type=PriceType.LastTrade))

        await executor.control_task()

        # Mid of 103 is past the 102 long level, so it entered rather than crashing.
        self.assertEqual(executor._triggered_side, TradeType.BUY)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_no_usable_price_skips_the_tick_instead_of_crashing(self, mock_price, rules_mock):
        """With nothing usable we do nothing — never guess, never raise."""
        rules_mock.return_value = self.trading_rules()
        mock_price.side_effect = lambda _c, _p, price_type=None: Decimal("NaN")
        executor = self.running_executor(self.config(
            entry_price=Decimal("100"), entry_offset_pct=Decimal("0.02"),
            trigger_price_type=PriceType.LastTrade))

        await executor.control_task()

        self.assertIsNone(executor._triggered_side)
        self.strategy.buy.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_stop_loss_skips_rather_than_guessing_on_a_bad_price(self, mock_price, rules_mock):
        """
        Deciding "not breached" from missing data would leave a live position unprotected
        without saying so, so the check is skipped and logged instead.
        """
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        mock_price.side_effect = self.price_feed(best_bid="100", last_trade="100")
        executor = self.running_executor(self.config(trigger_price_type=PriceType.LastTrade))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "1", "100")

        mock_price.side_effect = lambda _c, _p, price_type=None: Decimal("NaN")
        await executor.control_task()

        self.assertIsNone(executor.close_type)
        self.assertEqual(executor._status, RunnableStatus.RUNNING)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_symmetric_exit_bracket_after_a_long_fill(self, mock_price, rules_mock):
        """Long from 98 with a 2% bracket exits at 100 (limit) or 96 (watched trigger)."""
        mock_price.side_effect = self.price_feed(last_trade="98", best_bid="98", best_ask="98.1")
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        executor = self.running_executor(self.config(
            entry_price=Decimal("98"),
            entry_offset_pct=Decimal("0"),
            barriers=SimpleGridBarriers(take_profit=Decimal("0.0204"), stop_loss=Decimal("0.0204"))))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="98"), "1", "98")
        await executor.control_task()

        self.assertAlmostEqual(float(executor.take_profit_price), 100.0, places=1)
        self.assertAlmostEqual(float(executor.stop_loss_price), 96.0, places=1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_entry_timeout_closes_with_no_position(self, mock_price, rules_mock):
        """The level is never reached, so nothing is ever opened and nothing to unwind."""
        # Anchor 100 with a 2% step needs 102 to go long; the market never gets there.
        mock_price.side_effect = self.price_feed(best_bid="100", best_ask="100", last_trade="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_price=Decimal("100"), entry_offset_pct=Decimal("0.02"), entry_timeout=60))
        await executor.control_task()
        self.assertIsNone(executor._entry_orders[TradeType.BUY])

        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 61)
        await executor.control_task()

        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertIsNone(executor._close_order)
        self.assertEqual(executor.open_filled_amount, Decimal("0"))

    # ------------------------------------------------------------------ barriers

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_partial_fill_arms_take_profit_on_the_filled_amount(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("0.4")
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "0.4", "100")

        await executor.control_task()

        self.assertEqual(executor._filled_side, TradeType.BUY)
        self.assertEqual(executor.open_filled_amount, Decimal("0.4"))
        self.assertEqual(executor._take_profit_order.order_id, "OID-SELL-1")
        self.assertEqual(self.strategy.sell.call_args.args[2], Decimal("0.4"))
        self.assertEqual(self.strategy.sell.call_args.args[4], Decimal("105.00"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_stop_loss_fires_on_configured_trigger_price(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        # The level has to be reached before anything opens, so trigger the entry first.
        mock_price.side_effect = self.price_feed(best_bid="100", last_trade="100")
        executor = self.running_executor(self.config(trigger_price_type=PriceType.LastTrade))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "1", "100")

        # Now the last trade prints below the stop while the book is still intact.
        mock_price.side_effect = self.price_feed(best_bid="99", last_trade="97.5")
        await executor.control_task()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertIsNotNone(executor._close_order)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_stop_loss_holds_when_trigger_price_is_above_the_stop(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="97", last_trade="99")
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        executor = self.running_executor(self.config(trigger_price_type=PriceType.LastTrade))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "1", "100")

        await executor.control_task()

        self.assertIsNone(executor.close_type)
        self.assertEqual(executor._take_profit_order.order_id, "OID-SELL-1")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_take_profit_fill_reports_close_price_and_type(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "1", "100")
        await executor.control_task()

        executor._take_profit_order.order = self.register(self.fill(
            self.in_flight("OID-SELL-1", TradeType.SELL, amount="1", price="105"), "1", "105", trade_id="2"))
        executor.process_order_completed_event(
            None, None, BuyOrderCompletedEvent(
                timestamp=START_TS, order_id="OID-SELL-1", base_asset="BTC", quote_asset="USDT",
                base_asset_amount=Decimal("1"), quote_asset_amount=Decimal("105"),
                order_type=OrderType.LIMIT))

        self.assertEqual(executor.close_type, CloseType.TAKE_PROFIT)
        self.assertEqual(executor.close_price, Decimal("105"))
        self.assertEqual(executor.get_custom_info()["close_price"], Decimal("105"))
        self.assertEqual(executor.trade_pnl_pct, Decimal("0.05"))

    # ------------------------------------------------------------------ regressions

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_cancelling_a_partially_filled_entry_keeps_the_position(self, mock_price, rules_mock):
        """
        Cancelling the unfilled remainder must not discard the tracked order, because its
        fills are the only record of the position we are holding.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("0.4")
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "0.4", "100")
        executor._filled_side = TradeType.BUY

        executor.process_order_canceled_event(
            None, None, OrderCancelledEvent(timestamp=START_TS, order_id="OID-BUY-1"))

        self.assertIsNotNone(executor._entry_orders[TradeType.BUY])
        self.assertEqual(executor.open_filled_amount, Decimal("0.4"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_entry_filled_without_trade_updates_still_arms_the_exit(self, mock_price, rules_mock):
        """
        A venue can report an order FILLED while its trade updates are still in flight,
        leaving executed_amount_base at zero and every size derived from it collapsing too.

        The completion event carries zero amounts here because that is what the venue
        produces: ClientOrderTracker builds it from the same empty field, so it cannot be
        used to recover the size.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        executor = self.running_executor(self.config())
        await executor.control_task()

        order = self.register(self.in_flight(
            "OID-BUY-1", TradeType.BUY, amount="1", price="100", state=OrderState.FILLED))
        executor._entry_orders[TradeType.BUY].order = order
        # The precondition that broke it: terminal state, nothing filled as far as the
        # order object is concerned.
        self.assertEqual(order.executed_amount_base, Decimal("0"))

        executor.process_order_completed_event(
            None, None, BuyOrderCompletedEvent(
                timestamp=START_TS, order_id="OID-BUY-1", base_asset="BTC", quote_asset="USDT",
                base_asset_amount=Decimal("0"), quote_asset_amount=Decimal("0"),
                order_type=OrderType.MARKET))

        self.assertEqual(executor._filled_side, TradeType.BUY)
        self.assertEqual(executor.open_filled_amount, Decimal("1"))
        self.assertEqual(executor.entry_price, Decimal("100"))

        await executor.control_task()
        self.assertIsNotNone(executor._take_profit_order)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_entry_remainder_is_only_cancelled_once(self, mock_price, rules_mock):
        """
        The order stays open until the venue acknowledges the cancel, so re-sending it on
        every tick would burn rate limit for nothing.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("0.4")
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.register(self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "0.4", "100"))

        await executor.control_task()
        await executor.control_task()
        await executor.control_task()

        entry_cancels = [c for c in self.strategy.cancel.call_args_list
                         if c.kwargs.get("order_id") == "OID-BUY-1"]
        self.assertEqual(len(entry_cancels), 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_cancelling_an_unfilled_entry_clears_the_slot(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.in_flight("OID-BUY-1", TradeType.BUY)

        executor.process_order_canceled_event(
            None, None, OrderCancelledEvent(timestamp=START_TS, order_id="OID-BUY-1"))

        self.assertIsNone(executor._entry_orders[TradeType.BUY])

    # ------------------------------------------------------------------ lifecycle

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_moves_to_shutting_down(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        executor.early_stop()
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_keeping_position(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        executor.early_stop(keep_position=True)
        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)

    @patch.object(SimpleGridExecutor, "validate_sufficient_balance")
    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_both_oco_refused_on_a_spot_connector(self, mock_price, rules_mock, _):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.strategy.connectors = {"binance": MagicMock(spec=ExchangePyBase)}
        executor = SimpleGridExecutor(self.strategy, self.config(
            connector_name="binance", entry_mode=SimpleGridEntryMode.BOTH_OCO,
            entry_offset_pct=Decimal("0.02")))

        await executor.on_start()

        self.assertEqual(executor.close_type, CloseType.FAILED)
        self.assertEqual(executor._status, RunnableStatus.TERMINATED)

    @patch.object(SimpleGridExecutor, "adjust_order_candidates")
    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_both_oco_budgets_one_leg_not_two(self, mock_price, rules_mock, adjust_mock):
        """
        Only one entry order is ever sent, so both_oco must not reserve collateral twice.

        Budgeting both sides together makes an affordable leg fail as if it were double the
        size, and every executor terminates with INSUFFICIENT_BALANCE.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            entry_mode=SimpleGridEntryMode.BOTH_OCO, entry_offset_pct=Decimal("0.02")))
        adjust_mock.side_effect = lambda _connector, candidates: candidates

        await executor.validate_sufficient_balance()

        self.assertNotEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        # One call per side, each carrying a single candidate — never both summed together.
        self.assertEqual(adjust_mock.call_count, 2)
        for call in adjust_mock.call_args_list:
            self.assertEqual(len(call.args[1]), 1)

    @patch.object(SimpleGridExecutor, "adjust_order_candidates")
    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_a_leg_that_does_not_fit_is_still_refused(self, mock_price, rules_mock, adjust_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())

        def _zero(_connector, candidates):
            for candidate in candidates:
                candidate.amount = Decimal("0")
            return candidates

        adjust_mock.side_effect = _zero

        await executor.validate_sufficient_balance()

        self.assertEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_custom_info_carries_what_the_controller_needs(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = SimpleGridExecutor(self.strategy, self.config(entry_price=Decimal("100")))
        info = executor.get_custom_info()
        for key in ("side", "close_price", "close_type", "entry_price", "take_profit_price", "stop_loss_price"):
            self.assertIn(key, info)
