from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import BuyOrderCompletedEvent, OrderCancelledEvent, OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridBarriers,
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.executors.simple_grid_executor.simple_grid_executor import SimpleGridExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

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

    # ------------------------------------------------------------------ entry placement

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_long_entry_rests_at_best_bid(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        await executor.control_task()
        self.assertEqual(executor._entry_orders[TradeType.BUY].order_id, "OID-BUY-1")
        self.assertEqual(self.strategy.buy.call_args.args[4], Decimal("99"))
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_short_entry_rests_at_best_ask(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY))
        await executor.control_task()
        self.assertEqual(executor._entry_orders[TradeType.SELL].order_id, "OID-SELL-1")
        self.assertEqual(self.strategy.sell.call_args.args[4], Decimal("101"))
        self.strategy.buy.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_both_oco_places_an_entry_on_each_side(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        await executor.control_task()
        self.assertEqual(executor._entry_orders[TradeType.BUY].order_id, "OID-BUY-1")
        self.assertEqual(executor._entry_orders[TradeType.SELL].order_id, "OID-SELL-1")

    # ------------------------------------------------------------------ first fill wins

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_first_fill_cancels_the_opposite_entry(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        await executor.control_task()

        executor._entry_orders[TradeType.BUY].order = self.register(
            self.fill(self.in_flight("OID-BUY-1", TradeType.BUY), "1", "99"))
        executor._entry_orders[TradeType.SELL].order = self.register(
            self.in_flight("OID-SELL-1", TradeType.SELL))

        executor.process_order_filled_event(
            None, None, OrderFilledEvent(
                timestamp=START_TS, order_id="OID-BUY-1", trading_pair="BTC-USDT",
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT,
                price=Decimal("99"), amount=Decimal("1"),
                trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.2"))])))

        self.assertEqual(executor._filled_side, TradeType.BUY)
        self.strategy.cancel.assert_called_once()
        self.assertEqual(self.strategy.cancel.call_args.kwargs["order_id"], "OID-SELL-1")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_opposing_fill_nets_against_the_position(self, mock_price, rules_mock):
        """If a cancel loses the race and both sides fill, the venue nets them."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("0.3")
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        executor._entry_orders[TradeType.BUY] = TrackedOrder("OID-BUY-1")
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY), "1", "100")
        executor._entry_orders[TradeType.SELL] = TrackedOrder("OID-SELL-1")
        executor._entry_orders[TradeType.SELL].order = self.fill(
            self.in_flight("OID-SELL-1", TradeType.SELL), "0.7", "100")
        executor._filled_side = TradeType.BUY

        self.assertEqual(executor.open_filled_amount, Decimal("0.3"))

    # ------------------------------------------------------------------ entry chasing

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_entry_is_reposted_when_touch_price_drifts(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.in_flight("OID-BUY-1", TradeType.BUY, price="99")

        # Touch walks up beyond the threshold, and the min interval has elapsed.
        mock_price.side_effect = self.price_feed(best_bid="99.5")
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 5)
        await executor.control_task()

        self.strategy.cancel.assert_called_once()
        self.assertEqual(executor._repost_count[TradeType.BUY], 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_repost_is_suppressed_inside_min_interval(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(min_repost_interval=30))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.in_flight("OID-BUY-1", TradeType.BUY, price="99")

        mock_price.side_effect = self.price_feed(best_bid="99.5")
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 5)
        await executor.control_task()

        self.strategy.cancel.assert_not_called()
        self.assertEqual(executor._repost_count[TradeType.BUY], 0)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_chasing_gives_up_past_the_drift_cap(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(max_entry_drift=Decimal("0.01")))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.in_flight("OID-BUY-1", TradeType.BUY, price="99")

        # 99 -> 102 is ~3%, past the 1% cap: entering here is not the trade we sized for.
        mock_price.side_effect = self.price_feed(best_bid="102")
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 5)
        await executor.control_task()

        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_entry_timeout_closes_with_no_position(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_timeout=60))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.in_flight("OID-BUY-1", TradeType.BUY)

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
        # Last trade prints below the stop while the book is still intact.
        mock_price.side_effect = self.price_feed(best_bid="99", last_trade="97.5")
        rules_mock.return_value = self.trading_rules()
        self.set_quantize("1")
        executor = self.running_executor(self.config(trigger_price_type=PriceType.LastTrade))
        await executor.control_task()
        executor._entry_orders[TradeType.BUY].order = self.fill(
            self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="100"), "1", "100")

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
            connector_name="binance", entry_mode=SimpleGridEntryMode.BOTH_OCO))

        await executor.on_start()

        self.assertEqual(executor.close_type, CloseType.FAILED)
        self.assertEqual(executor._status, RunnableStatus.TERMINATED)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_custom_info_carries_what_the_controller_needs(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = SimpleGridExecutor(self.strategy, self.config(entry_price=Decimal("100")))
        info = executor.get_custom_info()
        for key in ("side", "close_price", "close_type", "entry_price", "take_profit_price", "stop_loss_price"):
            self.assertIn(key, info)
