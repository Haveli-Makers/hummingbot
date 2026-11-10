import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionSide, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
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
        strategy.buy.side_effect = [f"OID-BUY-{i}" for i in range(1, 10)]
        strategy.sell.side_effect = [f"OID-SELL-{i}" for i in range(1, 10)]
        strategy.cancel.return_value = None
        connector = MagicMock(spec=ExchangePyBase)
        # Quantization is identity here; the tests are about which price we choose, not
        # about tick rounding.
        connector.quantize_order_price.side_effect = lambda trading_pair, price: price
        connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        connector.supported_order_types.return_value = [OrderType.LIMIT, OrderType.MARKET]
        strategy.connectors = {"coindcx_perpetual": connector, "binance": connector}
        return strategy

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def trading_rules():
        rules = MagicMock(spec=TradingRule)
        rules.min_order_size = Decimal("0.1")
        rules.min_notional_size = Decimal("1")
        rules.min_price_increment = Decimal("0.01")
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
            # Where the previous leg ended. With the 5% / 2% steps below, that puts the
            # resting entry at 95.00 and the trigger at 102.00.
            entry_reference_price=Decimal("100"),
            trigger_price_type=PriceType.MidPrice,
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

    def track(self, executor, slot, order_id, side, amount="1", price="100",
              state=OrderState.OPEN, filled=None, fill_price=None):
        """Attach a tracked order to one of the executor's slots, optionally pre-filled."""
        order = self.register(self.in_flight(order_id, side, amount=amount, price=price, state=state))
        if filled is not None:
            self.fill(order, filled, fill_price or price)
        tracked = TrackedOrder(order_id=order_id)
        tracked.order = order
        if slot == "entry":
            executor._entry_orders[side] = tracked
        else:
            setattr(executor, slot, tracked)
        return tracked

    def open_long(self, executor, amount="1", price="100"):
        """Put the executor into a filled long position entered at `price`."""
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY,
                   amount=amount, price=price, filled=amount, fill_price=price)
        executor._filled_side = TradeType.BUY
        return executor

    def open_short(self, executor, amount="1", price="100"):
        self.track(executor, "entry", "OID-SELL-1", TradeType.SELL,
                   amount=amount, price=price, filled=amount, fill_price=price)
        executor._filled_side = TradeType.SELL
        return executor

    @staticmethod
    def order_args(call):
        """ExecutorBase.place_order calls strategy.buy/sell positionally."""
        _connector, _pair, amount, order_type, price, position_action = call.args
        return {"amount": amount, "order_type": order_type, "price": price,
                "position_action": position_action}

    @staticmethod
    def cancel_event(order_id):
        return OrderCancelledEvent(timestamp=START_TS, order_id=order_id, exchange_order_id=f"E{order_id}")

    # ------------------------------------------------------------------ the anchor and the bracket

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_bracket_is_measured_from_the_fill(self, mock_price, rules_mock):
        """
        Every position risks exactly one step and targets exactly one step, wherever the fill
        landed. The reference only decides where the entry waits, not what it is worth.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor, price="99")

        self.assertEqual(executor.entry_price, Decimal("99"))
        # 5% and 2% of the FILL (99), not of the reference (100).
        self.assertEqual(executor.take_profit_price, Decimal("103.95"))
        self.assertEqual(executor.stop_loss_price, Decimal("97.02"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_bracket_prices_short_are_mirrored(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY))
        self.open_short(executor)

        self.assertEqual(executor.take_profit_price, Decimal("95.00"))
        self.assertEqual(executor.stop_loss_price, Decimal("102.00"))

    # ------------------------------------------------------------------ maker entries

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_buy_rests_one_step_below_the_reference(self, mock_price, rules_mock):
        """
        Not at the touch. A grid level a full step away cannot cross by accident, which is
        what makes the resting side reliably a maker fill.
        """
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())

        executor.control_entry_orders()

        self.strategy.buy.assert_called_once()
        # reference 100, take profit step 5% -> 95.00
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("95.00"))
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["order_type"], OrderType.LIMIT)
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_sell_rests_one_step_above_the_reference(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY))

        executor.control_entry_orders()

        self.strategy.sell.assert_called_once()
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("105.00"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_both_oco_rests_one_maker_order_on_each_side_of_the_book(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))

        executor.control_entry_orders()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("95.00"))
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("105.00"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_post_only_is_used_where_the_venue_supports_it(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.strategy.connectors["coindcx_perpetual"].supported_order_types.return_value = [
            OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]
        executor = self.running_executor(self.config())

        executor.control_entry_orders()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["order_type"], OrderType.LIMIT_MAKER)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_plain_limit_where_post_only_is_not_offered(self, mock_price, rules_mock):
        """CoinDCX futures report allow_post_only == false; LIMIT_MAKER would be rejected."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.strategy.connectors["coindcx_perpetual"].supported_order_types.return_value = [
            OrderType.LIMIT, OrderType.MARKET]
        executor = self.running_executor(self.config())

        executor.control_entry_orders()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["order_type"], OrderType.LIMIT)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_trigger_is_a_plain_limit_even_where_post_only_exists(self, mock_price, rules_mock):
        """
        The trigger crosses on purpose, and a post-only order priced to cross is rejected
        outright by every venue that offers post-only. Invisible on CoinDCX, which has no
        post-only at all — on any venue that does, the trigger would never fill.
        """
        rules_mock.return_value = self.trading_rules()
        self.strategy.connectors["coindcx_perpetual"].supported_order_types.return_value = [
            OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]
        executor = self.running_executor(self.config())

        # reference 100, stop loss step 2% -> the trigger sits at 102.00
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()

        args = self.order_args(self.strategy.buy.call_args)
        self.assertEqual(args["order_type"], OrderType.LIMIT)
        self.assertEqual(args["price"], Decimal("102.30"))

    # ------------------------------------------------------------------ first fill wins

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_first_fill_cancels_the_other_side(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99",
                   filled="1", fill_price="99")
        self.track(executor, "entry", "OID-SELL-1", TradeType.SELL, price="101")

        executor._detect_entry_fill()

        self.assertEqual(executor.side, TradeType.BUY)
        self.strategy.cancel.assert_called_once()
        self.assertEqual(self.strategy.cancel.call_args.kwargs["order_id"], "OID-SELL-1")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_only_one_side_is_ever_taken(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, filled="1", fill_price="99")
        executor._detect_entry_fill()

        # A late fill on the losing side must not flip the direction.
        self.track(executor, "entry", "OID-SELL-1", TradeType.SELL, filled="1", fill_price="101")
        executor._detect_entry_fill()

        self.assertEqual(executor.side, TradeType.BUY)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_both_sides_filling_reports_the_net_position(self, mock_price, rules_mock):
        """A cancel can lose the race; the venue nets what is left, so we must too."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, filled="1", fill_price="99")
        executor._filled_side = TradeType.BUY
        self.track(executor, "entry", "OID-SELL-1", TradeType.SELL, filled="0.4", fill_price="101")

        self.assertEqual(executor.open_filled_amount, Decimal("0.6"))

    # ------------------------------------------------------------------ take profit

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_take_profit_rests_at_the_anchor_based_price(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)

        executor.control_barriers()

        self.strategy.sell.assert_called_once()
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("105.00"))
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["order_type"], OrderType.LIMIT)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_partial_fill_arms_take_profit_on_the_filled_amount(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99",
                   filled="0.5", fill_price="99")
        executor._filled_side = TradeType.BUY

        executor.control_barriers()

        self.assertEqual(self.order_args(self.strategy.sell.call_args)["amount"], Decimal("0.5"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_take_profit_fill_reports_close_price_and_type(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL,
                   price="105", filled="1", fill_price="105", state=OrderState.FILLED)

        executor.process_order_completed_event(None, MagicMock(), SellOrderCompletedEvent(
            timestamp=START_TS, order_id="OID-SELL-1", base_asset="BTC", quote_asset="USDT",
            base_asset_amount=Decimal("1"), quote_asset_amount=Decimal("105"),
            order_type=OrderType.LIMIT))

        self.assertEqual(executor.close_type, CloseType.TAKE_PROFIT)
        self.assertEqual(executor.close_price, Decimal("105"))
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_entry_remainder_is_only_cancelled_once(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99",
                   filled="0.5", fill_price="99")
        executor._filled_side = TradeType.BUY

        executor.control_barriers()
        executor.control_barriers()

        cancels = [c for c in self.strategy.cancel.call_args_list
                   if c.kwargs["order_id"] == "OID-BUY-1"]
        self.assertEqual(len(cancels), 1)

    # ------------------------------------------------------------------ the chasing stop loss

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_stop_loss_leaves_passively_at_the_touch_and_pulls_the_take_profit(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")

        # Anchor 100, stop loss 2% -> level 98. Mid reaches it.
        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()

        self.assertTrue(executor._stop_loss_triggered)
        # The take profit must not be able to fill behind us.
        self.assertIn("OID-SELL-1", [c.kwargs["order_id"] for c in self.strategy.cancel.call_args_list])

        # The exit follows that cancel, and rests one tick above the best bid — maker, and
        # the best offer in the book.
        tp.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-1"))
        executor._place_scheduled_exit()   # the settle delay elapses
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.91"))
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["order_type"], OrderType.LIMIT)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_follows_the_book_down(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_requote_pct=Decimal("0.0005"), stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")
        self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")

        # Threshold is 0.0005 * 98 = 0.049; the ask fell by 1.0.
        mock_price.side_effect = self.price_feed(best_bid="96.9", best_ask="97.1", mid="97")
        executor.control_barriers()

        self.assertEqual(self.strategy.cancel.call_args.kwargs["order_id"], "OID-SELL-2")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_never_retreats_upwards(self, mock_price, rules_mock):
        """
        Ratcheted on purpose. If the sell followed the ask back up it would stay ahead of the
        market forever and a recovery would never fill it; left where it is, the rebound runs
        into it and the leg closes, which is what a stop is for.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")
        self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")

        mock_price.side_effect = self.price_feed(best_bid="98.9", best_ask="99.1", mid="99")
        executor.control_barriers()

        cancelled = [c.kwargs["order_id"] for c in self.strategy.cancel.call_args_list]
        self.assertNotIn("OID-SELL-2", cancelled)
        self.assertEqual(executor._sl_chase_order.order.price, Decimal("98.1"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_recovery_does_not_trip_the_drift_cap(self, mock_price, rules_mock):
        """Only movement away from the stop counts; marketing out on a rebound is the worst case."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.005"))))
        self.open_long(executor)
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")
        self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")

        # 2% above the stop level — far past the 0.5% cap, but in our favour.
        mock_price.side_effect = self.price_feed(best_bid="99.9", best_ask="100.1", mid="100")
        executor.control_barriers()

        self.assertEqual(executor._status, RunnableStatus.RUNNING)
        self.assertIsNone(executor.close_type)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_gives_up_and_takes_the_market_past_the_drift_cap(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.005"))))
        self.open_long(executor)
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")
        chase = self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")

        # 97.4 is 0.61% below the stop level of 98, past the 0.5% cap.
        mock_price.side_effect = self.price_feed(best_bid="97.3", best_ask="97.5", mid="97.4")
        executor.control_barriers()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)
        # The passive chase order is pulled first; the crossing exit follows its cancel.
        self.assertTrue(executor._close_pending)
        chase.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-2"))
        executor._place_scheduled_exit()   # the settle delay elapses

        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertLess(close["price"], Decimal("97.3"), "the exit has to cross, not rest")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_chasing_can_be_switched_off_for_a_straight_crossing_exit(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"), stop_loss_chase=False)))
        self.open_long(executor)

        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        # 20 ticks through the bid at 97.90, so it crosses instead of resting.
        self.assertEqual(close["price"], Decimal("97.70"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_short_stop_triggers_above_the_anchor_and_exits_inside_the_spread(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY,
                        barriers=SimpleGridBarriers(
                            take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                            stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_short(executor)

        # Anchor 100, short stop 2% above -> 102.
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_barriers()

        self.assertTrue(executor._stop_loss_triggered)
        # One tick below the best ask, not down at the bid.
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("102.09"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_filled_chase_order_closes_the_leg_as_a_stop_loss(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")
        self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1",
                   filled="1", fill_price="98.1", state=OrderState.FILLED)

        executor.process_order_completed_event(None, MagicMock(), SellOrderCompletedEvent(
            timestamp=START_TS, order_id="OID-SELL-2", base_asset="BTC", quote_asset="USDT",
            base_asset_amount=Decimal("1"), quote_asset_amount=Decimal("98.1"),
            order_type=OrderType.LIMIT))

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor.close_price, Decimal("98.1"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_partial_fills_from_abandoned_chase_attempts_still_count(self, mock_price, rules_mock):
        """
        The controller anchors the next leg on close_price, so an exit spread across several
        re-quotes has to report the volume weighted average, not just the last order.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor, amount="2")
        executor._stop_loss_triggered = True
        executor._stop_loss_trigger_price = Decimal("98")

        # First attempt takes half at 98.1, then is cancelled by a re-quote. Its fills are
        # retired but still count towards the exit.
        first = self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, amount="2",
                           price="98.1", filled="1", fill_price="98.1")
        first.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-2"))
        self.assertIn(first, executor._spent_exit_orders)

        # Second attempt takes the rest lower down.
        self.track(executor, "_sl_chase_order", "OID-SELL-3", TradeType.SELL, amount="1",
                   price="97.1", filled="1", fill_price="97.1")

        self.assertEqual(executor.close_filled_amount, Decimal("2"))
        self.assertEqual(executor.close_price, Decimal("97.6"))  # (98.1 + 97.1) / 2

    # ------------------------------------------------------------------ prices that are not there

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_nan_trigger_price_falls_back_to_mid(self, mock_price, rules_mock):
        """CoinDCX perpetuals never publish a last trade, so LastTrade sits at NaN forever."""
        rules_mock.return_value = self.trading_rules()
        prices = {
            PriceType.BestBid: Decimal("97.9"),
            PriceType.BestAsk: Decimal("98.1"),
            PriceType.LastTrade: Decimal("NaN"),
            PriceType.MidPrice: Decimal("98"),
        }
        mock_price.side_effect = lambda _c, _p, price_type=PriceType.MidPrice: prices[price_type]
        executor = self.running_executor(self.config(trigger_price_type=PriceType.LastTrade))
        self.open_long(executor)

        executor.control_barriers()

        self.assertTrue(executor._stop_loss_triggered)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_no_usable_price_skips_the_tick_instead_of_crashing(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        mock_price.side_effect = lambda _c, _p, price_type=PriceType.MidPrice: Decimal("NaN")
        executor = self.running_executor(self.config(entry_reference_price=None))

        executor.control_entry_orders()

        self.strategy.buy.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_stop_loss_skips_rather_than_guessing_on_a_bad_price(self, mock_price, rules_mock):
        """Deciding 'not breached' from missing data leaves a position silently unprotected."""
        rules_mock.return_value = self.trading_rules()
        mock_price.side_effect = lambda _c, _p, price_type=PriceType.MidPrice: Decimal("NaN")
        executor = self.running_executor(self.config())
        self.open_long(executor)

        executor.control_stop_loss()

        self.assertFalse(executor._stop_loss_triggered)
        self.assertEqual(executor._status, RunnableStatus.RUNNING)

    # ------------------------------------------------------------------ lifecycle

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_entry_timeout_closes_with_no_position(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_timeout=10))
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99")
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 30)

        executor.control_entry_orders()

        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertEqual(executor.open_filled_amount, Decimal("0"))
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_entry_filled_without_trade_updates_still_arms_the_exit(self, mock_price, rules_mock):
        """
        executed_amount_base is only populated by trade updates. CoinDCX marked an entry
        FILLED while its fills were still in flight, so every size derived from it collapsed
        to zero, no take profit was placed, and a real position sat unprotected.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        order = self.register(self.in_flight("OID-BUY-1", TradeType.BUY, amount="1", price="99",
                                             state=OrderState.FILLED))
        tracked = TrackedOrder(order_id="OID-BUY-1")
        tracked.order = order
        executor._entry_orders[TradeType.BUY] = tracked

        self.assertEqual(order.executed_amount_base, Decimal("0"))  # the venue told us nothing
        executor._detect_entry_fill()
        executor.control_barriers()

        self.assertEqual(executor.side, TradeType.BUY)
        self.assertEqual(executor.open_filled_amount, Decimal("1"))
        self.strategy.sell.assert_called_once()
        # the venue's own price for the order, 99, is what the bracket is built from
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("103.95"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_cancelling_a_partly_filled_entry_keeps_the_position(self, mock_price, rules_mock):
        """Its fills are the only record of what we hold; dropping it would lose the position."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99",
                   filled="0.5", fill_price="99")
        executor._filled_side = TradeType.BUY

        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-BUY-1"))

        self.assertIsNotNone(executor._entry_orders[TradeType.BUY])
        self.assertEqual(executor.open_filled_amount, Decimal("0.5"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_cancelling_an_unfilled_entry_clears_the_slot(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99")

        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-BUY-1"))

        self.assertIsNone(executor._entry_orders[TradeType.BUY])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_flattens_the_position_without_waiting_for_a_control_tick(
            self, mock_price, rules_mock):
        """
        The framework calls early_stop, waits a bounded few seconds, then tears the connectors
        down. The cancel goes out immediately and the close follows the moment the venue
        acknowledges it — roughly a tenth of a second, not the tick or two it would take if
        this were left to the shutdown loop.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()

        executor.early_stop()

        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertIn("OID-SELL-1", [c.kwargs["order_id"] for c in self.strategy.cancel.call_args_list])
        self.assertTrue(executor._close_pending)

        tp.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-1"))
        executor._place_scheduled_exit()   # the settle delay elapses

        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertEqual(close["amount"], Decimal("1"))
        self.assertLess(close["price"], Decimal("99"), "the exit has to cross, not rest")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_with_nothing_open_just_cancels(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="99")

        executor.early_stop()

        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(self.strategy.cancel.call_args.kwargs["order_id"], "OID-BUY-1")
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_keeping_position_does_not_close_anything(self, mock_price, rules_mock):
        """Handing the position on is the one case where not flattening is the point."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)

        executor.early_stop(keep_position=True)

        self.assertEqual(executor.close_type, CloseType.POSITION_HOLD)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_stopping_with_a_position_still_open_is_logged_loudly(self, mock_price, rules_mock):
        """Nothing else is watching it once we are gone, so it cannot be left silent."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)

        with patch.object(executor.logger(), "error") as error:
            executor.stop()

        self.assertEqual(error.call_count, 1)
        self.assertIn("STILL OPEN", error.call_args.args[0])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_clean_stop_says_nothing(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        self.track(executor, "_close_order", "OID-SELL-2", TradeType.SELL,
                   price="98", filled="1", fill_price="98", state=OrderState.FILLED)

        with patch.object(executor.logger(), "error") as error:
            executor.stop()

        error.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_both_oco_refused_on_a_spot_connector(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(connector_name="binance", entry_mode=SimpleGridEntryMode.BOTH_OCO))

        with patch.object(SimpleGridExecutor, "stop") as stop_mock:
            await executor.on_start()

        self.assertEqual(executor.close_type, CloseType.FAILED)
        stop_mock.assert_called_once()

    # ------------------------------------------------------------------ balance

    @patch.object(SimpleGridExecutor, "adjust_order_candidates")
    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_both_oco_budgets_one_leg_not_two(self, mock_price, rules_mock, adjust_mock):
        """
        Both sides do rest at once, but only one can ever become a position. Budgeting them
        together refused legs that fit perfectly well — a 7 USDT leg checked as 14.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        adjust_mock.side_effect = lambda _connector, candidates: candidates
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))

        await executor.validate_sufficient_balance()

        self.assertNotEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        # One candidate per call, never both summed into one.
        for call in adjust_mock.call_args_list:
            self.assertEqual(len(call.args[1]), 1)

    @patch.object(SimpleGridExecutor, "adjust_order_candidates")
    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_a_leg_that_does_not_fit_is_still_refused(self, mock_price, rules_mock, adjust_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()

        def zero_out(_connector, candidates):
            for candidate in candidates:
                candidate.amount = Decimal("0")
            return candidates

        adjust_mock.side_effect = zero_out
        executor = self.running_executor(self.config())

        with patch.object(SimpleGridExecutor, "stop"):
            await executor.validate_sufficient_balance()

        self.assertEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)

    # ------------------------------------------------------------------ reporting

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_custom_info_carries_what_the_controller_needs(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor, price="99")

        info = executor.get_custom_info()

        self.assertEqual(info["side"], TradeType.BUY)
        self.assertEqual(info["reference_price"], Decimal("100"))
        self.assertEqual(info["entry_price"], Decimal("99"))
        self.assertEqual(info["take_profit_price"], Decimal("103.95"))
        self.assertEqual(info["stop_loss_price"], Decimal("97.02"))
        self.assertFalse(info["stop_loss_triggered"])
        self.assertIn("close_price", info)
        self.assertIn("close_type", info)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_custom_info_reports_where_the_entries_are_resting(self, mock_price, rules_mock):
        """Nothing sits at a level any more, so this is how a watching leg proves it is alive."""
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.BOTH_OCO))

        executor.control_entry_orders()
        info = executor.get_custom_info()

        self.assertEqual(info["resting_entries"], {"BUY": Decimal("95.00"), "SELL": Decimal("105.00")})

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_missing_order_book_is_reported_once_not_every_tick(self, mock_price, rules_mock):
        """
        Every entry is priced off the touch, so no book means no orders at all — and a silent
        executor looks identical to one waiting patiently for the market.
        """
        rules_mock.return_value = self.trading_rules()
        prices = {
            PriceType.BestBid: Decimal("NaN"),
            PriceType.BestAsk: Decimal("NaN"),
            PriceType.LastTrade: Decimal("100"),
            PriceType.MidPrice: Decimal("100"),
        }
        mock_price.side_effect = lambda _c, _p, price_type=PriceType.MidPrice: prices[price_type]
        # No reference, so this leg prices its entry off the touch — the only case a missing
        # book can block.
        executor = self.running_executor(self.config(entry_reference_price=None))

        with patch.object(executor.logger(), "warning") as warn:
            executor.control_entry_orders()
            executor.control_entry_orders()
            executor.control_entry_orders()

        self.strategy.buy.assert_not_called()
        self.assertEqual(warn.call_count, 1)
        self.assertIn("order book has not arrived", warn.call_args.args[0])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_book_coming_back_resumes_quoting(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        prices = {
            PriceType.BestBid: Decimal("NaN"),
            PriceType.BestAsk: Decimal("NaN"),
            PriceType.LastTrade: Decimal("100"),
            PriceType.MidPrice: Decimal("100"),
        }
        mock_price.side_effect = lambda _c, _p, price_type=PriceType.MidPrice: prices[price_type]
        executor = self.running_executor(self.config(entry_reference_price=None))
        executor.control_entry_orders()
        self.strategy.buy.assert_not_called()

        mock_price.side_effect = self.price_feed(best_bid="99.9", best_ask="100.1")
        executor.control_entry_orders()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("99.9"))

    # ------------------------------------------------------------------ aggressive maker exits

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_long_exits_one_tick_above_the_best_bid(self, mock_price, rules_mock):
        """
        Best bid + one tick, not the best ask. Both earn the maker fee, but this one is the
        best offer in the book and fills first — and an exit that never fills just chases the
        market down into the drift cap.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)

        # Wide book so the two candidate prices are clearly different.
        mock_price.side_effect = self.price_feed(best_bid="97.50", best_ask="98.50", mid="98")
        executor.control_barriers()

        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.51"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_short_exits_one_tick_below_the_best_ask(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY,
                        barriers=SimpleGridBarriers(
                            take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                            stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_short(executor)

        mock_price.side_effect = self.price_feed(best_bid="101.50", best_ask="102.50", mid="102")
        executor.control_barriers()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("102.49"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_exit_offset_can_be_backed_off_from_the_touch(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_maker_offset_ticks=5,
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)

        mock_price.side_effect = self.price_feed(best_bid="97.50", best_ask="98.50", mid="98")
        executor.control_barriers()

        # bid + 5 ticks: further from the market, better price, further back in the queue.
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.55"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_on_a_one_tick_spread_the_exit_lands_on_our_own_touch(self, mock_price, rules_mock):
        """XRP-USDT trades a one-tick spread, where the aggressive price IS the far touch."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)

        mock_price.side_effect = self.price_feed(best_bid="97.99", best_ask="98.00", mid="98")
        executor.control_barriers()

        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("98.00"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_exit_never_crosses_even_if_quantization_rounds_the_wrong_way(self, mock_price, rules_mock):
        """A 'maker' order that crosses pays taker, fills worse, and on a post-only venue is rejected."""
        rules_mock.return_value = self.trading_rules()
        # Quantize hard onto a coarse grid so the computed price would land back on the bid.
        connector = self.strategy.connectors["coindcx_perpetual"]
        connector.quantize_order_price.side_effect = \
            lambda trading_pair, price: Decimal(price).quantize(Decimal("1"))
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)

        mock_price.side_effect = self.price_feed(best_bid="97.50", best_ask="98.50", mid="98")
        executor.control_barriers()

        price = self.order_args(self.strategy.sell.call_args)["price"]
        self.assertGreater(price, Decimal("97.50"), "a sell at or below the best bid crosses")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_does_not_overwrite_a_take_profit_that_already_fired(self, mock_price, rules_mock):
        """
        The orchestrator calls early_stop on everything not yet TERMINATED, so a leg that has
        just won lands here too. Relabelling it would report a win as an early stop and cost
        the controller both its tally and its anchor.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL,
                   price="105", filled="1", fill_price="105", state=OrderState.FILLED)
        executor.process_order_completed_event(None, MagicMock(), SellOrderCompletedEvent(
            timestamp=START_TS, order_id="OID-SELL-1", base_asset="BTC", quote_asset="USDT",
            base_asset_amount=Decimal("1"), quote_asset_amount=Decimal("105"),
            order_type=OrderType.LIMIT))
        self.strategy.sell.reset_mock()

        executor.early_stop()

        self.assertEqual(executor.close_type, CloseType.TAKE_PROFIT)
        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_twice_does_not_send_two_close_orders(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)

        executor.early_stop()
        executor.early_stop()

        self.assertEqual(self.strategy.sell.call_count, 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_early_stop_covers_a_position_a_held_leg_left_behind(self, mock_price, rules_mock):
        """An expired leg that somehow holds stock still has to be flattened, not relabelled."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        executor.close_type = CloseType.EXPIRED
        executor._status = RunnableStatus.SHUTTING_DOWN

        executor.early_stop()

        self.assertEqual(executor.close_type, CloseType.EXPIRED)
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["order_type"], OrderType.LIMIT)

    # ------------------------------------------------------------------ the urgent exit

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_urgent_exit_is_a_crossing_limit_not_a_market_order(self, mock_price, rules_mock):
        """
        CoinDCX answers a reduce_only market order with
            400 "Reduce Only Order is only applicable for Limit Order"
        and the connector must send reduce_only or the venue demands margin for a fresh
        opposite position. A limit priced through the book satisfies both.
        """
        mock_price.side_effect = self.price_feed(best_bid="97.50", best_ask="98.50", mid="98")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)

        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        # 20 ticks of 0.01 through the bid.
        self.assertEqual(close["price"], Decimal("97.30"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_short_urgent_exit_crosses_upward_through_the_ask(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed(best_bid="97.50", best_ask="98.50", mid="98")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY))
        self.open_short(executor, price="99")

        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

        close = self.order_args(self.strategy.buy.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertEqual(close["price"], Decimal("98.70"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_market_closes_are_still_available_for_venues_that_take_them(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                close_order_type=OrderType.MARKET)))
        self.open_long(executor)

        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

        self.assertEqual(self.order_args(self.strategy.sell.call_args)["order_type"], OrderType.MARKET)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_gets_a_go_even_when_the_price_is_already_through_the_level(
            self, mock_price, rules_mock):
        """
        A tick only notices the stop once the price is past it. Charging that gap to the
        patience budget meant a tight cap skipped the chase entirely and went straight to a
        crossing order every single time — which is what happened live.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.0003"))))
        self.open_long(executor)

        # Stop level is 98.00; the tick lands at 97.90, already 0.10% through it — far more
        # than the 0.03% cap.
        mock_price.side_effect = self.price_feed(best_bid="97.89", best_ask="97.91", mid="97.90")
        executor.control_barriers()

        self.assertTrue(executor._stop_loss_triggered)
        self.assertEqual(executor._status, RunnableStatus.RUNNING, "should still be chasing")
        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertEqual(close["price"], Decimal("97.90"), "a passive exit one tick above the bid")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_cap_still_bites_on_drift_after_the_stop_fired(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.005"))))
        self.open_long(executor)
        mock_price.side_effect = self.price_feed(best_bid="97.89", best_ask="97.91", mid="97.90")
        executor.control_barriers()
        self.assertEqual(executor._status, RunnableStatus.RUNNING)

        # Another 0.6% below where the stop actually fired, past the 0.5% cap.
        mock_price.side_effect = self.price_feed(best_bid="97.28", best_ask="97.30", mid="97.29")
        executor.control_barriers()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    # ------------------------------------------------------------------ the close race

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_close_waits_for_a_resting_exit_to_be_cancelled(self, mock_price, rules_mock):
        """
        A cancel is a request, not an instant. While the reduce-only take profit is still
        live, a second reduce-only order for the same position makes it two-for-one and the
        venue refuses with "Insufficient funds" — one retry burned for nothing.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()

        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.EARLY_STOP)

        self.assertIn("OID-SELL-1", [c.kwargs["order_id"] for c in self.strategy.cancel.call_args_list])
        self.strategy.sell.assert_not_called()
        self.assertTrue(executor._close_pending)
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)
        self.assertEqual(executor._status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_close_goes_out_once_the_venue_has_settled(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()
        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.EARLY_STOP)

        tp.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-1"))
        executor._place_scheduled_exit()   # the settle delay elapses

        self.assertFalse(executor._close_pending)
        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertEqual(close["amount"], Decimal("1"))
        self.assertIsNotNone(executor._close_order)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_close_goes_out_at_once_when_nothing_is_resting(self, mock_price, rules_mock):
        """No conflict to wait for, so waiting would only delay the exit."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        self.strategy.sell.reset_mock()

        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.EARLY_STOP)

        self.assertFalse(executor._close_pending)
        self.assertEqual(self.strategy.sell.call_count, 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_the_shutdown_loop_covers_a_cancel_that_is_never_acknowledged(
            self, mock_price, rules_mock):
        """The position must not stay uncovered because a confirmation went missing."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()
        executor.place_close_order_and_cancel_open_orders(close_type=CloseType.EARLY_STOP)
        self.strategy.sell.assert_not_called()

        # The venue closed it but no event ever reached us; the next shutdown pass notices.
        tp.order.current_state = OrderState.CANCELED

        async def _no_sleep(_delay):
            return None

        executor._sleep = _no_sleep
        await executor.control_shutdown_process()

        self.assertEqual(self.strategy.sell.call_count, 1)
        self.assertIsNotNone(executor._close_order)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_waits_for_the_take_profit_cancel(self, mock_price, rules_mock):
        """
        Live, the chase went out one millisecond after the cancel was acknowledged and the
        venue still refused it: two reduce-only orders for one position. The retry a second
        later crossed the spread and paid taker — the exact fee the chase exists to avoid.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()

        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()

        self.assertTrue(executor._stop_loss_triggered)
        self.assertIn("OID-SELL-1", [c.kwargs["order_id"] for c in self.strategy.cancel.call_args_list])
        self.strategy.sell.assert_not_called()
        self.assertIsNone(executor._sl_chase_order)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_chase_goes_in_once_the_venue_has_settled(self, mock_price, rules_mock):
        """Not on the next tick: a second of standing still is what turned it into a taker."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()
        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()

        tp.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-1"))
        executor._place_scheduled_exit()   # the settle delay elapses

        self.assertIsNotNone(executor._sl_chase_order)
        chase = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(chase["order_type"], OrderType.LIMIT)
        self.assertEqual(chase["price"], Decimal("97.91"), "one tick above the bid — maker")

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_nothing_resting_means_the_chase_goes_straight_in(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(
            self.config(barriers=SimpleGridBarriers(
                take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.strategy.sell.reset_mock()

        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()

        self.assertIsNotNone(executor._sl_chase_order)
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.91"))

    # ------------------------------------------------------------------ the cancel settle backoff

    def stop_out_with_a_resting_take_profit(self, executor, mock_price):
        """Trigger the stop while the take profit is still in the book, and cancel it."""
        tp = self.track(executor, "_take_profit_order", "OID-SELL-1", TradeType.SELL, price="105")
        self.strategy.sell.reset_mock()
        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_barriers()
        tp.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-1"))
        return tp

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_exit_waits_out_the_settle_delay_before_it_is_sent(self, mock_price, rules_mock):
        """
        CoinDCX confirms a cancel before it frees the collateral behind it, so an exit sent on
        the confirmation is still refused. Waiting deliberately beats being rejected.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.25,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)

        self.stop_out_with_a_resting_take_profit(executor, mock_price)

        self.strategy.sell.assert_not_called()
        self.assertTrue(executor._exit_placement_blocked())
        self.assertEqual(executor._exit_blocked_until, START_TS + 0.25)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_exit_goes_in_once_the_delay_has_passed(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.25,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)

        executor._place_scheduled_exit()

        self.assertIsNotNone(executor._sl_chase_order)
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.91"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_refusal_doubles_the_wait(self, mock_price, rules_mock):
        """Six of these in one live run. Retrying at the same cadence just repeats them."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.25, exit_retry_max_delay=2.0,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)
        executor._place_scheduled_exit()
        chase_id = executor._sl_chase_order.order_id

        executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
            timestamp=START_TS, order_id=chase_id, order_type=OrderType.LIMIT))
        self.assertEqual(executor._exit_retry_delay, 0.5)

        executor._place_scheduled_exit()
        executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
            timestamp=START_TS, order_id=executor._sl_chase_order.order_id,
            order_type=OrderType.LIMIT))
        self.assertEqual(executor._exit_retry_delay, 1.0)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_backoff_grows_even_from_a_zero_settle_delay(self, mock_price, rules_mock):
        """
        cancel_settle_delay is correctly 0 on a venue that settles cancels synchronously, and
        0 doubles to 0 for ever — so every refusal retried in the same instant and ten of them
        burned the whole retry budget inside a second. Seen on CoinEx, where a post-only exit
        is refused whenever the book moves underneath it ("Retrying in 0.00s").
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.chasing_executor(mock_price, cancel_settle_delay=0.0)
        self.assertEqual(executor._exit_retry_delay, 0.0)

        self.refuse_exit(executor, "This order can't be Maker only and has been canceled.")
        first = executor._exit_retry_delay
        executor._place_scheduled_exit()
        self.refuse_exit(executor, "This order can't be Maker only and has been canceled.")

        self.assertGreater(first, 0.0)
        self.assertGreater(executor._exit_retry_delay, first)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_backoff_is_capped(self, mock_price, rules_mock):
        """A stop that cannot place its exit is the worst state, so keep trying often."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=1.0, exit_retry_max_delay=2.0,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)
        for _ in range(5):
            executor._place_scheduled_exit()
            if executor._sl_chase_order is None:
                continue
            executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
                timestamp=START_TS, order_id=executor._sl_chase_order.order_id,
                order_type=OrderType.LIMIT))

        self.assertEqual(executor._exit_retry_delay, 2.0)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_accepted_exit_clears_the_backoff(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.25,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)
        executor._place_scheduled_exit()
        chase_id = executor._sl_chase_order.order_id
        executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
            timestamp=START_TS, order_id=chase_id, order_type=OrderType.LIMIT))
        self.assertEqual(executor._exit_retry_delay, 0.5)

        executor._place_scheduled_exit()
        executor.process_order_created_event(None, MagicMock(), SellOrderCreatedEvent(
            timestamp=START_TS, type=OrderType.LIMIT, trading_pair="BTC-USDT",
            amount=Decimal("1"), price=Decimal("97.91"),
            order_id=executor._sl_chase_order.order_id, creation_timestamp=START_TS))

        self.assertEqual(executor._exit_retry_delay, 0.25)
        self.assertFalse(executor._exit_placement_blocked())

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_control_tick_does_not_jump_the_settle_delay(self, mock_price, rules_mock):
        """The fallback path must not send the very order the delay exists to hold back."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.25,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)

        executor.control_barriers()

        self.strategy.sell.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_the_scheduled_exit_really_fires_on_its_own(self, mock_price, rules_mock):
        """
        The other tests fire the timer by hand. This one lets the real task run, so a broken
        schedule cannot hide behind them — a stop whose exit is never sent is the worst
        failure this executor has.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=0.01,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)

        self.assertIsNotNone(executor._exit_retry_task, "a real loop should have scheduled it")
        self.strategy.sell.assert_not_called()

        await executor._exit_retry_task

        self.assertIsNotNone(executor._sl_chase_order)
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.91"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_stopping_cancels_a_pending_retry(self, mock_price, rules_mock):
        """A timer that outlives its executor would place an order nobody is tracking."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(
            cancel_settle_delay=5.0,
            barriers=SimpleGridBarriers(take_profit=Decimal("0.05"), stop_loss=Decimal("0.02"),
                                        stop_loss_max_drift_pct=Decimal("0.05"))))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)
        task = executor._exit_retry_task
        self.assertIsNotNone(task)

        executor.stop()
        await asyncio.sleep(0)

        self.assertTrue(task.cancelled() or task.done())

    # ------------------------------------------------------------------ cancels that were not

    def cancelled_entry(self, executor, order_id="OID-BUY-1", price="99"):
        """Place an entry and have the venue confirm a cancel for it."""
        tracked = self.track(executor, "entry", order_id, TradeType.BUY, price=price)
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event(order_id))
        return tracked

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_confirmed_cancel_frees_the_slot_but_keeps_watching(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())

        tracked = self.cancelled_entry(executor)

        self.assertIsNone(executor._entry_orders[TradeType.BUY], "the slot is free to re-quote")
        self.assertEqual([o for _, o, _ in executor._cancelled_entries], [tracked])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_order_that_fills_after_a_confirmed_cancel_is_adopted(self, mock_price, rules_mock):
        """
        Live on 2026-08-26: CoinDCX confirmed a cancel and filled the same order 30s later.
        The executor had let go, so the fill matched nothing, no exits were armed, and the
        shutdown flatten saw a position of zero while a real one sat on the venue.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        tracked = self.cancelled_entry(executor)

        self.fill(tracked.order, "1", "99")
        executor._reap_cancelled_entries()

        self.assertEqual(executor.side, TradeType.BUY)
        self.assertEqual(executor.open_filled_amount, Decimal("1"))
        self.assertEqual(executor.entry_price, Decimal("99"))
        self.assertEqual(executor._cancelled_entries, [])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_adopted_fill_gets_its_exits_armed(self, mock_price, rules_mock):
        """The whole point: a position nobody is watching is the dangerous state."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        tracked = self.cancelled_entry(executor)
        self.fill(tracked.order, "1", "99")
        executor._reap_cancelled_entries()

        executor.control_barriers()

        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("103.95"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_adopted_fill_is_flattened_by_early_stop(self, mock_price, rules_mock):
        """This is the step that would have saved the manual cleanup."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        tracked = self.cancelled_entry(executor)
        self.fill(tracked.order, "1", "99")
        executor._reap_cancelled_entries()
        self.strategy.sell.reset_mock()

        executor.early_stop()

        self.assertEqual(executor.amount_to_close, Decimal("1"))
        close = self.order_args(self.strategy.sell.call_args)
        self.assertEqual(close["order_type"], OrderType.LIMIT)
        self.assertEqual(close["amount"], Decimal("1"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    async def test_a_late_fill_during_shutdown_is_still_caught(self, mock_price, rules_mock):
        """The leg had already given up when the ghost filled, so the watch has to outlive it."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        tracked = self.cancelled_entry(executor)
        executor.close_type = CloseType.EXPIRED
        executor._status = RunnableStatus.SHUTTING_DOWN

        async def _no_sleep(_delay):
            return None

        executor._sleep = _no_sleep
        self.fill(tracked.order, "1", "99")
        await executor.control_task()

        self.assertEqual(executor.open_filled_amount, Decimal("1"))
        self.assertEqual(executor.amount_to_close, Decimal("1"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_cancelled_entry_is_forgotten_once_the_watch_expires(self, mock_price, rules_mock):
        """Watching for ever would keep dead orders in the fee and id lists indefinitely."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(cancelled_entry_watch_seconds=60))
        self.cancelled_entry(executor)

        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 61)
        executor._reap_cancelled_entries()

        self.assertEqual(executor._cancelled_entries, [])
        self.assertEqual(executor.open_filled_amount, Decimal("0"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_position_sums_a_replacement_and_an_adopted_ghost(self, mock_price, rules_mock):
        """
        If the replacement filled too, we hold both. Reporting only one would leave half a
        position unhedged and half of it unclosed at shutdown.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        ghost = self.cancelled_entry(executor, order_id="OID-BUY-1", price="99")
        self.track(executor, "entry", "OID-BUY-2", TradeType.BUY, price="98",
                   filled="1", fill_price="98")
        executor._detect_entry_fill()

        self.fill(ghost.order, "1", "99")
        executor._reap_cancelled_entries()

        self.assertEqual(executor.open_filled_amount, Decimal("2"))
        self.assertEqual(executor.entry_price, Decimal("98.5"))

    # ------------------------------------------------------------------ exits that were not cancelled

    def venue_connector(self, amount=None, pair="BTC-USDT"):
        """
        A connector that also answers account_positions.

        The spec'd mock in setUp deliberately does not — that stands in for a venue we cannot
        read, which callers must treat as "might be holding something".
        """
        connector = MagicMock()
        connector.quantize_order_price.side_effect = lambda trading_pair, price: price
        connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
        connector.supported_order_types.return_value = [OrderType.LIMIT, OrderType.MARKET]
        connector.account_positions = {}
        if amount is not None:
            connector.account_positions = {pair: Position(
                trading_pair=pair, position_side=PositionSide.LONG,
                unrealized_pnl=Decimal("0"), entry_price=Decimal("99"),
                amount=Decimal(amount), leverage=Decimal("1"))}
        self.strategy.connectors["coindcx_perpetual"] = connector
        return connector

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_cancelled_exit_that_fills_later_still_counts(self, mock_price, rules_mock):
        """
        Live on 2026-08-26: CoinDCX filled the chasing exit and confirmed its cancel in the
        same instant. Because the order showed no fills at that moment it was thrown away, so
        the fill reached nothing and the executor kept trying to close a flat position.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        chase = self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")

        chase.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-2"))
        self.assertEqual(executor.close_filled_amount, Decimal("0"))

        # ...and the fill lands a heartbeat after the cancel was confirmed.
        self.fill(chase.order, "1", "98.1")

        self.assertEqual(executor.close_filled_amount, Decimal("1"))
        self.assertEqual(executor.amount_to_close, Decimal("0"))
        self.assertEqual(executor.close_price, Decimal("98.1"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_late_exit_fill_ends_the_retries(self, mock_price, rules_mock):
        """Twelve rejections in a row, because we did not know we had already closed."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        chase = self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")
        chase.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-2"))
        self.assertFalse(executor.open_and_close_volume_match())

        self.fill(chase.order, "1", "98.1")

        self.assertTrue(executor.open_and_close_volume_match())

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_unfilled_cancelled_exit_changes_nothing(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        self.open_long(executor)
        chase = self.track(executor, "_sl_chase_order", "OID-SELL-2", TradeType.SELL, price="98.1")
        chase.order.current_state = OrderState.CANCELED

        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-SELL-2"))

        self.assertEqual(executor.close_filled_amount, Decimal("0"))
        self.assertEqual(executor.amount_to_close, Decimal("1"))

    # ------------------------------------------------------------------ the warning must not cry wolf

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_still_open_warning_defers_to_the_venue(self, mock_price, rules_mock):
        """
        It told the operator to go and flatten a position that did not exist. That spends the
        credibility of the one message that has to be believed.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount=None)   # the exchange says we are flat
        executor = self.running_executor(self.config())
        self.open_long(executor)

        with patch.object(executor.logger(), "error") as error:
            executor.stop()

        error.assert_not_called()

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_still_open_warning_fires_when_the_venue_agrees(self, mock_price, rules_mock):
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount="1")    # the exchange confirms the position
        executor = self.running_executor(self.config())
        self.open_long(executor)

        with patch.object(executor.logger(), "error") as error:
            executor.stop()

        self.assertEqual(error.call_count, 1)
        self.assertIn("STILL OPEN", error.call_args.args[0])

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_still_open_warning_fires_when_the_venue_cannot_be_read(self, mock_price, rules_mock):
        """Silence about a position that might be open is the wrong way to be wrong."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())   # spec'd mock: no account_positions
        self.open_long(executor)

        with patch.object(executor.logger(), "error") as error:
            executor.stop()

        self.assertEqual(error.call_count, 1)
        self.assertIn("STILL OPEN", error.call_args.args[0])

    # ------------------------------------------------------------------ collateral refusals

    INSUFFICIENT = ('Error executing request POST .../orders/create. HTTP status is 400. '
                    'Error: {"code":400,"message":"Insufficient funds","status":"error"}')

    def refuse_exit(self, executor, message=None):
        """Have the venue reject whichever exit is currently in flight."""
        order_id = (executor._sl_chase_order or executor._close_order).order_id
        executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
            timestamp=START_TS, order_id=order_id, order_type=OrderType.LIMIT,
            error_message=message))
        return order_id

    def chasing_executor(self, mock_price, **overrides):
        params = dict(cancel_settle_delay=0.25, exit_retry_max_delay=2.0,
                      barriers=SimpleGridBarriers(take_profit=Decimal("0.05"),
                                                  stop_loss=Decimal("0.02"),
                                                  stop_loss_max_drift_pct=Decimal("0.05")))
        params.update(overrides)
        executor = self.running_executor(self.config(**params))
        self.open_long(executor)
        self.stop_out_with_a_resting_take_profit(executor, mock_price)
        executor._place_scheduled_exit()
        return executor

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_one_collateral_refusal_is_still_treated_as_the_settle_race(self, mock_price, rules_mock):
        """
        cancel_settle_delay exists because the venue frees collateral a moment after it
        confirms the cancel, so the first refusal is genuinely ambiguous. Standing down on it
        would slow every ordinary stop loss down to catch the rare pathological one.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.chasing_executor(mock_price)

        self.refuse_exit(executor, self.INSUFFICIENT)

        self.assertEqual(executor._exit_retry_delay, 0.5)   # the ordinary doubling backoff
        self.assertEqual(executor._consecutive_collateral_refusals, 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_second_collateral_refusal_waits_instead_of_replacing(self, mock_price, rules_mock):
        """
        Live on 2026-09-02: eleven replacements in eight seconds, every one refused, because
        the margin was held by the exit we had just been told was cancelled. Sending them
        faster cannot work — the obstacle is the replacement's own predecessor.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.chasing_executor(mock_price, collateral_refusal_wait=3.0)

        self.refuse_exit(executor, self.INSUFFICIENT)
        executor._place_scheduled_exit()
        self.refuse_exit(executor, self.INSUFFICIENT)

        self.assertEqual(executor._consecutive_collateral_refusals, 2)
        self.assertEqual(executor._exit_blocked_until, START_TS + 3.0)
        self.assertTrue(executor._exit_placement_blocked())

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_refusal_on_the_merits_never_takes_the_collateral_path(self, mock_price, rules_mock):
        """A rejected price is our problem to fix, and re-placing is the way to fix it."""
        rules_mock.return_value = self.trading_rules()
        executor = self.chasing_executor(mock_price)

        self.refuse_exit(executor, "Cannot place reduce only order for given price")
        executor._place_scheduled_exit()
        self.refuse_exit(executor, "Cannot place reduce only order for given price")

        self.assertEqual(executor._consecutive_collateral_refusals, 0)
        self.assertEqual(executor._exit_retry_delay, 1.0)   # still doubling

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_an_accepted_exit_clears_the_collateral_count(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.chasing_executor(mock_price)
        self.refuse_exit(executor, self.INSUFFICIENT)
        self.assertEqual(executor._consecutive_collateral_refusals, 1)

        executor._reset_exit_backoff()

        self.assertEqual(executor._consecutive_collateral_refusals, 0)

    # ------------------------------------------------------------------ nothing left to close

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_flat_venue_after_a_refusal_settles_the_leg(self, mock_price, rules_mock):
        """
        The position closed on the order we were told had been cancelled. Every reduce-only
        order after that is refused by definition, so there is nothing to gain by sending
        another twenty times to reach a conclusion the position feed already shows.
        """
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount=None)   # the exchange says we are flat
        executor = self.chasing_executor(mock_price)
        executor.close_type = CloseType.STOP_LOSS
        self.refuse_exit(executor, self.INSUFFICIENT)

        settled = executor._settle_as_closed_if_venue_is_flat()

        self.assertTrue(settled)
        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor._status, RunnableStatus.TERMINATED)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_flat_venue_is_ignored_until_an_exit_has_been_refused(self, mock_price, rules_mock):
        """
        The safety property of the check above, and the reason it is armed by a refusal.

        A position that has just filled takes a moment to appear in the position feed. Reading
        that lag as "already closed" would skip the exit on a position we really do hold — and
        the stop loss only exists inside this process, so nothing else would ever close it.
        """
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount=None)   # feed has not caught up with our fill yet
        executor = self.chasing_executor(mock_price)
        executor.close_type = CloseType.STOP_LOSS

        self.assertFalse(executor._venue_confirms_the_position_is_gone())
        self.assertFalse(executor._settle_as_closed_if_venue_is_flat())
        self.assertNotEqual(executor._status, RunnableStatus.TERMINATED)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_venue_that_still_holds_the_position_is_never_settled(self, mock_price, rules_mock):
        """Refusals or not, a real position has to be closed."""
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount="1")    # the exchange confirms the position
        executor = self.chasing_executor(mock_price)
        executor.close_type = CloseType.STOP_LOSS
        self.refuse_exit(executor, self.INSUFFICIENT)

        self.assertFalse(executor._settle_as_closed_if_venue_is_flat())
        self.assertNotEqual(executor._status, RunnableStatus.TERMINATED)

    # ------------------------------------------------------------------ exits that ran out of retries

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_rejected_exits_against_a_flat_venue_keep_their_close_type(self, mock_price, rules_mock):
        """
        Live on 2026-09-02: the chase was cancelled, the cancel was confirmed, and the order
        filled as a maker five seconds later regardless. Every replacement in between was
        refused, because the venue will not let us double-close a position already on its way
        out — so a run of rejections ending with the venue flat means the exit HAPPENED.

        Calling that FAILED halts the run, sends the operator after a position that is not
        there, and drops a real stop loss out of both the win rate and the drawdown, because
        the controller reads FAILED as a leg that never traded.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount=None)   # the exchange says we are flat
        executor = self.running_executor(self.config())
        self.open_long(executor)
        executor.close_type = CloseType.STOP_LOSS
        executor._current_retries = executor._max_retries + 1

        executor.evaluate_max_retries()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_rejected_exits_while_the_venue_still_holds_it_are_a_failure(self, mock_price, rules_mock):
        """The position is real and nothing is going to close it. That is what FAILED is for."""
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount="1")    # the exchange confirms the position
        executor = self.running_executor(self.config())
        self.open_long(executor)
        executor.close_type = CloseType.STOP_LOSS
        executor._current_retries = executor._max_retries + 1

        executor.evaluate_max_retries()

        self.assertEqual(executor.close_type, CloseType.FAILED)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_rejected_exits_against_an_unreadable_venue_are_a_failure(self, mock_price, rules_mock):
        """
        A venue we cannot read is not evidence of anything, so it cannot be the reason to
        downgrade a failure. Only an explicit "no position" earns that.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())   # spec'd mock: no account_positions
        self.open_long(executor)
        executor.close_type = CloseType.STOP_LOSS
        executor._current_retries = executor._max_retries + 1

        executor.evaluate_max_retries()

        self.assertEqual(executor.close_type, CloseType.FAILED)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_leg_that_never_opened_still_fails_on_exhausted_retries(self, mock_price, rules_mock):
        """
        No fill means there is nothing for a flat venue to corroborate. An entry the venue
        kept refusing is exactly the case FAILED and the halt streak exist to catch.
        """
        mock_price.side_effect = self.price_feed()
        rules_mock.return_value = self.trading_rules()
        self.venue_connector(amount=None)
        executor = self.running_executor(self.config())
        executor.close_type = CloseType.STOP_LOSS
        executor._current_retries = executor._max_retries + 1

        executor.evaluate_max_retries()

        self.assertEqual(executor.close_type, CloseType.FAILED)

    # ------------------------------------------------------------------ the entry trigger

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_resting_entry_does_not_follow_the_market(self, mock_price, rules_mock):
        """
        It is a grid level, not a quote. Chasing the touch is what let a stale book fill an
        entry as a taker; a price a full step away simply cannot cross.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101", mid="100")
        executor.control_entry_orders()
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="95")

        mock_price.side_effect = self.price_feed(best_bid="97", best_ask="99", mid="98")
        executor.control_entry_orders()

        self.strategy.cancel.assert_not_called()
        self.assertEqual(self.strategy.buy.call_count, 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_trigger_opens_on_the_wrong_side_when_the_market_gets_there_first(
            self, mock_price, rules_mock):
        """
        Without this, a grid the market walks away from never trades again — the resting
        order sits a step below a price that is never coming back.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101", mid="100")
        executor.control_entry_orders()
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="95")
        self.strategy.buy.reset_mock()

        # reference 100, stop loss step 2% -> the trigger sits at 102.00
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()

        # The resting order goes first and the trigger waits on its collateral, so nothing is
        # bought on this pass. See test_the_trigger_waits_for_the_cancelled_order_s_margin.
        self.assertEqual(self.strategy.cancel.call_args.kwargs["order_id"], "OID-BUY-1")
        self.assertEqual(executor._entry_trigger_pending, TradeType.BUY)
        self.strategy.buy.assert_not_called()

        executor._entry_orders[TradeType.BUY] = None
        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 1)
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()

        self.assertTrue(executor._entry_triggered)
        # crossing: 20 ticks of 0.01 through the ask
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("102.30"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_trigger_waits_for_the_cancelled_order_s_margin(self, mock_price, rules_mock):
        """
        Live on 2026-09-02: the trigger fired, the resting order was cancelled, and the
        crossing order went out in the same breath. CoinDCX was still holding 660 INR against
        the order it had just agreed to cancel, so it refused the trigger for want of funds and
        the leg opened nothing at all.

        The wait is measured from the CONFIRMATION, not from the request: the venue frees the
        collateral a beat after it says the order is gone.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(cancel_settle_delay=0.5))
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101", mid="100")
        executor.control_entry_orders()
        resting = self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="95")
        self.strategy.buy.reset_mock()

        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()
        self.strategy.buy.assert_not_called()

        # The cancel confirms; the collateral is released a moment after this, not on it.
        resting.order.current_state = OrderState.CANCELED
        executor.process_order_canceled_event(None, MagicMock(), self.cancel_event("OID-BUY-1"))
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()
        self.strategy.buy.assert_not_called()

        type(self.strategy).current_timestamp = PropertyMock(return_value=START_TS + 0.5)
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()

        self.assertTrue(executor._entry_triggered)
        self.assertIsNone(executor._entry_trigger_pending)
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("102.30"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_refused_trigger_can_be_taken_again(self, mock_price, rules_mock):
        """
        The latch marks a trigger we have TAKEN, not one we have merely reached.

        Setting it on the attempt meant a refused trigger order looked like a filled one:
        _entry_trigger_reached never fired again, so the leg dropped back to resting a full
        step below a market that had just moved up, and sat there until it timed out. That is
        what the 17:44 leg on 2026-09-02 did with the rest of its two minutes.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())

        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()
        self.assertTrue(executor._entry_triggered)
        trigger_id = executor._entry_orders[TradeType.BUY].order_id

        executor.process_order_failed_event(None, MagicMock(), MarketOrderFailureEvent(
            timestamp=START_TS, order_id=trigger_id, order_type=OrderType.LIMIT,
            error_message=self.INSUFFICIENT))

        self.assertFalse(executor._entry_triggered)

        # The market is still past the trigger, so the next pass takes it again rather than
        # resting a step the wrong way.
        self.strategy.buy.reset_mock()
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()

        self.assertTrue(executor._entry_triggered)
        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("102.30"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_short_chain_triggers_below_its_reference(self, mock_price, rules_mock):
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_mode=SimpleGridEntryMode.SHORT_ONLY))

        # reference 100, stop loss step 2% -> the trigger sits at 98.00
        mock_price.side_effect = self.price_feed(best_bid="97.9", best_ask="98.1", mid="98")
        executor.control_entry_orders()

        self.assertTrue(executor._entry_triggered)
        self.assertEqual(self.order_args(self.strategy.sell.call_args)["price"], Decimal("97.70"))

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_trigger_only_fires_once(self, mock_price, rules_mock):
        """A second crossing order would open a position we never wanted twice."""
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")

        executor.control_entry_orders()
        executor.control_entry_orders()
        executor.control_entry_orders()

        self.assertEqual(self.strategy.buy.call_count, 1)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_the_first_leg_rests_at_the_touch_and_has_no_trigger(self, mock_price, rules_mock):
        """There is no previous exit to measure a step from, so there is nothing to trigger."""
        mock_price.side_effect = self.price_feed(best_bid="99", best_ask="101", mid="100")
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config(entry_reference_price=None))

        executor.control_entry_orders()

        self.assertEqual(self.order_args(self.strategy.buy.call_args)["price"], Decimal("99"))
        self.assertIsNone(executor._entry_trigger_price(TradeType.BUY))
        self.assertFalse(executor._entry_triggered)

    @patch.object(SimpleGridExecutor, "get_trading_rules")
    @patch.object(SimpleGridExecutor, "get_price")
    def test_a_triggered_entry_still_brackets_from_its_own_fill(self, mock_price, rules_mock):
        """
        The whole point of measuring from the fill: opening at the trigger is already the bad
        outcome, and it should not also start the next position with a broken bracket.
        """
        rules_mock.return_value = self.trading_rules()
        executor = self.running_executor(self.config())
        mock_price.side_effect = self.price_feed(best_bid="101.9", best_ask="102.1", mid="102")
        executor.control_entry_orders()
        self.track(executor, "entry", "OID-BUY-1", TradeType.BUY, price="102.30",
                   filled="1", fill_price="102.30")
        executor._detect_entry_fill()

        self.assertEqual(executor.entry_price, Decimal("102.30"))
        self.assertEqual(executor.take_profit_price, Decimal("107.4150"))
        self.assertEqual(executor.stop_loss_price, Decimal("100.2540"))
