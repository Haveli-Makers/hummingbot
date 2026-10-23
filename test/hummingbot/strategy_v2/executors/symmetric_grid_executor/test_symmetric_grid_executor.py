from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, PropertyMock, patch

from controllers.generic.symmetric_grid import SymmetricGrid, SymmetricGridConfig
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent, OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.executors.symmetric_grid_executor.data_types import (
    SymmetricGridExecutorConfig,
    SymmetricGridLevel,
    SymmetricGridOrderState,
)
from hummingbot.strategy_v2.executors.symmetric_grid_executor.symmetric_grid_executor import SymmetricGridExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestSymmetricGridExecutor(IsolatedAsyncioWrapperTestCase):
    """Unit tests for SymmetricGridExecutor covering order placement, refresh on price change,
    fill tracking / PnL, failure cooldown, and insufficient-funds disabling."""

    def setUp(self) -> None:
        super().setUp()
        self.strategy = self._create_mock_strategy()

    # ── helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _create_mock_strategy():
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).current_timestamp = PropertyMock(return_value=1_000_000)
        strategy.buy.side_effect = [f"OID-BUY-{i}" for i in range(1, 30)]
        strategy.sell.side_effect = [f"OID-SELL-{i}" for i in range(1, 30)]
        strategy.cancel.return_value = None

        connector = MagicMock(spec=ExchangePyBase)
        connector.get_price_by_type.return_value = Decimal("100")
        trading_rule = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        type(connector).trading_rules = PropertyMock(return_value={"ETH-USDT": trading_rule})
        connector.budget_checker.adjust_candidates.side_effect = lambda c: c
        connector.get_available_balance.return_value = Decimal("10000")
        connector._order_tracker = MagicMock()
        connector._order_tracker.fetch_order.return_value = None

        strategy.connectors = {"binance": connector}
        return strategy

    def _default_config(self, **overrides) -> SymmetricGridExecutorConfig:
        defaults = dict(
            id="test-pmm",
            timestamp=1_000_000,
            connector_name="binance",
            trading_pair="ETH-USDT",
            spread_percentages=[Decimal("0.01"), Decimal("0.02")],
            order_amounts_quote=[Decimal("500"), Decimal("500")],
            min_order_amount_quote=Decimal("1"),
        )
        defaults.update(overrides)
        return SymmetricGridExecutorConfig(**defaults)

    def _make_executor(self, config=None, **config_overrides) -> SymmetricGridExecutor:
        cfg = config or self._default_config(**config_overrides)
        executor = SymmetricGridExecutor(strategy=self.strategy, config=cfg, update_interval=0.5)
        return executor

    def _set_executor_running(self, executor: SymmetricGridExecutor):
        executor._status = RunnableStatus.RUNNING

    def _make_filled_in_flight_order(self, order_id, side, price, amount_base, amount_quote, fee="0.1"):
        order = InFlightOrder(
            client_order_id=order_id,
            exchange_order_id=f"E-{order_id}",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=side,
            amount=amount_base,
            price=price,
            creation_timestamp=1_000_000,
            initial_state=OrderState.COMPLETED,
        )
        order.update_with_trade_update(
            TradeUpdate(
                trade_id=f"T-{order_id}",
                client_order_id=order_id,
                exchange_order_id=f"E-{order_id}",
                trading_pair="ETH-USDT",
                fill_price=price,
                fill_base_amount=amount_base,
                fill_quote_amount=amount_quote,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal(fee))]),
                fill_timestamp=1_000_001,
            )
        )
        return order

    # ── Level Generation ─────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_level_generation(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self.assertEqual(len(executor.grid_levels), 2)
        self.assertEqual(executor.grid_levels[0].spread_pct, Decimal("0.01"))
        self.assertEqual(executor.grid_levels[1].spread_pct, Decimal("0.02"))
        # buy = fair * (1-spread), sell = fair * (1+spread)
        self.assertEqual(executor.grid_levels[0].get_buy_price(Decimal("100")), Decimal("99"))
        self.assertEqual(executor.grid_levels[0].get_sell_price(Decimal("100")), Decimal("101"))

    # ── Initial Order Placement ──────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_manage_orders_places_buy_and_sell(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()

        for level in executor.grid_levels:
            self.assertIsNotNone(level.buy_order, f"Level {level.id} should have buy order")
            self.assertIsNotNone(level.sell_order, f"Level {level.id} should have sell order")

        self.assertEqual(executor.grid_levels[0].buy_order.order_id, "OID-BUY-1")
        self.assertEqual(executor.grid_levels[0].sell_order.order_id, "OID-SELL-1")
        self.assertEqual(executor.grid_levels[1].buy_order.order_id, "OID-BUY-2")
        self.assertEqual(executor.grid_levels[1].sell_order.order_id, "OID-SELL-2")

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_manage_orders_respects_order_frequency(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(order_frequency=10)
        self._set_executor_running(executor)

        executor.manage_orders()
        self.assertEqual(executor.max_order_creation_timestamp, 1_000_000)

        for level in executor.grid_levels:
            level.reset_level()
        executor.manage_orders()
        for level in executor.grid_levels:
            self.assertIsNone(level.buy_order)
            self.assertIsNone(level.sell_order)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_manage_orders_respects_max_batch(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(max_orders_per_batch=1)
        self._set_executor_running(executor)

        executor.manage_orders()
        total_orders = sum(
            (1 if lv.buy_order else 0) + (1 if lv.sell_order else 0)
            for lv in executor.grid_levels
        )
        self.assertEqual(total_orders, 1)

    # ── Pending Side Logic ───────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_pending_side_buy_only(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)
        executor.grid_levels[0].pending_side = "buy"
        executor.grid_levels[1].pending_side = "buy"

        executor.manage_orders()
        self.assertIsNotNone(executor.grid_levels[0].buy_order)
        self.assertIsNone(executor.grid_levels[0].sell_order)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_pending_side_sell_only(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)
        executor.grid_levels[0].pending_side = "sell"
        executor.grid_levels[1].pending_side = "sell"

        executor.manage_orders()
        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIsNotNone(executor.grid_levels[0].sell_order)

    # ── Fill Processing ──────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_process_filled_buy_waits_for_sell(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        level = executor.grid_levels[0]
        tracked = TrackedOrder(order_id="OID-BUY-FILL")
        tracked.order = self._make_filled_in_flight_order(
            "OID-BUY-FILL", TradeType.BUY, Decimal("99"), Decimal("5"), Decimal("495"),
        )
        level.buy_order = tracked
        level.sell_order = None
        level.pending_side = "buy"

        executor.process_filled_orders()

        self.assertIsNone(level.buy_order)
        self.assertEqual(level.pending_side, "sell")
        self.assertEqual(len(executor._filled_orders), 1)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_process_filled_sell_waits_for_buy(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        level = executor.grid_levels[0]
        tracked = TrackedOrder(order_id="OID-SELL-FILL")
        tracked.order = self._make_filled_in_flight_order(
            "OID-SELL-FILL", TradeType.SELL, Decimal("101"), Decimal("5"), Decimal("505"),
        )
        level.sell_order = tracked
        level.buy_order = None
        level.pending_side = "sell"

        executor.process_filled_orders()

        self.assertIsNone(level.sell_order)
        self.assertEqual(level.pending_side, "buy")
        self.assertEqual(len(executor._filled_orders), 1)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_fill_clears_insufficient_funds_flags(self, _mock_price, mock_rules):
        """When a buy fills, sell insufficient-funds flags should be cleared (and vice-versa)."""
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor._level_insufficient_funds["L0_sell"] = True
        executor._level_insufficient_funds["L1_sell"] = True

        level = executor.grid_levels[0]
        tracked = TrackedOrder(order_id="OID-BUY-FILL")
        tracked.order = self._make_filled_in_flight_order(
            "OID-BUY-FILL", TradeType.BUY, Decimal("99"), Decimal("5"), Decimal("495"),
        )
        level.buy_order = tracked
        level.pending_side = "buy"

        executor.process_filled_orders()

        self.assertNotIn("L0_sell", executor._level_insufficient_funds)
        self.assertNotIn("L1_sell", executor._level_insufficient_funds)

    # ── PnL Metrics ──────────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_realized_pnl_after_buy_and_sell(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()

        executor._filled_orders = [
            {"trade_type": "BUY", "executed_amount_base": "5", "executed_amount_quote": "495", "cumulative_fee_paid_quote": "0.5"},
            {"trade_type": "SELL", "executed_amount_base": "5", "executed_amount_quote": "505", "cumulative_fee_paid_quote": "0.5"},
        ]
        executor.mid_price = Decimal("100")
        executor.update_realized_pnl_metrics()

        self.assertEqual(executor.realized_pnl_quote, Decimal("9"))
        self.assertEqual(executor.net_inventory_base, Decimal("0"))
        self.assertEqual(executor.unrealized_pnl_quote, Decimal("0"))

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_unrealized_pnl_with_net_long(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()

        executor._filled_orders = [
            {"trade_type": "BUY", "executed_amount_base": "10", "executed_amount_quote": "495", "cumulative_fee_paid_quote": "0"},
        ]
        executor.mid_price = Decimal("55")
        executor.update_realized_pnl_metrics()

        self.assertEqual(executor.unrealized_pnl_quote, Decimal("55"))

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_no_fills_resets_metrics(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        executor.realized_pnl_quote = Decimal("42")  # dirty state
        executor._filled_orders = []
        executor.update_realized_pnl_metrics()
        self.assertEqual(executor.realized_pnl_quote, Decimal("0"))

    # ── Refresh on Price Change ──────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_orders_updates_fair_price(self, mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.005"))
        self._set_executor_running(executor)

        executor.fair_price = Decimal("100")
        executor._last_refresh_timestamp = 0

        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal("101")):
            executor.refresh_orders_on_price_change()

        self.assertEqual(executor.fair_price, Decimal("101"))

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_no_action_within_tolerance(self, mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.01"))
        self._set_executor_running(executor)
        executor.fair_price = Decimal("100")

        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal("100.5")):
            executor.refresh_orders_on_price_change()

        self.assertEqual(executor.fair_price, Decimal("100"))

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_rejects_invalid_fair_price(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.001"))
        self._set_executor_running(executor)
        executor.fair_price = Decimal("100")

        for invalid_price in [Decimal("NaN"), Decimal("0"), Decimal("-1")]:
            with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=invalid_price):
                executor.refresh_orders_on_price_change()

            self.assertEqual(executor.fair_price, Decimal("100"))
            self.strategy.cancel.assert_not_called()

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_rejects_large_fair_price_jump(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.001"))
        self._set_executor_running(executor)
        executor.fair_price = Decimal("100")
        executor._last_refresh_timestamp = 0

        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal("151")):
            executor.refresh_orders_on_price_change()

        self.assertEqual(executor.fair_price, Decimal("100"))
        self.strategy.cancel.assert_not_called()

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_cooldown_prevents_rapid_refresh(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.001"))
        self._set_executor_running(executor)
        executor.fair_price = Decimal("100")
        executor._last_refresh_timestamp = 999_999.5

        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal("102")):
            executor.refresh_orders_on_price_change()

        self.assertEqual(executor.fair_price, Decimal("100"))

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_refresh_queues_placed_levels(self, mock_price, mock_rules):
        """When price moves beyond tolerance and levels have ORDER_PLACED state, they are queued for refresh."""
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(price_refresh_tolerance=Decimal("0.005"))
        self._set_executor_running(executor)

        executor.manage_orders()
        executor.fair_price = Decimal("100")
        executor._last_refresh_timestamp = 0

        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal("102")):
            executor.refresh_orders_on_price_change()

        self.assertEqual(executor.fair_price, Decimal("102"))
        self.assertTrue(self.strategy.cancel.called)

    # ── Failure Cooldown ─────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_level_failure_cooldown(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        level_key = "L0_buy"
        for _ in range(executor._max_level_failures):
            executor._record_level_failure(level_key)

        self.assertTrue(executor._is_level_on_cooldown(level_key))

        type(self.strategy).current_timestamp = PropertyMock(
            return_value=1_000_000 + executor._level_failure_cooldown + 1
        )
        self.assertFalse(executor._is_level_on_cooldown(level_key))
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_000)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_level_on_cooldown_skips_order_placement(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        for _ in range(executor._max_level_failures):
            executor._record_level_failure("L0_buy")

        executor.manage_orders()

        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIsNotNone(executor.grid_levels[0].sell_order)
        self.assertIsNotNone(executor.grid_levels[1].buy_order)
        self.assertIsNotNone(executor.grid_levels[1].sell_order)

    # ── Insufficient Funds Disabling ─────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_insufficient_funds_disables_level(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor._level_insufficient_funds["L0_sell"] = self.strategy.current_timestamp

        self.assertTrue(executor._is_level_on_cooldown("L0_sell"))

        executor.manage_orders()
        self.assertIsNone(executor.grid_levels[0].sell_order)
        self.assertIsNotNone(executor.grid_levels[0].buy_order)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_insufficient_funds_cooldown_handles_zero_timestamp(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1)

        executor._level_insufficient_funds["L0_sell"] = 0

        self.assertTrue(executor._is_level_on_cooldown("L0_sell"))
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_000)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_process_order_failed_insufficient_funds(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()
        buy_order_id = executor.grid_levels[0].buy_order.order_id

        event = MarketOrderFailureEvent(
            timestamp=1_000_001,
            order_id=buy_order_id,
            order_type=OrderType.LIMIT,
            error_message="Insufficient funds to place order",
        )
        executor.process_order_failed_event(None, MagicMock(), event)

        self.assertIn("L0_buy", executor._level_insufficient_funds)
        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIn(buy_order_id, executor._failed_orders)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_process_order_failed_generic(self, _mock_price, mock_rules):
        """Non-balance failures should record failure but not set insufficient_funds flag."""
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()
        sell_order_id = executor.grid_levels[0].sell_order.order_id

        event = MarketOrderFailureEvent(
            timestamp=1_000_001,
            order_id=sell_order_id,
            order_type=OrderType.LIMIT,
            error_message="Network timeout",
        )
        executor.process_order_failed_event(None, MagicMock(), event)

        self.assertNotIn("L0_sell", executor._level_insufficient_funds)
        self.assertIsNone(executor.grid_levels[0].sell_order)
        self.assertIn(sell_order_id, executor._failed_orders)

    # ── Order Canceled Event ─────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_process_order_canceled_event(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()
        buy_order_id = executor.grid_levels[0].buy_order.order_id

        event = OrderCancelledEvent(
            timestamp=1_000_001,
            order_id=buy_order_id,
        )
        executor.process_order_canceled_event(None, MagicMock(), event)

        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIn(buy_order_id, executor._canceled_orders)

    # ── Cancel All Orders ────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_cancel_all_orders(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()
        executor.cancel_all_orders()
        self.assertTrue(self.strategy.cancel.called)

    # ── Barriers (stop-loss, time-limit) ─────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_stop_loss_triggers(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(stop_loss=Decimal("0.05"))
        self._set_executor_running(executor)

        executor._filled_orders = [
            {"trade_type": "BUY", "executed_amount_base": "10", "executed_amount_quote": "1000", "cumulative_fee_paid_quote": "0"},
        ]
        executor.mid_price = Decimal("90")
        executor.update_realized_pnl_metrics()

        self.assertTrue(executor.stop_loss_condition())

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_time_limit_expiry(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(time_limit=60)
        self._set_executor_running(executor)

        self.assertEqual(executor.end_time, 1_000_060)
        self.assertFalse(executor.is_expired)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_061)
        self.assertTrue(executor.is_expired)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_000)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_no_time_limit(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self.assertIsNone(executor.end_time)
        self.assertFalse(executor.is_expired)

    # ── Check Barriers in control_task ───────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_control_task_triggers_stop_loss(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(stop_loss=Decimal("0.01"))
        self._set_executor_running(executor)

        executor._filled_orders = [
            {"trade_type": "BUY", "executed_amount_base": "10", "executed_amount_quote": "1000", "cumulative_fee_paid_quote": "0"},
        ]
        _mock_price.return_value = Decimal("50")  # bought at 100, mid now 50

        await executor.control_task()

        self.assertEqual(executor.close_type, CloseType.STOP_LOSS)
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_control_task_triggers_time_limit(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor(time_limit=60)
        self._set_executor_running(executor)

        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_061)
        await executor.control_task()

        self.assertEqual(executor.close_type, CloseType.TIME_LIMIT)
        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        type(self.strategy).current_timestamp = PropertyMock(return_value=1_000_000)

    # ── Early Stop ───────────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_early_stop(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.early_stop()

        self.assertEqual(executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(executor.close_type, CloseType.EARLY_STOP)

    # ── Max Retries ──────────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_max_retries_stops_executor(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor._current_retries = executor._max_retries + 1
        executor.evaluate_max_retries()

        self.assertEqual(executor.close_type, CloseType.FAILED)

    # ── Custom Info ──────────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_get_custom_info(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()

        info = executor.get_custom_info()
        self.assertIn("fair_price", info)
        self.assertIn("levels", info)
        self.assertEqual(len(info["levels"]), 2)
        self.assertEqual(info["levels"][0]["id"], "L0")
        self.assertEqual(info["levels"][0]["spread_pct"], float(Decimal("0.01")))

    # ── Properties ───────────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_is_trading_and_is_active(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()

        executor._status = RunnableStatus.RUNNING
        self.assertTrue(executor.is_active)

        executor._status = RunnableStatus.TERMINATED
        self.assertFalse(executor.is_active)

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_is_perpetual(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self.assertFalse(executor.is_perpetual)

    # ── Validate Sufficient Balance ──────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    @patch.object(SymmetricGridExecutor, "adjust_order_candidates")
    async def test_validate_insufficient_buy_balance_stops(self, mock_adjust, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        zero_candidate = MagicMock()
        zero_candidate.amount = Decimal("0")
        mock_adjust.return_value = [zero_candidate]

        executor = self._make_executor()
        self._set_executor_running(executor)

        await executor.validate_sufficient_balance()

        self.assertEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)

    # ── Order Created Event → Update Tracked Orders ──────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    def test_update_tracked_orders_with_order_id(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        self._set_executor_running(executor)

        executor.manage_orders()
        buy_order_id = executor.grid_levels[0].buy_order.order_id

        in_flight = InFlightOrder(
            client_order_id=buy_order_id,
            exchange_order_id="E-123",
            trading_pair="ETH-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("5"),
            price=Decimal("99"),
            creation_timestamp=1_000_000,
        )
        connector = self.strategy.connectors["binance"]
        connector._order_tracker.fetch_order.return_value = in_flight

        executor.update_tracked_orders_with_order_id(buy_order_id)
        self.assertEqual(executor.grid_levels[0].buy_order.order, in_flight)

    # ── Shutdown Process ─────────────────────────────────────────────

    @patch.object(SymmetricGridExecutor, "get_trading_rules")
    @patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
    async def test_shutdown_completes_when_no_active_orders(self, _mock_price, mock_rules):
        mock_rules.return_value = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))
        executor = self._make_executor()
        executor._status = RunnableStatus.SHUTTING_DOWN

        with patch.object(executor, "_sleep", return_value=None):
            await executor.control_shutdown_process()

        self.assertEqual(executor.status, RunnableStatus.TERMINATED)


class TestSymmetricGridLevel(IsolatedAsyncioWrapperTestCase):
    """Unit tests for the SymmetricGridLevel data type."""

    def test_buy_and_sell_prices(self):
        level = SymmetricGridLevel(id="L0", spread_pct=Decimal("0.01"), amount_quote=Decimal("500"))
        self.assertEqual(level.get_buy_price(Decimal("100")), Decimal("99"))
        self.assertEqual(level.get_sell_price(Decimal("100")), Decimal("101"))

    def test_initial_state(self):
        level = SymmetricGridLevel(id="L0", spread_pct=Decimal("0.01"), amount_quote=Decimal("500"))
        self.assertEqual(level.buy_state, SymmetricGridOrderState.NOT_ACTIVE)
        self.assertEqual(level.sell_state, SymmetricGridOrderState.NOT_ACTIVE)
        self.assertEqual(level.pending_side, "both")

    def test_reset_level(self):
        level = SymmetricGridLevel(id="L0", spread_pct=Decimal("0.01"), amount_quote=Decimal("500"))
        level.buy_order = TrackedOrder(order_id="B1")
        level.sell_order = TrackedOrder(order_id="S1")
        level.pending_side = "sell"

        level.reset_level()
        self.assertIsNone(level.buy_order)
        self.assertIsNone(level.sell_order)
        self.assertEqual(level.pending_side, "both")

    def test_order_placed_state(self):
        level = SymmetricGridLevel(id="L0", spread_pct=Decimal("0.01"), amount_quote=Decimal("500"))
        level.buy_order = TrackedOrder(order_id="B1")
        self.assertEqual(level.buy_state, SymmetricGridOrderState.ORDER_PLACED)


class TestSymmetricGridExecutorConfig(IsolatedAsyncioWrapperTestCase):
    """Validation tests for SymmetricGridExecutorConfig."""

    def test_valid_config(self):
        config = SymmetricGridExecutorConfig(
            id="test",
            timestamp=1_000_000,
            connector_name="binance",
            trading_pair="ETH-USDT",
            spread_percentages=[Decimal("0.01")],
            order_amounts_quote=[Decimal("500")],
        )
        self.assertEqual(config.type, "symmetric_grid_executor")

    def test_mismatched_lengths_raises(self):
        with self.assertRaises(ValueError):
            SymmetricGridExecutorConfig(
                id="test",
                timestamp=1_000_000,
                connector_name="binance",
                trading_pair="ETH-USDT",
                spread_percentages=[Decimal("0.01"), Decimal("0.02")],
                order_amounts_quote=[Decimal("500")],
            )

    def test_negative_spread_raises(self):
        with self.assertRaises(ValueError):
            SymmetricGridExecutorConfig(
                id="test",
                timestamp=1_000_000,
                connector_name="binance",
                trading_pair="ETH-USDT",
                spread_percentages=[Decimal("-0.01")],
                order_amounts_quote=[Decimal("500")],
            )

    def test_zero_amount_raises(self):
        with self.assertRaises(ValueError):
            SymmetricGridExecutorConfig(
                id="test",
                timestamp=1_000_000,
                connector_name="binance",
                trading_pair="ETH-USDT",
                spread_percentages=[Decimal("0.01")],
                order_amounts_quote=[Decimal("0")],
            )

    def test_spread_ge_one_raises(self):
        with self.assertRaises(ValueError):
            SymmetricGridExecutorConfig(
                id="test",
                timestamp=1_000_000,
                connector_name="binance",
                trading_pair="ETH-USDT",
                spread_percentages=[Decimal("1.0")],
                order_amounts_quote=[Decimal("500")],
            )


class TestSymmetricGridReviewFixes(IsolatedAsyncioWrapperTestCase):
    """Regression tests for PnL, refresh, fill handling, sizing and perpetual behaviour."""

    RULE = TradingRule(trading_pair="ETH-USDT", min_notional_size=Decimal("1"))

    def setUp(self) -> None:
        super().setUp()
        self.now = 1_000_000
        self.strategy = MagicMock(spec=ScriptStrategyBase)
        type(self.strategy).current_timestamp = PropertyMock(side_effect=lambda: self.now)
        self.strategy.buy.side_effect = [f"OID-BUY-{i}" for i in range(1, 50)]
        self.strategy.sell.side_effect = [f"OID-SELL-{i}" for i in range(1, 50)]
        connector = MagicMock(spec=ExchangePyBase)
        connector.get_price_by_type.return_value = Decimal("100")
        type(connector).trading_rules = PropertyMock(return_value={"ETH-USDT": self.RULE})
        connector.budget_checker.adjust_candidates.side_effect = lambda c: c
        connector.get_available_balance.return_value = Decimal("10000")
        connector._order_tracker = MagicMock()
        connector._order_tracker.fetch_order.return_value = None
        self.connector = connector
        self.strategy.connectors = {"binance": connector}
        rules_patch = patch.object(SymmetricGridExecutor, "get_trading_rules", return_value=self.RULE)
        self.price = patch.object(SymmetricGridExecutor, "get_price", return_value=Decimal("100"))
        rules_patch.start()
        self.price_mock = self.price.start()
        self.addCleanup(rules_patch.stop)
        self.addCleanup(self.price.stop)

    def _executor(self, **overrides) -> SymmetricGridExecutor:
        cfg = dict(id="t", timestamp=self.now, connector_name="binance", trading_pair="ETH-USDT",
                   spread_percentages=[Decimal("0.01")], order_amounts_quote=[Decimal("500")],
                   price_refresh_tolerance=Decimal("0.005"))
        cfg.update(overrides)
        executor = SymmetricGridExecutor(strategy=self.strategy, config=SymmetricGridExecutorConfig(**cfg))
        executor._status = RunnableStatus.RUNNING
        return executor

    @staticmethod
    def _order(order_id, side, price, amount, filled_base=Decimal("0"), state=OrderState.OPEN):
        order = InFlightOrder(client_order_id=order_id, exchange_order_id=f"E-{order_id}",
                              trading_pair="ETH-USDT", order_type=OrderType.LIMIT, trade_type=side,
                              amount=amount, price=price, creation_timestamp=1_000_000, initial_state=state)
        if filled_base > 0:
            order.update_with_trade_update(TradeUpdate(
                trade_id=f"T-{order_id}", client_order_id=order_id, exchange_order_id=f"E-{order_id}",
                trading_pair="ETH-USDT", fill_price=price, fill_base_amount=filled_base,
                fill_quote_amount=filled_base * price,
                fee=AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0"))]),
                fill_timestamp=1_000_001))
        return order

    def _start_refresh(self, executor, new_price="102"):
        executor._last_refresh_timestamp = 0
        with patch.object(SymmetricGridExecutor, "get_fair_price", return_value=Decimal(new_price)):
            executor.refresh_orders_on_price_change()

    def _cancel(self, executor, order_id):
        executor.process_order_canceled_event(None, MagicMock(), OrderCancelledEvent(self.now, order_id))

    # PnL counts held inventory at mid, not as a realised loss
    def test_first_buy_fill_without_price_move_is_flat_and_does_not_stop(self):
        executor = self._executor(stop_loss=Decimal("0.05"))
        executor._filled_orders = [{"trade_type": "BUY", "executed_amount_base": "5",
                                    "executed_amount_quote": "500", "cumulative_fee_paid_quote": "0"}]
        executor.mid_price = Decimal("100")
        executor.update_realized_pnl_metrics()
        self.assertEqual(Decimal("0"), executor.get_net_pnl_quote())
        self.assertFalse(executor.stop_loss_condition())

    def test_net_pnl_pct_is_measured_against_the_budget(self):
        executor = self._executor()  # budget 500
        executor._filled_orders = [{"trade_type": "BUY", "executed_amount_base": "5",
                                    "executed_amount_quote": "500", "cumulative_fee_paid_quote": "0"}]
        executor.mid_price = Decimal("90")
        executor.update_realized_pnl_metrics()
        self.assertEqual(Decimal("-50"), executor.get_net_pnl_quote())
        self.assertEqual(Decimal("-0.1"), executor.get_net_pnl_pct())

    # a refresh-cancel timeout re-sends the cancel and keeps the order tracked
    def test_refresh_timeout_keeps_unconfirmed_order_and_resends_cancel(self):
        executor = self._executor()
        executor.manage_orders()
        buy_id = executor.grid_levels[0].buy_order.order_id
        self._start_refresh(executor)
        cancels_sent = self.strategy.cancel.call_count
        self.now += 11
        executor.refresh_orders_on_price_change()
        self.assertEqual(buy_id, executor.grid_levels[0].buy_order.order_id)
        self.assertGreater(self.strategy.cancel.call_count, cancels_sent)
        self.assertIn(0, executor._refresh_pending_levels)

    # a fill that arrives after a confirmed cancel is still counted, for a while
    def test_late_fill_after_confirmed_cancel_is_counted(self):
        executor = self._executor()
        executor.manage_orders()
        buy_id = executor.grid_levels[0].buy_order.order_id
        self._cancel(executor, buy_id)
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(token="USDT", amount=Decimal("0.1"))])
        fill = OrderFilledEvent(self.now, buy_id, "ETH-USDT", TradeType.BUY, OrderType.LIMIT,
                                Decimal("99"), Decimal("2"), fee)
        executor.process_order_filled_event(None, MagicMock(), fill)
        self.assertEqual(1, len(executor._filled_orders))
        self.assertEqual("2", executor._filled_orders[0]["executed_amount_base"])

        self.now += 61
        executor._prune_cancel_watch()
        executor.process_order_filled_event(None, MagicMock(), fill)
        self.assertEqual(1, len(executor._filled_orders))

    # a fill that races a refresh cancel is recorded, not overwritten
    def test_fill_racing_refresh_cancel_is_recorded(self):
        executor = self._executor()
        executor.manage_orders()
        level = executor.grid_levels[0]
        buy_id, sell_id = level.buy_order.order_id, level.sell_order.order_id
        self._start_refresh(executor)
        level.buy_order.order = self._order(buy_id, TradeType.BUY, Decimal("99"), Decimal("5"),
                                            filled_base=Decimal("5"), state=OrderState.FILLED)
        self._cancel(executor, sell_id)
        recorded = [o for o in executor._filled_orders if o.get("client_order_id") == buy_id]
        self.assertEqual(1, len(recorded))
        self.assertEqual("sell", level.pending_side)
        self.assertIsNone(level.buy_order)
        self.assertIsNotNone(level.sell_order)

    # the filled part of a cancelled order is recorded
    def test_partial_fill_is_recorded_when_order_is_cancelled(self):
        executor = self._executor()
        executor.manage_orders()
        level = executor.grid_levels[0]
        buy_id = level.buy_order.order_id
        level.buy_order.order = self._order(buy_id, TradeType.BUY, Decimal("99"), Decimal("5"),
                                            filled_base=Decimal("3"), state=OrderState.PARTIALLY_FILLED)
        self._cancel(executor, buy_id)
        executor.update_realized_pnl_metrics()
        self.assertEqual(Decimal("3"), executor.net_inventory_base)

    # a refresh completes when a pending order fills instead of cancelling
    async def test_refresh_completes_when_pending_order_fills(self):
        executor = self._executor()
        executor.manage_orders()
        level = executor.grid_levels[0]
        buy_id, sell_id = level.buy_order.order_id, level.sell_order.order_id
        self._start_refresh(executor)
        self._cancel(executor, sell_id)
        self.assertIn(0, executor._refresh_pending_levels)
        level.buy_order.order = self._order(buy_id, TradeType.BUY, Decimal("99"), Decimal("5"),
                                            filled_base=Decimal("5"), state=OrderState.FILLED)
        await executor.control_task()
        self.assertEqual(set(), executor._refresh_pending_levels)
        self.assertIsNotNone(level.sell_order)

    # each level is re-placed as soon as its own cancels confirm
    def test_refresh_replaces_each_level_as_soon_as_it_is_empty(self):
        executor = self._executor(spread_percentages=[Decimal("0.01"), Decimal("0.02")],
                                  order_amounts_quote=[Decimal("500"), Decimal("500")])
        executor.manage_orders()
        l0 = executor.grid_levels[0]
        old_buy, old_sell = l0.buy_order.order_id, l0.sell_order.order_id
        self._start_refresh(executor)
        self._cancel(executor, old_buy)
        self.assertIsNone(l0.buy_order)
        self._cancel(executor, old_sell)
        self.assertNotEqual(old_buy, l0.buy_order.order_id)
        self.assertNotEqual(old_sell, l0.sell_order.order_id)
        self.assertEqual({1}, executor._refresh_pending_levels)

    # after a buy fills, the level waits for its sell instead of buying again
    def test_level_does_not_buy_again_until_its_sell_fills(self):
        executor = self._executor()
        executor.manage_orders()
        level = executor.grid_levels[0]
        buy_id = level.buy_order.order_id
        level.buy_order.order = self._order(buy_id, TradeType.BUY, Decimal("99"), Decimal("5"),
                                            filled_base=Decimal("5"), state=OrderState.FILLED)
        executor.process_filled_orders()
        executor.manage_orders()
        self.assertIsNone(level.buy_order)
        self.assertIsNotNone(level.sell_order)

    # with a fixed fair price, a level that is through the market waits instead of chasing
    def test_fixed_fair_price_does_not_chase_the_best_bid(self):
        self.price_mock.return_value = Decimal("98")  # market fell below the 99 buy level
        executor = self._executor(fair_price=Decimal("100"))
        executor.manage_orders()
        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIsNotNone(executor.grid_levels[0].sell_order)

    def test_live_mid_fair_price_still_clamps_below_the_best_bid(self):
        self.price_mock.return_value = Decimal("98")
        executor = self._executor()
        executor.manage_orders()
        self.assertIsNotNone(executor.grid_levels[0].buy_order)

    # perpetuals: sells need margin, not base; one-way mode only
    def test_perpetual_sells_placed_without_base_balance_in_oneway_mode(self):
        self.connector.position_mode = PositionMode.ONEWAY
        with patch.object(SymmetricGridExecutor, "is_perpetual", new_callable=PropertyMock, return_value=True), \
                patch.object(SymmetricGridExecutor, "get_available_balance", return_value=Decimal("0")):
            executor = self._executor()
            executor.manage_orders()
        self.assertIsNotNone(executor.grid_levels[0].sell_order)
        self.assertIsNotNone(executor.grid_levels[0].buy_order)

    def test_perpetual_in_hedge_mode_places_nothing(self):
        self.connector.position_mode = PositionMode.HEDGE
        with patch.object(SymmetricGridExecutor, "is_perpetual", new_callable=PropertyMock, return_value=True):
            executor = self._executor()
            executor.manage_orders()
        self.assertIsNone(executor.grid_levels[0].buy_order)
        self.assertIsNone(executor.grid_levels[0].sell_order)

    # late fill data falls back to the order's own size and price
    def test_fill_fallback_uses_the_orders_own_price_and_amount(self):
        executor = self._executor()
        executor.manage_orders()
        level = executor.grid_levels[0]
        buy_id = level.buy_order.order_id
        level.buy_order.order = self._order(buy_id, TradeType.BUY, Decimal("95"), Decimal("2"), state=OrderState.FILLED)
        executor.fair_price = Decimal("100")
        executor.process_filled_orders()
        self.assertEqual(Decimal("2"), Decimal(executor._filled_orders[-1]["executed_amount_base"]))
        self.assertEqual(Decimal("190"), Decimal(executor._filled_orders[-1]["executed_amount_quote"]))

    # a refresh no longer wipes pending-sell reservations of other levels
    def test_refresh_keeps_other_levels_pending_sell_reservations(self):
        executor = self._executor(spread_percentages=[Decimal("0.01"), Decimal("0.02")],
                                  order_amounts_quote=[Decimal("500"), Decimal("500")])
        executor.manage_orders()
        l0, l1 = executor.grid_levels
        reserved = executor._sell_base_pending[l1.sell_order.order_id]
        self._start_refresh(executor)
        self._cancel(executor, l0.buy_order.order_id)
        self._cancel(executor, l0.sell_order.order_id)
        self.assertEqual(reserved, executor._sell_base_pending.get(l1.sell_order.order_id))

    # a small base shortfall after our own buy still lets the sell go out
    def test_sell_uses_available_base_when_slightly_short(self):
        received_base = Decimal("1.08") * Decimal("0.999")  # 110 INR @ ~101.2, 0.01 step, 0.1% fee in base
        with patch.object(SymmetricGridExecutor, "get_available_balance", return_value=received_base):
            executor = self._executor(spread_percentages=[Decimal("0.001")], order_amounts_quote=[Decimal("110")])
            executor.fair_price = Decimal("101.30")
            executor.manage_orders()
        self.assertIsNotNone(executor.grid_levels[0].sell_order)
        self.assertEqual(received_base, executor._sell_base_pending[executor.grid_levels[0].sell_order.order_id])

    def test_sell_still_skipped_when_far_short(self):
        with patch.object(SymmetricGridExecutor, "get_available_balance", return_value=Decimal("1")):
            executor = self._executor()
            executor.manage_orders()
        self.assertIsNone(executor.grid_levels[0].sell_order)

    # no hidden 100-quote floor; the exchange minimum still applies
    def test_min_order_amount_quote_defaults_to_zero(self):
        executor = self._executor(order_amounts_quote=[Decimal("20")])
        executor.manage_orders()
        self.assertIsNotNone(executor.grid_levels[0].buy_order)

    # a zero or negative fixed fair price is rejected up front
    def test_non_positive_fair_price_is_rejected(self):
        for value in ("0", "-1"):
            with self.assertRaises(ValueError):
                self._executor(fair_price=Decimal(value))

    # balance validated once; a short balance keeps the executor stopped
    async def test_on_start_validates_once_and_stays_stopped_when_balance_is_short(self):
        executor = self._executor(time_limit=60)
        executor._status = RunnableStatus.NOT_STARTED
        self.now += 120  # the time limit has also passed

        async def short_balance():
            executor.close_type = CloseType.INSUFFICIENT_BALANCE
            executor.stop()

        with patch.object(SymmetricGridExecutor, "validate_sufficient_balance", side_effect=short_balance) as validate:
            await executor.on_start()
        self.assertEqual(1, validate.call_count)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(CloseType.INSUFFICIENT_BALANCE, executor.close_type)

    # "Skipping sell" is logged once per level until that sell is placed
    def test_skipping_sell_is_logged_once_until_the_sell_is_placed(self):
        logger = MagicMock()
        with patch.object(SymmetricGridExecutor, "logger", return_value=logger), \
                patch.object(SymmetricGridExecutor, "get_available_balance", return_value=Decimal("0")) as balance:
            executor = self._executor()

            def skips():
                return [c for c in logger.info.call_args_list if "Skipping sell" in str(c)]

            for _ in range(3):
                executor.manage_orders()
            self.assertEqual(1, len(skips()))

            balance.return_value = Decimal("10000")
            executor.manage_orders()
            self.assertIsNotNone(executor.grid_levels[0].sell_order)

            executor.grid_levels[0].sell_order = None  # e.g. cancelled, and the balance is short again
            balance.return_value = Decimal("0")
            executor.manage_orders()
            self.assertEqual(2, len(skips()))

    # not enough balance to start is also shown in the bot
    async def test_insufficient_balance_notifies_the_app(self):
        zero = MagicMock()
        zero.amount = Decimal("0")
        with patch.object(SymmetricGridExecutor, "adjust_order_candidates", return_value=[zero]):
            executor = self._executor()
            await executor.validate_sufficient_balance()
        self.assertEqual(CloseType.INSUFFICIENT_BALANCE, executor.close_type)
        self.strategy.notify_hb_app_with_timestamp.assert_called_once()
        self.assertIn("not enough USDT", self.strategy.notify_hb_app_with_timestamp.call_args[0][0])

    def test_early_stop_leaves_a_finished_executor_alone(self):
        executor = self._executor()
        executor.close_type = CloseType.INSUFFICIENT_BALANCE
        executor.stop()

        executor.early_stop()  # what the kill switch sends to every executor of the controller

        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(CloseType.INSUFFICIENT_BALANCE, executor.close_type)
        self.assertFalse(executor.is_active)


class TestSymmetricGridController(IsolatedAsyncioWrapperTestCase):
    """Controller defaults, halting after a protective stop, config validation and status text."""

    def setUp(self) -> None:
        super().setUp()
        self.mdp = MagicMock()
        self.mdp.time.return_value = 1_000_000
        self.controller = SymmetricGrid(SymmetricGridConfig(id="c1"), market_data_provider=self.mdp,
                                        actions_queue=MagicMock())

    @staticmethod
    def _stopped(close_type, close_timestamp=1_000_100):
        return MagicMock(id=f"ex-{close_type.name}", is_active=False, close_type=close_type,
                         close_timestamp=close_timestamp)

    def test_defaults(self):
        config = SymmetricGridConfig(id="c1")
        self.assertEqual(Decimal("0.0005"), config.price_refresh_tolerance)
        self.assertEqual(PositionMode.ONEWAY, config.position_mode)
        self.assertEqual(Decimal("0"), config.min_order_amount_quote)

    def test_stays_stopped_after_a_protective_exit(self):
        for close_type in (CloseType.STOP_LOSS, CloseType.TIME_LIMIT, CloseType.INSUFFICIENT_BALANCE, CloseType.FAILED):
            self.controller.executors_info = [self._stopped(close_type)]
            self.assertEqual([], self.controller.determine_executor_actions(), close_type)

    def test_creates_a_new_executor_after_an_early_stop(self):
        self.controller.executors_info = [self._stopped(CloseType.EARLY_STOP)]
        actions = self.controller.determine_executor_actions()
        self.assertEqual(1, len(actions))
        self.assertIsInstance(actions[0], CreateExecutorAction)

    def test_restart_resumes_after_a_protective_exit(self):
        self.controller.executors_info = [self._stopped(CloseType.STOP_LOSS, close_timestamp=1_000_100)]
        self.mdp.time.return_value = 1_000_200
        with patch.object(ControllerBase, "start"):
            self.controller.start()
        self.assertEqual(1, len(self.controller.determine_executor_actions()))

    def test_passes_sizing_and_pacing_options_to_the_executor(self):
        controller = SymmetricGrid(
            SymmetricGridConfig(id="c2", min_order_amount_quote=Decimal("120"), order_frequency=5,
                                max_orders_per_batch=3, safe_extra_spread=Decimal("0.0002")),
            market_data_provider=self.mdp, actions_queue=MagicMock())
        executor_config = controller.determine_executor_actions()[0].executor_config
        self.assertEqual(Decimal("120"), executor_config.min_order_amount_quote)
        self.assertEqual(5, executor_config.order_frequency)
        self.assertEqual(3, executor_config.max_orders_per_batch)
        self.assertEqual(Decimal("0.0002"), executor_config.safe_extra_spread)
        self.assertEqual(Decimal("0.0005"), executor_config.price_refresh_tolerance)

    # the controller config rejects a zero or negative reference price at load
    def test_non_positive_reference_price_is_rejected(self):
        for value in ("0", "-5"):
            with self.assertRaises(ValueError):
                SymmetricGridConfig(id="c", reference_price=Decimal(value))
        self.assertEqual(Decimal("100"), SymmetricGridConfig(id="c", reference_price=Decimal("100")).reference_price)
        self.assertIsNone(SymmetricGridConfig(id="c").reference_price)

    # after the kill switch, status says the controller is stopped
    def test_status_says_stopped_after_the_kill_switch(self):
        self.mdp.get_price_by_type.return_value = Decimal("100")
        controller = SymmetricGrid(SymmetricGridConfig(id="c3", manual_kill_switch=True),
                                   market_data_provider=self.mdp, actions_queue=MagicMock())
        controller.executors_info = [self._stopped(CloseType.EARLY_STOP)]
        text = "\n".join(controller.to_format_status())
        self.assertIn("Controller stopped (manual kill switch)", text)
        self.assertNotIn("will create on next tick", text)

    def test_status_without_kill_switch_still_says_it_will_create(self):
        self.mdp.get_price_by_type.return_value = Decimal("100")
        text = "\n".join(self.controller.to_format_status())
        self.assertIn("will create on next tick", text)
