import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from pydantic import ValidationError

from controllers.generic.grid_v2 import GridV2Executor, GridV2ExecutorConfig
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.best_price_executor.data_types import BestPriceExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.symmetric_grid_executor.data_types import SymmetricGridExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class TestGridV2Executor(IsolatedAsyncioWrapperTestCase):

    def test_default_config_is_valid_for_controller_creation(self):
        config = GridV2ExecutorConfig()

        self.assertEqual(config.spreads, [Decimal("0.01"), Decimal("0.02")])
        self.assertEqual(config.amounts_quote, [Decimal("10"), Decimal("20")])

    def setUp(self):
        self.mock_market_data_provider = MagicMock()
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)

        self.config = GridV2ExecutorConfig(
            id="test",
            controller_name="grid_v2_executor",
            connector_name="binance_perpetual",
            trading_pair="ETH-USDT",
            spreads="0.01,0.02",
            amounts_quote="10,20",
            total_balance=Decimal("0"),
            max_exposure=Decimal("100"),
            target_profit=Decimal("0.01"),
            price_refresh_tolerance=Decimal("0.05"),
            price_diff=Decimal("0"),
            order_frequency=3,
            min_order_amount_quote=Decimal("5"),
            open_order_type=OrderType.LIMIT_MAKER,
        )

        self.controller = GridV2Executor(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def create_symmetric_grid_executor_info(
        self,
        is_active: bool = True,
        net_inventory_base: Decimal = Decimal("0"),
        filled_orders: list | None = None,
    ) -> ExecutorInfo:
        return ExecutorInfo(
            id="symmetric_grid_id",
            timestamp=1234,
            type="symmetric_grid_executor",
            status=RunnableStatus.RUNNING if is_active else RunnableStatus.TERMINATED,
            config=SymmetricGridExecutorConfig(
                timestamp=1234,
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                spread_percentages=self.config.spreads,
                order_amounts_quote=self.config.amounts_quote,
                order_type=self.config.open_order_type,
                leverage=self.config.leverage,
                level_id="grid_v2_symmetric",
            ),
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"),
            is_active=is_active,
            is_trading=is_active,
            custom_info={
                "net_inventory_base": net_inventory_base,
                "filled_orders": filled_orders or [],
            },
            controller_id=self.config.id,
        )

    def create_target_profit_executor_info(self, side: TradeType = TradeType.SELL) -> ExecutorInfo:
        return ExecutorInfo(
            id="tp_executor_id",
            timestamp=1234,
            type="order_executor",
            status=RunnableStatus.RUNNING,
            config=OrderExecutorConfig(
                timestamp=1234,
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=Decimal("1"),
                price=Decimal("101"),
                position_action=PositionAction.CLOSE,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
                level_id=f"grid_v2_tp_{side.name.lower()}",
            ),
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"),
            is_active=True,
            is_trading=True,
            custom_info={},
            controller_id=self.config.id,
        )

    def test_parse_spread_and_amount_lists(self):
        self.assertEqual(self.config.spreads, [Decimal("0.01"), Decimal("0.02")])
        self.assertEqual(self.config.amounts_quote, [Decimal("10"), Decimal("20")])

    async def test_update_processed_data(self):
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("100")
        await self.controller.update_processed_data()

        self.assertEqual(self.controller.processed_data["fair_price"], Decimal("100"))
        self.assertEqual(self.controller.processed_data["mid_price"], Decimal("100"))

    async def test_determine_executor_actions_creates_symmetric_grid_executor(self):
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("100")
        await self.controller.update_processed_data()

        actions = self.controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertEqual(len(create_actions), 1)
        executor_config = create_actions[0].executor_config
        self.assertIsInstance(executor_config, SymmetricGridExecutorConfig)
        self.assertEqual(executor_config.connector_name, self.config.connector_name)
        self.assertEqual(executor_config.trading_pair, self.config.trading_pair)
        self.assertEqual(executor_config.spread_percentages, [Decimal("0.01"), Decimal("0.02")])
        self.assertEqual(executor_config.order_amounts_quote, [Decimal("10"), Decimal("20")])
        self.assertEqual(executor_config.order_type, OrderType.LIMIT_MAKER)
        self.assertEqual(executor_config.order_frequency, 3)
        self.assertEqual(executor_config.max_orders_per_batch, 8)
        self.assertEqual(executor_config.safe_extra_spread, Decimal("0.0001"))
        self.assertEqual(executor_config.price_refresh_tolerance, Decimal("0.05"))
        self.assertEqual(executor_config.min_order_amount_quote, Decimal("5"))
        self.assertIsNone(executor_config.fair_price)
        self.assertEqual(executor_config.level_id, "grid_v2_symmetric")

    def test_determine_executor_actions_does_not_create_when_symmetric_grid_is_active(self):
        self.controller.executors_info = [self.create_symmetric_grid_executor_info()]

        actions = self.controller.determine_executor_actions()

        self.assertEqual(actions, [])

    def test_determine_executor_actions_creates_target_profit_from_symmetric_grid_exposure(self):
        self.mock_market_data_provider.time.return_value = 12345
        self.controller.executors_info = [
            self.create_symmetric_grid_executor_info(
                net_inventory_base=Decimal("0.1"),
                filled_orders=[
                    {
                        "trade_type": "BUY",
                        "executed_amount_base": "0.1",
                        "executed_amount_quote": "10",
                    }
                ],
            )
        ]

        actions = self.controller.determine_executor_actions()

        self.assertEqual(len(actions), 1)
        executor_config = actions[0].executor_config
        self.assertIsInstance(executor_config, OrderExecutorConfig)
        self.assertEqual(executor_config.side, TradeType.SELL)
        self.assertEqual(executor_config.amount, Decimal("0.1"))
        self.assertEqual(executor_config.price, Decimal("101.00"))
        self.assertEqual(executor_config.position_action, PositionAction.CLOSE)
        self.assertEqual(executor_config.execution_strategy, ExecutionStrategy.LIMIT_MAKER)
        self.assertEqual(executor_config.level_id, "grid_v2_tp_sell")

    def test_determine_executor_actions_skips_duplicate_target_profit(self):
        self.controller.executors_info = [
            self.create_symmetric_grid_executor_info(
                net_inventory_base=Decimal("0.1"),
                filled_orders=[
                    {
                        "trade_type": "BUY",
                        "executed_amount_base": "0.1",
                        "executed_amount_quote": "10",
                    }
                ],
            ),
            self.create_target_profit_executor_info(),
        ]

        actions = self.controller.determine_executor_actions()

        self.assertEqual(actions, [])

    def test_determine_executor_actions_creates_exposure_exit_when_above_max_exposure(self):
        self.config.max_exposure = Decimal("0.05")
        self.mock_market_data_provider.time.return_value = 12345
        self.controller.executors_info = [
            self.create_symmetric_grid_executor_info(net_inventory_base=Decimal("0.1"))
        ]

        actions = self.controller.determine_executor_actions()

        self.assertEqual(len(actions), 1)
        executor_config = actions[0].executor_config
        self.assertIsInstance(executor_config, BestPriceExecutorConfig)
        self.assertEqual(executor_config.side, TradeType.SELL)
        self.assertEqual(executor_config.amount, Decimal("0.1"))
        self.assertEqual(executor_config.position_action, PositionAction.CLOSE)
        self.assertEqual(executor_config.level_id, "grid_v2_exposure_exit_sell")

    def test_config_requires_matching_spreads_and_amounts(self):
        with self.assertRaises(ValidationError):
            GridV2ExecutorConfig(
                spreads="0.01,0.02",
                amounts_quote="10,20,30",
            )
