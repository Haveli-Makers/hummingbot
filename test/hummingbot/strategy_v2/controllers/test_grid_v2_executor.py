import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.data_type.common import PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from controllers.generic.grid_v2_executor import GridV2Executor, GridV2ExecutorConfig
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase


class TestGridV2Executor(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)

        self.config = GridV2ExecutorConfig(
            id="test",
            controller_name="grid_v2_executor",
            connector_name="binance_perpetual",
            trading_pair="ETH-USDT",
            buy_spreads="0.01,0.02",
            sell_spreads="0.01,0.02",
            buy_amounts_quote="10,20",
            sell_amounts_quote="10,20",
            total_balance=Decimal("1000"),
            max_exposure=Decimal("100"),
            target_profit=Decimal("0.01"),
            price_refresh_tolerance=Decimal("0.05"),
            price_diff=Decimal("0"),
        )

        self.controller = GridV2Executor(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def test_parse_spread_and_amount_lists(self):
        self.assertEqual(self.config.buy_spreads, [Decimal("0.01"), Decimal("0.02")])
        self.assertEqual(self.config.sell_spreads, [Decimal("0.01"), Decimal("0.02")])
        self.assertEqual(self.config.buy_amounts_quote, [Decimal("10"), Decimal("20")])
        self.assertEqual(self.config.sell_amounts_quote, [Decimal("10"), Decimal("20")])

    async def test_update_processed_data(self):
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("100")
        await self.controller.update_processed_data()

        self.assertEqual(self.controller.processed_data["fair_price"], Decimal("100"))
        self.assertEqual(self.controller.processed_data["mid_price"], Decimal("100"))

    async def test_determine_executor_actions_creates_grid_orders(self):
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("100")
        await self.controller.update_processed_data()

        actions = self.controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertEqual(len(create_actions), 4)
        self.assertTrue(all(isinstance(action.executor_config, OrderExecutorConfig) for action in create_actions))

    async def test_determine_executor_actions_refreshes_on_price_move(self):
        self.controller._last_fair_price = Decimal("100")
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("110")

        active_executor = MagicMock(spec=ExecutorInfo)
        active_executor.is_active = True
        active_executor.config = MagicMock()
        active_executor.config.type = "order_executor"
        active_executor.config.level_id = "grid_open_buy_0"
        active_executor.id = "executor_1"
        self.controller.executors_info = [active_executor]

        await self.controller.update_processed_data()
        actions = self.controller.determine_executor_actions()

        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], StopExecutorAction)
