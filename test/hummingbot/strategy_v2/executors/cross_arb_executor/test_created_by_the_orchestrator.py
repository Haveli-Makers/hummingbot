"""
The path the bot really takes: the controller's action goes to ExecutorOrchestrator, which builds
the executor, starts its loop and reports on it every tick.

The other executor tests (and the dry run) construct CrossArbExecutor directly, so they never saw
the arguments the orchestrator passes. The first live attempt died there — "unexpected keyword
argument 'max_retries'" — before a single order went out. These tests hold that door open.
"""
import asyncio
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.cross_arb_executor.fakes import FakeConnector, FakeStrategy
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock, patch

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_arb_executor.cross_arb_executor import CrossArbExecutor
from hummingbot.strategy_v2.executors.cross_arb_executor.data_types import CrossArbExecutorConfig, MismatchPolicy
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

D = Decimal
CONTROLLER = "cross_arb_test"


class CreatedByTheOrchestratorTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.csx = FakeConnector("csx", bid=D("99"), ask=D("100"))
        self.wazirx = FakeConnector("wazirx", bid=D("102"), ask=D("103"))
        self.strategy = FakeStrategy({"csx": self.csx, "wazirx": self.wazirx})
        # What the orchestrator reads from a StrategyV2Base while it sets itself up.
        self.strategy.controllers = {CONTROLLER: MagicMock()}
        self.strategy.markets = {"csx": {"SOL-INR"}, "wazirx": {"SOL-INR"}}

        recorder = MagicMock(spec=MarketsRecorder)
        recorder.get_all_executors.return_value = []
        recorder.get_all_positions.return_value = []
        patcher = patch.object(MarketsRecorder, "get_instance", return_value=recorder)
        patcher.start()
        self.addCleanup(patcher.stop)
        # The same settings StrategyV2Base passes.
        self.orchestrator = ExecutorOrchestrator(strategy=self.strategy, executors_update_interval=1.0,
                                                 executors_max_retries=10)

    def action(self, **overrides) -> CreateExecutorAction:
        config = CrossArbExecutorConfig(
            timestamp=1_000.0,
            buying_market=ConnectorPair(connector_name="csx", trading_pair="SOL-INR"),
            selling_market=ConnectorPair(connector_name="wazirx", trading_pair="SOL-INR"),
            order_amount=D("1"), buy_price_cap=D("100"), sell_price_floor=D("102"), **overrides)
        return CreateExecutorAction(executor_config=config, controller_id=CONTROLLER)

    async def test_the_orchestrator_can_build_and_start_it(self):
        self.orchestrator.execute_actions([self.action()])

        executors = self.orchestrator.active_executors[CONTROLLER]
        self.assertEqual(1, len(executors))
        executor = executors[0]
        self.assertIsInstance(executor, CrossArbExecutor)
        try:
            await asyncio.sleep(0.05)       # let the loop the orchestrator started take its first step
            self.assertEqual(2, len(self.strategy.sent), "both legs should have gone out")
        finally:
            executor.stop()

    async def test_its_reports_work_while_it_runs(self):
        """The orchestrator builds these every tick; an exception here blinds the controller."""
        self.orchestrator.execute_actions([self.action()])
        executor = self.orchestrator.active_executors[CONTROLLER][0]
        try:
            await asyncio.sleep(0.05)
            reports = self.orchestrator.get_all_reports()
            info = reports[CONTROLLER]["executors"][0]
            self.assertEqual("cross_arb_executor", info.type)
            self.assertEqual("SOL-INR", info.custom_info["pair"])
            self.assertIsNotNone(reports[CONTROLLER]["performance"])
        finally:
            executor.stop()

    async def test_a_held_mismatch_becomes_a_position_it_can_report(self):
        """
        mismatch_policy: hold used to break the orchestrator's position tracking (AttributeError:
        no connector_name). Now the kept coin is filed as a position on the venue that holds it,
        and only the matched part counts as the attempt's profit — nothing is counted twice.
        """
        self.strategy.market_data_provider = MagicMock()
        self.strategy.market_data_provider.get_price_by_type.return_value = D("101")
        self.orchestrator.executors_update_interval = 0.01
        self.orchestrator.execute_actions([self.action(mismatch_policy=MismatchPolicy.HOLD, fill_timeout=10)])
        executor = self.orchestrator.active_executors[CONTROLLER][0]
        try:
            await asyncio.sleep(0.05)                   # both legs out
            self.strategy.order("buy-1").fill()
            self.strategy.order("sell-2").fill(amount=D("0.3"))
            self.strategy.advance(11)
            await asyncio.sleep(0.05)                   # deadline: the rest of the sell is cancelled
            self.strategy.order("sell-2").cancel()
            self.strategy.advance(1)
            await asyncio.sleep(0.05)                   # 0.7 kept on purpose
            self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
            reports = self.orchestrator.get_all_reports()
        finally:
            executor.stop()

        position = reports[CONTROLLER]["positions"][0]
        self.assertEqual((position.connector_name, position.side, position.amount), ("csx", TradeType.BUY, D("0.7")))
        self.assertEqual(position.unrealized_pnl_quote, D("0.7"))     # bought at 100, now 101
        performance = reports[CONTROLLER]["performance"]
        self.assertEqual(performance.realized_pnl_quote, D("0.6"))    # the matched 0.3 x 2, no TDS set
