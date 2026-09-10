import asyncio
import json
import math
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pandas as pd

from hummingbot.connector.test_support.mock_paper_exchange import MockPaperExchange
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import PositionMode, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo, PerformanceReport


class TestStrategyV2Base(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        self.start: pd.Timestamp = pd.Timestamp("2021-01-01", tz="UTC")
        self.end: pd.Timestamp = pd.Timestamp("2021-01-01 01:00:00", tz="UTC")
        self.start_timestamp: float = self.start.timestamp()
        self.end_timestamp: float = self.end.timestamp()
        self.clock_tick_size = 1
        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)
        self.connector: MockPaperExchange = MockPaperExchange()
        self.connector_name: str = "mock_paper_exchange"
        self.trading_pair: str = "HBOT-USDT"
        self.strategy_config = StrategyV2ConfigBase(markets={self.connector_name: {self.trading_pair}},
                                                    candles_config=[])
        with patch('asyncio.create_task', return_value=MagicMock()):
            # Initialize the strategy with mock components
            with patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()):
                with patch('hummingbot.strategy.strategy_v2_base.ExecutorOrchestrator') as MockExecutorOrchestrator:
                    with patch('hummingbot.strategy.strategy_v2_base.MarketDataProvider') as MockMarketDataProvider:
                        self.strategy = StrategyV2Base({self.connector_name: self.connector}, config=self.strategy_config)
                        # Set mocks to strategy attributes
                        self.strategy.executor_orchestrator = MockExecutorOrchestrator.return_value
                        self.strategy.market_data_provider = MockMarketDataProvider.return_value
                        self.strategy.controllers = {'controller_1': MagicMock(), 'controller_2': MagicMock()}
        self.strategy.logger().setLevel(1)

    async def test_start(self):
        self.assertFalse(self.strategy.ready_to_trade)
        self.strategy.start(Clock(ClockMode.BACKTEST), self.start_timestamp)
        self.strategy.tick(self.start_timestamp + 10)
        self.assertTrue(self.strategy.ready_to_trade)

    def test_init_markets(self):
        StrategyV2Base.init_markets(self.strategy_config)
        self.assertIn(self.connector_name, StrategyV2Base.markets)
        self.assertIn(self.trading_pair, StrategyV2Base.markets[self.connector_name])

    def test_store_actions_proposal(self):
        # Setup test executors with all required fields
        executor_1 = ExecutorInfo(
            id="1",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            timestamp=10,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=False,
            is_trading=False,
            custom_info={}
        )
        executor_2 = ExecutorInfo(
            id="2",
            controller_id="controller_2",
            type="position_executor",
            status=RunnableStatus.RUNNING,
            timestamp=20,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=True,
            is_trading=True,
            custom_info={}
        )
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [executor_1], "positions": [], "performance": None},
            "controller_2": {"executors": [executor_2], "positions": [], "performance": None}
        }
        self.strategy.closed_executors_buffer = 0

        actions = self.strategy.store_actions_proposal()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].executor_id, "1")

    def test_get_executors_by_controller(self):
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [MagicMock(), MagicMock()], "positions": [], "performance": None},
            "controller_2": {"executors": [MagicMock()], "positions": [], "performance": None}
        }

        executors = self.strategy.get_executors_by_controller("controller_1")
        self.assertEqual(len(executors), 2)

    def test_get_all_executors(self):
        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {"executors": [MagicMock(), MagicMock()], "positions": [], "performance": None},
            "controller_2": {"executors": [MagicMock()], "positions": [], "performance": None}
        }

        executors = self.strategy.get_all_executors()
        self.assertEqual(len(executors), 3)

    def test_set_leverage(self):
        mock_connector = MagicMock()
        self.strategy.connectors = {"mock": mock_connector}
        self.strategy.set_leverage("mock", "HBOT-USDT", 2)
        mock_connector.set_leverage.assert_called_with("HBOT-USDT", 2)

    def test_set_position_mode(self):
        mock_connector = MagicMock()
        self.strategy.connectors = {"mock": mock_connector}
        self.strategy.set_position_mode("mock", PositionMode.HEDGE)
        mock_connector.set_position_mode.assert_called_with(PositionMode.HEDGE)

    def test_filter_executors(self):
        executors = [MagicMock(status=RunnableStatus.RUNNING), MagicMock(status=RunnableStatus.TERMINATED)]
        filtered = StrategyV2Base.filter_executors(executors, lambda x: x.status == RunnableStatus.RUNNING)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].status, RunnableStatus.RUNNING)

    def test_is_perpetual(self):
        self.assertTrue(StrategyV2Base.is_perpetual("binance_perpetual"))
        self.assertFalse(StrategyV2Base.is_perpetual("binance"))

    @patch.object(StrategyV2Base, "create_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "stop_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "store_actions_proposal", return_value=[])
    @patch.object(StrategyV2Base, "update_controllers_configs")
    @patch.object(StrategyV2Base, "update_executors_info")
    @patch("hummingbot.data_feed.market_data_provider.MarketDataProvider.ready", new_callable=PropertyMock)
    @patch("hummingbot.strategy_v2.executors.executor_orchestrator.ExecutorOrchestrator.execute_action")
    async def test_on_tick(self, mock_execute_action, mock_ready, mock_update_executors_info,
                           mock_update_controllers_configs,
                           mock_store_actions_proposal, mock_stop_actions_proposal, mock_create_actions_proposal):
        mock_ready.return_value = True
        self.strategy.on_tick()

        # Assertions to ensure that methods are called
        mock_update_executors_info.assert_called_once()
        mock_update_controllers_configs.assert_called_once()

        # Verify that the respective action proposal methods are called
        mock_create_actions_proposal.assert_called_once()
        mock_stop_actions_proposal.assert_called_once()
        mock_store_actions_proposal.assert_called_once()

        # Since no actions are returned, execute_action should not be called
        mock_execute_action.assert_not_called()

    async def test_on_stop(self):
        # Make the executor orchestrator stop method async
        self.strategy.executor_orchestrator.stop = AsyncMock()

        await self.strategy.on_stop()

        # Check if stop methods are called on each component
        self.strategy.executor_orchestrator.stop.assert_called_once()
        self.strategy.market_data_provider.stop.assert_called_once()

        # Check if stop is called on each controller
        for controller in self.strategy.controllers.values():
            controller.stop.assert_called_once()

    async def test_on_stop_publishes_empty_reports(self):
        self.strategy.executor_orchestrator.stop = AsyncMock()
        self.strategy.mqtt_enabled = True
        publisher = MagicMock()
        self.strategy._pub = publisher
        # "main" holds executors that belong to no controller - it is published while the
        # strategy runs, so it has to be cleared on the way out too
        self.strategy.controller_reports = {"controller_1": {}, "main": {}}

        await self.strategy.on_stop()

        publisher.assert_called_once_with({
            "controller_1": {"performance": {}, "custom_info": {}},
            "controller_2": {"performance": {}, "custom_info": {}},
            "main": {"performance": {}, "custom_info": {}},
        })
        # The publisher is released, and must not be used again afterwards
        self.assertIsNone(self.strategy._pub)

    def _set_clock(self, timestamp: float):
        """`current_timestamp` is NaN until the clock starts, which the rate limit treats as
        'not running yet'. Tests that expect a report have to look like a running strategy."""
        type(self.strategy).current_timestamp = PropertyMock(return_value=timestamp)
        self.addCleanup(delattr, type(self.strategy), "current_timestamp")

    def _enable_mqtt(self, publisher=None):
        """The publisher is resolved on every report now rather than latched at start(), so a
        test that expects a report has to stand in for a live bridge."""
        publisher = publisher if publisher is not None else MagicMock()
        self.strategy._pub = publisher
        self.strategy.mqtt_enabled = True
        patcher = patch.object(StrategyV2Base, "_resolve_publisher", return_value=publisher)
        patcher.start()
        self.addCleanup(patcher.stop)
        return publisher

    def test_publish_performance_reports(self):
        report = PerformanceReport(realized_pnl_quote=Decimal("5.67"),
                                   volume_traded=Decimal("1000.5"),
                                   close_type_counts={CloseType.TAKE_PROFIT: 3})
        self.strategy.controller_reports = {
            "controller_1": {"executors": [], "positions": [], "performance": report},
        }
        publisher = self._enable_mqtt()
        self._set_clock(self.start_timestamp)

        self.strategy.publish_performance_reports()

        payload = publisher.call_args[0][0]
        self.assertEqual({"controller_1"}, set(payload.keys()))
        self.assertEqual({"performance", "custom_info"}, set(payload["controller_1"].keys()))

        performance = payload["controller_1"]["performance"]
        self.assertEqual(5.67, performance["realized_pnl_quote"])
        self.assertEqual(1000.5, performance["volume_traded"])
        self.assertEqual({"CloseType.TAKE_PROFIT": 3}, performance["close_type_counts"])
        # Consumers add up and round these, so they have to be real numbers
        self.assertIsInstance(performance["realized_pnl_quote"], float)
        # And the whole payload has to reach the broker as JSON
        self.assertEqual(payload, json.loads(json.dumps(payload)))

    def test_publish_performance_reports_does_nothing_without_mqtt(self):
        self.strategy.controller_reports = {"controller_1": {"performance": PerformanceReport()}}
        self._set_clock(self.start_timestamp)

        # The bridge is not up, so the resolver hands back nothing
        publisher = MagicMock()
        self.strategy._pub = publisher
        with patch.object(StrategyV2Base, "_resolve_publisher", return_value=None):
            self.strategy.publish_performance_reports()
        publisher.assert_not_called()

        # ...and a strategy on its way out never publishes either
        self.strategy._is_stop_triggered = True
        with patch.object(StrategyV2Base, "_resolve_publisher", return_value=publisher):
            self.strategy.publish_performance_reports()
        publisher.assert_not_called()

    def test_publish_performance_reports_waits_for_the_clock(self):
        # current_timestamp is NaN until the strategy is actually running
        self.strategy.controller_reports = {"controller_1": {"performance": PerformanceReport()}}
        publisher = self._enable_mqtt()

        self.strategy.publish_performance_reports()

        publisher.assert_not_called()
        # ...and the rate limit must not have been poisoned with NaN
        self.assertFalse(math.isnan(self.strategy._last_performance_report_ts))

    def test_publish_performance_reports_is_rate_limited(self):
        self.strategy.controller_reports = {"controller_1": {"performance": PerformanceReport()}}
        publisher = self._enable_mqtt()
        self.strategy.performance_report_interval = 1.0

        self._set_clock(self.start_timestamp)
        self.strategy.publish_performance_reports()
        self.assertEqual(1, publisher.call_count)

        # A second tick inside the interval is dropped
        self.strategy.publish_performance_reports()
        self.assertEqual(1, publisher.call_count)

        # ...and once the interval has passed, reporting resumes
        type(self.strategy).current_timestamp = PropertyMock(return_value=self.start_timestamp + 1)
        self.strategy.publish_performance_reports()
        self.assertEqual(2, publisher.call_count)

    def test_publish_performance_reports_swallows_publisher_errors(self):
        self.strategy.controller_reports = {"controller_1": {"performance": PerformanceReport()}}
        publisher = self._enable_mqtt(MagicMock(side_effect=Exception("broker is gone")))
        self._set_clock(self.start_timestamp)

        # A broker that goes away must never interrupt trading
        self.strategy.publish_performance_reports()

        publisher.assert_called_once()

    @patch.object(StrategyV2Base, "publish_performance_reports")
    def test_update_executors_info_publishes_reports(self, mock_publish):
        self.strategy.executor_orchestrator.get_all_reports = MagicMock(return_value={})

        self.strategy.update_executors_info()

        mock_publish.assert_called_once()

    # ---------------------------------------------------------------- custom_info

    def test_custom_info_comes_from_the_controller(self):
        self.strategy.controllers["controller_1"].get_custom_info.return_value = {
            "signal": 1, "spread": Decimal("0.0015"),
        }
        self.strategy.controller_reports = {
            "controller_1": {"performance": PerformanceReport()},
        }
        publisher = self._enable_mqtt()
        self._set_clock(self.start_timestamp)

        self.strategy.publish_performance_reports()

        custom_info = publisher.call_args[0][0]["controller_1"]["custom_info"]
        # Decimals coming out of a controller are converted like everything else
        self.assertEqual({"signal": 1, "spread": 0.0015}, custom_info)

    def test_custom_info_is_empty_for_ids_with_no_controller(self):
        # "main" holds executors that belong to no controller, so there is nobody to ask
        self.strategy.controller_reports = {"main": {"performance": PerformanceReport()}}
        publisher = self._enable_mqtt()
        self._set_clock(self.start_timestamp)

        self.strategy.publish_performance_reports()

        self.assertEqual({}, publisher.call_args[0][0]["main"]["custom_info"])

    def test_one_bad_controller_does_not_silence_the_report(self):
        self.strategy.controllers["controller_1"].get_custom_info.side_effect = Exception("boom")
        self.strategy.controller_reports = {
            "controller_1": {"performance": PerformanceReport(realized_pnl_quote=Decimal("2.5"))},
        }
        publisher = self._enable_mqtt()
        self._set_clock(self.start_timestamp)

        self.strategy.publish_performance_reports()

        payload = publisher.call_args[0][0]["controller_1"]
        self.assertEqual({}, payload["custom_info"])
        # ...and the performance half still went out
        self.assertEqual(2.5, payload["performance"]["realized_pnl_quote"])

    def test_controller_base_custom_info_defaults_to_empty(self):
        from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
        self.assertEqual({}, ControllerBase.get_custom_info(MagicMock()))

    def test_custom_info_of_the_wrong_type_is_ignored(self):
        # An overridden hook returning a non-dict would otherwise reach the JSON encoder
        # and take the whole report down with it
        self.strategy.controllers["controller_1"].get_custom_info.return_value = ["not", "a dict"]
        self.strategy.controller_reports = {
            "controller_1": {"performance": PerformanceReport(volume_traded=Decimal("9"))},
        }
        publisher = self._enable_mqtt()
        self._set_clock(self.start_timestamp)

        self.strategy.publish_performance_reports()

        payload = publisher.call_args[0][0]["controller_1"]
        self.assertEqual({}, payload["custom_info"])
        self.assertEqual(9.0, payload["performance"]["volume_traded"])
        self.assertEqual(payload, json.loads(json.dumps(payload)))

    # ---------------------------------------------------------------- publisher lifecycle

    def test_publisher_is_resolved_when_mqtt_starts_after_the_strategy(self):
        # A strategy started before `mqtt start` used to stay silent for its whole run
        self.strategy._pub = None
        self.strategy.mqtt_enabled = False

        with patch("hummingbot.client.hummingbot_application.HummingbotApplication") as mock_app:
            mock_app.main_application.return_value._mqtt = MagicMock()
            with patch("hummingbot.strategy.strategy_v2_base.ETopicPublisher") as mock_pub_cls:
                resolved = self.strategy._resolve_publisher()

        self.assertIsNotNone(resolved)
        self.assertTrue(self.strategy.mqtt_enabled)
        mock_pub_cls.assert_called_once_with("performance", use_bot_prefix=True)

    def test_publisher_is_dropped_when_mqtt_stops_mid_run(self):
        self.strategy._pub = MagicMock()
        self.strategy.mqtt_enabled = True

        with patch("hummingbot.client.hummingbot_application.HummingbotApplication") as mock_app:
            mock_app.main_application.return_value._mqtt = None
            resolved = self.strategy._resolve_publisher()

        self.assertIsNone(resolved)
        self.assertIsNone(self.strategy._pub)
        self.assertFalse(self.strategy.mqtt_enabled)

    def test_parse_markets_str_valid(self):
        test_input = "binance.JASMY-USDT,RLC-USDT:kucoin.BTC-USDT"
        expected_output = {
            "binance": {"JASMY-USDT", "RLC-USDT"},
            "kucoin": {"BTC-USDT"}
        }
        result = StrategyV2ConfigBase.parse_markets_str(test_input)
        self.assertEqual(result, expected_output)

    def test_parse_markets_str_invalid(self):
        test_input = "invalid format"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_markets_str(test_input)

    def test_parse_candles_config_str_valid(self):
        test_input = "binance.JASMY-USDT.1m.500:kucoin.BTC-USDT.5m.200"
        result = StrategyV2ConfigBase.parse_candles_config_str(test_input)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].connector, "binance")
        self.assertEqual(result[0].trading_pair, "JASMY-USDT")
        self.assertEqual(result[0].interval, "1m")
        self.assertEqual(result[0].max_records, 500)

    def test_parse_candles_config_str_invalid_format(self):
        test_input = "invalid.format"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_candles_config_str(test_input)

    def test_parse_candles_config_str_invalid_max_records(self):
        test_input = "binance.JASMY-USDT.1m.invalid"
        with self.assertRaises(ValueError):
            StrategyV2ConfigBase.parse_candles_config_str(test_input)

    def create_mock_executor_config(self):
        return MagicMock(
            timestamp=1234567890,
            trading_pair="ETH-USDT",
            connector_name="binance",
            side="BUY",
            entry_price=Decimal("100"),
            amount=Decimal("1"),
            other_required_field=MagicMock()  # Add other fields as required by specific executor config
        )

    def test_executors_info_to_df(self):
        executor_1 = ExecutorInfo(
            id="1",
            controller_id="controller_1",
            type="position_executor",
            status=RunnableStatus.TERMINATED,
            timestamp=10,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=False,
            is_trading=False,
            custom_info={}
        )
        executor_2 = ExecutorInfo(
            id="2",
            controller_id="controller_2",
            type="position_executor",
            status=RunnableStatus.RUNNING,
            timestamp=20,
            config=PositionExecutorConfig(id="test", timestamp=1234567890, trading_pair="ETH-USDT",
                                          connector_name="binance",
                                          side=TradeType.BUY, entry_price=Decimal("100"), amount=Decimal("1")),
            net_pnl_pct=Decimal(0),
            net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0),
            filled_amount_quote=Decimal(0),
            is_active=True,
            is_trading=True,
            custom_info={}
        )

        executors_info = [executor_1, executor_2]
        df = StrategyV2Base.executors_info_to_df(executors_info)

        # Assertions to validate the DataFrame structure and content
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns),
                         ['id',
                          'timestamp',
                          'type',
                          'status',
                          'config',
                          'net_pnl_pct',
                          'net_pnl_quote',
                          'cum_fees_quote',
                          'filled_amount_quote',
                          'is_active',
                          'is_trading',
                          'custom_info',
                          'close_timestamp',
                          'close_type',
                          'controller_id',
                          'side'])
        self.assertEqual(df.iloc[0]['id'], '2')  # Since the dataframe is sorted by status
        self.assertEqual(df.iloc[1]['id'], '1')
        self.assertEqual(df.iloc[0]['status'], RunnableStatus.RUNNING)
        self.assertEqual(df.iloc[1]['status'], RunnableStatus.TERMINATED)

    def create_mock_performance_report(self):
        return PerformanceReport(
            realized_pnl_quote=Decimal('100'),
            unrealized_pnl_quote=Decimal('50'),
            unrealized_pnl_pct=Decimal('5'),
            realized_pnl_pct=Decimal('10'),
            global_pnl_quote=Decimal('150'),
            global_pnl_pct=Decimal('15'),
            volume_traded=Decimal('1000'),
            close_type_counts={CloseType.TAKE_PROFIT: 10, CloseType.STOP_LOSS: 5}
        )

    def test_format_status(self):
        # Mock dependencies
        self.strategy.ready_to_trade = True
        self.strategy.markets = {"mock_paper_exchange": {"ETH-USDT"}}
        controller_mock = MagicMock()
        controller_mock.to_format_status.return_value = ["Mock status for controller"]
        self.strategy.controllers = {"controller_1": controller_mock}

        mock_report_controller_1 = MagicMock()
        mock_report_controller_1.realized_pnl_quote = Decimal("100.00")
        mock_report_controller_1.unrealized_pnl_quote = Decimal("50.00")
        mock_report_controller_1.global_pnl_quote = Decimal("150.00")
        mock_report_controller_1.global_pnl_pct = Decimal("15.00")
        mock_report_controller_1.volume_traded = Decimal("1000.00")
        mock_report_controller_1.close_type_counts = {CloseType.TAKE_PROFIT: 10, CloseType.STOP_LOSS: 5}

        # Mock executor for the table
        mock_executor = ExecutorInfo(
            id="12312", timestamp=1234567890, status=RunnableStatus.TERMINATED,
            config=self.get_position_config_market_short(), net_pnl_pct=Decimal(0), net_pnl_quote=Decimal(0),
            cum_fees_quote=Decimal(0), filled_amount_quote=Decimal(0), is_active=False, is_trading=False,
            custom_info={}, type="position_executor", controller_id="controller_1")

        # Set up controller_reports with the new structure
        self.strategy.controller_reports = {
            "controller_1": {
                "executors": [mock_executor],
                "positions": [],
                "performance": mock_report_controller_1
            }
        }

        # Call format_status
        status = self.strategy.format_status()

        # Assertions
        self.assertIn("Mock status for controller", status)
        self.assertIn("Controller: controller_1", status)
        self.assertIn("$100.00", status)  # Check for performance data in the summary table
        self.assertIn("$50.00", status)
        self.assertIn("$150.00", status)

    async def test_listen_to_executor_actions(self):
        self.strategy.actions_queue = MagicMock()
        # Simulate some actions being returned, followed by an exception to break the loop.
        self.strategy.actions_queue.get = AsyncMock(side_effect=[
            [CreateExecutorAction(controller_id="controller_1",
                                  executor_config=self.get_position_config_market_short())],
            Exception,
            asyncio.CancelledError,
        ])
        self.strategy.executor_orchestrator.execute_actions = MagicMock()
        controller_mock = MagicMock()
        self.strategy.controllers = {"controller_1": controller_mock}

        # Test for exception handling inside the method.
        try:
            await self.strategy.listen_to_executor_actions()
        except asyncio.CancelledError:
            pass

        # Check assertions here to verify the actions were handled as expected.
        self.assertEqual(self.strategy.executor_orchestrator.execute_actions.call_count, 1)

    def get_position_config_market_short(self):
        return PositionExecutorConfig(id="test-2", timestamp=1234567890, trading_pair="ETH-USDT",
                                      connector_name="binance",
                                      side=TradeType.SELL, entry_price=Decimal("100"), amount=Decimal("1"),
                                      triple_barrier_config=TripleBarrierConfig())
