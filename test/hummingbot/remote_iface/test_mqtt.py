import asyncio
from decimal import Decimal
from test.mock.mock_mqtt_server import FakeMQTTBroker
from typing import Awaitable
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from async_timeout import timeout

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.test_support.mock_paper_exchange import MockPaperExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderExpiredEvent, SellOrderCreatedEvent
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.remote_iface.mqtt import MQTTGateway, MQTTMarketEventForwarder


@patch("hummingbot.remote_iface.mqtt.MQTTGateway._INTERVAL_HEALTH_CHECK", 0.0)
@patch("hummingbot.remote_iface.mqtt.MQTTGateway._INTERVAL_RESTART_LONG", 0.0)
class RemoteIfaceMQTTTests(TestCase):
    # logging.Level required to receive logs from the exchange
    level = 0

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.instance_id = 'TEST_ID'
        cls.fake_err_msg = "Some error"

        cls.command_topics = [
            'start',
            'stop',
            'config',
            'import',
            'status',
            'history',
            'balance/limit',
            'balance/paper',
        ]
        cls.START_URI = 'hbot/$instance_id/start'
        cls.STOP_URI = 'hbot/$instance_id/stop'
        cls.CONFIG_URI = 'hbot/$instance_id/config'
        cls.IMPORT_URI = 'hbot/$instance_id/import'
        cls.STATUS_URI = 'hbot/$instance_id/status'
        cls.HISTORY_URI = 'hbot/$instance_id/history'
        cls.BALANCE_LIMIT_URI = 'hbot/$instance_id/balance/limit'
        cls.BALANCE_PAPER_URI = 'hbot/$instance_id/balance/paper'
        cls.fake_mqtt_broker = FakeMQTTBroker()

    def setUp(self) -> None:
        super().setUp()

        self._original_async_loop = asyncio.get_event_loop()
        self.async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.async_loop)

        self.client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.client_config_map.instance_id = self.instance_id
        self.hbapp = HummingbotApplication(client_config_map=self.client_config_map)
        self.client_config_map.mqtt_bridge.mqtt_port = 1888
        self.client_config_map.mqtt_bridge.mqtt_commands = 1
        self.client_config_map.mqtt_bridge.mqtt_events = 1

        self.log_records = []
        # self.async_run_with_timeout(read_system_configs_from_yml())
        self.gateway = MQTTGateway(self.hbapp)
        self.test_market: MockPaperExchange = MockPaperExchange(
            client_config_map=self.client_config_map)
        self.hbapp.markets = {
            "test_market_paper_trade": self.test_market
        }
        self.resume_test_event = asyncio.Event()
        self.hbapp.logger().setLevel(1)
        self.hbapp.logger().addHandler(self)
        self.gateway.logger().setLevel(1)
        self.gateway.logger().addHandler(self)
        # Restart interval Patcher
        self.restart_interval_patcher = patch(
            'hummingbot.remote_iface.mqtt.MQTTGateway._INTERVAL_RESTART_SHORT',
            new_callable=PropertyMock
        )
        self.addCleanup(self.restart_interval_patcher.stop)
        self.restart_interval_mock = self.restart_interval_patcher.start()
        self.restart_interval_mock.return_value = 0.0
        # MQTT Transport Patcher
        self.mqtt_transport_patcher = patch(
            'commlib.transports.mqtt.MQTTTransport'
        )
        self.addCleanup(self.mqtt_transport_patcher.stop)
        self.mqtt_transport_mock = self.mqtt_transport_patcher.start()
        self.mqtt_transport_mock.side_effect = self.fake_mqtt_broker.create_transport
        # MQTT Patch Loggers Patcher
        self.patch_loggers_patcher = patch(
            'hummingbot.remote_iface.mqtt.MQTTGateway.patch_loggers'
        )
        self.addCleanup(self.patch_loggers_patcher.stop)
        self.patch_loggers_mock = self.patch_loggers_patcher.start()
        self.patch_loggers_mock.return_value = None

    def tearDown(self):
        self.async_loop.run_until_complete(asyncio.sleep(0.1))
        self.gateway.stop()
        del self.gateway
        self.async_loop.run_until_complete(asyncio.sleep(0.1))
        self.fake_mqtt_broker.clear()
        self.restart_interval_patcher.stop()
        self.mqtt_transport_patcher.stop()
        self.patch_loggers_patcher.stop()

        self.async_loop.stop()
        self.async_loop.close()
        asyncio.set_event_loop(self._original_async_loop)

        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and str(record.getMessage()) == str(message) for record in self.log_records)

    async def wait_for_logged(self, log_level: str, message: str):
        try:
            async with timeout(3):
                while not self._is_logged(log_level=log_level, message=message):
                    await asyncio.sleep(0.1)
        except asyncio.TimeoutError as e:
            print(f"Message: {message} was not logged.")
            print(f"Received Logs: {[record.getMessage() for record in self.log_records]}")
            raise e

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.async_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    async def _create_exception_and_unlock_test_with_event_async(self, *args, **kwargs):
        self.resume_test_event.set()
        raise RuntimeError(self.fake_err_msg)

    def _create_exception_and_unlock_test_with_event(self, *args, **kwargs):
        self.resume_test_event.set()
        raise RuntimeError(self.fake_err_msg)

    def _create_exception_and_unlock_test_with_event_not_impl(self, *args, **kwargs):
        self.resume_test_event.set()
        raise NotImplementedError(self.fake_err_msg)

    def is_msg_received(self, *args, **kwargs):
        return self.fake_mqtt_broker.is_msg_received(*args, **kwargs)

    async def wait_for_rcv(self, topic, content=None, msg_key='msg'):
        try:
            async with timeout(3):
                while not self.is_msg_received(topic=topic, content=content, msg_key=msg_key):
                    await asyncio.sleep(0.1)
        except asyncio.TimeoutError as e:
            print(f"Topic: {topic} was not received.")
            print(f"Received Messages: {self.fake_mqtt_broker.received_msgs}")
            raise e

    def start_mqtt(self):
        self.gateway.start()
        self.gateway.start_market_events_fw()

    def get_topic_for(
            self,
            topic
    ):
        return topic.replace('$instance_id', self.hbapp.instance_id)

    def build_fake_strategy(
            self,
            status_check_all_mock: MagicMock,
            load_strategy_config_map_from_file: MagicMock,
            invalid_strategy: bool = True,
            empty_name: bool = False
    ):
        if empty_name:
            strategy_name = ''
        elif invalid_strategy:
            strategy_name = "some_strategy"
        else:
            strategy_name = "avellaneda_market_making"
        status_check_all_mock.return_value = True
        strategy_conf_var = ConfigVar("strategy", None)
        strategy_conf_var.value = strategy_name
        load_strategy_config_map_from_file.return_value = {"strategy": strategy_conf_var}
        return strategy_name

    def send_fake_import_cmd(
            self,
            status_check_all_mock: MagicMock,
            load_strategy_config_map_from_file: MagicMock,
            invalid_strategy: bool = True,
            empty_name: bool = False
    ):
        import_topic = self.get_topic_for(self.IMPORT_URI)

        strategy_name = self.build_fake_strategy(
            status_check_all_mock=status_check_all_mock,
            load_strategy_config_map_from_file=load_strategy_config_map_from_file,
            invalid_strategy=invalid_strategy,
            empty_name=empty_name
        )

        self.fake_mqtt_broker.publish_to_subscription(import_topic, {'strategy': strategy_name})

    @staticmethod
    def emit_order_created_event(
            market: MockPaperExchange,
            order: LimitOrder
    ):
        event_cls = BuyOrderCreatedEvent if order.is_buy else SellOrderCreatedEvent
        event_tag = MarketEvent.BuyOrderCreated if order.is_buy else MarketEvent.SellOrderCreated
        market.trigger_event(
            event_tag,
            message=event_cls(
                order.creation_timestamp,
                OrderType.LIMIT,
                order.trading_pair,
                order.quantity,
                order.price,
                order.client_order_id,
                order.creation_timestamp * 1e-6
            )
        )

    @staticmethod
    def emit_order_expired_event(market: MockPaperExchange):
        event_cls = OrderExpiredEvent
        event_tag = MarketEvent.OrderExpired
        market.trigger_event(
            event_tag,
            message=event_cls(
                1671819499,
                "OID1"
            )
        )

    def build_fake_trades(self):
        ts = 1671819499
        config_file_path = "some-strategy.yml"
        strategy_name = "pure_market_making"
        market = "binance"
        symbol = "HBOT-COINALPHA"
        base_asset = "HBOT"
        quote_asset = "COINALPHA"
        order_id = "OID1"
        order = Order(
            id=order_id,
            config_file_path=config_file_path,
            strategy=strategy_name,
            market=market,
            symbol=symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            creation_timestamp=0,
            order_type="LMT",
            amount=4,
            leverage=0,
            price=Decimal(1000),
            last_status="PENDING",
            last_update_timestamp=0,
        )
        trades = [
            TradeFill(
                config_file_path=config_file_path,
                strategy=strategy_name,
                market=market,
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                timestamp=ts,
                order_id=order_id,
                trade_type=TradeType.BUY.name,
                order_type=OrderType.LIMIT.name,
                price=Decimal(1000),
                amount=Decimal(1),
                trade_fee='{}',
                exchange_trade_id="EOID1",
                order=order),
            TradeFill(
                config_file_path=config_file_path,
                strategy=strategy_name,
                market=market,
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                timestamp=ts,
                order_id=order_id,
                trade_type=TradeType.BUY.name,
                order_type=OrderType.LIMIT.name,
                price=Decimal(1000),
                amount=Decimal(1),
                trade_fee='{}',
                exchange_trade_id="EOID1",
                order=order)
        ]
        trade_list = list([TradeFill.to_bounty_api_json(t) for t in trades])
        for t in trade_list:
            t['trade_timestamp'] = str(t['trade_timestamp'])
        return trade_list

    @patch("hummingbot.client.command.balance_command.BalanceCommand.balance")
    def test_mqtt_command_balance_limit_failure(
            self,
            balance_mock: MagicMock
    ):
        balance_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        msg = {
            'exchange': 'binance',
            'asset': 'BTC-USD',
            'amount': '1.0',
        }

        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.BALANCE_LIMIT_URI), msg)

        self.async_run_with_timeout(self.resume_test_event.wait())

        topic = f"test_reply/hbot/{self.instance_id}/balance/limit"
        msg = {'status': 400, 'msg': self.fake_err_msg, 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.balance_command.BalanceCommand.balance")
    def test_mqtt_command_balance_paper_failure(
            self,
            balance_mock: MagicMock
    ):
        balance_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        msg = {
            'exchange': 'binance',
            'asset': 'BTC-USD',
            'amount': '1.0',
        }

        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.BALANCE_PAPER_URI), msg)

        self.async_run_with_timeout(self.resume_test_event.wait())

        topic = f"test_reply/hbot/{self.instance_id}/balance/paper"
        msg = {'status': 400, 'msg': self.fake_err_msg, 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.config_command.ConfigCommand.config")
    def test_mqtt_command_config_updates_configurable_keys(
            self,
            config_mock: MagicMock
    ):
        config_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        config_msg = {
            'params': [
                ('skata', 90),
            ]
        }

        self.fake_mqtt_broker.publish_to_subscription(
            self.get_topic_for(self.CONFIG_URI),
            config_msg
        )
        topic = f"test_reply/hbot/{self.instance_id}/config"
        msg = {'changes': [], 'config': {}, 'status': 400, 'msg': "Invalid param key(s): ['skata']"}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.config_command.ConfigCommand.config")
    def test_mqtt_command_config_failure(
            self,
            config_mock: MagicMock
    ):
        config_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.CONFIG_URI), {})

        self.async_run_with_timeout(self.resume_test_event.wait())

        topic = f"test_reply/hbot/{self.instance_id}/config"
        msg = {'changes': [], 'config': {}, 'status': 400, 'msg': self.fake_err_msg}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.history_command.HistoryCommand.history")
    def test_mqtt_command_history_failure(
            self,
            history_mock: MagicMock
    ):
        history_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.HISTORY_URI), {})

        self.async_run_with_timeout(self.resume_test_event.wait())

        topic = f"test_reply/hbot/{self.instance_id}/history"
        msg = {'status': 400, 'msg': self.fake_err_msg, 'trades': []}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.import_command.load_strategy_config_map_from_file")
    @patch("hummingbot.client.command.status_command.StatusCommand.status_check_all")
    @patch("hummingbot.client.command.import_command.ImportCommand.import_config_file", new_callable=AsyncMock)
    def test_mqtt_command_import_failure(
            self,
            import_mock: AsyncMock,
            status_check_all_mock: MagicMock,
            load_strategy_config_map_from_file: MagicMock
    ):
        import_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        self.start_mqtt()
        self.send_fake_import_cmd(status_check_all_mock=status_check_all_mock,
                                  load_strategy_config_map_from_file=load_strategy_config_map_from_file,
                                  invalid_strategy=False)

        topic = f"test_reply/hbot/{self.instance_id}/import"
        msg = {'status': 400, 'msg': 'Some error'}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.import_command.load_strategy_config_map_from_file")
    @patch("hummingbot.client.command.status_command.StatusCommand.status_check_all")
    @patch("hummingbot.client.command.import_command.ImportCommand.import_config_file", new_callable=AsyncMock)
    def test_mqtt_command_import_empty_strategy(
            self,
            import_mock: AsyncMock,
            status_check_all_mock: MagicMock,
            load_strategy_config_map_from_file: MagicMock
    ):
        import_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        topic = f"test_reply/hbot/{self.instance_id}/import"
        msg = {'status': 400, 'msg': 'Empty strategy_name given!'}
        self.start_mqtt()
        self.send_fake_import_cmd(status_check_all_mock=status_check_all_mock,
                                  load_strategy_config_map_from_file=load_strategy_config_map_from_file,
                                  invalid_strategy=False,
                                  empty_name=True)
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.status_command.StatusCommand.strategy_status", new_callable=AsyncMock)
    def test_mqtt_command_status_no_strategy_running(
            self,
            strategy_status_mock: AsyncMock
    ):
        strategy_status_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        self.start_mqtt()
        self.fake_mqtt_broker.publish_to_subscription(
            self.get_topic_for(self.STATUS_URI),
            {'async_backend': 0}
        )
        topic = f"test_reply/hbot/{self.instance_id}/status"
        msg = {'status': 400, 'msg': 'No strategy is currently running!', 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.status_command.StatusCommand.strategy_status", new_callable=AsyncMock)
    def test_mqtt_command_status_async(
            self,
            strategy_status_mock: AsyncMock
    ):
        strategy_status_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        self.hbapp.strategy = {}
        self.start_mqtt()
        self.fake_mqtt_broker.publish_to_subscription(
            self.get_topic_for(self.STATUS_URI),
            {'async_backend': 1}
        )
        topic = f"test_reply/hbot/{self.instance_id}/status"
        msg = {'status': 200, 'msg': '', 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))
        self.hbapp.strategy = None

    @patch("hummingbot.client.command.status_command.StatusCommand.strategy_status", new_callable=AsyncMock)
    def test_mqtt_command_status_sync(
            self,
            strategy_status_mock: AsyncMock
    ):
        strategy_status_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        self.hbapp.strategy = {}
        self.start_mqtt()
        self.fake_mqtt_broker.publish_to_subscription(
            self.get_topic_for(self.STATUS_URI),
            {'async_backend': 0}
        )
        topic = f"test_reply/hbot/{self.instance_id}/status"
        msg = {'status': 400, 'msg': 'Some error', 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))
        self.hbapp.strategy = None

    @patch("hummingbot.client.command.status_command.StatusCommand.strategy_status", new_callable=AsyncMock)
    def test_mqtt_command_status_failure(
            self,
            strategy_status_mock: AsyncMock
    ):
        strategy_status_mock.side_effect = self._create_exception_and_unlock_test_with_event_async
        self.start_mqtt()
        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.STATUS_URI), {})
        topic = f"test_reply/hbot/{self.instance_id}/status"
        msg = {'status': 400, 'msg': 'No strategy is currently running!', 'data': ''}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    @patch("hummingbot.client.command.stop_command.StopCommand.stop")
    def test_mqtt_command_stop_failure(
            self,
            stop_mock: MagicMock
    ):
        stop_mock.side_effect = self._create_exception_and_unlock_test_with_event
        self.start_mqtt()

        self.fake_mqtt_broker.publish_to_subscription(self.get_topic_for(self.STOP_URI), {})

        self.async_run_with_timeout(self.resume_test_event.wait())

        topic = f"test_reply/hbot/{self.instance_id}/stop"
        msg = {'status': 400, 'msg': self.fake_err_msg}
        self.async_run_with_timeout(self.wait_for_rcv(topic, msg, msg_key='data'), timeout=10)
        self.assertTrue(self.is_msg_received(topic, msg, msg_key='data'))

    def test_mqtt_event_buy_order_created(self):
        self.start_mqtt()

        order = LimitOrder(client_order_id="HBOT_1",
                           trading_pair="HBOT-USDT",
                           is_buy=True,
                           base_currency="HBOT",
                           quote_currency="USDT",
                           price=Decimal("100"),
                           quantity=Decimal("1.5")
                           )

        self.emit_order_created_event(self.test_market, order)

        events_topic = f"hbot/{self.instance_id}/events"

        evt_type = "BuyOrderCreated"
        self.async_run_with_timeout(self.wait_for_rcv(events_topic, evt_type, msg_key='type'), timeout=10)
        self.assertTrue(self.is_msg_received(events_topic, evt_type, msg_key='type'))

    def test_mqtt_event_sell_order_created(self):
        self.start_mqtt()

        order = LimitOrder(client_order_id="HBOT_1",
                           trading_pair="HBOT-USDT",
                           is_buy=False,
                           base_currency="HBOT",
                           quote_currency="USDT",
                           price=Decimal("100"),
                           quantity=Decimal("1.5")
                           )

        self.emit_order_created_event(self.test_market, order)

        events_topic = f"hbot/{self.instance_id}/events"

        evt_type = "SellOrderCreated"
        self.async_run_with_timeout(self.wait_for_rcv(events_topic, evt_type, msg_key='type'), timeout=10)
        self.assertTrue(self.is_msg_received(events_topic, evt_type, msg_key='type'))

    def test_mqtt_event_order_expired(self):
        self.start_mqtt()

        self.emit_order_expired_event(self.test_market)

        events_topic = f"hbot/{self.instance_id}/events"

        evt_type = "OrderExpired"
        self.async_run_with_timeout(self.wait_for_rcv(events_topic, evt_type, msg_key='type'), timeout=10)
        self.assertTrue(self.is_msg_received(events_topic, evt_type, msg_key='type'))

    def test_mqtt_subscribed_topics(self):
        self.start_mqtt()
        self.assertTrue(self.gateway is not None)
        subscribed_mqtt_topics = sorted(list([f"hbot/{self.instance_id}/{topic}"
                                              for topic in (self.command_topics + ['external/event/*'])]))
        self.assertEqual(subscribed_mqtt_topics, sorted(list(self.fake_mqtt_broker.subscriptions.keys())))

    @patch("hummingbot.remote_iface.mqtt.mqtts_logger", None)
    def test_mqtt_eventforwarder_logger(self):
        self.assertTrue(MQTTMarketEventForwarder.logger() is not None)
        self.start_mqtt()

    def test_mqtt_eventforwarder_unknown_events(self):
        self.start_mqtt()
        test_evt = {"unknown": "you don't know me"}
        self.gateway._market_events._send_mqtt_event(event_tag=999,
                                                     pubsub=None,
                                                     event=test_evt)

        events_topic = f"hbot/{self.instance_id}/events"

        evt_type = "Unknown"
        self.async_run_with_timeout(self.wait_for_rcv(events_topic, evt_type, msg_key='type'), timeout=10)
        self.assertTrue(self.is_msg_received(events_topic, evt_type, msg_key='type'))
        self.assertTrue(self.is_msg_received(events_topic, test_evt, msg_key='data'))

    def test_mqtt_eventforwarder_invalid_events(self):
        self.start_mqtt()
        self.gateway._market_events._send_mqtt_event(event_tag=999,
                                                     pubsub=None,
                                                     event="i feel empty")

        events_topic = f"hbot/{self.instance_id}/events"

        evt_type = "Unknown"
        self.async_run_with_timeout(
            self.wait_for_rcv(events_topic, evt_type, msg_key='type'), timeout=10)
        self.assertTrue(self.is_msg_received(events_topic, evt_type, msg_key='type'))
        self.assertTrue(self.is_msg_received(events_topic, {}, msg_key='data'))

    def test_mqtt_notifier_fakes(self):
        self.start_mqtt()
        self.assertEqual(self.gateway._notifier.start(), None)
        self.assertEqual(self.gateway._notifier.stop(), None)

    def test_mqtt_gateway_check_health(self):
        tmp = self.gateway._start_health_monitoring_loop
        self.gateway._start_health_monitoring_loop = lambda: None
        self.start_mqtt()
        self.assertTrue(self.gateway._check_connections())
        self.gateway._rpc_services[0]._transport._connected = False
        self.assertFalse(self.gateway._check_connections())
        self.gateway._rpc_services[0]._transport._connected = True
        s = self.gateway.create_subscriber(topic='TEST', on_message=lambda x: {})
        s.run()
        self.assertTrue(self.gateway._check_connections())
        s._transport._connected = False
        self.assertFalse(self.gateway._check_connections())
        prev_pub = self.gateway._publishers
        prev__sub = self.gateway._subscribers
        self.gateway._publishers = []
        self.gateway._subscribers = []
        self.gateway._rpc_services[0]._transport._connected = False
        self.assertFalse(self.gateway._check_connections())
        self.gateway._publishers = prev_pub
        self.gateway._subscribers = prev__sub
        self.gateway._start_health_monitoring_loop = tmp

    @patch("hummingbot.remote_iface.mqtt.MQTTGateway.health", new_callable=PropertyMock)
    def test_mqtt_gateway_check_health_restarts(
            self,
            health_mock: PropertyMock
    ):
        health_mock.return_value = True
        status_topic = f"hbot/{self.instance_id}/status_updates"
        self.start_mqtt()
        self.async_run_with_timeout(
            self.wait_for_logged("DEBUG", f"Started Heartbeat Publisher <hbot/{self.instance_id}/hb>"), timeout=10)
        self.async_run_with_timeout(self.wait_for_rcv(status_topic, 'online'), timeout=10)
        self.async_run_with_timeout(self.wait_for_logged("DEBUG", "Monitoring MQTT Gateway health for disconnections."),
                                    timeout=10)
        self.log_records.clear()
        health_mock.return_value = False
        self.restart_interval_mock.return_value = None
        self.async_run_with_timeout(
            self.wait_for_logged("WARNING", "MQTT Gateway is disconnected, attempting to reconnect."), timeout=10)
        fake_err = "'<=' not supported between instances of 'NoneType' and 'int'"
        self.async_run_with_timeout(self.wait_for_logged("ERROR",
                                                         f"MQTT Gateway failed to reconnect: {fake_err}. Sleeping 10 seconds before retry."),
                                    timeout=10)
        self.assertFalse(
            self._is_logged(
                "WARNING",
                "MQTT Gateway successfully reconnected.",
            )
        )
        self.assertTrue(self.is_msg_received(status_topic, 'offline'))
        self.log_records.clear()
        self.restart_interval_mock.return_value = 0.0
        self.hbapp.strategy = True
        self.async_run_with_timeout(
            self.wait_for_logged("WARNING", "MQTT Gateway is disconnected, attempting to reconnect."), timeout=10)
        health_mock.return_value = True
        self.async_run_with_timeout(self.wait_for_logged("WARNING", "MQTT Gateway successfully reconnected."),
                                    timeout=10)
        self.assertTrue(
            self._is_logged(
                "WARNING",
                "MQTT Gateway successfully reconnected.",
            )
        )

    def test_mqtt_gateway_stop(self):
        self.start_mqtt()
        self.assertTrue(self.gateway._check_connections())
        self.gateway.stop()
        self.assertFalse(self.gateway._check_connections())

    def test_eevent_queue_factory(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import EEventQueueFactory, ExternalEventFactory
        queue = ExternalEventFactory.create_queue('test')
        self.assertTrue(queue is not None)

        from collections import deque
        dq = deque()
        EEventQueueFactory._on_event(dq, {'a': 1}, 'testevent')
        self.assertTrue(1)

    def test_eevent_listener_factory(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import ExternalEventFactory

        def clb(msg, event_name):
            pass

        ExternalEventFactory.create_async('test.a.b', clb)
        ExternalEventFactory.remove_listener('test.a.b', clb)
        try:
            MQTTGateway._instance = None
            ExternalEventFactory.create_async('test.a.b', clb)
            ExternalEventFactory.remove_listener('test.a.b', clb)
        except Exception:
            self.assertTrue(1)
        else:
            self.assertTrue(0)

    def test_etopic_queue_factory(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import ETopicQueueFactory, ExternalTopicFactory
        queue = ExternalTopicFactory.create_queue('test/a/b')
        self.assertTrue(queue is not None)

        from collections import deque
        dq = deque()
        ETopicQueueFactory._on_message(dq, {'a': 1}, 'test/external')
        self.assertTrue(1)

    def test_etopic_listener_factory(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import ExternalTopicFactory

        def clb(msg, topic):
            pass

        listener = ExternalTopicFactory.create_async('test/a/b', clb)
        self.assertTrue(listener is not None)
        ExternalTopicFactory.remove_listener(listener)

    def test_external_events_add_remove(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import MQTTGateway

        def clb(msg, event_name):
            pass

        gw = MQTTGateway.main()
        self.assertTrue(len(gw._external_events._listeners.get('*')) == 0)
        gw.add_external_event_listener('*', clb)
        self.assertTrue(len(gw._external_events._listeners.get('*')) == 1)
        gw.remove_external_event_listener('*', clb)
        self.assertTrue(len(gw._external_events._listeners.get('*')) == 0)
        gw.add_external_event_listener('test.a.b', clb)
        self.assertTrue(len(gw._external_events._listeners.get('test.a.b')) == 1)
        gw.remove_external_event_listener('test.a.b', clb)
        self.assertTrue(len(gw._external_events._listeners.get('test.a.b')) == 0)

    def test_mqtt_log_handler(self):
        import logging

        from hummingbot.logger import HummingbotLogger
        from hummingbot.remote_iface.mqtt import MQTTLogHandler
        self.start_mqtt()

        handler = MQTTLogHandler(self.hbapp, self.gateway)
        handler.emit(logging.LogRecord('', 1, '', '', '', '', ''))
        self.assertTrue(1)

        logger = HummingbotLogger('testlogger')
        self.gateway.add_log_handler(logger)
        self.gateway.remove_log_handler(logger)
        logger = self.gateway._get_root_logger()
        self.assertTrue(logger is not None)
        self.gateway._remove_log_handlers()

    def test_market_events(self):
        self.start_mqtt()
        from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee
        from hummingbot.remote_iface.mqtt import MQTTGateway

        gw = MQTTGateway.main()
        gw._market_events._make_event_payload({
            'a': 'a',
            'b': 1,
            'c': Decimal('1.0'),
            'd': DeductedFromReturnsTradeFee(),
            'e': AddedToCostTradeFee(),
            'f': {'a': 1},
            'type': 'TEST',
            'order_type': 'BUY',
            'trade_type': 'LIMIT',
        })
        self.assertTrue(1)

    def test_etopic_listener_class(self):
        from hummingbot.remote_iface.mqtt import ETopicListener

        def clb(msg, topic):
            pass

        listener = ETopicListener('test', clb, use_bot_prefix=False)
        self.assertTrue(listener is not None)
        listener = ETopicListener('test', clb, use_bot_prefix=True)
        self.assertTrue(listener is not None)

        self.start_mqtt()
        listener = ETopicListener('test', clb, use_bot_prefix=True)
        self.assertTrue(listener is not None)

        prev_gw = MQTTGateway.main()
        MQTTGateway._instance = None
        try:
            listener = ETopicListener('test', clb, use_bot_prefix=False)
        except Exception:
            self.assertTrue(1)
        else:
            self.assertFalse(1)
        MQTTGateway._instance = prev_gw

    def test_eevent_queue_factory_class(self):
        from hummingbot.remote_iface.mqtt import EEventQueueFactory
        self.start_mqtt()

        equeue = EEventQueueFactory.create(event_name='test', queue_size=2)
        self.assertTrue(equeue is not None)

        prev_gw = MQTTGateway.main()
        MQTTGateway._instance = None
        try:
            equeue = EEventQueueFactory.create(event_name='test', queue_size=2)
        except Exception:
            self.assertTrue(1)
        else:
            self.assertFalse(1)
        MQTTGateway._instance = prev_gw

    def test_eevent_listener_factory_class(self):
        from hummingbot.remote_iface.mqtt import EEventListenerFactory
        self.start_mqtt()

        def clb(msg, topic):
            pass

        EEventListenerFactory.create(event_name='test', callback=clb)
        prev_gw = MQTTGateway.main()
        MQTTGateway._instance = None
        try:
            EEventListenerFactory.create(event_name='test',
                                         callback=clb)
        except Exception:
            self.assertTrue(1)
        else:
            self.assertFalse(1)
        try:
            EEventListenerFactory.remove(event_name='test',
                                         callback=clb)
        except Exception:
            self.assertTrue(1)
        else:
            self.assertFalse(1)
        MQTTGateway._instance = prev_gw

    def test_mqtt_external_events_class(self):
        from hummingbot.remote_iface.messages import ExternalEventMessage
        from hummingbot.remote_iface.mqtt import MQTTExternalEvents

        self.start_mqtt()

        def clb(msg, topic):
            pass

        eevents = MQTTExternalEvents(self.hbapp, self.gateway)
        eevents.add_global_listener(clb)
        ename = eevents._event_uri_to_name('hbot/bot1/external/event/e1')
        self.assertTrue(ename == "e1")
        eevents.add_listener('e1', clb)
        eevents.add_listener('e1', clb)
        eevents._on_event_arrived(ExternalEventMessage(),
                                  'hbot/bot1/external/event/e1')
        self.assertTrue(len(eevents._listeners) == 2)
        self.assertTrue('*' in eevents._listeners)
        self.assertTrue('e1' in eevents._listeners)
        self.assertTrue(ename in eevents._listeners)

        eevents._listeners = {}
        eevents.add_global_listener(clb)
        eevents.remove_global_listener(clb)
        eevents.add_listener('test_event', clb)
        eevents.remove_listener('test_event', clb)
        eevents.add_listener('test_event', clb)
        eevents.add_listener('test_event', clb)
        eevents.remove_listener('test_event', clb)

    def test_mqtt_gateway_health(self):
        health = self.gateway.health
        self.assertFalse(health)

    def test_mqtt_gateway_namespace_wrong_lastchar(self):
        prev_ns = self.gateway._hb_app.client_config_map.mqtt_bridge.mqtt_namespace
        self.gateway._hb_app.client_config_map.mqtt_bridge.mqtt_namespace = 'test/'
        gw = MQTTGateway(self.hbapp)
        self.assertTrue(gw.namespace == 'test')
        gw.stop()
        del gw
        self.gateway._hb_app.client_config_map.mqtt_bridge.mqtt_namespace = prev_ns

    def test_etopic_publisher(self):
        self.start_mqtt()
        from hummingbot.remote_iface.mqtt import EMTopicPublisher, ETopicPublisher
        test_msg = {
            "a": "test",
            "b": 1,
            "c": False,
            "d": {},
            "e": []
        }
        pub = ETopicPublisher('test/a/b', use_bot_prefix=False)
        pub.send(test_msg)
        self.assertTrue(1)
        pub2 = EMTopicPublisher(use_bot_prefix=False)
        pub2.send("test/a/b", test_msg)
        pub2.send("test/c/d", test_msg)
        self.assertTrue(1)


class MQTTNotifierContractTests(TestCase):
    """The notifier only works if its method name matches what NotifierBase's callers use.

    A rename in NotifierBase once left MQTTNotifier overriding nothing, so every notification
    fell through to the base class and sat in a queue that nothing drained - MQTTNotifier
    overrides start() to a no-op, so the draining task was never created. Nothing reached the
    /notify topic. These tests pin the contract so that cannot happen silently again.
    """

    def test_notifier_overrides_the_method_its_callers_actually_call(self):
        from hummingbot.notifier.notifier_base import NotifierBase
        from hummingbot.remote_iface.mqtt import MQTTNotifier

        self.assertIn("add_message_to_queue", vars(MQTTNotifier))
        self.assertIsNot(
            MQTTNotifier.add_message_to_queue,
            NotifierBase.add_message_to_queue,
            "MQTTNotifier must override add_message_to_queue, not shadow some other name",
        )

    def test_notify_publishes_instead_of_queueing(self):
        from hummingbot.remote_iface.mqtt import MQTTNotifier

        notifier = MQTTNotifier.__new__(MQTTNotifier)
        notifier._message_queue = asyncio.Queue()
        notifier.notify_pub = MagicMock()

        MQTTNotifier.add_message_to_queue(notifier, "an order was placed")

        notifier.notify_pub.publish.assert_called_once()
        published = notifier.notify_pub.publish.call_args[0][0]
        self.assertEqual("an order was placed", published.msg)
        # ...and nothing was left sitting in the base class queue
        self.assertTrue(notifier._message_queue.empty())


class ToMQTTPayloadTests(TestCase):
    """`to_mqtt_payload` is a pure function - it needs no gateway and no broker."""

    def test_to_mqtt_payload_scalars(self):
        from hummingbot.remote_iface.mqtt import to_mqtt_payload

        # Decimals become floats, so that consumers can do arithmetic on them
        self.assertEqual(1.5, to_mqtt_payload(Decimal("1.5")))
        self.assertIsInstance(to_mqtt_payload(Decimal("1.5")), float)
        # Enums become their name, not their raw value
        self.assertEqual("TradeType.BUY", to_mqtt_payload(TradeType.BUY))
        # Everything else is left alone
        for value in ("a", 1, 1.5, True, None):
            self.assertEqual(value, to_mqtt_payload(value))

    def test_to_mqtt_payload_containers(self):
        from hummingbot.remote_iface.mqtt import to_mqtt_payload
        self.assertEqual([1.5, 2.5], to_mqtt_payload([Decimal("1.5"), Decimal("2.5")]))
        self.assertEqual([1.5], to_mqtt_payload((Decimal("1.5"),)))
        self.assertEqual({"a": {"b": [1.5]}},
                         to_mqtt_payload({"a": {"b": [Decimal("1.5")]}}))
        # Enum keys are converted too, otherwise the dict is not JSON serializable
        self.assertEqual({"TradeType.BUY": 1}, to_mqtt_payload({TradeType.BUY: 1}))

    def test_to_mqtt_payload_pydantic_model_is_json_serializable(self):
        import json

        from hummingbot.remote_iface.mqtt import to_mqtt_payload
        from hummingbot.strategy_v2.models.executors import CloseType
        from hummingbot.strategy_v2.models.executors_info import PerformanceReport

        report = PerformanceReport(
            realized_pnl_quote=Decimal("5.67"),
            volume_traded=Decimal("1000.5"),
            close_type_counts={CloseType.TAKE_PROFIT: 3},
        )
        payload = to_mqtt_payload(report)

        self.assertEqual(5.67, payload["realized_pnl_quote"])
        self.assertIsInstance(payload["realized_pnl_quote"], float)
        self.assertEqual({"CloseType.TAKE_PROFIT": 3}, payload["close_type_counts"])
        # The whole point: this has to survive the trip through the broker
        self.assertEqual(payload, json.loads(json.dumps(payload)))


class _FakeLoop:
    """Deterministic stand-in for the event loop's clock and call_later."""

    def __init__(self):
        self.now = 1000.0
        self._scheduled = []

    def time(self):
        return self.now

    def call_later(self, delay, callback, *args):
        handle = MagicMock()
        self._scheduled.append((self.now + delay, callback, args, handle))
        return handle

    def advance(self, seconds):
        self.now += seconds
        due = [item for item in self._scheduled if item[0] <= self.now]
        self._scheduled = [item for item in self._scheduled if item[0] > self.now]
        for _, callback, args, handle in due:
            if not handle.cancel.called:
                callback(*args)


class PublishThrottleTests(TestCase):

    def setUp(self):
        from hummingbot.remote_iface.mqtt import PublishThrottle
        self.loop = _FakeLoop()
        self.published = []
        self.throttle = PublishThrottle(self.loop, 0.1, lambda key, trigger: self.published.append((key, trigger)))

    def test_first_change_publishes_immediately(self):
        self.throttle.request("a", "order_book")
        self.assertEqual([("a", "order_book")], self.published)

    def test_burst_collapses_to_one_trailing_publish_with_the_latest_trigger(self):
        self.throttle.request("a", "order_book")
        for trigger in ("order_book", "trade", "funding"):
            self.throttle.request("a", trigger)
        self.assertEqual([("a", "order_book")], self.published)

        self.loop.advance(0.1)

        self.assertEqual([("a", "order_book"), ("a", "funding")], self.published)

    def test_keys_are_throttled_independently(self):
        self.throttle.request("a", "x")
        self.throttle.request("b", "y")
        self.assertEqual([("a", "x"), ("b", "y")], self.published)

    def test_publishes_immediately_again_once_the_interval_has_passed(self):
        self.throttle.request("a", "x")
        self.loop.advance(0.2)
        self.throttle.request("a", "y")
        self.assertEqual([("a", "x"), ("a", "y")], self.published)

    def test_cancel_drops_pending_publishes(self):
        self.throttle.request("a", "x")
        self.throttle.request("a", "y")
        self.throttle.cancel()
        self.loop.advance(1)
        self.assertEqual([("a", "x")], self.published)

    def test_mark_published_defers_the_next_change(self):
        self.throttle.mark_published("a")
        self.throttle.request("a", "x")
        self.assertEqual([], self.published)

        self.loop.advance(0.1)

        self.assertEqual([("a", "x")], self.published)


class MarketDataPayloadTests(TestCase):
    trading_pair = "ZEC-USDT"

    def setUp(self):
        from hummingbot.core.data_type.order_book import OrderBook
        from hummingbot.core.data_type.order_book_row import OrderBookRow
        self.order_book = OrderBook()
        self.order_book.apply_snapshot(
            [OrderBookRow(100.0 - i, 1.0 + i, 7) for i in range(15)],
            [OrderBookRow(101.0 + i, 2.0 + i, 7) for i in range(15)],
            7,
        )
        self.connector = MagicMock()
        self.connector.name = "coindcx_perpetual"
        self.connector.get_order_book.return_value = self.order_book
        self.connector.get_funding_info.side_effect = KeyError(self.trading_pair)

    def _build(self, last_trade=None, depth=10):
        from hummingbot.remote_iface.mqtt import build_market_data_payload
        return build_market_data_payload(self.connector, self.trading_pair, depth=depth, trigger="order_book",
                                         last_trade=last_trade)

    def test_book_is_cut_to_depth_with_the_best_price_first(self):
        book = self._build()["order_book"]

        self.assertEqual(10, len(book["bids"]))
        self.assertEqual(10, len(book["asks"]))
        self.assertEqual([100.0, 1.0], book["bids"][0])
        self.assertEqual([91.0, 10.0], book["bids"][-1])
        self.assertEqual([101.0, 2.0], book["asks"][0])
        self.assertEqual([110.0, 11.0], book["asks"][-1])

    def test_top_of_book_mid_and_spread(self):
        payload = self._build()

        self.assertEqual(("coindcx_perpetual", self.trading_pair, "order_book", 7),
                         (payload["connector"], payload["trading_pair"], payload["trigger"], payload["update_id"]))
        self.assertEqual((100.0, 101.0, 100.5), (payload["best_bid"], payload["best_ask"], payload["mid_price"]))
        self.assertAlmostEqual(1.0 / 100.5, payload["spread_pct"])

    def test_no_trade_yet_is_null_rather_than_nan(self):
        self.assertIsNone(self._build()["last_trade_price"])

    def test_last_trade_is_included(self):
        from hummingbot.core.data_type.common import TradeType
        from hummingbot.core.event.events import OrderBookTradeEvent
        trade = OrderBookTradeEvent(self.trading_pair, 1789.0, TradeType.SELL, 100.5, 0.3, "t-1")
        self.order_book.apply_trade(trade)

        payload = self._build(last_trade=trade)

        self.assertEqual(100.5, payload["last_trade_price"])
        self.assertEqual(
            {"timestamp": 1789.0, "type": "TradeType.SELL", "price": 100.5, "amount": 0.3, "trade_id": "t-1"},
            payload["last_trade"])

    def test_funding_is_included_for_perpetuals(self):
        from hummingbot.core.data_type.funding_info import FundingInfo
        self.connector.get_funding_info.side_effect = None
        self.connector.get_funding_info.return_value = FundingInfo(
            self.trading_pair, Decimal("100.1"), Decimal("100.2"), 1789142400, Decimal("0.0001"))

        self.assertEqual(
            {"rate": 0.0001, "mark_price": 100.2, "index_price": 100.1, "next_funding_utc_timestamp": 1789142400},
            self._build()["funding"])

    def test_no_funding_when_the_connector_has_none(self):
        self.assertIsNone(self._build()["funding"])

    def test_nothing_is_built_until_the_pair_has_an_order_book(self):
        self.connector.get_order_book.side_effect = ValueError("No order book exists")
        self.assertIsNone(self._build())

    def test_empty_book_has_null_prices(self):
        from hummingbot.core.data_type.order_book import OrderBook
        self.connector.get_order_book.return_value = OrderBook()

        payload = self._build()

        self.assertEqual({"bids": [], "asks": []}, payload["order_book"])
        self.assertEqual((None, None, None, None),
                         (payload["best_bid"], payload["best_ask"], payload["mid_price"], payload["spread_pct"]))

    def test_payload_fits_the_message_and_survives_json(self):
        import json

        from hummingbot.remote_iface.messages import MarketDataMessage
        dumped = MarketDataMessage(**self._build()).model_dump()
        self.assertEqual(dumped, json.loads(json.dumps(dumped)))


class AccountDataPayloadTests(TestCase):

    def setUp(self):
        from hummingbot.connector.derivative.position import Position
        from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
        from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
        available = {"USDT": Decimal("900.1"), "ZEC": Decimal("0")}
        self.connector = MagicMock()
        self.connector.name = "coindcx_perpetual"
        self.connector.get_all_balances.return_value = {"USDT": Decimal("923.4"), "ZEC": Decimal("0.008")}
        self.connector.get_available_balance.side_effect = lambda asset: available[asset]
        self.connector.account_positions = {
            "ZEC-USDT": Position("ZEC-USDT", PositionSide.LONG, Decimal("0.03"), Decimal("797.2"),
                                 Decimal("0.008"), Decimal("1")),
        }
        self.connector.in_flight_orders = {
            "haveli-1": InFlightOrder(
                client_order_id="haveli-1",
                trading_pair="ZEC-USDT",
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("0.008"),
                creation_timestamp=1789.0,
                price=Decimal("796"),
                exchange_order_id="ex-1",
                initial_state=OrderState.OPEN,
                leverage=1,
                position=PositionAction.OPEN,
            ),
        }

    def _build(self, connector=None):
        from hummingbot.remote_iface.mqtt import build_account_data_payload
        return build_account_data_payload(connector or self.connector, "OrderFilled")

    def test_balances_carry_total_and_available(self):
        payload = self._build()

        self.assertEqual(("coindcx_perpetual", "OrderFilled"), (payload["connector"], payload["trigger"]))
        self.assertEqual({"USDT": {"total": 923.4, "available": 900.1}, "ZEC": {"total": 0.008, "available": 0.0}},
                         payload["balances"])

    def test_positions(self):
        self.assertEqual(
            [{"trading_pair": "ZEC-USDT", "side": "PositionSide.LONG", "amount": 0.008, "entry_price": 797.2,
              "unrealized_pnl": 0.03, "leverage": 1.0}],
            self._build()["positions"])

    def test_open_orders(self):
        self.assertEqual(
            [{"client_order_id": "haveli-1", "exchange_order_id": "ex-1", "trading_pair": "ZEC-USDT",
              "trade_type": "TradeType.BUY", "order_type": "OrderType.LIMIT", "price": 796.0, "amount": 0.008,
              "executed_amount_base": 0.0, "state": "OrderState.OPEN", "position": "PositionAction.OPEN",
              "leverage": 1, "creation_timestamp": 1789.0}],
            self._build()["open_orders"])

    def test_spot_connector_has_no_positions(self):
        from types import SimpleNamespace
        spot = SimpleNamespace(
            name="binance",
            get_all_balances=lambda: {"BTC": Decimal("1")},
            get_available_balance=lambda asset: Decimal("0.5"),
            in_flight_orders={},
        )

        payload = self._build(spot)

        self.assertEqual([], payload["positions"])
        self.assertEqual({"BTC": {"total": 1.0, "available": 0.5}}, payload["balances"])

    def test_payload_fits_the_message_and_survives_json(self):
        import json

        from hummingbot.remote_iface.messages import AccountDataMessage
        dumped = AccountDataMessage(**self._build()).model_dump()
        self.assertEqual(dumped, json.loads(json.dumps(dumped)))


def _fake_connector_class():
    from hummingbot.connector.perpetual_trading import PerpetualTrading
    from hummingbot.core.data_type.order_book import OrderBook
    from hummingbot.core.data_type.order_book_row import OrderBookRow
    from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
    from hummingbot.core.pubsub import PubSub

    class FakeLivePerpetualConnector(PubSub):
        """A live perpetual connector made of the real tracker, order book, perpetual state and PubSub."""

        def __init__(self, name, trading_pair):
            super().__init__()
            self.name = name
            self.ready = True
            self.order_book_tracker = OrderBookTracker(data_source=MagicMock(), trading_pairs=[trading_pair])
            order_book = OrderBook()
            order_book.apply_snapshot([OrderBookRow(100.0, 1.0, 1)], [OrderBookRow(101.0, 1.0, 1)], 1)
            self.order_book_tracker._order_books[trading_pair] = order_book
            self._perpetual_trading = PerpetualTrading([trading_pair])
            self.balances = {"USDT": Decimal("923.4")}
            self.in_flight_orders = {}

        @property
        def order_books(self):
            return self.order_book_tracker.order_books

        @property
        def account_positions(self):
            return self._perpetual_trading.account_positions

        def get_order_book(self, trading_pair):
            if trading_pair not in self.order_books:
                raise ValueError(f"No order book exists for '{trading_pair}'.")
            return self.order_books[trading_pair]

        def get_funding_info(self, trading_pair):
            return self._perpetual_trading.get_funding_info(trading_pair)

        def get_all_balances(self):
            return dict(self.balances)

        def get_available_balance(self, asset):
            return self.balances[asset]

    return FakeLivePerpetualConnector


class MQTTDataPublisherTests(TestCase):
    trading_pair = "ZEC-USDT"

    def setUp(self):
        self.loop = _FakeLoop()
        self.connector = _fake_connector_class()("coindcx_perpetual", self.trading_pair)
        self.hb_app = MagicMock()
        self.hb_app.ev_loop = self.loop
        self.hb_app.instance_id = "bot-1"
        self.hb_app.markets = {"coindcx_perpetual": self.connector}
        self.hb_app.client_config_map.mqtt_bridge.mqtt_market_data_depth = 10
        self.hb_app.client_config_map.mqtt_bridge.mqtt_data_snapshot_interval = 5.0
        self.node = MagicMock()
        self.node.namespace = "hbot"
        self.snapshot_task = MagicMock()
        patcher = patch("hummingbot.remote_iface.mqtt.safe_ensure_future", side_effect=self._start_task)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _start_task(self, coroutine, loop=None):
        coroutine.close()  # the snapshot loop is exercised through publish_snapshot()
        return self.snapshot_task

    def _market(self):
        from hummingbot.remote_iface.mqtt import MQTTMarketDataPublisher
        publisher = MQTTMarketDataPublisher(self.hb_app, self.node)
        self.addCleanup(publisher.stop)
        return publisher

    def _account(self):
        from hummingbot.remote_iface.mqtt import MQTTAccountDataPublisher
        publisher = MQTTAccountDataPublisher(self.hb_app, self.node)
        self.addCleanup(publisher.stop)
        return publisher

    @staticmethod
    def _published(publisher):
        return [call.args[0] for call in publisher.publisher.publish.call_args_list]

    def _trade(self, price=100.5):
        from hummingbot.core.data_type.common import TradeType
        from hummingbot.core.event.events import OrderBookTradeEvent
        return OrderBookTradeEvent(self.trading_pair, 1789.0, TradeType.BUY, price, 0.2, "t-9")

    # ---------------------------------------------------------------- market data

    def test_market_data_topic(self):
        self._market()
        self.assertEqual("hbot/bot-1/market_data", self.node.create_publisher.call_args.kwargs["topic"])

    def test_order_book_change_publishes_immediately(self):
        publisher = self._market()

        self.connector.order_book_tracker._notify_order_book_updated(self.trading_pair)

        messages = self._published(publisher)
        self.assertEqual(1, len(messages))
        self.assertEqual(("coindcx_perpetual", self.trading_pair, "order_book", 100.0),
                         (messages[0].connector, messages[0].trading_pair, messages[0].trigger, messages[0].best_bid))

    def test_busy_order_book_is_capped_at_ten_a_second_per_pair(self):
        publisher = self._market()

        for _ in range(20):
            self.connector.order_book_tracker._notify_order_book_updated(self.trading_pair)
        self.assertEqual(1, len(self._published(publisher)))

        self.loop.advance(0.1)
        self.assertEqual(2, len(self._published(publisher)))

    def test_trade_publishes_with_the_last_trade(self):
        publisher = self._market()

        self.connector.get_order_book(self.trading_pair).apply_trade(self._trade())

        message = self._published(publisher)[-1]
        self.assertEqual("trade", message.trigger)
        self.assertEqual(100.5, message.last_trade["price"])

    def test_funding_update_publishes(self):
        from hummingbot.core.data_type.funding_info import FundingInfo
        publisher = self._market()

        self.connector._perpetual_trading.initialize_funding_info(
            FundingInfo(self.trading_pair, Decimal("100"), Decimal("100.2"), 1789142400, Decimal("0.0001")))

        message = self._published(publisher)[-1]
        self.assertEqual("funding", message.trigger)
        self.assertEqual(0.0001, message.funding["rate"])

    def test_market_snapshot_covers_every_pair_with_a_book(self):
        publisher = self._market()

        publisher.publish_snapshot()

        self.assertEqual([("snapshot", self.trading_pair)],
                         [(message.trigger, message.trading_pair) for message in self._published(publisher)])

    def test_market_data_stop_detaches_every_listener(self):
        publisher = self._market()

        publisher.stop()
        self.connector.order_book_tracker._notify_order_book_updated(self.trading_pair)
        self.connector.get_order_book(self.trading_pair).apply_trade(self._trade())

        self.assertEqual([], self._published(publisher))
        self.assertEqual([], self.connector.order_book_tracker._order_book_update_listeners)
        self.assertEqual([], self.connector._perpetual_trading._funding_info_update_listeners)
        self.snapshot_task.cancel.assert_called_once()

    # ---------------------------------------------------------------- account data

    def test_account_data_topic(self):
        self._account()
        self.assertEqual("hbot/bot-1/account_data", self.node.create_publisher.call_args.kwargs["topic"])

    def test_order_event_publishes_account_state(self):
        from hummingbot.core.event.events import MarketEvent
        publisher = self._account()

        self.connector.trigger_event(MarketEvent.OrderFilled, MagicMock())

        message = self._published(publisher)[-1]
        self.assertEqual(("coindcx_perpetual", "OrderFilled"), (message.connector, message.trigger))
        self.assertEqual({"total": 923.4, "available": 923.4}, message.balances["USDT"])

    def test_position_change_publishes_account_state(self):
        from hummingbot.connector.derivative.position import Position
        from hummingbot.core.data_type.common import PositionSide
        publisher = self._account()

        self.connector._perpetual_trading.set_position(self.trading_pair, Position(
            self.trading_pair, PositionSide.LONG, Decimal("0"), Decimal("797"), Decimal("0.008"), Decimal("1")))

        message = self._published(publisher)[-1]
        self.assertEqual("position", message.trigger)
        self.assertEqual(self.trading_pair, message.positions[0]["trading_pair"])

    def test_fill_burst_is_capped_per_connector(self):
        from hummingbot.core.event.events import MarketEvent
        publisher = self._account()

        for event in (MarketEvent.OrderFilled, MarketEvent.BuyOrderCompleted, MarketEvent.BuyOrderCreated):
            self.connector.trigger_event(event, MagicMock())
        self.assertEqual(["OrderFilled"], [message.trigger for message in self._published(publisher)])

        self.loop.advance(0.1)
        self.assertEqual(["OrderFilled", "BuyOrderCreated"],
                         [message.trigger for message in self._published(publisher)])

    def test_account_snapshot_skips_connectors_that_are_not_ready(self):
        publisher = self._account()

        self.connector.ready = False
        publisher.publish_snapshot()
        self.assertEqual([], self._published(publisher))

        self.connector.ready = True
        publisher.publish_snapshot()
        self.assertEqual(["snapshot"], [message.trigger for message in self._published(publisher)])

    def test_account_data_stop_detaches_every_listener(self):
        from hummingbot.core.event.events import MarketEvent
        publisher = self._account()

        publisher.stop()
        self.connector.trigger_event(MarketEvent.OrderFilled, MagicMock())

        self.assertEqual([], self._published(publisher))
        self.assertEqual([], self.connector._perpetual_trading._position_update_listeners)

    def test_paper_trade_connectors_are_left_out(self):
        from hummingbot.core.event.events import MarketEvent
        with patch("hummingbot.remote_iface.mqtt._paper_trade_exchange_class", return_value=type(self.connector)):
            publisher = self._account()

        self.connector.trigger_event(MarketEvent.OrderFilled, MagicMock())

        self.assertEqual({}, publisher._connectors)
        self.assertEqual([], self._published(publisher))

    def test_connectors_removed_by_stop_publish_nothing(self):
        # `stop` removes the strategy's connectors from the app. Their frozen state must not keep going out.
        from hummingbot.core.event.events import MarketEvent
        market = self._market()
        account = self._account()
        self.hb_app.markets = {}

        market.publish_snapshot()
        account.publish_snapshot()
        self.connector.order_book_tracker._notify_order_book_updated(self.trading_pair)
        self.connector.trigger_event(MarketEvent.OrderFilled, MagicMock())
        self.loop.advance(1)

        self.assertEqual([], self._published(market))
        self.assertEqual([], self._published(account))

    def test_publish_failure_is_logged_once_per_outage(self):
        from hummingbot.core.event.events import MarketEvent
        from hummingbot.remote_iface import mqtt as mqtt_module
        self.node.create_publisher.return_value.publish.side_effect = Exception("broker unreachable")

        with patch.object(mqtt_module._MQTTDataPublisher, "logger") as logger:
            self._account()
            for _ in range(3):
                self.connector.trigger_event(MarketEvent.OrderFilled, MagicMock())
                self.loop.advance(0.2)

        self.assertEqual(1, logger.return_value.error.call_count)


class MQTTGatewayDataPublisherWiringTests(TestCase):

    def setUp(self):
        from commlib.node import NodeState

        from hummingbot.remote_iface import mqtt as mqtt_module
        self.gateway = mqtt_module.MQTTGateway.__new__(mqtt_module.MQTTGateway)
        self.gateway.stop = MagicMock()  # __del__ calls stop(); this bare instance has nothing to stop
        self.gateway._hb_app = MagicMock()
        self.gateway._market_data = None
        self.gateway._account_data = None
        self.gateway.state = NodeState.RUNNING
        market_patcher = patch.object(mqtt_module, "MQTTMarketDataPublisher")
        account_patcher = patch.object(mqtt_module, "MQTTAccountDataPublisher")
        self.market_cls = market_patcher.start()
        self.account_cls = account_patcher.start()
        self.addCleanup(market_patcher.stop)
        self.addCleanup(account_patcher.stop)

    def _enable(self, market_data, account_data):
        bridge_config = self.gateway._hb_app.client_config_map.mqtt_bridge
        bridge_config.mqtt_market_data = market_data
        bridge_config.mqtt_account_data = account_data

    def test_config_defaults_keep_both_publishers_off(self):
        from hummingbot.client.config.client_config_map import MQTTBridgeConfigMap
        bridge_config = MQTTBridgeConfigMap()
        self.assertEqual((False, 10, False, 5.0), (
            bridge_config.mqtt_market_data, bridge_config.mqtt_market_data_depth,
            bridge_config.mqtt_account_data, bridge_config.mqtt_data_snapshot_interval))

    def test_disabled_publishers_are_not_created(self):
        self._enable(False, False)

        self.gateway._start_data_publishers()

        self.market_cls.assert_not_called()
        self.account_cls.assert_not_called()

    def test_enabled_publishers_are_created_and_run(self):
        self._enable(True, True)

        self.gateway._start_data_publishers()

        self.market_cls.assert_called_once_with(self.gateway._hb_app, self.gateway)
        self.account_cls.assert_called_once_with(self.gateway._hb_app, self.gateway)
        self.market_cls.return_value.publisher.run.assert_called_once()
        self.account_cls.return_value.publisher.run.assert_called_once()

    def test_starting_again_stops_the_previous_publishers(self):
        self._enable(True, False)

        self.gateway._start_data_publishers()
        self.gateway._start_data_publishers()

        self.market_cls.return_value.stop.assert_called_once()

    def test_stopping_clears_the_publishers(self):
        self._enable(True, True)
        self.gateway._start_data_publishers()

        self.gateway._stop_data_publishers()

        self.assertIsNone(self.gateway._market_data)
        self.assertIsNone(self.gateway._account_data)
        self.market_cls.return_value.stop.assert_called_once()
        self.account_cls.return_value.stop.assert_called_once()
