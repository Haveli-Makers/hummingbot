import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS, wazirx_web_utils as web_utils
from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage


class WazirxAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = ""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.connector = WazirxExchange(
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain)
        self.data_source = WazirxAPIOrderBookDataSource(trading_pairs=[self.trading_pair],
                                                        connector=self.connector,
                                                        api_factory=self.connector._web_assistants_factory,
                                                        domain=self.domain)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.data_source._data_source_obsolete = True

    def test_get_new_order_book_successful(self):
        pass  # Placeholder for actual test

    def test_get_new_order_book_failure(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_diffs_successful(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_diffs_full_order_book_received(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_diffs_ignore_invalid_trading_pair(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_diffs_invalid_message(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_diffs_connection_failed(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_snapshots_successful(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_snapshots_ignore_invalid_trading_pair(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_snapshots_invalid_message(self):
        pass  # Placeholder for actual test

    def test_listen_for_order_book_snapshots_connection_failed(self):
        pass  # Placeholder for actual test