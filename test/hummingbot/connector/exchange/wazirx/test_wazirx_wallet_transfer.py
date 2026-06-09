import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import TransferState, TransferType, WalletTransfer
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent


class WazirxWalletTransferTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = WazirxExchange(
            wazirx_api_key="k",
            wazirx_api_secret="s",
            wazirx_master_api_key="mk",
            wazirx_master_api_secret="ms",
            wazirx_master_email="master@example.com",
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        self.exchange._set_current_timestamp(1000)
        self.completed_logger = EventLogger()
        self.failed_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.WalletTransferCompleted, self.completed_logger)
        self.exchange.add_listener(MarketEvent.WalletTransferFailed, self.failed_logger)

    def _server_time_url(self):
        return CONSTANTS.REST_URL + CONSTANTS.SERVER_TIME_PATH_URL

    def _transfer_url(self):
        return CONSTANTS.REST_URL + CONSTANTS.SUB_ACCOUNT_FUND_TRANSFER_PATH_URL

    def _transfer_url_pattern(self):
        # Params are sent verbatim in the query string, so match the endpoint regardless of query.
        return re.compile(re.escape(self._transfer_url()) + r".*")

    def _history_url(self):
        return CONSTANTS.REST_URL + CONSTANTS.SUB_ACCOUNT_FUND_TRANSFER_HISTORY_PATH_URL

    def _history_url_pattern(self):
        return re.compile(re.escape(self._history_url()) + r".*")

    @aioresponses()
    async def test_sub_to_master_transfer_success(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.post(self._transfer_url_pattern(), body=json.dumps({"status": "success", "txnId": 163}))

        transfer = WalletTransfer(
            client_transfer_id="t1",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub@example.com",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        self.assertEqual(0, len(self.failed_logger.event_log))
        self.assertEqual("163", self.exchange.get_transfer("t1").exchange_transfer_id)
        self.assertEqual(TransferState.COMPLETED, self.exchange.get_transfer("t1").state)

    @aioresponses()
    async def test_sub_to_master_transfer_failure(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.post(self._transfer_url_pattern(), status=400,
                      body=json.dumps({"code": 2136, "message": "Invalid transfer"}))

        transfer = WalletTransfer(
            client_transfer_id="t2",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub@example.com",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.failed_logger.event_log))
        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t2").state)

    def test_missing_master_credentials_raises(self):
        exchange = WazirxExchange(
            wazirx_api_key="k",
            wazirx_api_secret="s",
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        with self.assertRaises(ValueError):
            exchange.transfer_to_master(asset="USDT", amount=Decimal("5"), from_account="sub@example.com")

    def test_withdrawal_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.exchange.withdraw_to_address(asset="USDT", amount=Decimal("5"), address="0xabc")

    @aioresponses()
    async def test_get_sub_account_transfer_history(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        history_records = [
            {
                "id": 163,
                "asset": "usdt",
                "fromEmail": "sub@example.com",
                "toEmail": "master@example.com",
                "amount": "5",
                "status": "success",
            }
        ]
        mock_api.get(self._history_url_pattern(), body=json.dumps(history_records))

        result = await self.exchange.get_sub_account_transfer_history(limit=10)
        self.assertEqual(history_records, result)

    async def test_transfer_history_requires_master_credentials(self):
        exchange = WazirxExchange(
            wazirx_api_key="k",
            wazirx_api_secret="s",
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        with self.assertRaises(ValueError):
            await exchange.get_sub_account_transfer_history()
