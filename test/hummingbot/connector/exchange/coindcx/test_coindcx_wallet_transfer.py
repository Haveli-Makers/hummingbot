import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import TransferState, TransferType, WalletTransfer
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent


class CoindcxWalletTransferTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(
            coindcx_api_key="k",
            coindcx_api_secret="s",
            coindcx_master_api_key="mk",
            coindcx_master_api_secret="ms",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        self.exchange._set_current_timestamp(1000)
        self.completed_logger = EventLogger()
        self.failed_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.WalletTransferCompleted, self.completed_logger)
        self.exchange.add_listener(MarketEvent.WalletTransferFailed, self.failed_logger)

    def _user_info_url(self):
        return web_utils.private_rest_url(CONSTANTS.USER_INFO_PATH_URL, domain=self.exchange._domain)

    def _transfer_url(self):
        return web_utils.private_rest_url(CONSTANTS.SUB_ACCOUNT_TRANSFER_PATH_URL, domain=self.exchange._domain)

    @aioresponses()
    async def test_sub_to_master_transfer_success(self, mock_api):
        mock_api.post(self._user_info_url(), body=json.dumps([{"coindcx_id": "master-123"}]))
        mock_api.post(self._transfer_url(), body=json.dumps({"status": "success", "code": 200}))

        transfer = WalletTransfer(
            client_transfer_id="t1",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        self.assertEqual(0, len(self.failed_logger.event_log))
        self.assertEqual("t1", self.completed_logger.event_log[0].transfer_id)
        self.assertEqual("master-123", self.exchange._master_account_id)
        self.assertEqual(TransferState.COMPLETED, self.exchange.get_transfer("t1").state)

    @aioresponses()
    async def test_sub_to_master_transfer_failure(self, mock_api):
        mock_api.post(self._user_info_url(), body=json.dumps([{"coindcx_id": "master-123"}]))
        mock_api.post(self._transfer_url(), status=422, body=json.dumps({"message": "Invalid Request"}))

        transfer = WalletTransfer(
            client_transfer_id="t2",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.failed_logger.event_log))
        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t2").state)

    def test_missing_master_credentials_raises(self):
        exchange = CoindcxExchange(
            coindcx_api_key="k",
            coindcx_api_secret="s",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        with self.assertRaises(ValueError):
            exchange.transfer_to_master(asset="USDT", amount=Decimal("5"), from_account="sub-456")

    def test_withdrawal_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.exchange.withdraw_to_address(asset="USDT", amount=Decimal("5"), address="0xabc")
