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

    @aioresponses()
    async def test_sub_to_master_auto_resolves_both_ids(self, mock_api):
        # With the sub id omitted, the connector resolves the sub id from its own (primary) creds'
        # users/info and the master id from the master creds' users/info (two signed calls; same
        # URL, consumed in call order: own first, then master).
        url = self._user_info_url()
        mock_api.post(url, body=json.dumps([{"coindcx_id": "sub-self"}]))     # own (primary) call
        mock_api.post(url, body=json.dumps([{"coindcx_id": "master-123"}]))   # master call
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "success", "status": 200}))

        transfer = WalletTransfer(
            client_transfer_id="t-auto",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            # source (sub) and destination (master) both omitted -> resolved from creds
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        completed = self.exchange.get_transfer("t-auto")
        self.assertEqual("sub-self", completed.source)
        self.assertEqual("master-123", completed.destination)

    @aioresponses()
    async def test_master_to_sub_auto_resolves_both_ids(self, mock_api):
        # MASTER_TO_SUB resolves the master side first, then the sub side.
        url = self._user_info_url()
        mock_api.post(url, body=json.dumps([{"coindcx_id": "master-123"}]))   # master call (first)
        mock_api.post(url, body=json.dumps([{"coindcx_id": "sub-self"}]))     # own (primary) call
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "success", "status": 200}))

        transfer = WalletTransfer(
            client_transfer_id="t-auto-m2s",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
        )
        await self.exchange._create_transfer(transfer)

        completed = self.exchange.get_transfer("t-auto-m2s")
        self.assertEqual("master-123", completed.source)
        self.assertEqual("sub-self", completed.destination)

    def test_coindcx_does_not_require_explicit_sub_account(self):
        self.assertFalse(self.exchange.requires_explicit_sub_account)

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

    @aioresponses()
    async def test_get_user_info(self, mock_api):
        record = {
            "coindcx_id": "master-123",
            "first_name": "First",
            "last_name": "Last",
            "email": "user@example.com",
            "mobile_number": "000",
        }
        mock_api.post(self._user_info_url(), body=json.dumps([record]), repeat=True)

        info = await self.exchange.get_user_info(use_master=True)
        self.assertEqual(record, info)

        info_primary = await self.exchange.get_user_info()
        self.assertEqual(record, info_primary)

    @aioresponses()
    async def test_sub_to_master_transfer_success_with_live_response_shape(self, mock_api):
        # The live API returns {'message': 'success', 'status': 200, 'code': 200}
        # (status/message swapped relative to the docs).
        mock_api.post(self._user_info_url(), body=json.dumps([{"coindcx_id": "master-123"}]))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "success", "status": 200, "code": 200}))

        transfer = WalletTransfer(
            client_transfer_id="t-live",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("10"),
            creation_timestamp=1000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        self.assertEqual(0, len(self.failed_logger.event_log))
        self.assertEqual(TransferState.COMPLETED, self.exchange.get_transfer("t-live").state)

    @aioresponses()
    async def test_master_to_sub_transfer_success(self, mock_api):
        mock_api.post(self._user_info_url(), body=json.dumps([{"coindcx_id": "master-123"}]))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "success", "status": 200, "code": 200}))

        transfer = WalletTransfer(
            client_transfer_id="t-m2s",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="INR",
            amount=Decimal("10"),
            creation_timestamp=1000.0,
            destination="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        completed_transfer = self.exchange.get_transfer("t-m2s")
        self.assertEqual(TransferState.COMPLETED, completed_transfer.state)
        # The master side (source) defaults to the id resolved from the master credentials.
        self.assertEqual("master-123", completed_transfer.source)
        self.assertEqual("sub-456", completed_transfer.destination)

    @aioresponses()
    async def test_sub_to_master_transfer_rejected_body(self, mock_api):
        # HTTP 200 but a body that does not indicate success must be treated as a failure.
        mock_api.post(self._user_info_url(), body=json.dumps([{"coindcx_id": "master-123"}]))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Insufficient funds", "status": "error"}))

        transfer = WalletTransfer(
            client_transfer_id="t-rejected",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("10"),
            creation_timestamp=1000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.failed_logger.event_log))
        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t-rejected").state)

    def _sub_to_master(self, transfer_id: str, **overrides) -> WalletTransfer:
        fields = dict(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("10"),
            creation_timestamp=1000.0,
            source="sub-456",
        )
        fields.update(overrides)
        return WalletTransfer(**fields)

    @aioresponses()
    async def test_missing_coindcx_id_is_refused_before_sending(self, mock_api):
        # Without the fix the literal string "None" was cached and sent as to_account_id.
        mock_api.post(self._user_info_url(), body=json.dumps([{"email": "master@example.com"}]))

        transfer = self._sub_to_master("t-no-id")
        await self.exchange._create_transfer(transfer)  # the transfer endpoint is NOT mocked

        tracked = self.exchange.get_transfer("t-no-id")
        self.assertEqual(TransferState.FAILED, tracked.state)
        self.assertIn("no coindcx_id", tracked.error_message)
        self.assertFalse(transfer.request_sent)
        self.assertIsNone(self.exchange._master_account_id)  # nothing bogus cached

    @aioresponses()
    async def test_empty_user_info_list_raises_a_clear_error(self, mock_api):
        mock_api.post(self._user_info_url(), body=json.dumps([]))
        with self.assertRaises(IOError) as ctx:
            await self.exchange.get_user_info(use_master=True)
        self.assertIn("no account record", str(ctx.exception))

    async def test_self_transfer_is_refused_before_sending(self):
        transfer = self._sub_to_master("t-self", source="acct-1", destination="acct-1")
        await self.exchange._create_transfer(transfer)

        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t-self").state)
        self.assertFalse(transfer.request_sent)

    async def test_amount_a_float_would_round_is_refused_before_sending(self):
        transfer = self._sub_to_master("t-precise", destination="master-123",
                                       amount=Decimal("123456789.123456789"))
        await self.exchange._create_transfer(transfer)

        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t-precise").state)
        self.assertFalse(transfer.request_sent)

    @aioresponses()
    async def test_response_without_a_success_marker_is_unknown(self, mock_api):
        mock_api.post(self._transfer_url(), body=json.dumps({"data": {}}))
        unknown_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.WalletTransferUnknown, unknown_logger)

        await self.exchange._create_transfer(self._sub_to_master("t-unreadable", destination="master-123"))

        self.assertEqual(TransferState.UNKNOWN, self.exchange.get_transfer("t-unreadable").state)
        self.assertEqual(1, len(unknown_logger.event_log))
        self.assertEqual(0, len(self.completed_logger.event_log))

    @aioresponses()
    async def test_get_balances(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.USER_BALANCES_PATH_URL, domain=self.exchange._domain)
        records = [
            {"currency": "USDT", "balance": 10.5, "locked_balance": 0},
            {"currency": "BTC", "balance": 0, "locked_balance": 0},
        ]
        mock_api.post(url, body=json.dumps(records), repeat=True)

        non_zero = await self.exchange.get_balances(use_master=True)
        self.assertEqual(["USDT"], [b["currency"] for b in non_zero])

        all_balances = await self.exchange.get_balances(non_zero_only=False)
        self.assertEqual(2, len(all_balances))

    async def test_get_user_info_with_master_requires_master_creds(self):
        exchange = CoindcxExchange(
            coindcx_api_key="k",
            coindcx_api_secret="s",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        with self.assertRaises(ValueError):
            await exchange.get_user_info(use_master=True)
