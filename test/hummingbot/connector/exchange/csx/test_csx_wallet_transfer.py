import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS, csx_web_utils as web_utils
from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange
from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import TransferState, TransferType, WalletTransfer
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

# A valid 32-byte (64 hex char) Ed25519 private key so CsxAuth accepts it.
_VALID_SECRET = "aa" * 32


class CsxWalletTransferTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = CsxExchange(
            csx_api_key="primary",
            csx_api_secret=_VALID_SECRET,
            csx_master_api_key="master",
            csx_master_api_secret=_VALID_SECRET,
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        self.exchange._set_current_timestamp(1700000000)
        self.completed_logger = EventLogger()
        self.failed_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.WalletTransferCompleted, self.completed_logger)
        self.exchange.add_listener(MarketEvent.WalletTransferFailed, self.failed_logger)

    def _profile_url(self):
        return web_utils.private_rest_url(CONSTANTS.PROFILE_PATH_URL)

    def _transfer_url(self):
        return web_utils.private_rest_url(CONSTANTS.MASTER_TRANSFER_FUNDS_PATH_URL)

    @aioresponses()
    async def test_sub_to_master_transfer_resolves_master_from_parent_id(self, mock_api):
        # The connector's own (broker) profile exposes parentID = the master account's brokerID,
        # which is preferred over the profile's own brokerID when resolving the master side.
        mock_api.get(self._profile_url(),
                     body=json.dumps({"data": {"brokerID": "sub-self", "parentID": "master-123"}}))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Transferred funds successfully"}))

        transfer = WalletTransfer(
            client_transfer_id="t1",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        self.assertEqual(0, len(self.failed_logger.event_log))
        completed = self.exchange.get_transfer("t1")
        self.assertEqual(TransferState.COMPLETED, completed.state)
        self.assertEqual("sub-456", completed.source)
        self.assertEqual("master-123", completed.destination)  # resolved from parentID

    @aioresponses()
    async def test_master_to_sub_transfer_resolves_master_from_parent_id(self, mock_api):
        mock_api.get(self._profile_url(),
                     body=json.dumps({"data": {"brokerID": "sub-self", "parentID": "master-123"}}))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Transferred funds successfully"}))

        transfer = WalletTransfer(
            client_transfer_id="t2",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
            destination="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        completed = self.exchange.get_transfer("t2")
        self.assertEqual("master-123", completed.source)  # resolved from parentID
        self.assertEqual("sub-456", completed.destination)

    @aioresponses()
    async def test_explicit_master_account_skips_resolution(self, mock_api):
        # When the master id is passed explicitly, no profile lookup is needed.
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Transferred funds successfully"}))

        transfer = WalletTransfer(
            client_transfer_id="t-explicit",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
            source="sub-456",
            destination="master-explicit",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        self.assertEqual("master-explicit", self.exchange.get_transfer("t-explicit").destination)

    @aioresponses()
    async def test_transfer_rejected_message(self, mock_api):
        mock_api.get(self._profile_url(), body=json.dumps({"data": {"brokerID": "master-123"}}))
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Insufficient balance"}))

        transfer = WalletTransfer(
            client_transfer_id="t3",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
            source="sub-456",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.failed_logger.event_log))
        self.assertEqual(TransferState.FAILED, self.exchange.get_transfer("t3").state)

    @aioresponses()
    async def test_get_profile(self, mock_api):
        profile = {"data": {"brokerID": "master-123", "userName": "M"}}
        mock_api.get(self._profile_url(), body=json.dumps(profile), repeat=True)

        prof = await self.exchange.get_profile(use_master=True)
        self.assertEqual("master-123", prof.get("brokerID"))

    @aioresponses()
    async def test_master_balances_use_master_endpoint(self, mock_api):
        # Master balances must come from /api/v1/master/me/getBalance/ (note "Locked": null).
        master_balances = {"data": {"Available": {"INR": "100.5", "BTC": "0"}, "Locked": None}}
        mock_api.get(
            web_utils.private_rest_url(CONSTANTS.MASTER_BALANCE_PATH_URL),
            body=json.dumps(master_balances),
            repeat=True,
        )

        bal = await self.exchange.get_balances(use_master=True)
        self.assertEqual(Decimal("100.5"), bal["INR"]["total"])
        self.assertEqual(Decimal("0"), bal["INR"]["locked"])

    @aioresponses()
    async def test_regular_balances_use_v2_endpoint(self, mock_api):
        balances = {"data": {"Available": {"INR": "100.5"}, "Locked": {"INR": "5"}}}
        mock_api.get(
            web_utils.private_rest_url(CONSTANTS.BALANCE_V2_PATH_URL),
            body=json.dumps(balances),
            repeat=True,
        )

        bal = await self.exchange.get_balances(use_master=False)
        self.assertEqual(Decimal("105.5"), bal["INR"]["total"])

    def test_missing_master_credentials_raises(self):
        exchange = CsxExchange(
            csx_api_key="primary",
            csx_api_secret=_VALID_SECRET,
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        with self.assertRaises(ValueError):
            exchange.transfer_to_master(asset="INR", amount=Decimal("20"), from_account="sub-456")

    def test_withdrawal_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.exchange.withdraw_to_address(asset="INR", amount=Decimal("20"), address="0xabc")
