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
    async def test_sub_to_master_auto_resolves_both_sides_from_connected_creds(self, mock_api):
        # With both keys connected, omitting the sub id makes the connector resolve BOTH sides from
        # its own (primary/sub) profile in a single /me/ call: brokerID = sub, parentID = master.
        mock_api.get(self._profile_url(),
                     body=json.dumps({"data": {"brokerID": "sub-self", "parentID": "master-123"}}), repeat=True)
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Transferred funds successfully"}))

        transfer = WalletTransfer(
            client_transfer_id="t-auto",
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
            # source (sub) and destination (master) both omitted -> resolved by the connector
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        completed = self.exchange.get_transfer("t-auto")
        self.assertEqual("sub-self", completed.source)       # own brokerID
        self.assertEqual("master-123", completed.destination)  # parentID

    @aioresponses()
    async def test_master_to_sub_auto_resolves_both_sides(self, mock_api):
        mock_api.get(self._profile_url(),
                     body=json.dumps({"data": {"brokerID": "sub-self", "parentID": "master-123"}}), repeat=True)
        mock_api.post(self._transfer_url(), body=json.dumps({"message": "Transferred funds successfully"}))

        transfer = WalletTransfer(
            client_transfer_id="t-auto-m2s",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1700000000.0,
        )
        await self.exchange._create_transfer(transfer)

        completed = self.exchange.get_transfer("t-auto-m2s")
        self.assertEqual("master-123", completed.source)     # parentID
        self.assertEqual("sub-self", completed.destination)  # own brokerID

    def test_csx_does_not_require_explicit_sub_account(self):
        # CSX can resolve the sub id from the configured creds, so the public methods must not
        # demand it (omitting it is allowed; it is resolved in _place_internal_transfer).
        self.assertFalse(self.exchange.requires_explicit_sub_account)

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

    # ── External transfers (crypto withdrawal / deposit) ───────────────────────

    def _withdraw_url(self):
        return web_utils.private_rest_url(CONSTANTS.WITHDRAWAL_PATH_URL)

    @aioresponses()
    async def test_withdrawal_accepted_completes_with_request_id(self, mock_api):
        # CSX takes a RAW address (no whitelist) and returns the request id inside the message
        # string; with no status endpoint, an accepted request is COMPLETED.
        mock_api.post(self._withdraw_url(), body=json.dumps(
            {"message": "Withdrawal request processed successfully. Request ID: 08875d18-0ecb-418c-b6d0-d3b6cd737516"}
        ))
        transfer = WalletTransfer(
            client_transfer_id="wd1",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1700000000.0,
            address="0xabc0000000000000000000000000000000000000",
            network="eth",
        )
        update = await self.exchange._place_withdrawal(transfer)
        self.assertEqual(TransferState.COMPLETED, update.new_state)
        self.assertEqual("08875d18-0ecb-418c-b6d0-d3b6cd737516", update.exchange_transfer_id)

    @aioresponses()
    async def test_withdrawal_rejected_message_raises(self, mock_api):
        mock_api.post(self._withdraw_url(), body=json.dumps({"message": "Insufficient balance"}))
        transfer = WalletTransfer(
            client_transfer_id="wd2", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("5"), creation_timestamp=1700000000.0, address="0xabc", network="eth",
        )
        with self.assertRaises(IOError):
            await self.exchange._place_withdrawal(transfer)

    def test_withdrawal_requires_address_and_network(self):
        # Missing address -> ValueError up front (before scheduling); raw address, no whitelist.
        with self.assertRaises(ValueError):
            self.exchange.withdraw_to_address(asset="USDT", amount=Decimal("5"), network="eth")

    def test_csx_does_not_require_whitelisted_address(self):
        self.assertFalse(self.exchange.requires_whitelisted_address)

    @aioresponses()
    async def test_get_deposit_address_from_profile(self, mock_api):
        mock_api.get(self._profile_url(), body=json.dumps(
            {"data": {"brokerID": "b1", "walletAddress": [
                {"instrument": "btc", "address": "bc1qexample"},
                {"instrument": "usdt", "address": "0xdeposit"}]}}))
        result = await self.exchange.get_deposit_address(asset="USDT")
        self.assertEqual("0xdeposit", result["address"])

    @aioresponses()
    async def test_get_deposit_address_missing_asset_raises(self, mock_api):
        mock_api.get(self._profile_url(), body=json.dumps(
            {"data": {"walletAddress": [{"instrument": "btc", "address": "bc1qexample"}]}}))
        with self.assertRaises(ValueError):
            await self.exchange.get_deposit_address(asset="ETH")

    @aioresponses()
    async def test_placeholder_deposit_address_is_rejected(self, mock_api):
        # Verified live: CSX returns only {"instrument": "btc", "address": "NOTAVAILABLE123"} —
        # a placeholder. It must never be handed back as if it were a fundable address.
        mock_api.get(self._profile_url(), body=json.dumps(
            {"data": {"brokerID": "b1", "walletAddress": [
                {"instrument": "btc", "address": "NOTAVAILABLE123"}]}}))
        with self.assertRaises(ValueError) as ctx:
            await self.exchange.get_deposit_address(asset="BTC")
        self.assertIn("placeholder", str(ctx.exception).lower())

    def test_placeholder_address_detection(self):
        for value in ("NOTAVAILABLE123", "notavailable", "", None, "  "):
            self.assertTrue(self.exchange._is_placeholder_address(value), value)
        for value in ("bc1qexample", "0xdeadbeef", "TJZozM6TW5kP5mx3S2cxNQX3knj2SQzgWW"):
            self.assertFalse(self.exchange._is_placeholder_address(value), value)
