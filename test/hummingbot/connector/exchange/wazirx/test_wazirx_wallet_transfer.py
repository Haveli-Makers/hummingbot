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

    @aioresponses()
    async def test_master_to_sub_transfer_success(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.post(self._transfer_url_pattern(), body=json.dumps({"status": "success", "txnId": 164}))

        transfer = WalletTransfer(
            client_transfer_id="t-m2s",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1000.0,
            destination="sub@example.com",
        )
        await self.exchange._create_transfer(transfer)

        self.assertEqual(1, len(self.completed_logger.event_log))
        completed_transfer = self.exchange.get_transfer("t-m2s")
        self.assertEqual(TransferState.COMPLETED, completed_transfer.state)
        # The master side (source) defaults to the configured master email.
        self.assertEqual("master@example.com", completed_transfer.source)
        self.assertEqual("sub@example.com", completed_transfer.destination)

    async def test_master_to_sub_without_master_email_or_from_account_fails(self):
        exchange = WazirxExchange(
            wazirx_api_key="k",
            wazirx_api_secret="s",
            wazirx_master_api_key="mk",
            wazirx_master_api_secret="ms",
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        exchange._set_current_timestamp(1000)
        failed_logger = EventLogger()
        exchange.add_listener(MarketEvent.WalletTransferFailed, failed_logger)

        transfer = WalletTransfer(
            client_transfer_id="t-noemail",
            transfer_type=TransferType.MASTER_TO_SUB,
            asset="INR",
            amount=Decimal("20"),
            creation_timestamp=1000.0,
            destination="sub@example.com",
        )
        await exchange._create_transfer(transfer)

        self.assertEqual(1, len(failed_logger.event_log))
        self.assertEqual(TransferState.FAILED, exchange.get_transfer("t-noemail").state)

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

    # ── External transfers (withdraw / deposit) ────────────────────────────────

    def _address_book_url_pattern(self):
        return re.compile(re.escape(CONSTANTS.REST_URL + CONSTANTS.CRYPTO_WITHDRAW_ADDRESS_BOOK_PATH_URL) + r".*")

    def _withdraw_url_pattern(self):
        return re.compile(re.escape(CONSTANTS.REST_URL + CONSTANTS.CRYPTO_WITHDRAW_PATH_URL) + r"\?.*")

    def _withdraws_history_url_pattern(self):
        return re.compile(re.escape(CONSTANTS.REST_URL + CONSTANTS.CRYPTO_WITHDRAWS_PATH_URL) + r".*")

    def _coins_url_pattern(self):
        return re.compile(re.escape(CONSTANTS.REST_URL + CONSTANTS.COINS_PATH_URL) + r".*")

    def _address_book_entry(self, **overrides):
        """Mirrors a real entry: full (unmasked) address, bound to one network via networkObj."""
        entry = {
            "id": 980,
            "name": "cold wallet",
            "address": "TJZozM6TW5kP5mx3S2cxNQX3knj2SQzgWW",
            "currency": "usdt",
            "disabled": False,
            "isWithdrawAllowed": True,
            "isUniversal": True,
            "networkObj": {"name": "Tron (TRC20)", "network": "trx"},
            "provider": {"id": 338, "name": "CoinSwitch", "type": "exchange"},
            "verificationStatus": "unverified",  # live entries are withdrawable while 'unverified'
            "withdrawAllowedAfter": "2025-10-28T11:45:17Z",
        }
        entry.update(overrides)
        return entry

    def test_raw_address_rejected_because_whitelist_only(self):
        # WazirX only withdraws to Address Book entries, so a raw address must be refused up front.
        with self.assertRaises(ValueError):
            self.exchange.withdraw_to_address(asset="USDT", amount=Decimal("5"), address="0xabc")

    def _coins_payload(self, withdraw_enable=True, coin_disabled=False, **network_overrides):
        """Mirrors the real /sapi/v1/coins shape: a list, keyed by `currency` (NOT `coin`)."""
        network = {
            "network": "trx",
            "name": "Tron (TRC20)",
            "isDefault": True,
            "withdrawEnable": withdraw_enable,
            "minWithdrawAmount": "6.0",
            "maxWithdrawAmount": "20000",
            "withdrawFee": "3",
            "withdrawConsent": {"helpUrl": None, "message": "I confirm that this withdrawal ..."},
            "withdrawDesc": {"description": "This network is unavailable right now."},
        }
        network.update(network_overrides)
        record = {"currency": "usdt", "name": "Tether USD", "networkList": [network]}
        record["withdrawDetails"] = (
            {"disabled": True, "disabledMessage": {"description": "Crypto withdrawals are disabled for maintenance"}}
            if coin_disabled else {"disabled": False}
        )
        return [record]

    @aioresponses()
    async def test_withdrawal_is_submitted_not_completed(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))
        mock_api.get(self._coins_url_pattern(), body=json.dumps(self._coins_payload()))
        mock_api.post(self._withdraw_url_pattern(),
                      body=json.dumps({"id": 1234567, "withdrawOrderId": "cid"}))

        transfer = WalletTransfer(
            client_transfer_id="w1",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("10"),
            creation_timestamp=1000.0,
            address_book_id="cold wallet",  # resolved by name
        )
        update = await self.exchange._place_withdrawal(transfer)  # direct: avoids the polling loop

        # An on-chain withdrawal must NOT be reported as completed on submit.
        self.assertEqual(TransferState.SUBMITTED, update.new_state)
        self.assertEqual("1234567", update.exchange_transfer_id)
        self.assertEqual("980", transfer.address_book_id)
        # The network comes from the entry's networkObj, NOT from the coin's isDefault network.
        self.assertEqual("trx", transfer.network)

    @aioresponses()
    async def test_network_taken_from_address_book_entry_not_default(self, mock_api):
        # The coin's default network is eth, but the chosen entry is a TRC20 address -> must use trx.
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))
        eth_default = {"network": "eth", "name": "Ethereum (ERC20)", "isDefault": True}
        coins = self._coins_payload()
        coins[0]["networkList"].insert(0, {**coins[0]["networkList"][0], **eth_default})
        coins[0]["networkList"][1]["isDefault"] = False
        mock_api.get(self._coins_url_pattern(), body=json.dumps(coins))
        mock_api.post(self._withdraw_url_pattern(), body=json.dumps({"id": 42}))

        transfer = WalletTransfer(
            client_transfer_id="w-net-bind", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("10"), creation_timestamp=1000.0, address_book_id="980",
        )
        await self.exchange._place_withdrawal(transfer)
        self.assertEqual("trx", transfer.network)

    @aioresponses()
    async def test_network_conflicting_with_entry_is_rejected(self, mock_api):
        # Asking for eth against a TRC20 address must fail rather than lose the funds.
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))

        transfer = WalletTransfer(
            client_transfer_id="w-conflict", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("10"), creation_timestamp=1000.0, address_book_id="980", network="eth",
        )
        with self.assertRaises(ValueError) as ctx:
            await self.exchange._place_withdrawal(transfer)
        self.assertIn("bound to network", str(ctx.exception))

    @aioresponses()
    async def test_disabled_address_book_entry_is_rejected(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(),
                     body=json.dumps([self._address_book_entry(disabled=True)]))

        transfer = WalletTransfer(
            client_transfer_id="w-disabled", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("10"), creation_timestamp=1000.0, address_book_id="980",
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_withdrawal(transfer)

    @aioresponses()
    async def test_withdrawal_blocked_when_coin_under_maintenance(self, mock_api):
        # Observed live: withdrawDetails.disabled = true ("disabled for maintenance").
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))
        mock_api.get(self._coins_url_pattern(), body=json.dumps(self._coins_payload(coin_disabled=True)))

        transfer = WalletTransfer(
            client_transfer_id="w-maint", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("10"), creation_timestamp=1000.0, address_book_id="980",
        )
        with self.assertRaises(ValueError) as ctx:
            await self.exchange._place_withdrawal(transfer)
        self.assertIn("maintenance", str(ctx.exception).lower())

    @aioresponses()
    async def test_withdrawal_blocked_when_network_disabled(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))
        mock_api.get(self._coins_url_pattern(), body=json.dumps(self._coins_payload(withdraw_enable=False)))

        transfer = WalletTransfer(
            client_transfer_id="w-net", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("10"), creation_timestamp=1000.0, address_book_id="980",
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_withdrawal(transfer)

    @aioresponses()
    async def test_withdrawal_enforces_minimum_amount(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))
        mock_api.get(self._coins_url_pattern(), body=json.dumps(self._coins_payload()))

        transfer = WalletTransfer(
            client_transfer_id="w-min", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("1"), creation_timestamp=1000.0, address_book_id="980",  # below 6.0 min
        )
        with self.assertRaises(ValueError) as ctx:
            await self.exchange._place_withdrawal(transfer)
        self.assertIn("minimum", str(ctx.exception).lower())

    @aioresponses()
    async def test_consent_resolved_from_currency_keyed_record(self, mock_api):
        # Regression: /sapi/v1/coins keys the asset as `currency`, not `coin`.
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._coins_url_pattern(), body=json.dumps(self._coins_payload()))

        entry = await self.exchange._resolve_withdraw_network("usdt")
        self.assertEqual("trx", entry.get("network"))
        self.assertTrue(self.exchange._consent_message(entry).startswith("I confirm"))

    @aioresponses()
    async def test_withdrawal_rejects_unknown_address_book_entry(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(), body=json.dumps([self._address_book_entry()]))

        transfer = WalletTransfer(
            client_transfer_id="w2",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            address_book_id="not-whitelisted",
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_withdrawal(transfer)

    @aioresponses()
    async def test_withdrawal_rejects_entry_still_in_cooldown(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._address_book_url_pattern(),
                     body=json.dumps([self._address_book_entry(isWithdrawAllowed=False)]))

        transfer = WalletTransfer(
            client_transfer_id="w3",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            address_book_id="980",
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_withdrawal(transfer)

    @aioresponses()
    async def test_withdrawal_status_maps_success_and_failure(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        transfer = WalletTransfer(
            client_transfer_id="w4",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            state=TransferState.SUBMITTED,
        )

        # Real verified shape: status is the STRING "SUCCESS", tx hash is under `txid` (lowercase).
        mock_api.get(self._withdraws_history_url_pattern(), body=json.dumps(
            [{"withdrawOrderId": "w4", "status": "SUCCESS", "id": 23950948,
              "txid": "0x10d58120", "network": "erc20"}]))
        update = await self.exchange._request_transfer_status(transfer)
        self.assertEqual(TransferState.COMPLETED, update.new_state)
        self.assertEqual("0x10d58120", update.tx_hash)
        self.assertEqual("23950948", update.exchange_transfer_id)

        mock_api.get(self._withdraws_history_url_pattern(), body=json.dumps(
            [{"withdrawOrderId": "w4", "status": "Failed", "failureInfo": "bad address"}]))
        update = await self.exchange._request_transfer_status(transfer)
        self.assertEqual(TransferState.FAILED, update.new_state)
        self.assertEqual("bad address", update.misc_updates["error_message"])

    @aioresponses()
    async def test_withdrawal_status_stays_submitted_on_rate_limit(self, mock_api):
        # WazirX rate-limits the withdraw-history endpoint (429 / code 2136). A rate-limit hit must
        # be treated as "still pending", not raise or flip to FAILED.
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._withdraws_history_url_pattern(), status=429,
                     body=json.dumps({"message": "Too many api request", "code": 2136}))

        transfer = WalletTransfer(
            client_transfer_id="w-429", transfer_type=TransferType.WITHDRAWAL, asset="USDT",
            amount=Decimal("5"), creation_timestamp=1000.0, state=TransferState.SUBMITTED,
        )
        update = await self.exchange._request_transfer_status(transfer)
        self.assertEqual(TransferState.SUBMITTED, update.new_state)

    @aioresponses()
    async def test_withdrawal_status_pending_when_record_absent(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        mock_api.get(self._withdraws_history_url_pattern(), body=json.dumps([]))

        transfer = WalletTransfer(
            client_transfer_id="w5",
            transfer_type=TransferType.WITHDRAWAL,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            state=TransferState.SUBMITTED,
        )
        update = await self.exchange._request_transfer_status(transfer)
        self.assertEqual(TransferState.SUBMITTED, update.new_state)

    @aioresponses()
    async def test_get_deposit_address(self, mock_api):
        mock_api.get(self._server_time_url(), body=json.dumps({"serverTime": 1700000000000}), repeat=True)
        url = re.compile(re.escape(CONSTANTS.REST_URL + CONSTANTS.CRYPTO_DEPOSITS_ADDRESS_PATH_URL) + r".*")
        mock_api.get(url, body=json.dumps({"address": "1HP...89wv", "coin": "BTC", "tag": ""}))

        result = await self.exchange.get_deposit_address(asset="BTC", network="BTC")
        self.assertEqual("1HP...89wv", result["address"])

    async def test_get_deposit_address_requires_network(self):
        with self.assertRaises(ValueError):
            await self.exchange.get_deposit_address(asset="BTC")

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
