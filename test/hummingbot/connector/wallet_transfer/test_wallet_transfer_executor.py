import asyncio
import math
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferOutcomeUnknownError,
    TransferRejectedError,
    TransferState,
    TransferType,
    TransferUpdate,
    WalletTransfer,
)
from hummingbot.connector.wallet_transfer.wallet_transfer_executor import WalletTransferExecutorMixin
from hummingbot.core.event.events import MarketEvent


class _FakeConnector(WalletTransferExecutorMixin):
    """The mixin on its own: hooks are plain callables each test sets, events are recorded."""

    supports_sub_to_master_transfer = True
    supports_master_to_sub_transfer = True
    supports_withdrawal = True
    requires_explicit_sub_account = False
    TRANSFER_STATUS_POLL_INTERVAL = 0.0

    def __init__(self):
        self.name = "fake"
        self.current_timestamp = float("nan")  # never ticked, like a connector outside the clock
        self.events = []
        self._logger = MagicMock()
        self.resolve_accounts = None
        self.place = None
        self.status = None
        self.place_calls = 0

    def logger(self):
        return self._logger

    def trigger_event(self, event_tag, event):
        self.events.append((event_tag, event))

    async def _resolve_internal_transfer_accounts(self, transfer):
        if self.resolve_accounts is not None:
            self.resolve_accounts(transfer)

    async def _place_internal_transfer(self, transfer, **kwargs):
        self.place_calls += 1
        return await self.place(transfer)

    async def _place_withdrawal(self, transfer, **kwargs):
        self.place_calls += 1
        return await self.place(transfer)

    async def _request_transfer_status(self, transfer):
        return await self.status(transfer)


def _sent_then(exception):
    """A place hook that sends the request, then hits ``exception``."""
    async def place(transfer):
        transfer.request_sent = True
        raise exception
    return place


def _raise_before_sending(exception):
    async def place(transfer):
        raise exception
    return place


class WalletTransferExecutorTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = _FakeConnector()

    def _transfer(self, transfer_id="t1", transfer_type=TransferType.SUB_TO_MASTER, **overrides):
        fields = dict(
            client_transfer_id=transfer_id,
            transfer_type=transfer_type,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub-1",
            destination="master-1",
        )
        fields.update(overrides)
        return WalletTransfer(**fields)

    def _tags(self):
        return [tag for tag, _ in self.connector.events]

    def _event(self, tag):
        return next(event for event_tag, event in self.connector.events if event_tag == tag)

    async def _wait_until_done(self, transfer_id, timeout=2.0):
        async def done():
            while not self.connector.get_transfer(transfer_id).is_done:
                await asyncio.sleep(0.005)
        await asyncio.wait_for(done(), timeout)

    # ── amount validation ──────────────────────────────────────────────────────

    def test_amount_must_be_positive_and_finite_on_every_entry_point(self):
        bad_amounts = [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity"), "abc", None, 0]
        for amount in bad_amounts:
            with self.subTest(amount=amount):
                with self.assertRaises(ValueError):
                    self.connector.transfer_to_master(asset="USDT", amount=amount, from_account="sub-1")
                with self.assertRaises(ValueError):
                    self.connector.transfer_to_sub(asset="USDT", amount=amount, to_account="sub-1")
                with self.assertRaises(ValueError):
                    self.connector.withdraw_to_address(asset="USDT", amount=amount, address="0xabc")
        # Nothing was scheduled or tracked for any of them.
        self.assertEqual({}, self.connector.wallet_transfer_tracker.all_transfers)

    def test_amount_is_coerced_to_decimal(self):
        self.assertEqual(Decimal("1.5"), WalletTransferExecutorMixin._validated_transfer_amount("1.5"))
        self.assertEqual(Decimal("2"), WalletTransferExecutorMixin._validated_transfer_amount(2))

    # ── outcome classification: FAILED only when funds definitely did not move ──

    async def test_error_before_sending_is_failed(self):
        self.connector.place = _raise_before_sending(asyncio.TimeoutError())
        await self.connector._create_transfer(self._transfer())

        self.assertEqual(TransferState.FAILED, self.connector.get_transfer("t1").state)
        self.assertEqual([MarketEvent.WalletTransferFailed], self._tags())

    async def test_timeout_after_sending_is_unknown_not_failed(self):
        self.connector.place = _sent_then(asyncio.TimeoutError())
        await self.connector._create_transfer(self._transfer())

        transfer = self.connector.get_transfer("t1")
        self.assertEqual(TransferState.UNKNOWN, transfer.state)
        self.assertEqual([MarketEvent.WalletTransferUnknown], self._tags())
        event = self._event(MarketEvent.WalletTransferUnknown)
        self.assertEqual("t1", event.transfer_id)
        self.assertEqual(Decimal("5"), event.amount)
        self.assertEqual("TimeoutError", event.error_type)

    async def test_connection_reset_after_sending_is_unknown(self):
        self.connector.place = _sent_then(ConnectionResetError("reset by peer"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.UNKNOWN, self.connector.get_transfer("t1").state)

    async def test_http_4xx_after_sending_is_failed(self):
        self.connector.place = _sent_then(IOError(
            "Error executing request POST https://x/transfer. HTTP status is 400. Error: {\"message\": \"bad\"}"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.FAILED, self.connector.get_transfer("t1").state)

    async def test_http_5xx_after_sending_is_unknown(self):
        self.connector.place = _sent_then(IOError(
            "Error executing request POST https://x/transfer. HTTP status is 502. Error: N/A"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.UNKNOWN, self.connector.get_transfer("t1").state)

    async def test_explicit_rejection_after_sending_is_failed(self):
        self.connector.place = _sent_then(TransferRejectedError("Insufficient balance"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.FAILED, self.connector.get_transfer("t1").state)

    async def test_unreadable_response_is_unknown(self):
        self.connector.place = _sent_then(TransferOutcomeUnknownError("no success marker"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.UNKNOWN, self.connector.get_transfer("t1").state)

    async def test_value_error_after_sending_is_unknown(self):
        # e.g. a 2xx body that fails to parse: the request was executed, so this is not a failure.
        self.connector.place = _sent_then(ValueError("Expecting value: line 1 column 1"))
        await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.UNKNOWN, self.connector.get_transfer("t1").state)

    async def test_cancelled_after_sending_is_recorded_unknown(self):
        self.connector.place = _sent_then(asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.connector._create_transfer(self._transfer())
        self.assertEqual(TransferState.UNKNOWN, self.connector.get_transfer("t1").state)

    # ── account resolution ─────────────────────────────────────────────────────

    async def test_self_transfer_is_refused_before_sending(self):
        def same_account(transfer):
            transfer.source = transfer.destination = "acct-1"  # e.g. the configured account is the master

        self.connector.resolve_accounts = same_account
        self.connector.place = _sent_then(AssertionError("must not be called"))
        await self.connector._create_transfer(self._transfer(source=None, destination=None))

        transfer = self.connector.get_transfer("t1")
        self.assertEqual(TransferState.FAILED, transfer.state)
        self.assertIn("to itself", transfer.error_message)
        self.assertEqual(0, self.connector.place_calls)

    async def test_self_transfer_check_ignores_case_and_spaces(self):
        self.connector.place = _sent_then(AssertionError("must not be called"))
        await self.connector._create_transfer(self._transfer(source="Sub@Example.com", destination=" sub@example.com"))
        self.assertEqual(TransferState.FAILED, self.connector.get_transfer("t1").state)
        self.assertEqual(0, self.connector.place_calls)

    async def test_unresolved_side_is_refused_before_sending(self):
        self.connector.place = _sent_then(AssertionError("must not be called"))
        await self.connector._create_transfer(self._transfer(destination=None))

        transfer = self.connector.get_transfer("t1")
        self.assertEqual(TransferState.FAILED, transfer.state)
        self.assertIn("Could not determine both accounts", transfer.error_message)
        self.assertEqual(0, self.connector.place_calls)

    # ── withdrawal confirmation ────────────────────────────────────────────────

    async def test_unconfirmed_withdrawal_times_out_to_unknown_even_with_a_frozen_clock(self):
        # current_timestamp is NaN and never advances; the deadline must still be enforced.
        self.connector.TRANSFER_CONFIRMATION_TIMEOUT = 0.05
        self.connector.TRANSFER_STATUS_POLL_INTERVAL = 0.01

        async def submitted(transfer):
            transfer.request_sent = True
            return TransferUpdate(transfer.client_transfer_id, TransferState.SUBMITTED, float("nan"), "W1")

        async def still_pending(transfer):
            return TransferUpdate(transfer.client_transfer_id, TransferState.SUBMITTED, float("nan"))

        self.connector.place = submitted
        self.connector.status = still_pending
        await self.connector._create_transfer(self._transfer(transfer_type=TransferType.WITHDRAWAL))
        await self._wait_until_done("t1")  # _create_transfer scheduled the polling loop itself

        transfer = self.connector.get_transfer("t1")
        self.assertEqual(TransferState.UNKNOWN, transfer.state)  # accepted, so not FAILED
        self.assertEqual("TimeoutError", transfer.error_type)
        self.assertIn(MarketEvent.WalletTransferUnknown, self._tags())
        self.assertNotIn(MarketEvent.WalletTransferFailed, self._tags())

    async def test_repeated_status_errors_escalate_and_are_reported(self):
        self.connector.TRANSFER_CONFIRMATION_TIMEOUT = 0.1
        self.connector.TRANSFER_STATUS_POLL_INTERVAL = 0.005
        self.connector.TRANSFER_STATUS_ERROR_ESCALATION = 2

        async def submitted(transfer):
            transfer.request_sent = True
            return TransferUpdate(transfer.client_transfer_id, TransferState.SUBMITTED, 1.0, "W1")

        async def broken_status(transfer):
            raise IOError("status endpoint down")

        self.connector.place = submitted
        self.connector.status = broken_status
        await self.connector._create_transfer(self._transfer(transfer_type=TransferType.WITHDRAWAL))
        await self._wait_until_done("t1")  # _create_transfer scheduled the polling loop itself

        self.assertTrue(self.connector.logger().network.called)  # operator-visible warning
        transfer = self.connector.get_transfer("t1")
        self.assertEqual(TransferState.UNKNOWN, transfer.state)
        self.assertIn("status endpoint down", transfer.error_message)

    async def test_events_get_a_real_timestamp_when_the_connector_was_never_ticked(self):
        async def completed(transfer):
            transfer.request_sent = True
            return TransferUpdate(transfer.client_transfer_id, TransferState.COMPLETED, float("nan"))

        self.connector.place = completed
        await self.connector._create_transfer(self._transfer())

        self.assertTrue(all(math.isfinite(event.timestamp) for _, event in self.connector.events))
        self.assertTrue(math.isfinite(self.connector.get_transfer("t1").last_update_timestamp))

    # ── amount serialisation ───────────────────────────────────────────────────

    def test_exact_float_passes_amounts_a_float_represents_exactly(self):
        for amount in ("0.0002", "13.68", "10", "1e-8", "20000"):
            with self.subTest(amount=amount):
                self.assertEqual(float(amount), WalletTransferExecutorMixin._amount_as_exact_float(Decimal(amount)))

    def test_exact_float_refuses_amounts_a_float_would_round(self):
        with self.assertRaises(ValueError):
            WalletTransferExecutorMixin._amount_as_exact_float(Decimal("123456789.123456789"))
