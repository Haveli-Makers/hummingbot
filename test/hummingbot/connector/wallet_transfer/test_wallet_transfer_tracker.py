import unittest
from decimal import Decimal

from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferState,
    TransferType,
    TransferUpdate,
    WalletTransfer,
)
from hummingbot.connector.wallet_transfer.wallet_transfer_tracker import WalletTransferTracker
from hummingbot.core.event.events import MarketEvent


class _FakeConnector:
    """Minimal stand-in for a connector that just records triggered events."""

    def __init__(self):
        self.current_timestamp = 1000.0
        self.events = []

    def trigger_event(self, event_tag, event):
        self.events.append((event_tag, event))


class WalletTransferTrackerTest(unittest.TestCase):
    def setUp(self):
        self.connector = _FakeConnector()
        self.tracker = WalletTransferTracker(connector=self.connector)

    def _make_transfer(self, transfer_id: str = "t1") -> WalletTransfer:
        return WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.SUB_TO_MASTER,
            asset="USDT",
            amount=Decimal("5"),
            creation_timestamp=1000.0,
            source="sub",
            destination="master",
        )

    def _event_tags(self):
        return [tag for tag, _ in self.connector.events]

    def test_completion_fires_created_and_completed(self):
        transfer = self._make_transfer()
        self.tracker.start_tracking_transfer(transfer)

        self.tracker.process_transfer_update(
            TransferUpdate(
                client_transfer_id="t1",
                new_state=TransferState.COMPLETED,
                update_timestamp=1001.0,
                exchange_transfer_id="X1",
            )
        )

        tags = self._event_tags()
        self.assertIn(MarketEvent.WalletTransferCreated, tags)
        self.assertIn(MarketEvent.WalletTransferCompleted, tags)
        # No longer active; available in cache with the exchange id attached.
        self.assertNotIn("t1", self.tracker.active_transfers)
        self.assertEqual("X1", self.tracker.fetch_transfer("t1").exchange_transfer_id)

    def test_direct_failure_fires_failed_only(self):
        transfer = self._make_transfer("t2")
        self.tracker.start_tracking_transfer(transfer)

        self.tracker.process_transfer_update(
            TransferUpdate(
                client_transfer_id="t2",
                new_state=TransferState.FAILED,
                update_timestamp=1001.0,
                misc_updates={"error_message": "boom", "error_type": "IOError"},
            )
        )

        tags = self._event_tags()
        self.assertIn(MarketEvent.WalletTransferFailed, tags)
        self.assertNotIn(MarketEvent.WalletTransferCreated, tags)
        self.assertEqual("boom", self.tracker.fetch_transfer("t2").error_message)

    def test_submitted_then_completed_fires_each_event_once(self):
        transfer = self._make_transfer("t3")
        self.tracker.start_tracking_transfer(transfer)

        self.tracker.process_transfer_update(
            TransferUpdate(client_transfer_id="t3", new_state=TransferState.SUBMITTED, update_timestamp=1001.0)
        )
        self.tracker.process_transfer_update(
            TransferUpdate(client_transfer_id="t3", new_state=TransferState.COMPLETED, update_timestamp=1002.0)
        )

        tags = self._event_tags()
        self.assertEqual(1, tags.count(MarketEvent.WalletTransferCreated))
        self.assertEqual(1, tags.count(MarketEvent.WalletTransferCompleted))

    def test_update_for_unknown_transfer_is_noop(self):
        self.tracker.process_transfer_update(
            TransferUpdate(client_transfer_id="missing", new_state=TransferState.COMPLETED, update_timestamp=1.0)
        )
        self.assertEqual([], self.connector.events)


if __name__ == "__main__":
    unittest.main()
