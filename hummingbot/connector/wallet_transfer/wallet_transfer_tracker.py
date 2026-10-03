import logging
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferState,
    TransferUpdate,
    WalletTransfer,
)
from hummingbot.core.event.events import (
    MarketEvent,
    WalletTransferCompletedEvent,
    WalletTransferCreatedEvent,
    WalletTransferFailedEvent,
)
from hummingbot.logger.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.connector_base import ConnectorBase

wtt_logger = None


class WalletTransferTracker:
    """
    Tracks in-flight wallet transfers and emits the corresponding market events as their
    state changes. This is the transfer-side analogue of ``ClientOrderTracker``.
    """

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global wtt_logger
        if wtt_logger is None:
            wtt_logger = logging.getLogger(__name__)
        return wtt_logger

    def __init__(self, connector: "ConnectorBase") -> None:
        self._connector = connector
        self._in_flight_transfers: Dict[str, WalletTransfer] = {}
        self._cached_transfers: Dict[str, WalletTransfer] = {}

    @property
    def active_transfers(self) -> Dict[str, WalletTransfer]:
        return self._in_flight_transfers

    @property
    def all_transfers(self) -> Dict[str, WalletTransfer]:
        return {**self._cached_transfers, **self._in_flight_transfers}

    def start_tracking_transfer(self, transfer: WalletTransfer) -> None:
        self._in_flight_transfers[transfer.client_transfer_id] = transfer

    def stop_tracking_transfer(self, client_transfer_id: str) -> None:
        transfer = self._in_flight_transfers.pop(client_transfer_id, None)
        if transfer is not None:
            self._cached_transfers[client_transfer_id] = transfer

    def fetch_transfer(self, client_transfer_id: str) -> Optional[WalletTransfer]:
        return (
            self._in_flight_transfers.get(client_transfer_id)
            or self._cached_transfers.get(client_transfer_id)
        )

    def process_transfer_update(self, transfer_update: TransferUpdate) -> None:
        transfer = self._in_flight_transfers.get(transfer_update.client_transfer_id)
        if transfer is None:
            self.logger().debug(f"Transfer is no longer being tracked ({transfer_update})")
            return

        previous_state = transfer.state
        updated = transfer.update_with(transfer_update)
        if updated:
            self._trigger_events(transfer, previous_state)
            if transfer.is_done:
                self.stop_tracking_transfer(transfer.client_transfer_id)

    def _trigger_events(self, transfer: WalletTransfer, previous_state: TransferState) -> None:
        timestamp = self._connector.current_timestamp

        if previous_state == TransferState.PENDING_CREATE and transfer.state in (
            TransferState.SUBMITTED,
            TransferState.COMPLETED,
        ):
            self.logger().info(
                f"Initiated {transfer.transfer_type.value} of {transfer.amount} {transfer.asset} "
                f"({transfer.client_transfer_id})."
            )
            self._connector.trigger_event(
                MarketEvent.WalletTransferCreated,
                WalletTransferCreatedEvent(
                    timestamp=timestamp,
                    transfer_id=transfer.client_transfer_id,
                    transfer_type=transfer.transfer_type.value,
                    asset=transfer.asset,
                    amount=transfer.amount,
                    source=transfer.source,
                    destination=transfer.destination,
                    exchange_transfer_id=transfer.exchange_transfer_id,
                ),
            )

        if transfer.is_completed:
            self.logger().info(
                f"Completed {transfer.transfer_type.value} of {transfer.amount} {transfer.asset} "
                f"({transfer.client_transfer_id})."
            )
            self._connector.trigger_event(
                MarketEvent.WalletTransferCompleted,
                WalletTransferCompletedEvent(
                    timestamp=timestamp,
                    transfer_id=transfer.client_transfer_id,
                    transfer_type=transfer.transfer_type.value,
                    asset=transfer.asset,
                    amount=transfer.amount,
                    source=transfer.source,
                    destination=transfer.destination,
                    exchange_transfer_id=transfer.exchange_transfer_id,
                    tx_hash=transfer.tx_hash,
                ),
            )
        elif transfer.is_failure:
            self.logger().warning(
                f"Failed {transfer.transfer_type.value} ({transfer.client_transfer_id}): "
                f"{transfer.error_message}"
            )
            self._connector.trigger_event(
                MarketEvent.WalletTransferFailed,
                WalletTransferFailedEvent(
                    timestamp=timestamp,
                    transfer_id=transfer.client_transfer_id,
                    transfer_type=transfer.transfer_type.value,
                    error_message=transfer.error_message,
                    error_type=transfer.error_type,
                ),
            )
