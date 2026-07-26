from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, NamedTuple, Optional


class TransferCategory(Enum):
    """
    The broad class of a fund movement. The two classes behave very differently, so connectors
    implement them through separate hooks even though they share the tracking/event machinery.

    INTERNAL: funds stay inside the exchange (sub <-> master, spot <-> futures). These settle
              immediately, so they go PENDING_CREATE -> COMPLETED in one step.
    EXTERNAL: crypto leaves or enters the exchange (withdrawal to an address, deposit from one).
              These settle on-chain over time, so they go PENDING_CREATE -> SUBMITTED ->
              COMPLETED/FAILED and MUST be confirmed by polling rather than trusted on submit.
    """
    INTERNAL = "internal"
    EXTERNAL = "external"


class TransferType(Enum):
    """
    The kind of fund movement a connector can perform.

    New transfer kinds can be added here as more exchanges/operations are supported; each one
    belongs to exactly one :class:`TransferCategory`.
    """
    SUB_TO_MASTER = "sub_to_master"  # internal transfer from a sub-account to the master account
    MASTER_TO_SUB = "master_to_sub"  # internal transfer from the master account to a sub-account
    WITHDRAWAL = "withdrawal"  # external withdrawal to a wallet address

    @property
    def category(self) -> TransferCategory:
        if self is TransferType.WITHDRAWAL:
            return TransferCategory.EXTERNAL
        return TransferCategory.INTERNAL

    @property
    def is_external(self) -> bool:
        return self.category is TransferCategory.EXTERNAL


class TransferState(Enum):
    """
    Lifecycle states of a wallet transfer, mirroring the order lifecycle.

    Internal transfers usually go PENDING_CREATE -> COMPLETED in a single step.
    External withdrawals go PENDING_CREATE -> SUBMITTED -> COMPLETED/FAILED, where the
    transition out of SUBMITTED is confirmed asynchronously by polling the exchange.
    """
    PENDING_CREATE = 0
    SUBMITTED = 1
    COMPLETED = 2
    FAILED = 3


class TransferUpdate(NamedTuple):
    """
    An update to a tracked transfer, analogous to ``OrderUpdate`` for orders.
    """
    client_transfer_id: str
    new_state: TransferState
    update_timestamp: float  # seconds
    exchange_transfer_id: Optional[str] = None
    tx_hash: Optional[str] = None
    misc_updates: Optional[Dict[str, Any]] = None


@dataclass
class WalletTransfer:
    """
    Tracks the full state of a single wallet transfer while it is in flight.
    """
    client_transfer_id: str
    transfer_type: TransferType
    asset: str
    amount: Decimal
    creation_timestamp: float
    # --- internal transfers ---
    source: Optional[str] = None  # sub-account email/id for internal transfers
    destination: Optional[str] = None  # master-account email/id for internal transfers
    # --- external transfers (withdrawals) ---
    address: Optional[str] = None  # destination address, where the exchange accepts a raw address
    address_book_id: Optional[str] = None  # whitelisted Address Book entry id (or name), e.g. WazirX
    network: Optional[str] = None  # chain/network for withdrawals
    state: TransferState = TransferState.PENDING_CREATE
    exchange_transfer_id: Optional[str] = None
    tx_hash: Optional[str] = None
    last_update_timestamp: float = 0.0
    error_message: Optional[str] = None
    error_type: Optional[str] = None

    @property
    def category(self) -> TransferCategory:
        return self.transfer_type.category

    @property
    def is_done(self) -> bool:
        return self.state in {TransferState.COMPLETED, TransferState.FAILED}

    @property
    def is_completed(self) -> bool:
        return self.state == TransferState.COMPLETED

    @property
    def is_failure(self) -> bool:
        return self.state == TransferState.FAILED

    def update_with(self, transfer_update: TransferUpdate) -> bool:
        """
        Apply an update to this transfer.

        :return: True if the state changed or new exchange data was attached, False otherwise.
        """
        if transfer_update.client_transfer_id != self.client_transfer_id:
            return False

        previous_state = self.state
        had_new_exchange_id = (
            transfer_update.exchange_transfer_id is not None
            and transfer_update.exchange_transfer_id != self.exchange_transfer_id
        )

        self.state = transfer_update.new_state
        self.last_update_timestamp = transfer_update.update_timestamp
        if transfer_update.exchange_transfer_id is not None:
            self.exchange_transfer_id = transfer_update.exchange_transfer_id
        if transfer_update.tx_hash is not None:
            self.tx_hash = transfer_update.tx_hash
        if transfer_update.misc_updates:
            self.error_message = transfer_update.misc_updates.get("error_message", self.error_message)
            self.error_type = transfer_update.misc_updates.get("error_type", self.error_type)

        return previous_state != self.state or had_new_exchange_id
