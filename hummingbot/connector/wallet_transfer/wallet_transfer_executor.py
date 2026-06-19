import asyncio
import uuid
from decimal import Decimal
from typing import Optional

from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferState,
    TransferType,
    TransferUpdate,
    WalletTransfer,
)
from hummingbot.connector.wallet_transfer.wallet_transfer_tracker import WalletTransferTracker
from hummingbot.core.utils.async_utils import safe_ensure_future


class WalletTransferExecutorMixin:
    """
    Generic, opt-in wallet-transfer capability for connectors.

    Mix this into an exchange connector to expose fund-movement operations that mirror the
    order-placing lifecycle: a public method fires an async task and returns immediately with a
    client transfer id; the operation is tracked in flight, confirmed (synchronously for internal
    transfers, by polling for withdrawals), and surfaced through ``WalletTransfer*`` market events.

    A connector enables the operations it supports via the capability flags and by implementing the
    corresponding ``_place_*`` / ``_request_transfer_status`` hooks. Everything else (tracking,
    event emission, async confirmation, error handling) is provided here, so adding a new exchange
    only requires the exchange-specific request code.
    """

    # Capability flags - connectors override to advertise what they support.
    supports_sub_to_master_transfer: bool = False
    supports_master_to_sub_transfer: bool = False
    supports_withdrawal: bool = False

    # When False, the connector can resolve the sub-account identifier itself (e.g. from the
    # credentials supplied at `connect`), so callers may omit it. Connectors that can do this
    # override this to False and resolve the missing side in `_place_internal_transfer`.
    requires_explicit_sub_account: bool = True

    # Confirmation polling configuration (used for withdrawals).
    TRANSFER_STATUS_POLL_INTERVAL: float = 10.0
    TRANSFER_CONFIRMATION_TIMEOUT: float = 1800.0  # 30 minutes

    @property
    def wallet_transfer_tracker(self) -> WalletTransferTracker:
        tracker = getattr(self, "_wallet_transfer_tracker", None)
        if tracker is None:
            tracker = WalletTransferTracker(connector=self)
            self._wallet_transfer_tracker = tracker
        return tracker

    # ---------------------------------------------------------------------
    # Public API (fire-and-forget, mirroring buy()/sell())
    # ---------------------------------------------------------------------
    def transfer_to_master(
        self,
        asset: str,
        amount: Decimal,
        from_account: Optional[str] = None,
        to_account: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Transfer ``amount`` of ``asset`` from a sub-account to the master account.

        :param from_account: sub-account identifier (email for WazirX, account id for CoinDCX/CSX);
            may be omitted on connectors that resolve it from the configured credentials
        :param to_account: master-account identifier; defaults to the connector's configured master
        :return: the client transfer id
        """
        if not self.supports_sub_to_master_transfer:
            raise NotImplementedError(f"{self.name} does not support sub-account to master transfers.")
        if self.requires_explicit_sub_account and not from_account:
            raise ValueError("from_account (the sub-account identifier) is required for a sub-to-master transfer.")
        self._verify_master_credentials()

        transfer_id = self._generate_transfer_id(TransferType.SUB_TO_MASTER)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.SUB_TO_MASTER,
            asset=asset,
            amount=amount,
            creation_timestamp=self.current_timestamp,
            source=from_account,
            destination=to_account,
        )
        safe_ensure_future(self._create_transfer(transfer, **kwargs))
        return transfer_id

    def transfer_to_sub(
        self,
        asset: str,
        amount: Decimal,
        to_account: Optional[str] = None,
        from_account: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Transfer ``amount`` of ``asset`` from the master account to a sub-account.

        :param to_account: sub-account identifier (email for WazirX, account id for CoinDCX/CSX);
            may be omitted on connectors that resolve it from the configured credentials
        :param from_account: master-account identifier; defaults to the connector's configured master
        :return: the client transfer id
        """
        if not self.supports_master_to_sub_transfer:
            raise NotImplementedError(f"{self.name} does not support master to sub-account transfers.")
        if self.requires_explicit_sub_account and not to_account:
            raise ValueError("to_account (the sub-account identifier) is required for a master-to-sub transfer.")
        self._verify_master_credentials()

        transfer_id = self._generate_transfer_id(TransferType.MASTER_TO_SUB)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.MASTER_TO_SUB,
            asset=asset,
            amount=amount,
            creation_timestamp=self.current_timestamp,
            source=from_account,
            destination=to_account,
        )
        safe_ensure_future(self._create_transfer(transfer, **kwargs))
        return transfer_id

    def withdraw_to_address(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Withdraw ``amount`` of ``asset`` to an external wallet ``address``.

        :return: the client transfer id
        """
        if not self.supports_withdrawal:
            raise NotImplementedError(f"{self.name} does not support withdrawals to a wallet address yet.")

        transfer_id = self._generate_transfer_id(TransferType.WITHDRAWAL)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.WITHDRAWAL,
            asset=asset,
            amount=amount,
            creation_timestamp=self.current_timestamp,
            address=address,
            network=network,
        )
        safe_ensure_future(self._create_transfer(transfer, **kwargs))
        return transfer_id

    def get_transfer(self, transfer_id: str) -> Optional[WalletTransfer]:
        return self.wallet_transfer_tracker.fetch_transfer(transfer_id)

    # ---------------------------------------------------------------------
    # Internal orchestration
    # ---------------------------------------------------------------------
    async def _create_transfer(self, transfer: WalletTransfer, **kwargs) -> None:
        tracker = self.wallet_transfer_tracker
        tracker.start_tracking_transfer(transfer)
        try:
            if transfer.transfer_type == TransferType.WITHDRAWAL:
                transfer_update = await self._place_withdrawal(transfer, **kwargs)
            else:
                transfer_update = await self._place_internal_transfer(transfer, **kwargs)
            tracker.process_transfer_update(transfer_update)

            still_active = tracker.active_transfers.get(transfer.client_transfer_id)
            if still_active is not None and still_active.state == TransferState.SUBMITTED:
                # Withdrawal accepted but not yet settled - confirm asynchronously.
                safe_ensure_future(self._transfer_status_polling_loop(transfer.client_transfer_id))
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().network(
                f"Error submitting {transfer.transfer_type.value} transfer "
                f"({transfer.client_transfer_id}): {exception}",
                exc_info=True,
                app_warning_msg=f"Failed to submit {transfer.transfer_type.value} transfer to {self.name}. "
                                f"Check credentials and network connection.",
            )
            tracker.process_transfer_update(
                TransferUpdate(
                    client_transfer_id=transfer.client_transfer_id,
                    new_state=TransferState.FAILED,
                    update_timestamp=self.current_timestamp,
                    misc_updates={
                        "error_message": str(exception),
                        "error_type": type(exception).__name__,
                    },
                )
            )

    async def _transfer_status_polling_loop(self, client_transfer_id: str) -> None:
        """
        Polls the exchange for the status of a submitted withdrawal until it reaches a terminal
        state or the confirmation timeout elapses. This is what guarantees a withdrawal is actually
        confirmed before it is reported as completed.
        """
        start_timestamp = self.current_timestamp
        while True:
            transfer = self.wallet_transfer_tracker.active_transfers.get(client_transfer_id)
            if transfer is None:
                return  # reached a terminal state and is no longer tracked

            try:
                transfer_update = await self._request_transfer_status(transfer)
                self.wallet_transfer_tracker.process_transfer_update(transfer_update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    f"Unexpected error while polling status of transfer {client_transfer_id}."
                )

            if self.wallet_transfer_tracker.active_transfers.get(client_transfer_id) is None:
                return

            if self.current_timestamp - start_timestamp > self.TRANSFER_CONFIRMATION_TIMEOUT:
                self.wallet_transfer_tracker.process_transfer_update(
                    TransferUpdate(
                        client_transfer_id=client_transfer_id,
                        new_state=TransferState.FAILED,
                        update_timestamp=self.current_timestamp,
                        misc_updates={
                            "error_message": "Transfer confirmation timed out.",
                            "error_type": "TimeoutError",
                        },
                    )
                )
                return

            await asyncio.sleep(self.TRANSFER_STATUS_POLL_INTERVAL)

    def _generate_transfer_id(self, transfer_type: TransferType) -> str:
        prefix = getattr(self, "client_order_id_prefix", "") or ""
        return f"{prefix}xfer-{transfer_type.value}-{uuid.uuid4().hex}"

    # ---------------------------------------------------------------------
    # Hooks for connectors to override
    # ---------------------------------------------------------------------
    def _verify_master_credentials(self) -> None:
        """
        Raise a clear error if master-account credentials are required but not configured.
        Connectors that require master creds for transfers override this.
        """
        return None

    async def _place_internal_transfer(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        raise NotImplementedError

    async def _place_withdrawal(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        raise NotImplementedError

    async def _request_transfer_status(self, transfer: WalletTransfer) -> TransferUpdate:
        raise NotImplementedError
