import asyncio
import re
import time
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from hummingbot.connector.wallet_transfer.wallet_transfer_data_types import (
    TransferOutcomeUnknownError,
    TransferRejectedError,
    TransferState,
    TransferType,
    TransferUpdate,
    WalletTransfer,
    safe_timestamp,
)
from hummingbot.connector.wallet_transfer.wallet_transfer_tracker import WalletTransferTracker
from hummingbot.core.utils.async_utils import safe_ensure_future

# Both the shared RESTAssistant and the WazirX client put the status into their error text this way.
_HTTP_STATUS_PATTERN = re.compile(r"HTTP status is (\d{3})")


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

    Failure semantics matter because these calls move money. A transfer is only reported FAILED
    when the funds definitely did not move: it was refused before anything was sent, or the
    exchange explicitly rejected it. When the request was sent but its outcome cannot be read
    (timeout, dropped connection, 5xx, an unrecognisable response, or a withdrawal that is never
    confirmed), it is reported UNKNOWN and must be reconciled before anyone retries it.
    """

    # --- Capability flags: INTERNAL transfers (funds stay on the exchange) ---
    supports_sub_to_master_transfer: bool = False
    supports_master_to_sub_transfer: bool = False

    # When False, the connector can resolve the sub-account identifier itself (e.g. from the
    # credentials supplied at `connect`), so callers may omit it. Connectors that can do this
    # override this to False and resolve the missing side in `_resolve_internal_transfer_accounts`.
    requires_explicit_sub_account: bool = True

    # --- Capability flags: EXTERNAL transfers (crypto leaves / enters the exchange) ---
    supports_withdrawal: bool = False
    supports_deposit_address: bool = False
    # True when the exchange refuses arbitrary destinations and only withdraws to addresses that
    # were pre-whitelisted out of band (e.g. WazirX's Address Book). Such connectors take an
    # `address_book_id` instead of a raw `address`.
    requires_whitelisted_address: bool = False

    # Confirmation polling configuration (used for withdrawals).
    TRANSFER_STATUS_POLL_INTERVAL: float = 10.0
    TRANSFER_CONFIRMATION_TIMEOUT: float = 1800.0  # 30 minutes
    # Raise an operator-visible warning after this many consecutive failed status checks, and again
    # every time that many more fail, rather than only logging each one.
    TRANSFER_STATUS_ERROR_ESCALATION: int = 3

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
        amount = self._validated_transfer_amount(amount)
        if self.requires_explicit_sub_account and not from_account:
            raise ValueError("from_account (the sub-account identifier) is required for a sub-to-master transfer.")
        self._verify_master_credentials()

        transfer_id = self._generate_transfer_id(TransferType.SUB_TO_MASTER)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.SUB_TO_MASTER,
            asset=asset,
            amount=amount,
            creation_timestamp=self._transfer_timestamp(),
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
        amount = self._validated_transfer_amount(amount)
        if self.requires_explicit_sub_account and not to_account:
            raise ValueError("to_account (the sub-account identifier) is required for a master-to-sub transfer.")
        self._verify_master_credentials()

        transfer_id = self._generate_transfer_id(TransferType.MASTER_TO_SUB)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.MASTER_TO_SUB,
            asset=asset,
            amount=amount,
            creation_timestamp=self._transfer_timestamp(),
            source=from_account,
            destination=to_account,
        )
        safe_ensure_future(self._create_transfer(transfer, **kwargs))
        return transfer_id

    # ---------------------------------------------------------------------
    # Public API - EXTERNAL transfers (crypto leaves / enters the exchange)
    # ---------------------------------------------------------------------
    def withdraw_to_address(
        self,
        asset: str,
        amount: Decimal,
        address: Optional[str] = None,
        network: Optional[str] = None,
        address_book_id: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Withdraw ``amount`` of ``asset`` out of the exchange.

        Exchanges differ in how the destination is specified:

        * arbitrary-address exchanges take ``address`` (+ optional ``network``);
        * whitelist-only exchanges (``requires_whitelisted_address``) take ``address_book_id`` --
          the id, or the name, of an Address Book entry that was whitelisted out of band. A raw
          address cannot be used there, because the exchange only accepts the entry id.

        Unlike internal transfers this settles on-chain, so it is reported as SUBMITTED first and
        confirmed later by :meth:`_request_transfer_status` polling.

        :return: the client transfer id
        """
        if not self.supports_withdrawal:
            raise NotImplementedError(f"{self.name} does not support withdrawals to a wallet address yet.")
        amount = self._validated_transfer_amount(amount)
        if self.requires_whitelisted_address:
            if not address_book_id:
                raise ValueError(
                    f"{self.name} only withdraws to pre-whitelisted destinations: pass address_book_id "
                    f"(the Address Book entry id or name). A raw address cannot be used."
                )
        elif not address:
            raise ValueError("address is required to withdraw funds.")

        transfer_id = self._generate_transfer_id(TransferType.WITHDRAWAL)
        transfer = WalletTransfer(
            client_transfer_id=transfer_id,
            transfer_type=TransferType.WITHDRAWAL,
            asset=asset,
            amount=amount,
            creation_timestamp=self._transfer_timestamp(),
            address=address,
            address_book_id=address_book_id,
            network=network,
        )
        safe_ensure_future(self._create_transfer(transfer, **kwargs))
        return transfer_id

    async def get_deposit_address(self, asset: str, network: Optional[str] = None) -> dict:
        """
        Fetch the exchange-side address to deposit ``asset`` INTO the exchange.

        A deposit cannot be initiated through an API -- the counterparty sends to this address --
        so this is a plain read, not a tracked transfer.
        """
        if not self.supports_deposit_address:
            raise NotImplementedError(f"{self.name} does not expose a deposit address endpoint.")
        return await self._request_deposit_address(asset=asset, network=network)

    def get_transfer(self, transfer_id: str) -> Optional[WalletTransfer]:
        return self.wallet_transfer_tracker.fetch_transfer(transfer_id)

    # ---------------------------------------------------------------------
    # Internal orchestration
    # ---------------------------------------------------------------------
    async def _create_transfer(self, transfer: WalletTransfer, **kwargs) -> None:
        tracker = self.wallet_transfer_tracker
        tracker.start_tracking_transfer(transfer)
        try:
            # Dispatch on the transfer CATEGORY so internal and external movements stay separate.
            if transfer.transfer_type.is_external:
                transfer_update = await self._place_withdrawal(transfer, **kwargs)
            else:
                # Resolve omitted accounts first, so a self-transfer is refused before any request.
                await self._resolve_internal_transfer_accounts(transfer)
                self._validate_internal_transfer_accounts(transfer)
                transfer_update = await self._place_internal_transfer(transfer, **kwargs)
            tracker.process_transfer_update(transfer_update)

            still_active = tracker.active_transfers.get(transfer.client_transfer_id)
            if still_active is not None and still_active.state == TransferState.SUBMITTED:
                # Withdrawal accepted but not yet settled - confirm asynchronously.
                safe_ensure_future(self._transfer_status_polling_loop(transfer.client_transfer_id))
        except asyncio.CancelledError:
            if transfer.request_sent:
                # Cancelled mid-flight: the request may still execute, so don't leave it pending.
                self._record_transfer_outcome(
                    transfer, TransferState.UNKNOWN, "Cancelled after the request was sent.", "CancelledError"
                )
            raise
        except Exception as exception:
            outcome = self._classify_transfer_error(transfer, exception)
            if outcome is TransferState.UNKNOWN:
                self.logger().network(
                    f"Outcome unknown for {transfer.transfer_type.value} transfer "
                    f"({transfer.client_transfer_id}) after it was sent to {self.name}: {exception}",
                    exc_info=True,
                    app_warning_msg=f"{transfer.transfer_type.value} transfer {transfer.client_transfer_id} on "
                                    f"{self.name} may have gone through. Check the exchange before retrying.",
                )
            elif not transfer.request_sent:
                self.logger().network(
                    f"{transfer.transfer_type.value} transfer ({transfer.client_transfer_id}) was not sent to "
                    f"{self.name}: {exception}",
                    exc_info=True,
                    app_warning_msg=f"{transfer.transfer_type.value} transfer on {self.name} was not sent "
                                    f"(nothing moved): {exception}",
                )
            else:
                self.logger().network(
                    f"{self.name} rejected {transfer.transfer_type.value} transfer "
                    f"({transfer.client_transfer_id}): {exception}",
                    exc_info=True,
                    app_warning_msg=f"{self.name} rejected the {transfer.transfer_type.value} transfer "
                                    f"(nothing moved): {exception}",
                )
            # Some errors (e.g. asyncio.TimeoutError) carry no text; fall back to the type name.
            self._record_transfer_outcome(
                transfer, outcome, str(exception) or type(exception).__name__, type(exception).__name__
            )

    def _record_transfer_outcome(
        self, transfer: WalletTransfer, state: TransferState, error_message: str, error_type: str
    ) -> None:
        self.wallet_transfer_tracker.process_transfer_update(
            TransferUpdate(
                client_transfer_id=transfer.client_transfer_id,
                new_state=state,
                update_timestamp=self._transfer_timestamp(),
                misc_updates={"error_message": error_message, "error_type": error_type},
            )
        )

    @classmethod
    def _classify_transfer_error(cls, transfer: WalletTransfer, exception: Exception) -> TransferState:
        """
        FAILED only when the funds definitely did not move: the request was never sent, or the
        exchange answered and refused it. Anything else after sending -- a timeout, a dropped
        connection, a 5xx, a response that cannot be read -- may have executed, so it is UNKNOWN.
        """
        if isinstance(exception, TransferOutcomeUnknownError):
            return TransferState.UNKNOWN
        if isinstance(exception, TransferRejectedError) or not transfer.request_sent:
            return TransferState.FAILED
        status = cls._http_status_of(exception)
        if status is not None and 400 <= status < 500:
            return TransferState.FAILED  # the exchange answered with a client error: not executed
        return TransferState.UNKNOWN

    @staticmethod
    def _http_status_of(exception: Exception) -> Optional[int]:
        status = getattr(exception, "status", None)
        if isinstance(status, int):
            return status
        match = _HTTP_STATUS_PATTERN.search(str(exception))
        return int(match.group(1)) if match else None

    async def _transfer_status_polling_loop(self, client_transfer_id: str) -> None:
        """
        Polls the exchange for the status of a submitted withdrawal until it reaches a terminal
        state or the confirmation timeout elapses. This is what guarantees a withdrawal is actually
        confirmed before it is reported as completed.

        The timeout runs on the monotonic clock: ``current_timestamp`` only advances when the
        connector is ticked, so it can stand still (or be NaN) and the loop would never end. A
        withdrawal still unconfirmed at the timeout was accepted by the exchange and may yet settle,
        so it is reported UNKNOWN rather than FAILED.
        """
        deadline = time.monotonic() + self.TRANSFER_CONFIRMATION_TIMEOUT
        consecutive_errors = 0
        last_error: Optional[str] = None
        while True:
            transfer = self.wallet_transfer_tracker.active_transfers.get(client_transfer_id)
            if transfer is None:
                return  # reached a terminal state and is no longer tracked

            try:
                transfer_update = await self._request_transfer_status(transfer)
                self.wallet_transfer_tracker.process_transfer_update(transfer_update)
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                consecutive_errors += 1
                last_error = f"{type(exception).__name__}: {exception}"
                if consecutive_errors % self.TRANSFER_STATUS_ERROR_ESCALATION == 0:
                    self.logger().network(
                        f"Status check for transfer {client_transfer_id} failed {consecutive_errors} times "
                        f"in a row: {exception}",
                        exc_info=True,
                        app_warning_msg=f"Cannot confirm transfer {client_transfer_id} on {self.name}: "
                                        f"its status check keeps failing.",
                    )
                else:
                    self.logger().exception(
                        f"Unexpected error while polling status of transfer {client_transfer_id}."
                    )

            if self.wallet_transfer_tracker.active_transfers.get(client_transfer_id) is None:
                return

            if time.monotonic() >= deadline:
                self.wallet_transfer_tracker.process_transfer_update(
                    TransferUpdate(
                        client_transfer_id=client_transfer_id,
                        new_state=TransferState.UNKNOWN,
                        update_timestamp=self._transfer_timestamp(),
                        misc_updates={
                            "error_message": (
                                f"Not confirmed within {self.TRANSFER_CONFIRMATION_TIMEOUT:.0f}s of being accepted"
                                + (f" (last status error: {last_error})" if last_error else "")
                                + ". It may still settle; check the exchange's withdrawal history."
                            ),
                            "error_type": "TimeoutError",
                        },
                    )
                )
                return

            await asyncio.sleep(self.TRANSFER_STATUS_POLL_INTERVAL)

    def _generate_transfer_id(self, transfer_type: TransferType) -> str:
        prefix = getattr(self, "client_order_id_prefix", "") or ""
        return f"{prefix}xfer-{transfer_type.value}-{uuid.uuid4().hex}"

    def _transfer_timestamp(self) -> float:
        return safe_timestamp(self.current_timestamp)

    # ---------------------------------------------------------------------
    # Validation shared by every connector
    # ---------------------------------------------------------------------
    @staticmethod
    def _validated_transfer_amount(amount: Any) -> Decimal:
        """Coerce ``amount`` to a Decimal and refuse anything but a positive, finite value."""
        try:
            value = amount if isinstance(amount, Decimal) else Decimal(str(amount))
        except (InvalidOperation, TypeError, ValueError):
            raise ValueError(f"Transfer amount {amount!r} is not a number.")
        if not value.is_finite() or value <= 0:
            raise ValueError(f"Transfer amount must be a positive, finite number (got {amount!r}).")
        return value

    @staticmethod
    def _validate_internal_transfer_accounts(transfer: WalletTransfer) -> None:
        """Refuse an internal transfer whose two sides are missing or are the same account."""
        source, destination = transfer.source, transfer.destination
        if not source or not destination:
            raise ValueError(
                f"Could not determine both accounts for the {transfer.transfer_type.value} transfer "
                f"(from={source!r}, to={destination!r})."
            )
        if str(source).strip().lower() == str(destination).strip().lower():
            raise ValueError(
                f"Refusing a {transfer.transfer_type.value} transfer from account {source} to itself. "
                f"If the configured account is the master, pass the sub-account explicitly."
            )

    @staticmethod
    def _amount_as_exact_float(amount: Decimal) -> float:
        """
        ``amount`` as a float, for exchange APIs that take a JSON number. Refuses any value a binary
        float would round, so the exchange is never sent a different amount than was asked for.
        """
        value = float(amount)
        if Decimal(repr(value)) != amount:
            raise ValueError(
                f"Amount {amount} cannot be sent exactly as a number ({value!r}); use fewer decimal places."
            )
        return value

    # ---------------------------------------------------------------------
    # Hooks for connectors to override - INTERNAL transfers
    # ---------------------------------------------------------------------
    def _verify_master_credentials(self) -> None:
        """
        Raise a clear error if master-account credentials are required but not configured.
        Connectors that require master creds for transfers override this.
        """
        return None

    async def _resolve_internal_transfer_accounts(self, transfer: WalletTransfer) -> None:
        """
        Fill in whichever side of an internal transfer the caller omitted, typically the configured
        master account. Runs before anything is sent; raise ValueError if a side cannot be resolved.
        """
        return None

    async def _place_internal_transfer(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        """
        Move funds within the exchange and return a COMPLETED update.

        Set ``transfer.request_sent = True`` immediately before sending the request that moves the
        funds. Raise :class:`TransferRejectedError` when the exchange explicitly refuses, and
        :class:`TransferOutcomeUnknownError` when its answer neither confirms nor refuses.
        """
        raise NotImplementedError

    # ---------------------------------------------------------------------
    # Hooks for connectors to override - EXTERNAL transfers
    # ---------------------------------------------------------------------
    async def _request_deposit_address(self, asset: str, network: Optional[str] = None) -> dict:
        """Return the exchange-side deposit address payload for ``asset`` (read-only)."""
        raise NotImplementedError

    async def _place_withdrawal(self, transfer: WalletTransfer, **kwargs) -> TransferUpdate:
        """
        Submit a withdrawal and return a SUBMITTED update (or COMPLETED where the exchange offers
        no way to follow it up). Same error contract as :meth:`_place_internal_transfer`, including
        setting ``transfer.request_sent = True`` immediately before the withdrawal request.
        """
        raise NotImplementedError

    async def _request_transfer_status(self, transfer: WalletTransfer) -> TransferUpdate:
        raise NotImplementedError
