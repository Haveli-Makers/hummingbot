import os
from decimal import Decimal
from typing import Dict

from pydantic import Field, field_validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import (
    MarketEvent,
    WalletTransferCompletedEvent,
    WalletTransferCreatedEvent,
    WalletTransferFailedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

# Connectors that support the generic wallet-transfer capability (WalletTransferExecutorMixin).
SUPPORTED_CONNECTORS = ["wazirx", "coindcx", "csx"]
# Internal directions keep funds on the exchange; "withdraw" sends crypto OUT to a wallet.
SUPPORTED_DIRECTIONS = ["sub_to_master", "master_to_sub", "withdraw"]


class WalletTransferConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector: str = Field(
        default="wazirx",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the connector to transfer on ({', '.join(SUPPORTED_CONNECTORS)}): ",
            "prompt_on_new": True,
        },
    )
    direction: str = Field(
        default="withdraw",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the operation ({' / '.join(SUPPORTED_DIRECTIONS)}): ",
            "prompt_on_new": True,
        },
    )
    asset: str = Field(
        default="USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter the asset (e.g. USDT, INR): ",
            "prompt_on_new": True,
        },
    )
    amount: Decimal = Field(
        default=Decimal("1.2"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the amount: ",
            "prompt_on_new": True,
        },
    )
    # --- internal transfers (sub_to_master / master_to_sub) ---
    sub_account: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: (
                "[internal only] SUB account identifier "
                "(email for WazirX, coindcx_id for CoinDCX, brokerID for CSX; "
                "blank on CoinDCX/CSX to auto-resolve): "
            ),
            "prompt_on_new": True,
        },
    )
    master_account: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "[internal only] MASTER account override (blank = configured master): ",
            "prompt_on_new": True,
        },
    )
    # --- external transfers (withdraw) ---
    destination: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: (
                "[withdraw only] destination: a wallet address, OR for WazirX the Address Book "
                "entry id/name it was whitelisted under (e.g. 171347 or 'Binance'): "
            ),
            "prompt_on_new": True,
        },
    )
    network: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "[withdraw only] network/chain (e.g. eth, trx); blank = exchange default: ",
            "prompt_on_new": True,
        },
    )
    trading_pair: str = Field(
        default="BTC-INR",
        json_schema_extra={
            "prompt": lambda mi: "A trading pair used only to initialise the connector (e.g. BTC-INR): ",
            "prompt_on_new": True,
        },
    )

    @field_validator("connector", mode="before")
    @classmethod
    def _validate_connector(cls, value):
        value = str(value).strip().lower()
        if value not in SUPPORTED_CONNECTORS:
            raise ValueError(f"Connector must be one of: {', '.join(SUPPORTED_CONNECTORS)}")
        return value

    @field_validator("direction", mode="before")
    @classmethod
    def _validate_direction(cls, value):
        value = str(value).strip().lower()
        if value not in SUPPORTED_DIRECTIONS:
            raise ValueError(f"Operation must be one of: {', '.join(SUPPORTED_DIRECTIONS)}")
        return value


class WalletTransferExample(ScriptStrategyBase):
    """
    Example script showing the generic wallet-transfer capability end to end.

    Any connector that mixes in ``WalletTransferExecutorMixin`` (WazirX, CoinDCX, CSX) exposes the
    same fire-and-forget methods and emits the same ``WalletTransfer*`` market events, handled here
    exactly like an order:

    * internal (``sub_to_master`` / ``master_to_sub``) -> ``transfer_to_master`` / ``transfer_to_sub``
      -> settles immediately (Created -> Completed).
    * external (``withdraw``) -> ``withdraw_to_address`` -> Created (accepted, SUBMITTED) then, after
      the connector confirms it on-chain by polling, Completed (with the tx hash) or Failed.

    The transfer fires once on the first tick; the strategy keeps running so the connector can
    confirm a withdrawal. Stop it (``stop``) once you see the terminal event.
    """

    @classmethod
    def init_markets(cls, config: WalletTransferConfig):
        cls.markets = {config.connector: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: WalletTransferConfig):
        super().__init__(connectors)
        self.config = config
        self._transfer_started = False
        self._transfer_id = None

        # Subscribe to the wallet-transfer events on the target connector.
        self._created_forwarder = EventForwarder(self._on_transfer_created)
        self._completed_forwarder = EventForwarder(self._on_transfer_completed)
        self._failed_forwarder = EventForwarder(self._on_transfer_failed)
        connector = self.connectors[config.connector]
        connector.add_listener(MarketEvent.WalletTransferCreated, self._created_forwarder)
        connector.add_listener(MarketEvent.WalletTransferCompleted, self._completed_forwarder)
        connector.add_listener(MarketEvent.WalletTransferFailed, self._failed_forwarder)

    def on_tick(self):
        if self._transfer_started:
            return
        self._transfer_started = True

        connector = self.connectors[self.config.connector]
        try:
            if self.config.direction == "withdraw":
                self._transfer_id = self._start_withdrawal(connector)
            elif self.config.direction == "master_to_sub":
                self._transfer_id = connector.transfer_to_sub(
                    asset=self.config.asset,
                    amount=self.config.amount,
                    to_account=self.config.sub_account or None,
                    from_account=self.config.master_account or None,
                )
            else:  # sub_to_master
                self._transfer_id = connector.transfer_to_master(
                    asset=self.config.asset,
                    amount=self.config.amount,
                    from_account=self.config.sub_account or None,
                    to_account=self.config.master_account or None,
                )
            self.logger().info(
                f"Submitted {self.config.direction} {self._transfer_id}: "
                f"{self.config.amount} {self.config.asset} on {self.config.connector}. Waiting for confirmation..."
            )
        except Exception as exception:
            # e.g. missing destination, coin under maintenance, IP not whitelisted, below minimum.
            self.logger().error(f"Could not start {self.config.direction}: {exception}")

    def _start_withdrawal(self, connector) -> str:
        """
        Fire a withdrawal. Whitelist-only exchanges (WazirX) take an Address Book id/name; others
        take a raw address -- the connector advertises which via ``requires_whitelisted_address``.
        """
        network = self.config.network or None
        if getattr(connector, "requires_whitelisted_address", False):
            return connector.withdraw_to_address(
                asset=self.config.asset,
                amount=self.config.amount,
                address_book_id=self.config.destination,
                network=network,
            )
        return connector.withdraw_to_address(
            asset=self.config.asset,
            amount=self.config.amount,
            address=self.config.destination,
            network=network,
        )

    def _on_transfer_created(self, event: WalletTransferCreatedEvent):
        # For a withdrawal this means the exchange ACCEPTED it (SUBMITTED); on-chain settlement follows.
        self.logger().info(
            f"Transfer {event.transfer_id} accepted by {self.config.connector} "
            f"(exchange id: {event.exchange_transfer_id})."
        )

    def _on_transfer_completed(self, event: WalletTransferCompletedEvent):
        self.logger().info(
            f"Transfer {event.transfer_id} COMPLETED: {event.amount} {event.asset} "
            f"(exchange id: {event.exchange_transfer_id}, tx: {event.tx_hash})."
        )

    def _on_transfer_failed(self, event: WalletTransferFailedEvent):
        self.logger().error(f"Transfer {event.transfer_id} FAILED: {event.error_message}")

    async def on_stop(self):
        connector = self.connectors[self.config.connector]
        connector.remove_listener(MarketEvent.WalletTransferCreated, self._created_forwarder)
        connector.remove_listener(MarketEvent.WalletTransferCompleted, self._completed_forwarder)
        connector.remove_listener(MarketEvent.WalletTransferFailed, self._failed_forwarder)
