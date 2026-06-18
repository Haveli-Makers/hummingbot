import os
from decimal import Decimal
from typing import Dict

from pydantic import Field, field_validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent, WalletTransferCompletedEvent, WalletTransferFailedEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

# Connectors that support the generic wallet-transfer capability (WalletTransferExecutorMixin).
SUPPORTED_CONNECTORS = ["wazirx", "coindcx", "csx"]
SUPPORTED_DIRECTIONS = ["sub_to_master", "master_to_sub"]


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
        default="master_to_sub",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the transfer direction ({' / '.join(SUPPORTED_DIRECTIONS)}): ",
            "prompt_on_new": True,
        },
    )
    sub_account: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: (
                "Enter the SUB account identifier "
                "(email for WazirX, coindcx_id for CoinDCX, brokerID for CSX): "
            ),
            "prompt_on_new": True,
        },
    )
    asset: str = Field(
        default="INR",
        json_schema_extra={
            "prompt": lambda mi: "Enter the asset to transfer (e.g. USDT, INR): ",
            "prompt_on_new": True,
        },
    )
    amount: Decimal = Field(
        default=Decimal("10"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the amount to transfer: ",
            "prompt_on_new": True,
        },
    )
    master_account: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: (
                "Enter the MASTER account identifier to override auto-resolution "
                "(leave blank to use the connector's configured master): "
            ),
            "prompt_on_new": True,
        },
    )
    trading_pair: str = Field(
        default="BTC-INR",
        json_schema_extra={
            "prompt": lambda mi: "Enter a trading pair used only to initialise the connector (e.g. BTC-INR): ",
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
            raise ValueError(f"Direction must be one of: {', '.join(SUPPORTED_DIRECTIONS)}")
        return value


class WalletTransferExample(ScriptStrategyBase):
    """
    Example script that triggers a one-off sub-account -> master-account transfer and logs the
    outcome via wallet-transfer events.

    This demonstrates the generic wallet-transfer capability: any connector that mixes in
    ``WalletTransferExecutorMixin`` (e.g. WazirX, CoinDCX) exposes ``transfer_to_master`` and emits
    ``WalletTransferCompleted`` / ``WalletTransferFailed`` events, handled here exactly like an order.
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
        self._completed_forwarder = EventForwarder(self._on_transfer_completed)
        self._failed_forwarder = EventForwarder(self._on_transfer_failed)
        connector = self.connectors[config.connector]
        connector.add_listener(MarketEvent.WalletTransferCompleted, self._completed_forwarder)
        connector.add_listener(MarketEvent.WalletTransferFailed, self._failed_forwarder)

    def on_tick(self):
        if self._transfer_started:
            return
        self._transfer_started = True

        connector = self.connectors[self.config.connector]
        sub_account = self.config.sub_account
        master_account = self.config.master_account or None
        try:
            if self.config.direction == "master_to_sub":
                self._transfer_id = connector.transfer_to_sub(
                    asset=self.config.asset,
                    amount=self.config.amount,
                    to_account=sub_account,
                    from_account=master_account,
                )
            else:
                self._transfer_id = connector.transfer_to_master(
                    asset=self.config.asset,
                    amount=self.config.amount,
                    from_account=sub_account,
                    to_account=master_account,
                )
            self.logger().info(
                f"Submitted {self.config.direction} transfer {self._transfer_id}: "
                f"{self.config.amount} {self.config.asset} (sub={sub_account}) on {self.config.connector}."
            )
        except Exception as exception:
            self.logger().error(f"Failed to start transfer: {exception}")

    def _on_transfer_completed(self, event: WalletTransferCompletedEvent):
        self.logger().info(
            f"Transfer {event.transfer_id} COMPLETED: {event.amount} {event.asset} "
            f"(exchange id: {event.exchange_transfer_id})."
        )

    def _on_transfer_failed(self, event: WalletTransferFailedEvent):
        self.logger().error(f"Transfer {event.transfer_id} FAILED: {event.error_message}")

    async def on_stop(self):
        connector = self.connectors[self.config.connector]
        connector.remove_listener(MarketEvent.WalletTransferCompleted, self._completed_forwarder)
        connector.remove_listener(MarketEvent.WalletTransferFailed, self._failed_forwarder)
