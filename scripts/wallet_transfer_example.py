import os
from decimal import Decimal
from typing import Dict

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent, WalletTransferCompletedEvent, WalletTransferFailedEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class WalletTransferConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    # Connector that holds the MASTER account credentials (configure master creds via `connect`).
    connector: str = Field("wazirx")
    # A trading pair is only needed so the connector initializes and reports "ready".
    trading_pair: str = Field("BTC-INR")
    # Asset and amount to move between the accounts.
    asset: str = Field("USDT")
    amount: Decimal = Field(Decimal("1"))
    # Transfer direction: "sub_to_master" or "master_to_sub".
    direction: str = Field("sub_to_master")
    # The sub-account involved (email for WazirX, coindcx_id for CoinDCX). The master side of the
    # transfer is always the connector's configured master account, so it is not specified here.
    sub_account: str = Field("sub-account@example.com")
    # Optional: override the master-account identifier. Leave blank to use the configured master.
    master_account: str = Field("")


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
