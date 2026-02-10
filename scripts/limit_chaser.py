import asyncio
import os
from decimal import Decimal
from typing import Dict, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.order_executor.data_types import (
    ExecutionStrategy,
    LimitChaserConfig,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor

SUPPORTED_CONNECTORS = [
    "binance",
    "binance_perpetual",
    "binance_us",
    "kucoin",
    "gate_io",
    "mexc",
    "ascend_ex",
    "cube",
    "hyperliquid",
    "dexalot",
    "coindcx",
    "wazirx",
]


class LimitChaserExecutorConfig(BaseClientModel):
    """
    Configuration for the limit-chaser executor script.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the connector name ({', '.join(SUPPORTED_CONNECTORS)}): ",
            "prompt_on_new": True,
        },
    )
    trading_pair: str = Field(
        default="BTC-USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter the trading pair (e.g., BTC-USDT): ",
            "prompt_on_new": True,
        },
    )
    trade_type: str = Field(
        default="BUY",
        json_schema_extra={
            "prompt": lambda mi: "Enter the trade type (BUY or SELL): ",
            "prompt_on_new": True,
        },
    )
    amount: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the order amount in base (keep small for testing): ",
            "prompt_on_new": True,
        },
    )
    chaser_distance: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": lambda mi: "Distance from top of book (e.g., 0.001 = 0.1%): ",
            "prompt_on_new": True,
        },
    )
    refresh_threshold: Decimal = Field(
        default=Decimal("0.002"),
        json_schema_extra={
            "prompt": lambda mi: "When to reprice (fractional difference vs. current top price): ",
            "prompt_on_new": True,
        },
    )
    update_interval: float = Field(
        default=1.0,
        json_schema_extra={
            "prompt": lambda mi: "Control loop interval in seconds: ",
            "prompt_on_new": True,
        },
    )
    leverage: int = Field(
        default=1,
        json_schema_extra={
            "prompt": lambda mi: "Perpetual leverage (ignored for spot): ",
            "prompt_on_new": True,
        },
    )


class LimitChaserExecutorScript(ScriptStrategyBase):
    """
    Runs a single limit-chaser order using the v2 OrderExecutor.
    It keeps a maker order near the top of book and renews when price moves by `refresh_threshold`.
    """

    @classmethod
    def init_markets(cls, config: LimitChaserExecutorConfig):
        cls.markets = {config.connector_name: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: LimitChaserExecutorConfig):
        super().__init__(connectors, config)
        self.config = config
        self._executor: Optional[OrderExecutor] = None
        self._reported_close = False
        self._last_status_log = 0.0

    async def on_stop(self):
        if self._executor and not self._executor.is_closed:
            self._executor.early_stop()
            try:
                await asyncio.wait_for(self._executor.terminated.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.logger().warning("Timeout waiting for limit chaser executor to stop")

    def _get_initial_price(self, side: TradeType) -> Optional[Decimal]:
        """Return a top-of-book price to seed the executor budget check."""
        try:
            connector = self.connectors[self.config.connector_name]
            price_type = PriceType.BestBid if side == TradeType.BUY else PriceType.BestAsk
            price = connector.get_price_by_type(self.config.trading_pair, price_type)
            if price is None or price <= 0:
                price = connector.get_price_by_type(self.config.trading_pair, PriceType.MidPrice)
            if price is None or price <= 0:
                return None
            return Decimal(str(price))
        except Exception as e:
            self.logger().warning(f"Unable to fetch initial price: {e}")
            return None

    def _build_executor_config(self, seed_price: Decimal, side: TradeType) -> OrderExecutorConfig:
        return OrderExecutorConfig(
            id=f"lc_{int(self.current_timestamp)}",
            timestamp=self.current_timestamp,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=side,
            amount=self.config.amount,
            execution_strategy=ExecutionStrategy.LIMIT_CHASER,
            chaser_config=LimitChaserConfig(
                distance=self.config.chaser_distance,
                refresh_threshold=self.config.refresh_threshold,
            ),
            leverage=self.config.leverage,
            price=seed_price,
        )

    def on_tick(self):
        if self._executor is None:
            side = TradeType[self.config.trade_type.upper()]
            seed_price = self._get_initial_price(side)
            if seed_price is None:
                self.logger().warning("Waiting for a valid price before starting limit chaser...")
                return

            exec_config = self._build_executor_config(seed_price, side)
            self._executor = OrderExecutor(
                strategy=self,
                config=exec_config,
                update_interval=float(self.config.update_interval),
            )
            self._executor.start()
            self.logger().info(
                f"Started limit chaser on {self.config.connector_name} {self.config.trading_pair} "
                f"side={self.config.trade_type} amount={self.config.amount}"
            )
            return

        if self.current_timestamp - self._last_status_log > 5 and self._executor is not None:
            if self._executor._order and self._executor._order.order:
                price = self._executor._order.order.price
                self.logger().info(f"Active chaser order id={self._executor._order.order_id} price={price}")
            self._last_status_log = self.current_timestamp

        if self._executor.is_closed and not self._reported_close:
            self.logger().info(
                f"Chaser executor closed: close_type={self._executor.close_type} "
                f"held_orders={len(self._executor._held_position_orders)}"
            )
            self._reported_close = True
