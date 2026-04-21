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


class LimitChaserRoundTripConfig(BaseClientModel):
    """
    Configuration for the limit-chaser round-trip script.
    Executes a BUY then a SELL (or SELL then BUY) in a single session
    so that the full tax & profit report is generated.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field(
        default="coindcx",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the connector name ({', '.join(SUPPORTED_CONNECTORS)}): ",
            "prompt_on_new": True,
        },
    )
    trading_pair: str = Field(
        default="USDT-INR",
        json_schema_extra={
            "prompt": lambda mi: "Enter the trading pair (e.g., USDT-INR): ",
            "prompt_on_new": True,
        },
    )
    first_side: str = Field(
        default="BUY",
        json_schema_extra={
            "prompt": lambda mi: "Enter the first trade side (BUY or SELL): ",
            "prompt_on_new": True,
        },
    )
    amount: Decimal = Field(
        default=Decimal("1.2"),
        json_schema_extra={
            "prompt": lambda mi: "Enter the order amount in base: ",
            "prompt_on_new": True,
        },
    )
    chaser_distance: Decimal = Field(
        default=Decimal("0.0002"),
        json_schema_extra={
            "prompt": lambda mi: "Distance from top of book (e.g., 0.0002 = 0.02%): ",
            "prompt_on_new": True,
        },
    )
    refresh_threshold: Decimal = Field(
        default=Decimal("0.005"),
        json_schema_extra={
            "prompt": lambda mi: "When to reprice (fractional difference vs. current top price): ",
            "prompt_on_new": True,
        },
    )
    update_interval: float = Field(
        default=3.0,
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
    delay_between_legs: float = Field(
        default=5.0,
        json_schema_extra={
            "prompt": lambda mi: "Seconds to wait between BUY and SELL legs: ",
            "prompt_on_new": True,
        },
    )


class LimitChaserRoundTripScript(ScriptStrategyBase):
    @classmethod
    def init_markets(cls, config: LimitChaserRoundTripConfig):
        cls.markets = {config.connector_name: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: LimitChaserRoundTripConfig):
        super().__init__(connectors, config)
        self.config = config
        self._executor: Optional[OrderExecutor] = None
        self._leg: int = 0
        self._leg1_done: bool = False
        self._leg2_done: bool = False
        self._leg1_close_ts: float = 0.0
        self._last_status_log: float = 0.0
        self._reported_close: bool = False

        first = self.config.first_side.upper()
        if first == "BUY":
            self._sides = [TradeType.BUY, TradeType.SELL]
        else:
            self._sides = [TradeType.SELL, TradeType.BUY]

    async def on_stop(self):
        if self._executor and not self._executor.is_closed:
            self._executor.early_stop()
            try:
                await asyncio.wait_for(self._executor.terminated.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.logger().warning("Timeout waiting for executor to stop")

    def _get_initial_price(self, side: TradeType) -> Optional[Decimal]:
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
            id=f"lcrt_{side.name.lower()}_{int(self.current_timestamp)}",
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

    def _start_leg(self, side: TradeType):
        seed_price = self._get_initial_price(side)
        if seed_price is None:
            self.logger().warning("Waiting for a valid price...")
            return False

        exec_config = self._build_executor_config(seed_price, side)
        self._executor = OrderExecutor(
            strategy=self,
            config=exec_config,
            update_interval=float(self.config.update_interval),
        )
        self._executor.start()
        self._reported_close = False
        self.logger().info(
            f"[Leg {self._leg}] Started limit chaser {side.name} "
            f"{self.config.amount} {self.config.trading_pair}"
        )
        return True

    def on_tick(self):
        # --- Leg 1: Start first leg ---
        if self._leg == 0:
            self._leg = 1
            if not self._start_leg(self._sides[0]):
                self._leg = 0
            return

        if (self.current_timestamp - self._last_status_log > 5
                and self._executor is not None
                and not self._executor.is_closed):
            if self._executor._order and self._executor._order.order:
                price = self._executor._order.order.price
                self.logger().info(
                    f"[Leg {self._leg}] Active chaser {self._sides[self._leg - 1].name} "
                    f"id={self._executor._order.order_id} price={price}"
                )
            self._last_status_log = self.current_timestamp

        # --- Leg 1 filled → wait delay → start Leg 2 ---
        if self._leg == 1 and self._executor and self._executor.is_closed:
            if not self._leg1_done:
                self._leg1_done = True
                self._leg1_close_ts = self.current_timestamp
                self.logger().info(
                    f"[Leg 1] {self._sides[0].name} filled. "
                    f"Waiting {self.config.delay_between_legs}s before starting {self._sides[1].name}..."
                )

            if self.current_timestamp - self._leg1_close_ts >= self.config.delay_between_legs:
                self._leg = 2
                if not self._start_leg(self._sides[1]):
                    self._leg = 1
            return

        # --- Leg 2 filled → done ---
        if self._leg == 2 and self._executor and self._executor.is_closed and not self._reported_close:
            self.logger().info(
                f"[Leg 2] {self._sides[1].name} filled. "
                f"Round-trip complete! Check logs above for Tax & Profit Report."
            )
            self._reported_close = True
            self._leg2_done = True
