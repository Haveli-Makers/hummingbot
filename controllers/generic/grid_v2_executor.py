from decimal import Decimal
from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from hummingbot.core.data_type.common import (
    MarketDict,
    OrderType,
    PositionAction,
    PositionMode,
    PriceType,
    TradeType,
)
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.best_price_executor.data_types import BestPriceExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class GridV2ExecutorConfig(ControllerConfigBase):
    """
    Configuration required to run the GridV2Executor controller.
    """
    controller_type: str = "generic"
    controller_name: str = "grid_v2_executor"
    candles_config: List[CandlesConfig] = []

    connector_name: str = "binance_perpetual"
    trading_pair: str = "WLD-USDT"
    leverage: int = 20
    position_mode: PositionMode = PositionMode.HEDGE

    buy_spreads: List[Decimal] = Field(default_factory=list, json_schema_extra={"is_updatable": True})
    sell_spreads: List[Decimal] = Field(default_factory=list, json_schema_extra={"is_updatable": True})
    buy_amounts_quote: List[Decimal] = Field(default_factory=list, json_schema_extra={"is_updatable": True})
    sell_amounts_quote: List[Decimal] = Field(default_factory=list, json_schema_extra={"is_updatable": True})

    total_balance: Decimal = Field(default=Decimal("1000"), json_schema_extra={"is_updatable": True})
    max_exposure: Decimal = Field(default=Decimal("100"), json_schema_extra={"is_updatable": True})
    target_profit: Decimal = Field(default=Decimal("0.01"), ge=Decimal("0"), json_schema_extra={"is_updatable": True})
    price_refresh_tolerance: Decimal = Field(default=Decimal("0.01"), ge=Decimal("0"), json_schema_extra={"is_updatable": True})
    max_open_orders: int = Field(default=8, json_schema_extra={"is_updatable": True})
    order_frequency: int = Field(default=3, json_schema_extra={"is_updatable": True})
    price_diff: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), json_schema_extra={"is_updatable": True})
    keep_position: bool = Field(default=False, json_schema_extra={"is_updatable": True})

    @field_validator("buy_spreads", "sell_spreads", mode="before")
    def parse_spread_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            if value.strip() == "":
                return []
            return [Decimal(x.strip()) for x in value.split(",") if x.strip()]
        return [Decimal(str(x)) for x in value]

    @field_validator("buy_amounts_quote", "sell_amounts_quote", mode="before")
    def parse_amount_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            if value.strip() == "":
                return []
            return [Decimal(x.strip()) for x in value.split(",") if x.strip()]
        return [Decimal(str(x)) for x in value]

    @model_validator(mode="after")
    def validate_levels(self):
        if len(self.buy_spreads) != len(self.buy_amounts_quote):
            raise ValueError("buy_spreads and buy_amounts_quote must have the same number of entries")
        if len(self.sell_spreads) != len(self.sell_amounts_quote):
            raise ValueError("sell_spreads and sell_amounts_quote must have the same number of entries")
        if not self.buy_spreads and not self.sell_spreads:
            raise ValueError("At least one buy or sell level must be configured")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class GridV2Executor(ControllerBase):
    def __init__(self, config: GridV2ExecutorConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self._last_fair_price: Optional[Decimal] = None
        self.initialize_rate_sources()

    def initialize_rate_sources(self):
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=self.config.connector_name,
                          trading_pair=self.config.trading_pair)
        ])

    def active_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.executors_info if executor.is_active]

    def active_grid_open_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "order_executor" and
                executor.config.level_id and executor.config.level_id.startswith("grid_open_")]

    def active_target_profit_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "order_executor" and
                executor.config.level_id and executor.config.level_id.startswith("grid_tp_")]

    def should_refresh_grid(self, fair_price: Decimal) -> bool:
        if self._last_fair_price is None or self._last_fair_price == Decimal("0"):
            return False
        price_move = abs(fair_price - self._last_fair_price) / self._last_fair_price
        return price_move >= self.config.price_refresh_tolerance

    async def update_processed_data(self):
        fair_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        self.processed_data["fair_price"] = fair_price
        self.processed_data["mid_price"] = fair_price

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        fair_price = self.processed_data.get("fair_price")
        if fair_price is None:
            return []

        if self.should_refresh_grid(fair_price):
            actions.extend(self.stop_grid_open_executors())
            self._last_fair_price = fair_price
            return actions

        actions.extend(self.create_missing_grid_open_orders(fair_price))
        actions.extend(self.create_target_profit_orders())

        exposure = self.calculate_exposure()
        if abs(exposure) > self.config.max_exposure:
            actions.append(self.create_exposure_exit_action(exposure))

        self._last_fair_price = fair_price
        return actions

    def stop_grid_open_executors(self) -> List[StopExecutorAction]:
        return [StopExecutorAction(controller_id=self.config.id, executor_id=executor.id)
                for executor in self.active_grid_open_executors()]

    def get_all_grid_open_level_ids(self) -> set[str]:
        return {
            executor.config.level_id
            for executor in self.executors_info
            if executor.config.type == "order_executor"
            and executor.config.level_id
            and executor.config.level_id.startswith("grid_open_")
        }

    def get_all_target_profit_level_ids(self) -> set[str]:
        return {
            executor.config.level_id
            for executor in self.executors_info
            if executor.config.type == "order_executor"
            and executor.config.level_id
            and executor.config.level_id.startswith("grid_tp_")
        }

    def create_missing_grid_open_orders(self, fair_price: Decimal) -> List[CreateExecutorAction]:
        actions: List[CreateExecutorAction] = []
        existing_level_ids = self.get_all_grid_open_level_ids()
        active_open_count = len(self.active_grid_open_executors())

        for index, (spread, amount_quote) in enumerate(zip(self.config.buy_spreads, self.config.buy_amounts_quote)):
            if active_open_count >= self.config.max_open_orders:
                break
            level_id = f"grid_open_buy_{index}"
            if level_id in existing_level_ids:
                continue
            actions.append(self.create_order_executor(
                side=TradeType.BUY,
                price=fair_price * (Decimal("1") - spread),
                amount_quote=amount_quote,
                level_id=level_id,
                position_action=PositionAction.OPEN
            ))
            active_open_count += 1

        for index, (spread, amount_quote) in enumerate(zip(self.config.sell_spreads, self.config.sell_amounts_quote)):
            if active_open_count >= self.config.max_open_orders:
                break
            level_id = f"grid_open_sell_{index}"
            if level_id in existing_level_ids:
                continue
            actions.append(self.create_order_executor(
                side=TradeType.SELL,
                price=fair_price * (Decimal("1") + spread),
                amount_quote=amount_quote,
                level_id=level_id,
                position_action=PositionAction.OPEN
            ))
            active_open_count += 1

        return actions

    def create_order_executor(
        self,
        side: TradeType,
        price: Decimal,
        amount_quote: Decimal,
        level_id: str,
        position_action: PositionAction,
    ) -> CreateExecutorAction:
        amount = Decimal("0")
        if price > Decimal("0"):
            amount = amount_quote / price
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=amount,
                price=price,
                position_action=position_action,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
                level_id=level_id,
            )
        )

    def create_target_profit_orders(self) -> List[CreateExecutorAction]:
        actions: List[CreateExecutorAction] = []
        existing_tp_ids = self.get_all_target_profit_level_ids()

        for executor in self.executors_info:
            if executor.config.type != "order_executor" or not executor.config.level_id:
                continue
            if not executor.config.level_id.startswith("grid_open_"):
                continue
            if executor.is_active:
                continue
            if executor.filled_amount_quote <= Decimal("0"):
                continue

            level_id = executor.config.level_id
            target_level_id = f"grid_tp_{level_id}"
            if target_level_id in existing_tp_ids:
                continue

            avg_price = executor.config.price or self.processed_data.get("fair_price")
            if avg_price is None:
                continue

            tp_side = TradeType.SELL if executor.config.side == TradeType.BUY else TradeType.BUY
            tp_price = avg_price * (Decimal("1") + self.config.target_profit) if tp_side == TradeType.SELL else avg_price * (Decimal("1") - self.config.target_profit)

            actions.append(CreateExecutorAction(
                controller_id=self.config.id,
                executor_config=OrderExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    side=tp_side,
                    amount=executor.config.amount,
                    price=tp_price,
                    position_action=PositionAction.CLOSE,
                    execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                    leverage=self.config.leverage,
                    level_id=target_level_id,
                )
            ))

        return actions

    def calculate_exposure(self) -> Decimal:
        buys = sum(
            executor.filled_amount_quote
            for executor in self.executors_info
            if executor.config.type == "order_executor"
            and executor.config.level_id
            and executor.config.level_id.startswith("grid_open_buy_")
        )
        sells = sum(
            executor.filled_amount_quote
            for executor in self.executors_info
            if executor.config.type == "order_executor"
            and executor.config.level_id
            and executor.config.level_id.startswith("grid_open_sell_")
        )
        return sells - buys - self.config.total_balance

    def create_exposure_exit_action(self, exposure: Decimal) -> CreateExecutorAction:
        side = TradeType.BUY if exposure < Decimal("0") else TradeType.SELL
        fair_price = self.processed_data.get("fair_price") or Decimal("0")
        amount = Decimal("0")
        if fair_price > Decimal("0"):
            amount = abs(exposure) / fair_price
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=BestPriceExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=amount,
                position_action=PositionAction.CLOSE,
                price_diff=self.config.price_diff,
                leverage=self.config.leverage,
                level_id=f"exposure_exit_{side.name.lower()}"
            )
        )
