from decimal import Decimal
from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from hummingbot.core.data_type.common import MarketDict, OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.best_price_executor.data_types import BestPriceExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.symmetric_grid_executor.data_types import SymmetricGridExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class GridV2ExecutorConfig(ControllerConfigBase):
    """
    Configuration required to run the GridV2Executor controller.
    """
    controller_type: str = "generic"
    controller_name: str = "grid_v2"
    candles_config: List[CandlesConfig] = []

    connector_name: str = Field(
        default="binance_perpetual",
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the connector name (e.g., binance_perpetual): ",
        }
    )
    trading_pair: str = Field(
        default="WLD-USDT",
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the trading pair (e.g., WLD-USDT): ",
        }
    )
    leverage: int = Field(
        default=20,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the leverage to use (set 1 for spot): ",
        }
    )
    position_mode: PositionMode = Field(
        default=PositionMode.HEDGE,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the position mode (HEDGE/ONEWAY): ",
        }
    )

    buy_spreads: List[Decimal] = Field(
        default="0.01,0.02",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of buy spreads as decimals (e.g., 0.01,0.02): ",
            "is_updatable": True,
        }
    )
    sell_spreads: List[Decimal] = Field(
        default="0.01,0.02",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of sell spreads as decimals (e.g., 0.01,0.02): ",
            "is_updatable": True,
        }
    )
    buy_amounts_quote: List[Decimal] = Field(
        default="10,20",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of buy quote amounts (e.g., 10,20): ",
            "is_updatable": True,
        }
    )
    sell_amounts_quote: List[Decimal] = Field(
        default="10,20",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of sell quote amounts (e.g., 10,20): ",
            "is_updatable": True,
        }
    )

    total_balance: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the target base balance to maintain (e.g., 0): ",
            "is_updatable": True,
        }
    )
    max_exposure: Decimal = Field(
        default=Decimal("100"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the maximum net base exposure before forcing an exit (e.g., 100): ",
            "is_updatable": True,
        }
    )
    target_profit: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the target profit as a decimal (e.g., 0.01 for 1%): ",
            "is_updatable": True,
        }
    )
    price_refresh_tolerance: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the price refresh tolerance as a decimal (e.g., 0.01 for 1%): ",
            "is_updatable": True,
        }
    )
    max_open_orders: int = Field(
        default=8,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the maximum number of open grid orders: ",
            "is_updatable": True,
        }
    )
    order_frequency: int = Field(
        default=3,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the order frequency in seconds: ",
            "is_updatable": True,
        }
    )
    price_diff: Decimal = Field(
        default=Decimal("0"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the best-price executor price diff as a decimal (e.g., 0): ",
            "is_updatable": True,
        }
    )
    keep_position: bool = Field(
        default=False,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Keep position after stopping executors? (true/false): ",
            "is_updatable": True,
        }
    )
    open_order_type: OrderType = Field(
        default=OrderType.LIMIT_MAKER,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the open order type (LIMIT/LIMIT_MAKER): ",
            "is_updatable": True,
        }
    )
    max_orders_per_batch: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter max orders per batch, or leave blank for all levels: ",
            "is_updatable": True,
        }
    )
    min_order_amount_quote: Decimal = Field(
        default=Decimal("5"),
        gt=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the minimum order amount in quote: ",
            "is_updatable": True,
        }
    )
    stop_loss: Optional[Decimal] = Field(
        default=None,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter stop loss as a decimal, or leave blank to disable: ",
            "is_updatable": True,
        }
    )
    time_limit: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter time limit in seconds, or leave blank to disable: ",
            "is_updatable": True,
        }
    )

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

    @field_validator("open_order_type", mode="before")
    @classmethod
    def validate_order_type(cls, value) -> OrderType:
        if isinstance(value, OrderType):
            return value
        if value is None or value == "":
            return OrderType.LIMIT_MAKER
        if isinstance(value, int):
            return OrderType(value)
        if isinstance(value, str):
            cleaned = value.replace("OrderType.", "").upper()
            if cleaned in OrderType.__members__:
                return OrderType[cleaned]
        raise ValueError(f"Invalid order type: {value}")

    @field_validator("max_orders_per_batch", "time_limit", mode="before")
    @classmethod
    def parse_optional_int(cls, value):
        if value is None or value == "":
            return None
        return int(value)

    @field_validator("stop_loss", mode="before")
    @classmethod
    def parse_optional_decimal(cls, value):
        if value is None or value == "":
            return None
        return Decimal(str(value))

    @model_validator(mode="after")
    def validate_levels(self):
        if len(self.buy_spreads) != len(self.buy_amounts_quote):
            raise ValueError("buy_spreads and buy_amounts_quote must have the same number of entries")
        if len(self.sell_spreads) != len(self.sell_amounts_quote):
            raise ValueError("sell_spreads and sell_amounts_quote must have the same number of entries")
        if not self.buy_spreads and not self.sell_spreads:
            raise ValueError("At least one buy or sell level must be configured")
        if self.buy_spreads != self.sell_spreads:
            raise ValueError("grid_v2 uses SymmetricGridExecutor, so buy_spreads and sell_spreads must match")
        if self.buy_amounts_quote != self.sell_amounts_quote:
            raise ValueError("grid_v2 uses SymmetricGridExecutor, so buy_amounts_quote and sell_amounts_quote must match")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class GridV2Executor(ControllerBase):
    def __init__(self, config: GridV2ExecutorConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.initialize_rate_sources()

    def initialize_rate_sources(self):
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=self.config.connector_name,
                          trading_pair=self.config.trading_pair)
        ])

    def active_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.executors_info if executor.is_active]

    def active_grid_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "symmetric_grid_executor"]

    def active_target_profit_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "order_executor"
                and executor.config.level_id
                and executor.config.level_id.startswith("grid_v2_tp_")]

    def active_exposure_exit_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "best_price_executor"
                and executor.config.level_id
                and executor.config.level_id.startswith("grid_v2_exposure_exit_")]

    async def update_processed_data(self):
        fair_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        self.processed_data["fair_price"] = fair_price
        self.processed_data["mid_price"] = fair_price

    def determine_executor_actions(self) -> List[ExecutorAction]:
        active_grid_executors = self.active_grid_executors()
        if active_grid_executors:
            return self.create_risk_management_actions(active_grid_executors[0])

        return [CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=SymmetricGridExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                spread_percentages=list(self.config.buy_spreads),
                order_amounts_quote=list(self.config.buy_amounts_quote),
                fair_price=None,
                order_type=self.config.open_order_type,
                order_frequency=self.config.order_frequency,
                max_orders_per_batch=self.config.max_orders_per_batch,
                min_order_amount_quote=self.config.min_order_amount_quote,
                price_refresh_tolerance=self.config.price_refresh_tolerance,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
                leverage=self.config.leverage,
                level_id="grid_v2_symmetric",
            )
        )]

    @staticmethod
    def _to_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def create_risk_management_actions(self, grid_executor: ExecutorInfo) -> List[ExecutorAction]:
        exposure = self.calculate_exposure(grid_executor)
        if abs(exposure) > self.config.max_exposure:
            action = self.create_exposure_exit_action(exposure)
            return [action] if action is not None else []

        action = self.create_target_profit_action(grid_executor, exposure)
        return [action] if action is not None else []

    def calculate_exposure(self, grid_executor: ExecutorInfo) -> Decimal:
        custom_info = grid_executor.custom_info or {}
        net_inventory_base = self._to_decimal(custom_info.get("net_inventory_base"))
        return net_inventory_base - self.config.total_balance

    def get_average_entry_price_for_exposure(self, grid_executor: ExecutorInfo, exposure: Decimal) -> Decimal:
        custom_info = grid_executor.custom_info or {}
        filled_orders = custom_info.get("filled_orders", [])
        side_name = TradeType.BUY.name if exposure > Decimal("0") else TradeType.SELL.name
        total_base = Decimal("0")
        total_quote = Decimal("0")
        for order in filled_orders:
            if order.get("trade_type") != side_name:
                continue
            total_base += self._to_decimal(order.get("executed_amount_base"))
            total_quote += self._to_decimal(order.get("executed_amount_quote"))
        if total_base > Decimal("0"):
            return total_quote / total_base
        return Decimal("0")

    def create_target_profit_action(self, grid_executor: ExecutorInfo, exposure: Decimal) -> Optional[CreateExecutorAction]:
        if exposure == Decimal("0") or self.config.target_profit == Decimal("0"):
            return None
        if self.active_target_profit_executors():
            return None

        avg_price = self.get_average_entry_price_for_exposure(grid_executor, exposure)
        if avg_price <= Decimal("0"):
            return None

        side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
        price = avg_price * (Decimal("1") + self.config.target_profit) if side == TradeType.SELL else avg_price * (Decimal("1") - self.config.target_profit)
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=abs(exposure),
                price=price,
                position_action=PositionAction.CLOSE,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
                level_id=f"grid_v2_tp_{side.name.lower()}",
            )
        )

    def create_exposure_exit_action(self, exposure: Decimal) -> Optional[CreateExecutorAction]:
        side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
        level_id = f"grid_v2_exposure_exit_{side.name.lower()}"
        if any(executor.config.level_id == level_id for executor in self.active_exposure_exit_executors()):
            return None
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=BestPriceExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=abs(exposure),
                position_action=PositionAction.CLOSE,
                price_diff=self.config.price_diff,
                leverage=self.config.leverage,
                level_id=level_id,
            )
        )
