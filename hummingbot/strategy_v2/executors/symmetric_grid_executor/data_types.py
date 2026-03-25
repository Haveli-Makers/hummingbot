from decimal import Decimal
from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.models.executors import TrackedOrder


class FairPriceType(Enum):
    """Price type used to determine the fair price for the symmetric grid."""
    MidPrice = "MidPrice"


_FAIR_PRICE_TYPE_INT_MIGRATIONS = {1: "MidPrice"}


class SymmetricGridExecutorConfig(ExecutorConfigBase):
    """
    Configuration for the Symmetric Grid Executor.

    Places symmetric buy and sell orders at specified percentage distances from a fair price.
    When any order fills, it is automatically re-placed at the same price level.
    """
    type: Literal["symmetric_grid_executor"] = "symmetric_grid_executor"
    connector_name: str
    trading_pair: str

    spread_percentages: List[Decimal]
    order_amounts_quote: List[Decimal]
    fair_price: Optional[Decimal] = None
    fair_price_type: FairPriceType = FairPriceType.MidPrice

    order_type: OrderType = OrderType.LIMIT
    order_frequency: int = 0
    max_orders_per_batch: Optional[int] = None
    safe_extra_spread: Decimal = Decimal("0.0001")
    min_order_amount_quote: Decimal = Decimal("100")
    price_refresh_tolerance: Decimal = Decimal("0.0005")

    stop_loss: Optional[Decimal] = None
    time_limit: Optional[int] = None
    leverage: int = 20
    level_id: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("fair_price_type", mode="before")
    @classmethod
    def _migrate_fair_price_type(cls, v):
        """Accept legacy PriceType int values (e.g. 1 → 'MidPrice') from old DB records."""
        if isinstance(v, int):
            return _FAIR_PRICE_TYPE_INT_MIGRATIONS.get(v, v)
        return v

    @model_validator(mode="after")
    def validate_levels(self):
        if len(self.spread_percentages) != len(self.order_amounts_quote):
            raise ValueError("spread_percentages and order_amounts_quote must have the same length")
        if any(s <= 0 for s in self.spread_percentages):
            raise ValueError("All spread_percentages must be positive")
        if any(s >= 1 for s in self.spread_percentages):
            raise ValueError("All spread_percentages must be less than 1.0 (100%)")
        if any(a <= 0 for a in self.order_amounts_quote):
            raise ValueError("All order_amounts_quote must be positive")
        return self


class SymmetricGridOrderState(Enum):
    """State of an individual order (buy or sell) within a grid level."""
    NOT_ACTIVE = "NOT_ACTIVE"
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_FILLED = "ORDER_FILLED"


class SymmetricGridLevel(BaseModel):
    """
    Represents a single grid level with independent buy and sell orders.

    Each level is defined by a spread percentage from the fair price and an order amount.
    When a buy fills, a sell is placed at that level (and vice versa), creating a grid
    that alternates sides on each fill.

    pending_side tracks which side to place next:
      - "both" → place both buy and sell (initial state, if sufficient balance on both sides)
      - "buy"  → place a buy order (after a sell fills)
      - "sell" → place a sell order (after a buy fills)
    """
    id: str
    spread_pct: Decimal
    amount_quote: Decimal
    buy_order: Optional[TrackedOrder] = None
    sell_order: Optional[TrackedOrder] = None
    pending_side: str = "both"
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def buy_state(self) -> SymmetricGridOrderState:
        if self.buy_order is None:
            return SymmetricGridOrderState.NOT_ACTIVE
        elif self.buy_order.is_filled:
            return SymmetricGridOrderState.ORDER_FILLED
        else:
            return SymmetricGridOrderState.ORDER_PLACED

    @property
    def sell_state(self) -> SymmetricGridOrderState:
        if self.sell_order is None:
            return SymmetricGridOrderState.NOT_ACTIVE
        elif self.sell_order.is_filled:
            return SymmetricGridOrderState.ORDER_FILLED
        else:
            return SymmetricGridOrderState.ORDER_PLACED

    def get_buy_price(self, fair_price: Decimal) -> Decimal:
        """Calculate buy order price: fair_price * (1 - spread_pct)"""
        return fair_price * (1 - self.spread_pct)

    def get_sell_price(self, fair_price: Decimal) -> Decimal:
        """Calculate sell order price: fair_price * (1 + spread_pct)"""
        return fair_price * (1 + self.spread_pct)

    def reset_buy_order(self):
        self.buy_order = None

    def reset_sell_order(self):
        self.sell_order = None

    def reset_level(self):
        self.buy_order = None
        self.sell_order = None
        self.pending_side = "both"
