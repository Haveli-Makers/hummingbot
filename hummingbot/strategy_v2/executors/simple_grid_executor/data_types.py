from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class SimpleGridEntryMode(str, Enum):
    """
    Which side(s) the executor offers when it opens.

    BOTH_OCO places a passive entry on each side and lets the market pick: the first side
    to fill cancels the other. Spot connectors can only use LONG_ONLY.
    """
    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    BOTH_OCO = "both_oco"


class SimpleGridBarriers(BaseModel):
    """
    Exit conditions for a single grid leg.

    Deliberately independent of the shared TripleBarrierConfig: this executor must be able
    to grow its own fields without editing a model that position/grid/DCA executors rely on.
    """
    take_profit: Decimal
    stop_loss: Decimal
    time_limit: Optional[int] = None
    take_profit_order_type: OrderType = OrderType.LIMIT
    stop_loss_order_type: OrderType = OrderType.MARKET
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("take_profit", "stop_loss")
    @classmethod
    def validate_positive(cls, value: Decimal) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("take_profit and stop_loss must be greater than zero")
        return value

    @field_validator("stop_loss_order_type")
    @classmethod
    def validate_stop_loss_order_type(cls, value: OrderType) -> OrderType:
        # A resting limit order cannot express "get me out once price crosses X"; the
        # executor watches the trigger itself and exits at market.
        if value != OrderType.MARKET:
            raise ValueError("stop_loss_order_type must be OrderType.MARKET")
        return value


class SimpleGridExecutorConfig(ExecutorConfigBase):
    """
    One leg of the simple grid: a passive entry, then a take profit and a stop loss.

    The controller owns where the grid sits and when the next leg starts; this config
    describes a single leg in isolation.
    """
    type: Literal["simple_grid_executor"] = "simple_grid_executor"
    connector_name: str
    trading_pair: str
    entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.LONG_ONLY
    amount: Decimal

    # Entry. Left as None, the entry price is taken from the live touch price: best bid
    # for a long, best ask for a short.
    entry_price: Optional[Decimal] = None
    entry_order_type: OrderType = OrderType.LIMIT

    # Entry chasing. A resting order that the market walks away from never fills, so the
    # executor re-places it as the touch price drifts, bounded three ways.
    chase_entry: bool = True
    entry_repost_threshold: Decimal = Decimal("0.0005")
    min_repost_interval: float = 1.0
    max_entry_reposts: Optional[int] = None
    max_entry_drift: Optional[Decimal] = Decimal("0.01")
    entry_timeout: Optional[int] = None

    # On a partial fill the barriers arm against whatever filled; the unfilled remainder
    # is cancelled by default so take profit and stop loss stay pinned to one entry price.
    cancel_remainder_on_partial_fill: bool = True

    barriers: SimpleGridBarriers

    # Which price arms the stop loss. LastTrade matches how venues trigger their own stop
    # orders; BestBid/BestAsk is the more conservative "what would we actually get".
    trigger_price_type: PriceType = PriceType.LastTrade

    leverage: int = 1
    level_id: Optional[str] = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: Decimal) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("amount must be greater than zero")
        return value

    @field_validator("entry_order_type")
    @classmethod
    def validate_entry_order_type(cls, value: OrderType) -> OrderType:
        if not value.is_limit_type():
            raise ValueError("entry_order_type must be a limit type; the entry is passive by design")
        return value

    @model_validator(mode="after")
    def validate_chase_bounds(self):
        if self.chase_entry and self.max_entry_drift is not None and self.max_entry_drift <= Decimal("0"):
            raise ValueError("max_entry_drift must be greater than zero when chase_entry is enabled")
        return self

    def sides(self):
        """The trade sides this leg should open with."""
        if self.entry_mode == SimpleGridEntryMode.LONG_ONLY:
            return [TradeType.BUY]
        if self.entry_mode == SimpleGridEntryMode.SHORT_ONLY:
            return [TradeType.SELL]
        return [TradeType.BUY, TradeType.SELL]
