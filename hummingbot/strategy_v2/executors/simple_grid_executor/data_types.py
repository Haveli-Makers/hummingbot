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

    # The anchor the entry levels are measured from. None means use the live touch price.
    entry_price: Optional[Decimal] = None

    # The entry fires once the market reaches the level, so it crosses the spread. MARKET
    # guarantees we get in; a limit type risks the move leaving us behind.
    entry_order_type: OrderType = OrderType.MARKET

    # How far from the anchor each entry level sits: the long level one step ABOVE, the
    # short level one step BELOW. Zero means enter immediately at the anchor.
    entry_offset_pct: Decimal = Decimal("0")

    # Give up if neither level is reached within this many seconds.
    entry_timeout: Optional[int] = None

    # Seconds to wait for a passive entry before taking the price instead. A resting order
    # only fills if the market comes back to it, so in a market moving away it never does —
    # this is what stops a patient entry missing the move entirely. None means wait forever.
    entry_cross_after: Optional[float] = None

    # Spot can only ever buy, so it watches both levels and buys whichever one the market
    # reaches: a step up means join the rise, a step down means buy the dip. Without this a
    # long-only chain would sit idle through every fall, waiting for a rise to buy into.
    enter_on_either_level: bool = False

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

    @field_validator("entry_offset_pct")
    @classmethod
    def validate_entry_offset(cls, value: Decimal) -> Decimal:
        if value < Decimal("0"):
            raise ValueError("entry_offset_pct cannot be negative; the side already decides "
                             "which way the level sits relative to the anchor")
        return value

    @model_validator(mode="after")
    def validate_two_sided_needs_a_step(self):
        # With no step both levels collapse onto the anchor and both would trigger at once.
        if self.entry_mode == SimpleGridEntryMode.BOTH_OCO and self.entry_offset_pct == Decimal("0"):
            raise ValueError("both_oco needs a non-zero entry_offset_pct, otherwise the long and "
                             "short levels are the same price")
        return self

    def sides(self):
        """The trade sides this leg should open with."""
        if self.entry_mode == SimpleGridEntryMode.LONG_ONLY:
            return [TradeType.BUY]
        if self.entry_mode == SimpleGridEntryMode.SHORT_ONLY:
            return [TradeType.SELL]
        return [TradeType.BUY, TradeType.SELL]
