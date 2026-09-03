from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class SimpleGridEntryMode(str, Enum):
    """
    Which side(s) the executor offers when it opens.

    BOTH_OCO rests a maker order on each side of the book — a buy at the best bid and a
    sell at the best ask — and lets the market pick: the first side to fill cancels the
    other and locks the direction for the rest of the run. Spot connectors can only use
    LONG_ONLY.
    """
    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    BOTH_OCO = "both_oco"


class SimpleGridBarriers(BaseModel):
    """
    Exit conditions for a single grid leg.

    Deliberately independent of the shared TripleBarrierConfig: this executor must be able
    to grow its own fields without editing a model that position/grid/DCA executors rely on.

    Both distances are measured from the price the leg actually FILLED at, so the bracket is
    symmetric around the fill: a long filled at F takes profit at F * (1 + take_profit) and
    stops at F * (1 - stop_loss). What the entry was aiming at before it filled does not
    enter into it — the risk and the reward are the ones the position really has.
    """
    take_profit: Decimal
    stop_loss: Decimal
    time_limit: Optional[int] = None
    take_profit_order_type: OrderType = OrderType.LIMIT

    # The urgent exit: stop loss giving up, time limit, shutdown. NOT market — CoinDCX rejects
    # reduce_only on a market order ("Reduce Only Order is only applicable for Limit Order"),
    # and a close without reduce_only is treated as a fresh opposite position needing margin,
    # which fails exactly when you most need out. A limit priced through the book crosses like
    # a market order and bounds the slippage.
    close_order_type: OrderType = OrderType.LIMIT

    # How far through the book to price it: enough levels to actually fill, and still a hard
    # floor under the price.
    close_slippage_ticks: int = 20

    # Leave passively and follow the book down instead of crossing the moment the stop breaks.
    stop_loss_chase: bool = True

    # Where that exit rests, in ticks INSIDE the opposite touch. One tick is the most
    # aggressive price that still earns the maker fee — best in the book, first to fill.
    # Raising it is a better price further back in the queue, which for an exit is backwards.
    stop_loss_maker_offset_ticks: int = 1

    # Re-place once the touch has moved this far from our resting price. Re-posting on every
    # book change would burn rate limit and race our own cancels.
    stop_loss_requote_pct: Decimal = Decimal("0.0005")

    # How far past the stop level the chase may follow before crossing. The bound on what
    # patience can cost.
    stop_loss_max_drift_pct: Decimal = Decimal("0.001")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("take_profit", "stop_loss")
    @classmethod
    def validate_positive(cls, value: Decimal) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("take_profit and stop_loss must be greater than zero")
        return value

    @field_validator("stop_loss_maker_offset_ticks")
    @classmethod
    def validate_offset_ticks(cls, value: int) -> int:
        # Zero would put a sell on the bid and a buy on the ask, which crosses.
        if value < 1:
            raise ValueError("stop_loss_maker_offset_ticks must be at least 1; anything less "
                             "rests on the opposite touch and crosses the spread")
        return value

    @field_validator("stop_loss_requote_pct", "stop_loss_max_drift_pct")
    @classmethod
    def validate_non_negative(cls, value: Decimal) -> Decimal:
        if value < Decimal("0"):
            raise ValueError("stop loss chase distances cannot be negative")
        return value

    @field_validator("close_order_type")
    @classmethod
    def validate_close_order_type(cls, value: OrderType) -> OrderType:
        # LIMIT_MAKER would refuse to cross, which is the opposite of what an urgent exit is
        # for. MARKET is allowed for venues that accept it, but not for CoinDCX futures.
        if value not in (OrderType.LIMIT, OrderType.MARKET):
            raise ValueError("close_order_type must be OrderType.LIMIT (a crossing limit) or "
                             "OrderType.MARKET")
        return value

    @field_validator("close_slippage_ticks")
    @classmethod
    def validate_close_slippage(cls, value: int) -> int:
        if value < 1:
            raise ValueError("close_slippage_ticks must be at least 1, or the closing limit "
                             "rests at the touch instead of crossing")
        return value


class SimpleGridExecutorConfig(ExecutorConfigBase):
    """
    One leg of the simple grid: open a position, then close it one step either way.

    A leg is a round trip. Between legs the account is flat, so the order sequence across
    legs alternates buy, sell, buy, sell — one order per state change, all the same size.

    Both the entry and the exit are a fixed step from a reference price, and both work the
    same way: a resting limit at the favourable price, and a watched trigger at the
    unfavourable one.

        FLAT, reference P   rest BUY at P - step   |  trigger: price reaches P + step
        LONG at F           rest SELL at F + step  |  trigger: price reaches F - step

    The resting order is always a full step away from the market, so it cannot cross by
    accident and is reliably a maker fill. Only a triggered order ever pays taker.

    The controller owns where the grid sits and when the next leg starts; this config
    describes a single leg in isolation.
    """
    type: Literal["simple_grid_executor"] = "simple_grid_executor"
    connector_name: str
    trading_pair: str
    entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.LONG_ONLY
    amount: Decimal

    # Where the previous leg ended; the entry's two prices sit one step either side of it.
    # None on the very first leg, which rests at the live touch and has no trigger.
    entry_reference_price: Optional[Decimal] = None

    # Give up if the entry is never filled within this many seconds.
    entry_timeout: Optional[int] = None

    # How long to keep watching an entry the venue said it cancelled. A cancel is a claim, not
    # a fact: CoinDCX has acknowledged one and filled the same order thirty seconds later. A
    # fill on a watched order is still ours, and its exits still get armed.
    cancelled_entry_watch_seconds: float = 60.0

    # How long to let the venue release the collateral behind a cancelled reduce-only order
    # before sending its replacement. CoinDCX acknowledges a cancel BEFORE it frees the margin,
    # so an exit sent on the acknowledgement is refused as a second reduce-only order.
    cancel_settle_delay: float = 0.25

    # Every refusal doubles that wait, capped here. A stop that cannot place its exit is the
    # worst thing this executor does, so the ceiling stays low enough to keep trying often.
    exit_retry_max_delay: float = 2.0

    # Stand-down once a close has been refused for collateral this many times in a row. That
    # refusal is not a transient: the margin is held by a PREVIOUS exit we asked to cancel and
    # were told was gone, so replacing it faster cannot help — the obstacle is the
    # replacement's own predecessor, and it clears only when that order resolves. The first
    # refusal is still treated as the ordinary settle race cancel_settle_delay exists for.
    collateral_refusal_wait: float = 3.0
    collateral_refusals_before_waiting: int = 2

    # On a partial fill the barriers arm against whatever filled; the unfilled remainder
    # is cancelled by default so take profit and stop loss stay pinned to one entry price.
    cancel_remainder_on_partial_fill: bool = True

    barriers: SimpleGridBarriers

    # Which price arms the stop loss trigger. LastTrade matches how venues trigger their own
    # stop orders; BestBid/BestAsk is the more conservative "what would we actually get".
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

    @field_validator("cancel_settle_delay", "exit_retry_max_delay", "collateral_refusal_wait")
    @classmethod
    def validate_delays(cls, value: float) -> float:
        if value < 0:
            raise ValueError("retry delays cannot be negative")
        return value

    @field_validator("collateral_refusals_before_waiting")
    @classmethod
    def validate_refusal_threshold(cls, value: int) -> int:
        # Zero would stand down on the ordinary settle race the backoff already handles.
        if value < 1:
            raise ValueError("collateral_refusals_before_waiting must be at least 1")
        return value

    def sides(self):
        """The trade sides this leg should open with."""
        if self.entry_mode == SimpleGridEntryMode.LONG_ONLY:
            return [TradeType.BUY]
        if self.entry_mode == SimpleGridEntryMode.SHORT_ONLY:
            return [TradeType.SELL]
        return [TradeType.BUY, TradeType.SELL]
