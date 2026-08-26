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

    Both distances are measured from the leg's anchor, not from the price we actually filled
    at. A maker entry fills inside the bracket, so a better fill widens the take profit and
    narrows the stop rather than dragging both along with it.
    """
    take_profit: Decimal
    stop_loss: Decimal
    time_limit: Optional[int] = None
    take_profit_order_type: OrderType = OrderType.LIMIT

    # Order type for the urgent exit — the one used when the stop loss gives up on being
    # passive, on a time limit, and when the strategy is shut down.
    #
    # NOT market. CoinDCX rejects reduce_only on a market order outright:
    #     400 "Reduce Only Order is only applicable for Limit Order"
    # and the connector has to send reduce_only on a close, or the venue treats it as a
    # fresh opposite position and demands margin for it — which fails exactly when the
    # position is large against the wallet, i.e. the moment you most need out. A limit
    # priced through the book crosses and fills like a market order, is a limit order as far
    # as the venue is concerned, and bounds how much slippage we accept.
    close_order_type: OrderType = OrderType.LIMIT

    # How far through the book to price that crossing limit. Wide enough to clear several
    # levels so it actually fills; the price is still a hard floor under the fill.
    close_slippage_ticks: int = 20

    # Once the stop level is breached, exit with a maker limit resting at the touch price on
    # the exit side (a long sells at the best ask) and follow the book down rather than
    # crossing the spread immediately. Set False to go straight to market as before.
    stop_loss_chase: bool = True

    # Where the chasing exit rests, in ticks INSIDE the opposite touch: a sell one tick above
    # the best bid, a buy one tick below the best ask. That is the most aggressive price an
    # order can hold without crossing, so it is the best offer in the book and first to fill,
    # while still earning the maker fee. Raising it backs off towards our own touch — a better
    # price, but further back in the queue, which for an exit is the wrong way round.
    stop_loss_maker_offset_ticks: int = 1

    # Re-place the chasing exit once the touch price has moved this far from where our order
    # is resting. Re-posting on every book change would burn rate limit and risk racing our
    # own cancels.
    stop_loss_requote_pct: Decimal = Decimal("0.0005")

    # How far the price may drift past the ORIGINAL stop level before the chase is abandoned
    # and we take the market price. This is the bound on what patience can cost us.
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
    One leg of the simple grid: a maker entry resting at the touch, then a take profit and a
    stop loss measured from the anchor.

    The controller owns where the grid sits and when the next leg starts; this config
    describes a single leg in isolation.
    """
    type: Literal["simple_grid_executor"] = "simple_grid_executor"
    connector_name: str
    trading_pair: str
    entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.LONG_ONLY
    amount: Decimal

    # The anchor: the price the whole leg is measured from. The take profit and stop loss
    # hang off it, and the entry may only rest within a band around it. None means use the
    # live touch price when the leg starts.
    entry_price: Optional[Decimal] = None

    # The entry rests passively at the touch — a buy at the best bid, a sell at the best ask
    # — so it earns the maker fee instead of paying the taker one. It sits INSIDE the
    # bracket, which is what makes an anchor-measured take profit and stop loss reachable.
    #
    # How far from the touch to rest. Zero means join the touch exactly; a positive value
    # improves on it by that fraction of the price to gain queue priority.
    entry_price_improvement_pct: Decimal = Decimal("0")

    # Follow the touch as the book moves, but only re-place once it has drifted this far
    # from our resting price. Without a threshold every tick of the book is a cancel and a
    # re-post.
    entry_requote_pct: Decimal = Decimal("0.0005")

    # The entry may never rest further than this from the anchor. Beyond it the order is
    # pulled and we wait for the price to come back, because a fill out there would land
    # already past its own take profit or stop loss and close instantly for nothing. None
    # disables the band, which is only sensible in tests.
    entry_band_pct: Optional[Decimal] = Decimal("0.001")

    # Give up if the entry is never filled within this many seconds.
    entry_timeout: Optional[int] = None

    # How long to keep watching an entry the venue said it cancelled.
    #
    # A cancel is a claim, not a fact. CoinDCX has acknowledged a cancel and then filled the
    # same order thirty seconds later — leaving a real position that nothing was tracking,
    # with no take profit, no stop loss, and invisible to the shutdown flatten. While an
    # order is watched, a fill on it is still ours and its exits still get armed.
    cancelled_entry_watch_seconds: float = 60.0

    # How long to let the venue release the collateral behind a cancelled reduce-only order
    # before sending its replacement.
    #
    # CoinDCX acknowledges a cancel BEFORE it frees the margin. An exit sent on the
    # acknowledgement is still seen as a second reduce-only order against the same position
    # and refused with "Insufficient funds" — measured live, ~100ms after the ack was not
    # enough. Waiting a moment on purpose is cheaper than a rejection, because a rejection
    # costs a whole control tick and the price moves inside it.
    cancel_settle_delay: float = 0.25

    # Every refusal doubles that wait, capped here. A stop that cannot place its exit is the
    # worst thing this executor does, so the ceiling stays low enough to keep trying often.
    exit_retry_max_delay: float = 2.0

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

    @field_validator("cancel_settle_delay", "exit_retry_max_delay")
    @classmethod
    def validate_delays(cls, value: float) -> float:
        if value < 0:
            raise ValueError("retry delays cannot be negative")
        return value

    @field_validator("entry_price_improvement_pct", "entry_requote_pct")
    @classmethod
    def validate_non_negative(cls, value: Decimal) -> Decimal:
        if value < Decimal("0"):
            raise ValueError("entry price improvement and requote distances cannot be negative")
        return value

    @field_validator("entry_band_pct")
    @classmethod
    def validate_band(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and value <= Decimal("0"):
            raise ValueError("entry_band_pct must be greater than zero, or None to disable the band")
        return value

    def sides(self):
        """The trade sides this leg should open with."""
        if self.entry_mode == SimpleGridEntryMode.LONG_ONLY:
            return [TradeType.BUY]
        if self.entry_mode == SimpleGridEntryMode.SHORT_ONLY:
            return [TradeType.SELL]
        return [TradeType.BUY, TradeType.SELL]
