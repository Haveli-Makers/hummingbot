from decimal import Decimal
from enum import Enum
from typing import Literal, Optional

from pydantic import Field, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class CrossArbPhase(str, Enum):
    """
    Where one arbitrage attempt has got to.

    Every phase has a deadline, because the way the upstream arbitrage executor fails is by
    waiting forever for two orders to be "fully filled" — a partial fill or a cancel leaves it
    stuck, and its direction is then blocked until the bot restarts.
    """
    PLACING = "placing"          # sizing checked, orders going out
    WAITING = "waiting"          # watching both legs fill
    CLEANUP = "cleanup"          # cancelling what did not fill, and verifying the cancel
    RECONCILE = "reconcile"      # matching the two sides and fixing any difference
    DONE = "done"


class MismatchPolicy(str, Enum):
    """What to do when the two legs did not fill the same amount."""
    FLATTEN = "flatten"   # trade the difference away at once, accepting a small loss
    HOLD = "hold"         # keep it, report it, and let the controller pause the pair


class LegOrder(str, Enum):
    """
    Whether the two orders go out together or one after the other.

    SIMULTANEOUS is the default: each order already carries a price it cannot fill beyond, so
    the exposure is bounded, and waiting for the first leg only gives the gap more time to
    disappear. The sequential modes trade that speed for certainty on one side first; use them
    on a venue whose fills are slow to report.
    """
    SIMULTANEOUS = "simultaneous"
    BUY_FIRST = "buy_first"
    SELL_FIRST = "sell_first"


class CrossArbExecutorConfig(ExecutorConfigBase):
    """
    One arbitrage attempt: buy this much on one venue, sell the same amount on the other, at
    prices that are already decided.

    The controller does the deciding — which venues, which direction, how much, at what price —
    so the executor never re-reads the market to change its mind. It only carries the plan out
    and reports exactly what happened.
    """
    type: Literal["cross_arb_executor"] = "cross_arb_executor"

    buying_market: ConnectorPair
    selling_market: ConnectorPair

    # Base units, already valid on BOTH venues (quantity step, minimum size, minimum value).
    order_amount: Decimal

    # The crossing limit prices. A buy may not pay more than the cap, a sell may not take less
    # than the floor. This is what makes a limit order safe to use where a market order is not:
    # our WazirX book can be 5 s old, and a market order would fill at whatever is there now.
    buy_price_cap: Decimal
    sell_price_floor: Decimal

    leg_order: LegOrder = LegOrder.SIMULTANEOUS

    # Deadlines, in seconds. Together they bound one attempt.
    fill_timeout: float = Field(default=15.0)
    cleanup_timeout: float = Field(default=20.0)
    flatten_timeout: float = Field(default=20.0)

    # A cancel acknowledgement is a claim, not proof: CoinDCX has confirmed a cancel and filled
    # the same order 30 s later. Cancelled orders stay under watch for this long, and anything
    # that fills in that window is counted as ours.
    cancel_settle_delay: float = Field(default=0.25)
    cancelled_order_watch_seconds: float = Field(default=30.0)

    mismatch_policy: MismatchPolicy = MismatchPolicy.FLATTEN

    # A leftover worth less than this is not worth a trade. 0 means "use the venues' own
    # minimums", which is the only number that is always right.
    dust_threshold_quote: Decimal = Decimal("0")

    # Retries apply ONLY to the order that flattens a mismatch, and each one is re-priced
    # against the current book. The upstream executor re-sent a failed leg unchanged and
    # without re-checking the price, which is how a hedge turns into a directional bet.
    max_retries: int = 2

    # How far through the touch a crossing order is priced, in ticks. 0 is the quoted price.
    slippage_ticks: int = 0

    # Cost rates, for reporting only: the venues report fees on fills, but tax withheld on a
    # sale is not in any connector, so the net figure needs it from configuration.
    tds_pct: Decimal = Decimal("0")

    @model_validator(mode="after")
    def validate_markets(self):
        buy_base, buy_quote = split_hb_trading_pair(self.buying_market.trading_pair)
        sell_base, sell_quote = split_hb_trading_pair(self.selling_market.trading_pair)
        if buy_base != sell_base:
            raise ValueError(f"Both legs must trade the same coin: {buy_base} vs {sell_base}")
        if buy_quote != sell_quote:
            # Two different quote currencies would need a conversion rate in every calculation,
            # and getting that wrong is invisible: the upstream executor subtracts USDT from INR
            # and reports the difference as profit.
            raise ValueError(f"Both legs must settle in the same currency: {buy_quote} vs {sell_quote}")
        if self.buying_market == self.selling_market:
            raise ValueError("Buying and selling markets must be different")
        if self.order_amount <= 0:
            raise ValueError("order_amount must be positive")
        if self.buy_price_cap <= 0 or self.sell_price_floor <= 0:
            raise ValueError("buy_price_cap and sell_price_floor must be positive")
        return self

    @property
    def base_asset(self) -> str:
        return split_hb_trading_pair(self.buying_market.trading_pair)[0]

    @property
    def quote_asset(self) -> str:
        return split_hb_trading_pair(self.buying_market.trading_pair)[1]

    @property
    def expected_gross_pct(self) -> Optional[Decimal]:
        """The gap the controller acted on, as a percentage of the buy price."""
        if self.buy_price_cap <= 0:
            return None
        return (self.sell_price_floor - self.buy_price_cap) / self.buy_price_cap * Decimal("100")
