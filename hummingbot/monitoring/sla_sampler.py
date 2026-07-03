from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

# Reason codes for an out-of-spec sample (stable identifiers; Phase 4 keys its
# breach state machines on these).
ORDER_BOOK_STALE = "orderbook_stale"    # mid price unavailable or invalid
ONE_SIDE_MISSING = "one_side_missing"   # a side has no open orders at all
SPREAD_TOO_WIDE = "spread_too_wide"     # a side has orders, but none inside the band
DEPTH_BELOW_MIN = "depth_below_min"     # a side has orders in the band, but too small


@dataclass
class OpenOrder:
    """The minimal view of an open order that the SLA math needs."""
    is_buy: bool
    price: Decimal
    amount_remaining: Decimal  # base units still resting on the book


@dataclass
class SampleResult:
    """Outcome of one per-second SLA check."""
    in_spec: bool
    bid_depth: Decimal   # quote value of bid orders within the band
    ask_depth: Decimal   # quote value of ask orders within the band
    reasons: List[str]   # why the sample is out of spec (empty when in_spec)
    mid_price: Optional[Decimal]


def evaluate_sample(mid: Optional[Decimal],
                    orders: List[OpenOrder],
                    band_pct: Decimal,
                    min_depth_quote: Decimal) -> SampleResult:
    """
    Decide whether our standing orders meet the SLA *right now*.

    In spec means: on BOTH sides, the cumulative quote value of open orders whose
    price is within ``band_pct`` percent of ``mid`` is at least ``min_depth_quote``.
    Depth is measured against the live mid, not the configured spread, so a price
    move that leaves a resting order outside the band takes it out of the SLA.
    """
    if mid is None or mid.is_nan() or mid <= 0:
        return SampleResult(
            in_spec=False, bid_depth=Decimal("0"), ask_depth=Decimal("0"),
            reasons=[ORDER_BOOK_STALE], mid_price=mid,
        )

    band = band_pct / Decimal("100")
    reasons: List[str] = []
    depths = {}
    for side_is_buy in (True, False):
        side_orders = [o for o in orders if o.is_buy is side_is_buy]
        if side_is_buy:
            in_band = [o for o in side_orders if (mid - o.price) / mid <= band]
        else:
            in_band = [o for o in side_orders if (o.price - mid) / mid <= band]
        depth = sum((o.price * o.amount_remaining for o in in_band), Decimal("0"))
        depths[side_is_buy] = depth
        if depth < min_depth_quote:
            if len(side_orders) == 0:
                reason = ONE_SIDE_MISSING
            elif len(in_band) == 0:
                reason = SPREAD_TOO_WIDE
            else:
                reason = DEPTH_BELOW_MIN
            if reason not in reasons:
                reasons.append(reason)

    return SampleResult(
        in_spec=len(reasons) == 0,
        bid_depth=depths[True],
        ask_depth=depths[False],
        reasons=reasons,
        mid_price=mid,
    )
