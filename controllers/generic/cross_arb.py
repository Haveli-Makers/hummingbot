from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.cross_arb_executor.data_types import (
    CrossArbExecutorConfig,
    LegOrder,
    MismatchPolicy,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

s_decimal_0 = Decimal("0")

# The trading day is India's: today's realised result, which the loss limit and the cash floor
# both read, starts again at midnight IST.
IST = timezone(timedelta(hours=5, minutes=30))


class TriggerOn(str, Enum):
    """Which number the operator's threshold is compared against."""
    GROSS = "gross"   # the raw gap between the two venues, as specified by the team lead
    NET = "net"       # after fees, GST and tax — the number that actually decides profit


class CrossArbConfig(ControllerConfigBase):
    """
    Cross-exchange arbitrage: buy where the coin is cheap, sell where it is dear, at the best
    price on each side.

    Whether a coin and a threshold make money is the operator's call; everything that decides it
    is a setting here. What this controller owns is the part that must be right regardless:
    when it is safe to trade at all.
    """
    controller_type: str = "generic"
    controller_name: str = "cross_arb"
    candles_config: List[CandlesConfig] = []

    exchange_a: str = "csx"
    exchange_b: str = "wazirx"
    trading_pairs: List[str] = Field(default_factory=lambda: ["USDT-INR"])

    # The trigger, as a fraction: 0.0125 is 1.25%.
    min_profitability: Decimal = Field(default=Decimal("0.0125"), json_schema_extra={"is_updatable": True})
    trigger_on: TriggerOn = TriggerOn.GROSS

    # TDS. Every sale has 1% withheld, and it comes back only after 1.5–2 years, so on CSX↔WazirX a
    # gap below about 1.06% loses cash today even though it earns once the TDS is refunded. Gaps
    # from relaxed_min_profitability up to min_profitability are traded only on cash the day has
    # already made: today's realised result, after this trade's worst case and the worst case of
    # the attempts still live, must stay at or above cash_floor_quote. None, or a value not below
    # min_profitability, turns this off.
    relaxed_min_profitability: Optional[Decimal] = Field(default=Decimal("0.008"),
                                                         json_schema_extra={"is_updatable": True})
    cash_floor_quote: Decimal = Field(default=Decimal("100"), json_schema_extra={"is_updatable": True})

    # Costs, for the net figure. Measured live on 2026-09-23: CSX taker 0.05%, WazirX 0%, and 1%
    # TDS on the sell at both. A venue missing here is treated as free, which flatters the net
    # number — set it. gst_pct is added on top of the fee; for CSX and WazirX set it to 0: the live
    # balances (2026-09-30) matched to the paisa only with CSX's 0.05% as the whole charge.
    taker_fee_pct: Dict[str, Decimal] = Field(default_factory=dict, json_schema_extra={"is_updatable": True})
    gst_pct: Decimal = Field(default=Decimal("18"), json_schema_extra={"is_updatable": True})
    tds_pct: Decimal = Field(default=Decimal("1"), json_schema_extra={"is_updatable": True})

    # Sizing. order_amount_quote caps one attempt. total_amount_quote caps the money tied up in
    # live attempts at once, across every pair. Each attempt gives its money back within seconds,
    # so this limits exposure at any moment, not turnover — max_trades_per_hour and max_loss_quote
    # limit the rest. It overrides the framework's default of 100, which would silently shrink
    # every trade.
    order_amount_quote: Decimal = Field(default=Decimal("10000"), json_schema_extra={"is_updatable": True})
    total_amount_quote: Decimal = Field(default=Decimal("10000"), json_schema_extra={"is_updatable": True})
    # Both venues enforce a minimum order VALUE that they do not publish: ₹60 on CSX, ₹50 on
    # WazirX (found by being rejected live). The trading rules claim about ₹1, so this is the
    # only guard that stops an order the venue will refuse.
    min_order_amount_quote: Decimal = Field(default=Decimal("100"), json_schema_extra={"is_updatable": True})

    # A book that has not changed for this long is not to be traded on: a frozen feed shows a gap
    # that is not there. Both venues' books are REST snapshots, so this is a real risk.
    max_book_age: float = Field(default=10.0, json_schema_extra={"is_updatable": True})

    cooldown: int = Field(default=5, json_schema_extra={"is_updatable": True})
    max_trades_per_hour: int = Field(default=60, json_schema_extra={"is_updatable": True})
    max_loss_quote: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    max_consecutive_failures: int = Field(default=5, json_schema_extra={"is_updatable": True})
    manual_kill_switch: bool = Field(default=False, json_schema_extra={"is_updatable": True})

    # Inventory. A direction needs coin on the selling venue and cash on the buying one; when one
    # side runs out, that DIRECTION pauses while the other keeps trading. Anything below the
    # venue minimum cannot be traded at all, so this is the level at which we ask for a transfer.
    rebalance_below_quote: Decimal = Field(default=Decimal("500"), json_schema_extra={"is_updatable": True})
    rebalance_alerts: bool = Field(default=True, json_schema_extra={"is_updatable": True})

    # Passed to each executor. Defaults live in CrossArbExecutorConfig; these let an operator
    # change them without touching code.
    fill_timeout: float = Field(default=15.0, json_schema_extra={"is_updatable": True})
    cleanup_timeout: float = Field(default=20.0, json_schema_extra={"is_updatable": True})
    flatten_timeout: float = Field(default=20.0, json_schema_extra={"is_updatable": True})
    cancel_settle_delay: float = Field(default=0.25, json_schema_extra={"is_updatable": True})
    mismatch_policy: MismatchPolicy = MismatchPolicy.FLATTEN
    leg_order: LegOrder = LegOrder.SIMULTANEOUS
    slippage_ticks: int = Field(default=0, json_schema_extra={"is_updatable": True})
    dust_threshold_quote: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})

    def update_markets(self, markets: MarketDict) -> MarketDict:
        for exchange in (self.exchange_a, self.exchange_b):
            for pair in self.trading_pairs:
                markets.add_or_update(exchange, pair)
        return markets


class Opportunity:
    """One direction of one pair, as it looks right now, with the reason it cannot be traded."""

    def __init__(self, pair: str, buy_exchange: str, sell_exchange: str):
        self.pair, self.buy_exchange, self.sell_exchange = pair, buy_exchange, sell_exchange
        self.ask = self.bid = self.ask_qty = self.bid_qty = s_decimal_0
        self.gross_pct = self.net_pct = s_decimal_0
        self.amount = s_decimal_0            # base units, valid on both venues
        self.amount_quote = s_decimal_0
        self.worst_cash = s_decimal_0        # what this trade adds to today's cash, at its price limits
        self.relaxed = False                 # the gap is between the two triggers
        self.blocked_by: Optional[str] = None

    @property
    def key(self) -> Tuple[str, str, str]:
        return self.pair, self.buy_exchange, self.sell_exchange

    @property
    def tradable(self) -> bool:
        return self.blocked_by is None and self.amount > s_decimal_0

    def block(self, reason: str) -> "Opportunity":
        if self.blocked_by is None:
            self.blocked_by = reason
        return self

    def as_row(self) -> Dict[str, Any]:
        # A price we do not have is shown as "-", never as 0: a zero reads like a real quote.
        priced = self.ask > s_decimal_0 and self.bid > s_decimal_0
        sized = self.amount > s_decimal_0
        return {
            "pair": self.pair, "buy": self.buy_exchange, "sell": self.sell_exchange,
            "ask": f"{self.ask:.8g}" if self.ask > s_decimal_0 else "-",
            "bid": f"{self.bid:.8g}" if self.bid > s_decimal_0 else "-",
            "gross %": f"{self.gross_pct:.3f}" if priced else "-",
            "net %": f"{self.net_pct:.3f}" if priced else "-",
            "size": f"{self.amount:.8g}" if sized else "-",
            "size quote": f"{self.amount_quote:.2f}" if sized else "-",
            "blocked by": self.blocked_by or "-",
        }


class CrossArbController(ControllerBase):
    """
    Decides WHEN to arbitrage; the executor decides how.

    Every tick it prices both directions of every pair, applies the guards in order, and starts at
    most one executor per pair. Every rejection is named and counted, so the status display answers
    the question an operator actually asks: "why is it not trading?"
    """

    def __init__(self, config: CrossArbConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        self._skips: Dict[str, int] = defaultdict(int)
        self._last_attempt: Dict[str, float] = {}          # pair -> timestamp
        self._recent_trades: deque = deque(maxlen=1000)    # timestamps of executors started
        self._counted_executors: set = set()
        # Every finished attempt's net after fees and the full TDS: the day's cash result. Only this
        # run's attempts are known, so a restart starts the day again at 0, which is the safe side.
        self._realized_pnl: Decimal = s_decimal_0
        self._pnl_day: Optional[date] = None
        self._consecutive_failures: int = 0
        self._book_seen: Dict[Tuple[str, str], Tuple[int, float]] = {}   # (venue, pair) -> (uid, when)
        self._rebalance_needs: Dict[str, str] = {}
        self._stopped_reason: Optional[str] = None
        # Pairs whose last attempt ended keeping a mismatch (mismatch_policy: hold). They stay
        # paused until the bot restarts: someone has to look at the leftover first.
        self._paused_pairs: Dict[str, str] = {}

    # ── costs ────────────────────────────────────────────────────────────────

    def fee_rate(self, exchange: str) -> Decimal:
        """Taker fee as a fraction, including GST on the fee itself."""
        pct = self.config.taker_fee_pct.get(exchange, s_decimal_0)
        return pct / Decimal("100") * (Decimal("1") + self.config.gst_pct / Decimal("100"))

    def net_pct(self, buy_price: Decimal, sell_price: Decimal, buy_exchange: str, sell_exchange: str) -> Decimal:
        """
        What is left of the gap after both fees and the tax withheld from the sale, as a
        percentage of the buy price. TDS applies to the SELL side only, on both venues.
        """
        cost = buy_price * (Decimal("1") + self.fee_rate(buy_exchange))
        proceeds = sell_price * (Decimal("1") - self.fee_rate(sell_exchange) - self.config.tds_pct / Decimal("100"))
        return (proceeds - cost) / buy_price * Decimal("100")

    def worst_cash(self, buy_exchange: str, sell_exchange: str, pair: str, amount: Decimal,
                   buy_price: Decimal, sell_price: Decimal) -> Decimal:
        """
        What a trade adds to today's cash if both sides fill at their price limits: the sale, less
        the buy, both fees and the TDS withheld. The executor's orders cannot fill at worse prices
        than these, so a trade that fills on both sides cannot do worse. (An unwind after a one-sided
        fill can — that is what max_loss_quote is for.)
        """
        buy_limit = buy_price + self._slippage(buy_exchange, pair)
        sell_limit = max(sell_price - self._slippage(sell_exchange, pair), s_decimal_0)
        cost = buy_limit * (Decimal("1") + self.fee_rate(buy_exchange))
        proceeds = sell_limit * (Decimal("1") - self.fee_rate(sell_exchange) - self.config.tds_pct / Decimal("100"))
        return amount * (proceeds - cost)

    def _slippage(self, exchange: str, pair: str) -> Decimal:
        """How far past the quoted price the executor prices its order (slippage_ticks)."""
        if self.config.slippage_ticks <= 0:
            return s_decimal_0
        rules = self.market_data_provider.get_trading_rules(exchange, pair)
        tick = rules.min_price_increment if rules is not None else s_decimal_0
        return tick * self.config.slippage_ticks

    # ── the cash floor ───────────────────────────────────────────────────────

    def relaxed_trigger_pct(self) -> Optional[Decimal]:
        """The lower trigger in %, or None when it is off or not actually lower."""
        relaxed = self.config.relaxed_min_profitability
        if relaxed is None or relaxed >= self.config.min_profitability:
            return None
        return relaxed * Decimal("100")

    def cash_room(self) -> Decimal:
        """
        How much cash the relaxed trades may still spend today: the day's realised result, less what
        the live attempts could still cost, less the floor. A live attempt's gain is not counted
        until it has happened.
        """
        at_risk = sum((min(self._worst_cash_of(executor), s_decimal_0)
                       for executor in self.executors_info if executor.is_active), s_decimal_0)
        return self._realized_pnl + at_risk - self.config.cash_floor_quote

    def _worst_cash_of(self, executor) -> Decimal:
        config = executor.config
        return self.worst_cash(config.buying_market.connector_name, config.selling_market.connector_name,
                               config.buying_market.trading_pair, config.order_amount,
                               config.buy_price_cap, config.sell_price_floor)

    def cash_floor_reason(self) -> str:
        return f"cash floor: needs {self.config.min_profitability * Decimal('100'):.2f}%"

    def trading_day(self) -> date:
        return datetime.fromtimestamp(self.market_data_provider.time(), IST).date()

    def _start_new_day_if_due(self):
        today = self.trading_day()
        if today != self._pnl_day:
            self._pnl_day, self._realized_pnl = today, s_decimal_0

    # ── market data ──────────────────────────────────────────────────────────

    def top_of_book(self, exchange: str, pair: str) -> Optional[Tuple[Decimal, Decimal, Decimal, Decimal]]:
        """(bid, bid_qty, ask, ask_qty), or None when the book is unusable or stale."""
        try:
            book = self.market_data_provider.get_order_book(exchange, pair)
            bid = next(iter(book.bid_entries()), None)
            ask = next(iter(book.ask_entries()), None)
        except Exception:  # noqa: BLE001 - a connector without a book is simply not tradable
            return None
        if bid is None or ask is None or bid.price <= 0 or ask.price <= 0:
            return None
        if self._book_is_stale(exchange, pair, book):
            return None
        return (Decimal(str(bid.price)), Decimal(str(bid.amount)),
                Decimal(str(ask.price)), Decimal(str(ask.amount)))

    def _book_is_stale(self, exchange: str, pair: str, book) -> bool:
        """
        Stale means the venue has not sent us anything for a while, NOT that the prices have not
        moved: a quiet book is normal here — WazirX's alt books sit unchanged for many minutes.
        Every poll bumps the snapshot id even when the prices repeat, so that is what is watched.
        """
        now = self.market_data_provider.time()
        uid = getattr(book, "snapshot_uid", 0) or 0
        seen_uid, seen_at = self._book_seen.get((exchange, pair), (None, now))
        if seen_uid != uid:
            self._book_seen[(exchange, pair)] = (uid, now)
            return False
        self._book_seen[(exchange, pair)] = (seen_uid, seen_at)
        return (now - seen_at) > self.config.max_book_age

    def available(self, exchange: str, asset: str) -> Decimal:
        try:
            connector = self.market_data_provider.get_connector(exchange)
            return Decimal(str(connector.get_available_balance(asset)))
        except Exception:  # noqa: BLE001 - no connector, no balance, no trade
            return s_decimal_0

    # ── sizing ───────────────────────────────────────────────────────────────

    def size_for(self, opportunity: Opportunity) -> Opportunity:
        """
        The biggest amount that is valid on BOTH venues and covered by both balances.

        Quantizing per venue separately is what leaves dust behind, so the amount is rounded on
        one venue, then the other, and only accepted if it survives both.

        When the size comes out too small to trade, the refusal names the limit that made it so.
        If that limit is money — a balance, or the total budget — the fix is a transfer or a
        setting, and saying "rounds to zero" instead sends the operator looking for a rounding
        problem.
        """
        pair = opportunity.pair
        base, quote = pair.split("-")
        budget = self.budget_left()
        limits = [
            ("order size", self.config.order_amount_quote / opportunity.ask),
            ("total budget in use", max(budget, s_decimal_0) / opportunity.ask),
            ("seller's size", opportunity.ask_qty),
            ("buyer's size", opportunity.bid_qty),
            (f"not enough {quote} on {opportunity.buy_exchange}",
             self.available(opportunity.buy_exchange, quote) / opportunity.ask),
            (f"not enough {base} on {opportunity.sell_exchange}",
             self.available(opportunity.sell_exchange, base)),
        ]
        binding, wanted = min(limits, key=lambda limit: limit[1])
        money = binding if binding.startswith(("not enough", "total budget")) else None
        if wanted <= s_decimal_0:
            return opportunity.block(money or "nothing on offer")

        amount = self._quantize_for_both(opportunity, wanted)
        if amount <= s_decimal_0:
            return opportunity.block(money or "rounds to zero")

        opportunity.amount = amount
        opportunity.amount_quote = amount * opportunity.ask
        if opportunity.amount_quote < self.config.min_order_amount_quote:
            return opportunity.block(money or f"below min order value ({self.config.min_order_amount_quote})")
        if not self._clears_venue_rules(opportunity):
            return opportunity.block(money or "below a venue minimum")
        return opportunity

    def budget_left(self) -> Decimal:
        """total_amount_quote, less what the live attempts have committed across all pairs."""
        in_play = sum((executor.config.order_amount * executor.config.buy_price_cap
                       for executor in self.executors_info if executor.is_active), s_decimal_0)
        return self.config.total_amount_quote - in_play

    def _quantize_for_both(self, opportunity: Opportunity, wanted: Decimal) -> Decimal:
        amount = wanted
        for exchange in (opportunity.buy_exchange, opportunity.sell_exchange):
            amount = self.market_data_provider.quantize_order_amount(exchange, opportunity.pair, amount)
        # A second pass on the first venue: two different step sizes can disagree, and an amount
        # the buying venue would round again is an amount the two legs would not match on.
        settled = self.market_data_provider.quantize_order_amount(
            opportunity.buy_exchange, opportunity.pair, amount)
        return settled if settled == amount else s_decimal_0

    def _clears_venue_rules(self, opportunity: Opportunity) -> bool:
        for exchange, price in ((opportunity.buy_exchange, opportunity.ask),
                                (opportunity.sell_exchange, opportunity.bid)):
            rules = self.market_data_provider.get_trading_rules(exchange, opportunity.pair)
            if rules is None:
                continue
            if opportunity.amount < (rules.min_order_size or s_decimal_0):
                return False
            if opportunity.amount * price < (rules.min_notional_size or s_decimal_0):
                return False
        return True

    # ── the per-tick picture ─────────────────────────────────────────────────

    async def update_processed_data(self):
        self._start_new_day_if_due()
        opportunities: List[Opportunity] = []
        for pair in self.config.trading_pairs:
            for buy_exchange, sell_exchange in ((self.config.exchange_a, self.config.exchange_b),
                                                (self.config.exchange_b, self.config.exchange_a)):
                opportunities.append(self.evaluate(pair, buy_exchange, sell_exchange))
        self.update_inventory_needs()
        self.processed_data = {
            "opportunities": opportunities,
            "best": max((o for o in opportunities if o.tradable),
                        key=lambda o: o.gross_pct, default=None),
            "skips": dict(self._skips),
            "realized_pnl": self._realized_pnl,
            "cash_room": self.cash_room(),
            "rebalance_needs": dict(self._rebalance_needs),
            "halted": self._stopped_reason,
            "paused": dict(self._paused_pairs),
        }

    def evaluate(self, pair: str, buy_exchange: str, sell_exchange: str) -> Opportunity:
        opportunity = Opportunity(pair, buy_exchange, sell_exchange)
        buy_book = self.top_of_book(buy_exchange, pair)
        sell_book = self.top_of_book(sell_exchange, pair)
        # Keep whichever side we do have, and name the venue that is silent: "no fresh book" on
        # its own left the operator guessing which feed had stopped.
        if buy_book is not None:
            _, _, opportunity.ask, opportunity.ask_qty = buy_book
        if sell_book is not None:
            opportunity.bid, opportunity.bid_qty, _, _ = sell_book
        if buy_book is None or sell_book is None:
            silent = [name for name, book in ((buy_exchange, buy_book), (sell_exchange, sell_book))
                      if book is None]
            self._skips["no fresh book"] += 1
            return opportunity.block(f"no fresh book: {', '.join(silent)}")

        opportunity.gross_pct = (opportunity.bid - opportunity.ask) / opportunity.ask * Decimal("100")
        opportunity.net_pct = self.net_pct(opportunity.ask, opportunity.bid, buy_exchange, sell_exchange)

        if pair in self._paused_pairs:
            return self._skip(opportunity, "paused: holding a mismatch")

        measured = opportunity.gross_pct if self.config.trigger_on == TriggerOn.GROSS else opportunity.net_pct
        trigger = self.config.min_profitability * Decimal("100")
        relaxed = self.relaxed_trigger_pct()
        if measured < (trigger if relaxed is None else relaxed):
            return self._skip(opportunity, "gap below threshold")

        self.size_for(opportunity)
        if opportunity.blocked_by:
            self._skips[opportunity.blocked_by] += 1
            return opportunity

        opportunity.worst_cash = self.worst_cash(buy_exchange, sell_exchange, pair, opportunity.amount,
                                                 opportunity.ask, opportunity.bid)
        if measured < trigger:
            # Between the two triggers: only on cash the day has already made.
            opportunity.relaxed = True
            if self.cash_room() + opportunity.worst_cash < s_decimal_0:
                return self._skip(opportunity, self.cash_floor_reason())
        return opportunity

    def _skip(self, opportunity: Opportunity, reason: str) -> Opportunity:
        self._skips[reason] += 1
        return opportunity.block(reason)

    # ── inventory ────────────────────────────────────────────────────────────

    def update_inventory_needs(self):
        """
        Note which side of which pair is running low.

        A direction needs coin on the venue it sells at and cash on the venue it buys at, and each
        coin's gap tends to run one way, so one side drains. Below the alert level we ask for a
        transfer; below a venue's own minimum nothing can be traded at all.
        """
        self._rebalance_needs.clear()
        if not self.config.rebalance_alerts:
            return
        for pair in self.config.trading_pairs:
            base, quote = pair.split("-")
            for exchange in (self.config.exchange_a, self.config.exchange_b):
                book = self.top_of_book(exchange, pair)
                price = book[0] if book else None
                base_value = self.available(exchange, base) * price if price else None
                if base_value is not None and base_value < self.config.rebalance_below_quote:
                    self._rebalance_needs[f"{exchange}:{base}"] = (
                        f"{exchange} holds {base_value:.0f} {quote} of {base}; "
                        f"it cannot sell {pair} much longer")
                if self.available(exchange, quote) < self.config.rebalance_below_quote:
                    self._rebalance_needs[f"{exchange}:{quote}"] = (
                        f"{exchange} holds {self.available(exchange, quote):.0f} {quote}; "
                        f"it cannot buy {pair} much longer")

    # ── decisions ────────────────────────────────────────────────────────────

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self.account_for_finished_executors()
        halt = self.halt_reason()
        self._stopped_reason = halt
        if halt:
            self._skips[halt] += 1
            return []

        actions: List[ExecutorAction] = []
        # Sizing saw the budget and the cash room left by the attempts already live; attempts
        # started in this same tick, for other pairs, have to come out of them too.
        budget = self.budget_left()
        room = self.cash_room()
        for pair in self.config.trading_pairs:
            if pair in self._paused_pairs:
                # Checked here as well as when pricing: the attempt that caused the pause is only
                # counted at the top of this method, after this tick's prices were read.
                self._skips["paused: holding a mismatch"] += 1
                continue
            if self.has_live_executor(pair):
                self._skips["executor already running"] += 1
                continue
            if not self.cooldown_passed(pair):
                self._skips["cooldown"] += 1
                continue
            candidates = [o for o in self.processed_data.get("opportunities", [])
                          if o.pair == pair and o.tradable]
            if not candidates:
                continue
            best = max(candidates, key=lambda o: o.gross_pct)
            if best.amount_quote > budget:
                self._skips["total budget in use"] += 1
                continue
            if best.relaxed and room + best.worst_cash < s_decimal_0:
                self._skips[self.cash_floor_reason()] += 1
                continue
            budget -= best.amount_quote
            room += min(best.worst_cash, s_decimal_0)
            actions.append(self.create_action(best))
            self._last_attempt[pair] = self.market_data_provider.time()
            self._recent_trades.append(self.market_data_provider.time())
            self.logger().info(
                f"cross_arb: {best.pair} buy {best.buy_exchange} @ {best.ask} / sell "
                f"{best.sell_exchange} @ {best.bid} | {best.amount} ({best.amount_quote:.2f}) | "
                f"gross {best.gross_pct:.3f}% net {best.net_pct:.3f}% | cash {best.worst_cash:+.2f}"
                + (f" on the relaxed trigger, cash room left {room:.2f}" if best.relaxed else ""))
        return actions

    def create_action(self, opportunity: Opportunity) -> CreateExecutorAction:
        config = CrossArbExecutorConfig(
            timestamp=self.market_data_provider.time(),
            buying_market=ConnectorPair(connector_name=opportunity.buy_exchange, trading_pair=opportunity.pair),
            selling_market=ConnectorPair(connector_name=opportunity.sell_exchange, trading_pair=opportunity.pair),
            order_amount=opportunity.amount,
            buy_price_cap=opportunity.ask,
            sell_price_floor=opportunity.bid,
            leg_order=self.config.leg_order,
            fill_timeout=self.config.fill_timeout,
            cleanup_timeout=self.config.cleanup_timeout,
            flatten_timeout=self.config.flatten_timeout,
            cancel_settle_delay=self.config.cancel_settle_delay,
            mismatch_policy=self.config.mismatch_policy,
            dust_threshold_quote=self.config.dust_threshold_quote,
            slippage_ticks=self.config.slippage_ticks,
            tds_pct=self.config.tds_pct,
            controller_id=self.config.id,
        )
        return CreateExecutorAction(executor_config=config, controller_id=self.config.id)

    def halt_reason(self) -> Optional[str]:
        """Anything that stops the strategy opening new trades at all."""
        if self.config.manual_kill_switch:
            return "kill switch on"
        if self.config.max_loss_quote is not None and self._realized_pnl <= -abs(self.config.max_loss_quote):
            return f"daily loss limit reached ({self._realized_pnl:.2f})"
        if self._consecutive_failures >= self.config.max_consecutive_failures:
            return f"{self._consecutive_failures} failed attempts in a row"
        if self.trades_in_last_hour() >= self.config.max_trades_per_hour:
            return "trades per hour reached"
        return None

    def trades_in_last_hour(self) -> int:
        cutoff = self.market_data_provider.time() - 3600
        return sum(1 for ts in self._recent_trades if ts >= cutoff)

    def cooldown_passed(self, pair: str) -> bool:
        last = self._last_attempt.get(pair)
        return last is None or (self.market_data_provider.time() - last) >= self.config.cooldown

    def has_live_executor(self, pair: str) -> bool:
        return any(e.is_active and e.config.buying_market.trading_pair == pair for e in self.executors_info)

    def account_for_finished_executors(self):
        """
        Count each finished attempt once: realised profit for the day, and failures in a row.

        A run of failures usually means the venue is refusing us — carrying on would just repeat
        the same refusal every tick, which is how a stuck position gets hidden behind noise.
        """
        self._start_new_day_if_due()
        for executor in self.executors_info:
            if executor.is_active or executor.id in self._counted_executors:
                continue
            self._counted_executors.add(executor.id)
            self._realized_pnl += executor.net_pnl_quote or s_decimal_0
            if executor.close_type in (CloseType.FAILED, CloseType.INSUFFICIENT_BALANCE):
                self._consecutive_failures += 1
            elif executor.close_type == CloseType.COMPLETED and self.needed_an_unwind(executor):
                # It ends COMPLETED — nothing is left over — but one side did not fill as planned and
                # the other was traded back, at about 1.2% with the TDS on that sale. A venue that
                # refuses every order looks exactly like this, so it must count towards the brake.
                self._consecutive_failures += 1
                self.logger().warning(
                    f"cross_arb: attempt {executor.id} needed an unwind (net {executor.net_pnl_quote}); "
                    f"counted as a failure ({self._consecutive_failures} in a row)")
            elif executor.close_type == CloseType.COMPLETED:
                self._consecutive_failures = 0
            elif executor.close_type == CloseType.POSITION_HOLD:
                self.pause_for_held_mismatch(executor)

    @staticmethod
    def needed_an_unwind(executor) -> bool:
        return (executor.custom_info or {}).get("flatten_orders", 0) > 0

    def pause_for_held_mismatch(self, executor):
        """
        mismatch_policy: hold keeps a one-sided leftover instead of paying to unwind it. Trading the
        pair on top of that would pile more on, so the pair stops until someone has looked at it.
        """
        pair = executor.config.buying_market.trading_pair
        held = executor.custom_info.get("imbalance_base", "?")
        base = pair.split("-")[0]
        self._paused_pairs[pair] = f"attempt {executor.id} kept {held} {base} unmatched"
        self.logger().warning(
            f"cross_arb: {pair} PAUSED — attempt {executor.id} ended holding {held} {base} unmatched "
            f"(mismatch_policy: hold). Clear it by hand, then restart the bot to trade {pair} again.")

    # ── status ───────────────────────────────────────────────────────────────

    def to_format_status(self) -> List[str]:
        opportunities: List[Opportunity] = self.processed_data.get("opportunities", [])
        relaxed = self.relaxed_trigger_pct()
        trigger = f"trigger {self.config.min_profitability * 100:.2f}% on {self.config.trigger_on.value}"
        if relaxed is not None:
            trigger += f" ({relaxed:.2f}% while today's cash stays >= {self.config.cash_floor_quote})"
        room = (f"room for relaxed trades: {max(self.cash_room(), s_decimal_0):.2f} | "
                if relaxed is not None else "")
        lines = [
            f"\n  Cross-exchange arbitrage | {self.config.exchange_a} <-> {self.config.exchange_b} | "
            f"{trigger} | size <= {self.config.order_amount_quote} | in play <= {self.config.total_amount_quote}",
            f"  realised today (IST): {self._realized_pnl:.2f} | {room}failures in a row: "
            f"{self._consecutive_failures} | trades this hour: {self.trades_in_last_hour()}",
        ]
        if self._stopped_reason:
            lines.append(f"  HALTED: {self._stopped_reason}")
        for pair, reason in self._paused_pairs.items():
            lines.append(f"  PAUSED {pair}: {reason} — clear it by hand, then restart the bot")
        if opportunities:
            header = f"  {'pair':<10} {'buy':<9} {'sell':<9} {'ask':>12} {'bid':>12} {'gross %':>9} {'net %':>9} {'size':>12} {'blocked by':<28}"
            lines.append(header)
            for row in (o.as_row() for o in opportunities):
                lines.append(
                    f"  {row['pair']:<10} {row['buy']:<9} {row['sell']:<9} {row['ask']:>12} {row['bid']:>12} "
                    f"{row['gross %']:>9} {row['net %']:>9} {row['size quote']:>12} {row['blocked by']:<28}")
        if self._skips:
            top = sorted(self._skips.items(), key=lambda kv: -kv[1])[:6]
            lines.append("  not trading because: " + ", ".join(f"{reason} x{count}" for reason, count in top))
        for need in self._rebalance_needs.values():
            lines.append(f"  REBALANCE: {need}")
        active = [e for e in self.executors_info if e.is_active]
        if active:
            lines.append(f"  live attempts: {len(active)}")
        return lines
