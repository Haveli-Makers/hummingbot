import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketOrderFailureEvent, SellOrderCreatedEvent
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.cross_arb_executor.data_types import (
    CrossArbExecutorConfig,
    CrossArbPhase,
    LegOrder,
    MismatchPolicy,
)
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

s_decimal_0 = Decimal("0")


@dataclass
class ArbOrder:
    """One order this executor sent, with everything needed to judge and cancel it."""
    tracked: TrackedOrder
    side: TradeType
    connector_name: str
    trading_pair: str
    role: str                 # "leg" (part of the arbitrage) or "flatten" (fixing a mismatch)
    price: Decimal
    amount: Decimal
    cancel_sent_at: Optional[float] = None
    failed: bool = False
    late_fill_logged: bool = False
    assumed_full_fill: bool = False

    @property
    def order_id(self) -> Optional[str]:
        return self.tracked.order_id


@dataclass
class ArbResult:
    """What actually happened, in the quote currency. Everything here is measured, not assumed."""
    bought_base: Decimal = s_decimal_0
    bought_quote: Decimal = s_decimal_0
    sold_base: Decimal = s_decimal_0
    sold_quote: Decimal = s_decimal_0
    fees_quote: Decimal = s_decimal_0
    tds_quote: Decimal = s_decimal_0
    flatten_base: Decimal = s_decimal_0
    notes: List[str] = field(default_factory=list)

    @property
    def imbalance_base(self) -> Decimal:
        """Positive: we hold coin we did not sell. Negative: we sold coin we did not buy."""
        return self.bought_base - self.sold_base

    @property
    def net_quote(self) -> Decimal:
        return self.sold_quote - self.bought_quote - self.fees_quote - self.tds_quote


class CrossArbExecutor(ExecutorBase):
    """
    One cross-exchange arbitrage attempt, start to finish.

    Buys on one venue and sells on the other at prices the controller already decided, then does
    the part that actually matters: works out what really filled, cancels what did not, and makes
    sure we do not end up holding one side of a trade we meant to have both sides of.

    Three rules it exists to enforce, each one a way the upstream arbitrage executor loses money:

    1. Orders are crossing LIMITs with a price cap, never MARKET orders. Half our connectors
       cannot send a MARKET order at all, and a stale book makes an uncapped order dangerous.
    2. Every phase has a deadline. Nothing waits forever for a fill that is not coming.
    3. A cancel acknowledgement is not proof. Cancelled orders stay under watch, because a venue
       has confirmed a cancel and then filled the order half a minute later.
    """

    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    # How long the shutdown path may take. The framework tears the connectors down a bounded
    # number of seconds after early_stop(), so this stays well inside that window.
    SHUTDOWN_SECONDS = 15.0

    def __init__(self, strategy: ScriptStrategyBase, config: CrossArbExecutorConfig,
                 update_interval: float = 1.0):
        super().__init__(
            strategy=strategy,
            connectors=[config.buying_market.connector_name, config.selling_market.connector_name],
            config=config,
            update_interval=update_interval,
        )
        self.config: CrossArbExecutorConfig = config
        self._phase: CrossArbPhase = CrossArbPhase.PLACING
        self._orders: List[ArbOrder] = []
        self._by_order_id: Dict[str, ArbOrder] = {}

        self._placed_at: Optional[float] = None
        self._cleanup_started_at: Optional[float] = None
        self._reconcile_started_at: Optional[float] = None
        self._shutdown_deadline: Optional[float] = None
        self._flatten_attempts: int = 0
        self._end_reason: str = ""

    # ── small helpers ─────────────────────────────────────────────────────────

    @property
    def now(self) -> float:
        return self._strategy.current_timestamp

    @property
    def phase(self) -> CrossArbPhase:
        return self._phase

    @property
    def buy_leg(self) -> Optional[ArbOrder]:
        return next((o for o in self._orders if o.role == "leg" and o.side == TradeType.BUY), None)

    @property
    def sell_leg(self) -> Optional[ArbOrder]:
        return next((o for o in self._orders if o.role == "leg" and o.side == TradeType.SELL), None)

    @property
    def flatten_orders(self) -> List[ArbOrder]:
        return [o for o in self._orders if o.role == "flatten"]

    def market_of(self, side: TradeType):
        return self.config.buying_market if side == TradeType.BUY else self.config.selling_market

    def trading_rules(self, connector_name: str, trading_pair: str):
        return self.connectors[connector_name].trading_rules.get(trading_pair)

    def _filled_base(self, order: ArbOrder) -> Decimal:
        """
        How much of an order filled.

        ``executed_amount_base`` only moves when trade updates arrive, and some venues report an
        order FILLED while its fills are still in flight. Taking that zero at face value is how a
        real position becomes invisible, so a terminal FILLED state falls back to the order amount.
        """
        tracked = order.tracked
        live = tracked.executed_amount_base or s_decimal_0
        if live > s_decimal_0:
            return live
        in_flight = tracked.order
        if in_flight is not None and in_flight.current_state == OrderState.FILLED and in_flight.amount > 0:
            if not order.assumed_full_fill:
                order.assumed_full_fill = True
                self.logger().warning(
                    f"{self._tag} order {order.order_id} is FILLED but no fills arrived; assuming "
                    f"the full {in_flight.amount}. Amounts for this leg are approximate.")
            return in_flight.amount
        return s_decimal_0

    def _avg_price(self, order: ArbOrder) -> Decimal:
        tracked = order.tracked
        if tracked.executed_amount_base and tracked.executed_amount_base > s_decimal_0:
            price = tracked.average_executed_price
            if price and not price.is_nan() and price > s_decimal_0:
                return price
        return order.price

    def _is_done(self, order: ArbOrder) -> bool:
        if order.failed:
            return True
        in_flight = order.tracked.order
        return bool(in_flight and in_flight.is_done)

    @property
    def _tag(self) -> str:
        return f"CrossArb {self.config.id} {self.config.buying_market.trading_pair}:"

    # ── measurements ──────────────────────────────────────────────────────────

    @property
    def result(self) -> ArbResult:
        """Everything filled so far, from both legs and any flattening order."""
        result = ArbResult()
        for order in self._orders:
            filled = self._filled_base(order)
            if filled <= s_decimal_0:
                continue
            value = filled * self._avg_price(order)
            if order.side == TradeType.BUY:
                result.bought_base += filled
                result.bought_quote += value
            else:
                result.sold_base += filled
                result.sold_quote += value
            if order.role == "flatten":
                result.flatten_base += filled
            fees = order.tracked.cum_fees_quote or s_decimal_0
            if not fees.is_nan():
                result.fees_quote += fees
        result.tds_quote = result.sold_quote * self.config.tds_pct / Decimal("100")
        return result

    @property
    def reference_price(self) -> Decimal:
        return self.config.buy_price_cap

    @property
    def dust_threshold_quote(self) -> Decimal:
        """
        Below this, a leftover is not worth a trade — and often cannot be traded at all.

        Default: the larger of the two venues' minimum order values, because anything smaller
        would simply be rejected.
        """
        if self.config.dust_threshold_quote > s_decimal_0:
            return self.config.dust_threshold_quote
        minimums = [s_decimal_0]
        for market in (self.config.buying_market, self.config.selling_market):
            rules = self.trading_rules(market.connector_name, market.trading_pair)
            if rules is None:
                continue
            minimums.append(rules.min_notional_size or s_decimal_0)
            minimums.append((rules.min_order_size or s_decimal_0) * self.reference_price)
        return max(minimums)

    def _imbalance_is_dust(self, imbalance: Decimal) -> bool:
        return abs(imbalance) * self.reference_price <= self.dust_threshold_quote

    def get_net_pnl_quote(self) -> Decimal:
        return self.result.net_quote

    def get_net_pnl_pct(self) -> Decimal:
        """Profit as a share of what we SPENT. Dividing by the coin amount is the upstream bug."""
        result = self.result
        if result.bought_quote <= s_decimal_0:
            return s_decimal_0
        return result.net_quote / result.bought_quote

    def get_cum_fees_quote(self) -> Decimal:
        return self.result.fees_quote

    @property
    def filled_amount_quote(self) -> Decimal:
        result = self.result
        return result.bought_quote + result.sold_quote

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def validate_sufficient_balance(self):
        """Both sides must be funded before anything is sent; a rejected leg is a one-sided trade."""
        buy_market, sell_market = self.config.buying_market, self.config.selling_market
        quote_needed = self.config.order_amount * self.config.buy_price_cap
        quote_available = self.connectors[buy_market.connector_name].get_available_balance(self.config.quote_asset)
        base_available = self.connectors[sell_market.connector_name].get_available_balance(self.config.base_asset)
        if quote_available < quote_needed:
            self._fail_before_trading(
                f"{buy_market.connector_name} has {quote_available} {self.config.quote_asset}, "
                f"needs {quote_needed}")
        elif base_available < self.config.order_amount:
            self._fail_before_trading(
                f"{sell_market.connector_name} has {base_available} {self.config.base_asset}, "
                f"needs {self.config.order_amount}")

    def _fail_before_trading(self, reason: str) -> None:
        self.logger().info(f"{self._tag} not enough balance: {reason}")
        self._end_reason = f"insufficient balance: {reason}"
        self.close_type = CloseType.INSUFFICIENT_BALANCE
        self._phase = CrossArbPhase.DONE
        self.stop()

    async def control_task(self):
        # Read the venue's own view of every order first. The creation event normally attaches it,
        # but an event that never arrives must not make a real fill invisible: every amount here
        # is derived from these order objects.
        self._attach_orders()
        # Then, always: a fill on an order the venue said it had cancelled is still our trade.
        self._watch_cancelled_orders()

        if self._status == RunnableStatus.SHUTTING_DOWN:
            self._control_shutdown()
            return
        if self._status != RunnableStatus.RUNNING:
            return

        if self._phase == CrossArbPhase.PLACING:
            self._control_placing()
        elif self._phase == CrossArbPhase.WAITING:
            self._control_waiting()
        elif self._phase == CrossArbPhase.CLEANUP:
            self._control_cleanup()
        elif self._phase == CrossArbPhase.RECONCILE:
            self._control_reconcile()

    # ── phase 1: placing ──────────────────────────────────────────────────────

    def _control_placing(self):
        if self.config.leg_order in (LegOrder.SIMULTANEOUS, LegOrder.BUY_FIRST):
            self._place_leg(TradeType.BUY, self.config.order_amount)
        if self.config.leg_order in (LegOrder.SIMULTANEOUS, LegOrder.SELL_FIRST):
            self._place_leg(TradeType.SELL, self.config.order_amount)
        self._placed_at = self.now
        self._phase = CrossArbPhase.WAITING

    def _place_leg(self, side: TradeType, amount: Decimal) -> Optional[ArbOrder]:
        market = self.market_of(side)
        price = self.config.buy_price_cap if side == TradeType.BUY else self.config.sell_price_floor
        price = self._with_slippage(side, price, market.connector_name, market.trading_pair)
        return self._place(side, amount, price, market.connector_name, market.trading_pair, role="leg")

    def _place(self, side: TradeType, amount: Decimal, price: Decimal, connector_name: str,
               trading_pair: str, role: str) -> Optional[ArbOrder]:
        amount = self.connectors[connector_name].quantize_order_amount(trading_pair, amount)
        price = self.connectors[connector_name].quantize_order_price(trading_pair, price)
        if amount <= s_decimal_0:
            self.logger().warning(f"{self._tag} {role} {side.name} skipped: amount rounds to zero on "
                                  f"{connector_name}")
            return None
        order_id = self.place_order(
            connector_name=connector_name,
            trading_pair=trading_pair,
            order_type=OrderType.LIMIT,   # crossing limit: fills like a market order, cannot fill worse
            side=side,
            amount=amount,
            price=price,
        )
        order = ArbOrder(tracked=TrackedOrder(order_id=order_id), side=side, connector_name=connector_name,
                         trading_pair=trading_pair, role=role, price=price, amount=amount)
        self._orders.append(order)
        if order_id:
            self._by_order_id[order_id] = order
        self.logger().info(f"{self._tag} {role} {side.name} {amount} on {connector_name} at {price} "
                           f"(order {order_id})")
        return order

    def _with_slippage(self, side: TradeType, price: Decimal, connector_name: str,
                       trading_pair: str) -> Decimal:
        """Price a few ticks further through the book, to trade certainty for a worse bound."""
        if self.config.slippage_ticks <= 0:
            return price
        rules = self.trading_rules(connector_name, trading_pair)
        tick = rules.min_price_increment if rules else s_decimal_0
        offset = tick * self.config.slippage_ticks
        return price + offset if side == TradeType.BUY else max(price - offset, tick or s_decimal_0)

    # ── phase 2: waiting for fills ────────────────────────────────────────────

    def _control_waiting(self):
        self._place_second_leg_if_due()
        legs = [leg for leg in (self.buy_leg, self.sell_leg) if leg is not None]
        both_placed = self.buy_leg is not None and self.sell_leg is not None
        if both_placed and all(self._is_done(leg) for leg in legs):
            self._start_cleanup("both legs settled")
        elif self._waiting_timed_out():
            self._start_cleanup(f"fill timeout after {self.config.fill_timeout}s")

    def _place_second_leg_if_due(self):
        """
        Sequential mode: the second leg goes out once the first has something to hedge, and is
        sized to what the first leg ACTUALLY filled, never to what we hoped it would.
        """
        if self.config.leg_order == LegOrder.SIMULTANEOUS:
            return
        first_side = TradeType.BUY if self.config.leg_order == LegOrder.BUY_FIRST else TradeType.SELL
        second_side = TradeType.SELL if first_side == TradeType.BUY else TradeType.BUY
        first = self.buy_leg if first_side == TradeType.BUY else self.sell_leg
        second = self.sell_leg if first_side == TradeType.BUY else self.buy_leg
        if first is None or second is not None:
            return
        filled = self._filled_base(first)
        if filled <= s_decimal_0 or self._imbalance_is_dust(filled):
            return
        self._place_leg(second_side, min(filled, self.config.order_amount))

    def _waiting_timed_out(self) -> bool:
        return self._placed_at is not None and self.now - self._placed_at >= self.config.fill_timeout

    # ── phase 3: cleanup ──────────────────────────────────────────────────────

    def _start_cleanup(self, reason: str):
        self.logger().info(f"{self._tag} cleanup: {reason}")
        self._cleanup_started_at = self.now
        self._phase = CrossArbPhase.CLEANUP
        self._cancel_unfinished_orders()

    def _cancel_unfinished_orders(self):
        for order in self._orders:
            if order.cancel_sent_at is not None or self._is_done(order) or not order.order_id:
                continue
            self.logger().info(f"{self._tag} cancelling {order.side.name} {order.order_id} on "
                               f"{order.connector_name}")
            self._strategy.cancel(order.connector_name, order.trading_pair, order.order_id)
            order.cancel_sent_at = self.now

    def _control_cleanup(self):
        self._cancel_unfinished_orders()
        if self._cleanup_timed_out():
            self.logger().warning(
                f"{self._tag} cleanup timed out after {self.config.cleanup_timeout}s with orders still "
                f"open; going on to settle what is known. Late fills are still watched.")
            self._start_reconcile()
            return
        if not all(self._is_done(order) for order in self._orders):
            return
        # Everything the venue says is finished. Give a confirmed cancel a moment to prove itself
        # before acting on the numbers: a fill can still arrive right after the acknowledgement.
        last_cancel = max((o.cancel_sent_at for o in self._orders if o.cancel_sent_at), default=None)
        if last_cancel is not None and self.now - last_cancel < self.config.cancel_settle_delay:
            return
        self._start_reconcile()

    def _cleanup_timed_out(self) -> bool:
        return (self._cleanup_started_at is not None
                and self.now - self._cleanup_started_at >= self.config.cleanup_timeout)

    # ── phase 4: reconcile the two sides ──────────────────────────────────────

    def _start_reconcile(self):
        self._reconcile_started_at = self.now
        self._phase = CrossArbPhase.RECONCILE

    def _control_reconcile(self):
        result = self.result
        imbalance = result.imbalance_base
        still_working = [o for o in self._orders if not self._is_done(o)]

        if self._imbalance_is_dust(imbalance):
            # Being flat is not the end of it while one of our own orders is still live: a
            # flattening order that fills after we walk away leaves the position it was sent to
            # remove. This happens for real — the leftover gets filled by the "cancelled" leg,
            # and the flatten order is then pure exposure.
            if still_working and not self._reconcile_timed_out():
                self._cancel_unfinished_orders()
                return
            if result.bought_base <= s_decimal_0 and result.sold_base <= s_decimal_0:
                self._finish(CloseType.EXPIRED, "nothing filled before the deadline")
            elif still_working:
                self._finish(CloseType.FAILED,
                             "matched, but an order would not cancel before the deadline")
            else:
                self._finish(CloseType.COMPLETED, "both sides matched")
            return

        if self.config.mismatch_policy == MismatchPolicy.HOLD:
            self._finish(CloseType.POSITION_HOLD,
                         f"mismatch of {imbalance} {self.config.base_asset} held on purpose")
            return

        active_flatten = [o for o in self.flatten_orders if not self._is_done(o)]
        if active_flatten:
            return  # wait for it; the deadline below still applies

        if self._reconcile_timed_out() or self._flatten_attempts > self.config.max_retries:
            self._finish(CloseType.FAILED,
                         f"could not flatten a mismatch of {imbalance} {self.config.base_asset}")
            return

        self._place_flatten_order(imbalance)

    def _reconcile_timed_out(self) -> bool:
        return (self._reconcile_started_at is not None
                and self.now - self._reconcile_started_at >= self.config.flatten_timeout)

    def _place_flatten_order(self, imbalance: Decimal):
        """
        Trade the difference away at once.

        Holding it is a directional bet the strategy never intended to take, and it only gets
        more expensive while nobody is watching. We pay the spread on the leftover and move on.
        """
        side = TradeType.SELL if imbalance > s_decimal_0 else TradeType.BUY
        amount = abs(imbalance)
        venue = self._best_venue_for(side)
        if venue is None:
            self._finish(CloseType.FAILED, "no venue price available to flatten the mismatch")
            return
        connector_name, trading_pair, price = venue
        quantized = self.connectors[connector_name].quantize_order_amount(trading_pair, amount)
        rules = self.trading_rules(connector_name, trading_pair)
        too_small = (quantized <= s_decimal_0
                     or (rules is not None and (quantized < (rules.min_order_size or s_decimal_0)
                                                or quantized * price < (rules.min_notional_size or s_decimal_0))))
        if too_small:
            # Below the venue's own minimum there is no order to send. Say so plainly: the
            # controller reports it and the operator clears it by hand.
            self._finish(CloseType.COMPLETED,
                         f"leftover {amount} {self.config.base_asset} is below the venue minimum and "
                         f"cannot be traded away")
            return
        self._flatten_attempts += 1
        self._place(side, quantized, price, connector_name, trading_pair, role="flatten")

    def _best_venue_for(self, side: TradeType):
        """Where to flatten: whichever of our two venues quotes the better price for that side."""
        candidates = []
        for market in (self.config.buying_market, self.config.selling_market):
            price_type = PriceType.BestBid if side == TradeType.SELL else PriceType.BestAsk
            try:
                price = self.get_price(market.connector_name, market.trading_pair, price_type)
            except Exception:  # noqa: BLE001 - a venue without a book is simply not a candidate
                continue
            if price is None or price.is_nan() or price <= s_decimal_0:
                continue
            price = self._with_slippage(side, price, market.connector_name, market.trading_pair)
            candidates.append((market.connector_name, market.trading_pair, price))
        if not candidates:
            return None
        # Selling: the highest bid. Buying: the lowest ask.
        return max(candidates, key=lambda c: c[2]) if side == TradeType.SELL \
            else min(candidates, key=lambda c: c[2])

    # ── late fills on cancelled orders ────────────────────────────────────────

    def _attach_orders(self):
        """Pick up each order's live state from its connector, for anything not attached yet."""
        for order in self._orders:
            if order.tracked.order is not None or not order.order_id:
                continue
            in_flight = self.get_in_flight_order(order.connector_name, order.order_id)
            if in_flight is not None:
                order.tracked.order = in_flight

    def _watch_cancelled_orders(self):
        """
        A cancel is a claim, not proof.

        CoinDCX has confirmed a cancel and filled the same order 30 seconds later. Because every
        amount here is summed from the orders themselves, such a fill is counted automatically —
        this only makes sure it is noticed, and that we are still in a phase that can act on it.
        """
        for order in self._orders:
            if order.cancel_sent_at is None or order.late_fill_logged:
                continue
            if self._filled_base(order) <= s_decimal_0:
                continue
            order.late_fill_logged = True
            self.logger().warning(
                f"{self._tag} order {order.order_id} on {order.connector_name} filled "
                f"{self._filled_base(order)} AFTER we asked to cancel it. Counting it and "
                f"re-checking the balance between the two sides.")
            if self._phase == CrossArbPhase.DONE:
                continue
            self._phase = CrossArbPhase.RECONCILE
            self._reconcile_started_at = self.now

    # ── finishing ─────────────────────────────────────────────────────────────

    def _finish(self, close_type: CloseType, reason: str):
        result = self.result
        self._end_reason = reason
        self.close_type = close_type
        self._phase = CrossArbPhase.DONE
        self.logger().info(
            f"{self._tag} {close_type.name}: {reason}. bought {result.bought_base} for "
            f"{result.bought_quote}, sold {result.sold_base} for {result.sold_quote}, fees "
            f"{result.fees_quote}, tds {result.tds_quote}, net {result.net_quote} "
            f"{self.config.quote_asset}")
        self.stop()

    # ── shutdown ──────────────────────────────────────────────────────────────

    def early_stop(self, keep_position: bool = False):
        """
        Wind down inside this call, not over the next few ticks.

        The framework tears the connectors down a few seconds after asking us to stop, so the
        cancel and the flattening order go out now. Anything left for "the next pass" can easily
        fall outside that window, and a leg left behind outlives the bot that opened it.
        """
        if self._status in (RunnableStatus.TERMINATED,):
            return
        # Read the venue's view before deciding anything: this can be called before a single
        # control tick has run, and an unattached order looks like an order that never filled.
        self._attach_orders()
        self._shutdown_deadline = self.now + self.SHUTDOWN_SECONDS
        self._cancel_unfinished_orders()

        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self._end_reason = "stopped with the position kept on purpose"
            self._status = RunnableStatus.SHUTTING_DOWN
            return

        self.close_type = CloseType.EARLY_STOP
        self._end_reason = "stopped while the attempt was in flight"
        imbalance = self.result.imbalance_base
        if not self._imbalance_is_dust(imbalance) and self.config.mismatch_policy == MismatchPolicy.FLATTEN:
            self._place_flatten_order(imbalance)
        self._status = RunnableStatus.SHUTTING_DOWN

    def _control_shutdown(self):
        if self.close_type == CloseType.POSITION_HOLD:
            self.stop()
            return
        self._cancel_unfinished_orders()
        imbalance = self.result.imbalance_base
        if self._imbalance_is_dust(imbalance) and all(self._is_done(o) for o in self._orders):
            self.stop()
            return
        if self._shutdown_deadline is not None and self.now >= self._shutdown_deadline:
            if not self._imbalance_is_dust(imbalance):
                self.logger().error(
                    f"{self._tag} shutting down with {imbalance} {self.config.base_asset} unmatched. "
                    f"This is a real position on the venue — it needs clearing by hand.")
                self._end_reason = f"stopped with {imbalance} {self.config.base_asset} unmatched"
            self.stop()
            return
        if not self.flatten_orders or all(self._is_done(o) for o in self.flatten_orders):
            if not self._imbalance_is_dust(imbalance) and self._flatten_attempts <= self.config.max_retries:
                self._place_flatten_order(imbalance)

    # ── events ────────────────────────────────────────────────────────────────

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        order = self._by_order_id.get(event.order_id)
        if order is None:
            return
        order.tracked.order = self.get_in_flight_order(order.connector_name, event.order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """
        A refused order is information, not something to repeat.

        The upstream executor re-sent the same order on failure, without re-checking the price or
        the reason, which turns a rejected hedge into a naked position. Here the leg is marked
        failed and the reconcile phase deals with whatever the other side did.
        """
        order = self._by_order_id.get(event.order_id)
        if order is None:
            return
        order.failed = True
        self.logger().warning(f"{self._tag} {order.role} {order.side.name} order {event.order_id} on "
                              f"{order.connector_name} was refused by the venue")
        if order.role == "leg" and self._phase in (CrossArbPhase.PLACING, CrossArbPhase.WAITING):
            self._start_cleanup(f"{order.side.name} leg refused by {order.connector_name}")

    # ── reporting ─────────────────────────────────────────────────────────────

    def get_custom_info(self) -> Dict:
        result = self.result
        buy_leg, sell_leg = self.buy_leg, self.sell_leg
        return {
            "phase": self._phase.value,
            "pair": self.config.buying_market.trading_pair,
            "buy_connector": self.config.buying_market.connector_name,
            "sell_connector": self.config.selling_market.connector_name,
            "planned_amount": self.config.order_amount,
            "buy_price_cap": self.config.buy_price_cap,
            "sell_price_floor": self.config.sell_price_floor,
            "expected_gross_pct": self.config.expected_gross_pct,
            "bought_base": result.bought_base,
            "sold_base": result.sold_base,
            "bought_quote": result.bought_quote,
            "sold_quote": result.sold_quote,
            "buy_avg_price": self._avg_price(buy_leg) if buy_leg else None,
            "sell_avg_price": self._avg_price(sell_leg) if sell_leg else None,
            "fees_quote": result.fees_quote,
            "tds_quote": result.tds_quote,
            "net_quote": result.net_quote,
            "net_pct": self.get_net_pnl_pct() * Decimal("100"),
            "imbalance_base": result.imbalance_base,
            "flatten_orders": len(self.flatten_orders),
            "flatten_base": result.flatten_base,
            "late_fills": sum(1 for o in self._orders if o.late_fill_logged),
            "close_reason": self._end_reason,
        }

    def to_format_status(self) -> List[str]:
        result = self.result
        return [
            f"""
  Cross-exchange arbitrage | {self.config.buying_market.trading_pair} | {self._phase.value} | {self.close_type.name if self.close_type else '-'}
  - BUY {self.config.buying_market.connector_name} @ <= {self.config.buy_price_cap} | SELL {self.config.selling_market.connector_name} @ >= {self.config.sell_price_floor} | amount {self.config.order_amount}
  - filled: bought {result.bought_base} / sold {result.sold_base} | imbalance {result.imbalance_base}
  - fees {result.fees_quote} | tds {result.tds_quote} | net {result.net_quote} {self.config.quote_asset} ({self.get_net_pnl_pct() * Decimal('100'):.3f}%)
  {('  - ' + self._end_reason) if self._end_reason else ''}
"""
        ]
