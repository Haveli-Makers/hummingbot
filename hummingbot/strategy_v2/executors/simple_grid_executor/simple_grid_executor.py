import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class SimpleGridExecutor(ExecutorBase):
    """
    Runs a single leg of the simple grid strategy.

    A leg is one complete round trip: an entry, then whichever exit comes first out of the
    take profit, the stop loss and the time limit. The controller reads ``close_price`` back
    off this executor to place the next leg.

    Both ends work the same way — a resting limit at the price that favours us, and a watched
    trigger at the one that does not:

        FLAT, reference P   rest BUY at P - step   |  trigger: price reaches P + step
        LONG at F           rest SELL at F + step  |  trigger: price reaches F - step

    Three behaviours distinguish it from the other executors in this package:

    * **Maker by construction.** The resting order sits a full step away from the market, so
      it cannot cross by accident and reliably earns the maker fee. Only a trigger — an order
      placed because the price came to meet it — ever pays taker, and even that one rests
      passively and chases before it gives up and crosses.
    * **The bracket hangs off the fill.** Take profit and stop loss are measured from the
      price the entry actually filled at, not from what it was aiming at, so a long filled at
      F is bracketed symmetrically at F * (1 ± step).
    * **First fill wins.** In ``both_oco`` mode a maker order rests on each side of the book
      and the first one to fill cancels the other, so only one position is ever open. The
      controller then locks the run to that side.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: SimpleGridExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)
        self.config: SimpleGridExecutorConfig = config
        self.trading_rules = self.get_trading_rules(config.connector_name, config.trading_pair)

        # Where the previous leg ended. The opening order rests one step in our favour of
        # it, and one step against it is the trigger. None on the very first leg, which has
        # nothing to measure from and rests at the touch instead.
        self._reference: Optional[Decimal] = config.entry_reference_price
        # Set once a trigger order has actually been sent, so the leg stops re-triggering.
        # NOT set merely because the trigger was reached: an order the venue refuses leaves us
        # with no position and no resting order, and the market is still past the trigger, so
        # the leg has to be able to try again. See process_order_failed_event.
        self._entry_triggered = False
        # The side whose trigger order is waiting on a cancelled resting order to release its
        # collateral, and the moment it may be sent. The venue frees margin a beat AFTER it
        # confirms the cancel, so cancelling and placing in one breath is refused for funds —
        # which is what cost the 17:44 trigger on 2026-09-02.
        self._entry_trigger_pending: Optional[TradeType] = None
        self._entry_blocked_until: float = 0.0

        # Entry state, tracked per side so both_oco can rest two maker orders at once.
        self._entry_orders: Dict[TradeType, Optional[TrackedOrder]] = {side: None for side in config.sides()}
        self._entry_quoted_price: Dict[TradeType, Optional[Decimal]] = {side: None for side in config.sides()}
        self._filled_side: Optional[TradeType] = None
        self._entry_remainder_cancelled = False

        # Entries the venue told us it cancelled, kept under watch because that claim is not
        # always true, and the ones that went on to fill anyway and are now really ours.
        self._cancelled_entries: List[Tuple[TradeType, TrackedOrder, float]] = []
        self._adopted_entries: List[Tuple[TradeType, TrackedOrder]] = []

        # Order ids we have already asked to cancel. An order stays open until the venue
        # acknowledges, and re-sending the cancel every tick would burn rate limit and race
        # our own replacements.
        self._cancel_requested: Set[str] = set()

        self._take_profit_order: Optional[TrackedOrder] = None
        self._close_order: Optional[TrackedOrder] = None

        # Stop loss chase: the live passive exit, plus any earlier attempt that was
        # cancelled after partially filling — its fills are still part of the position.
        self._sl_chase_order: Optional[TrackedOrder] = None
        self._spent_exit_orders: List[TrackedOrder] = []
        self._stop_loss_triggered = False
        self._stop_loss_trigger_price: Optional[Decimal] = None
        # Where the market actually was when the stop fired, which is not the same as the
        # level: by the time a tick notices, the price is already through it.
        self._stop_loss_trigger_reference: Optional[Decimal] = None

        self._failed_orders: List[TrackedOrder] = []

        # A close waiting for resting exits to be cancelled before it is sent. See
        # place_close_order_and_cancel_open_orders.
        self._close_pending = False
        self._close_pending_price: Decimal = Decimal("NaN")

        # Backoff for placing a reduce-only exit after cancelling another one. The venue
        # needs a moment to release the collateral; see cancel_settle_delay.
        self._exit_retry_delay: float = config.cancel_settle_delay
        self._exit_blocked_until: float = 0.0
        self._exit_retry_task: Optional[asyncio.Task] = None

        # Closes the venue refused for want of collateral, in a row. That refusal means an
        # exit we were told had been cancelled is still holding the margin, so replacing it
        # faster cannot work — see collateral_refusal_wait.
        self._consecutive_collateral_refusals = 0
        # Whether any exit has been refused yet. Until one has, the venue's position feed is
        # not consulted before placing a close: a position that has just filled takes a moment
        # to appear there, and skipping the exit because of that lag would be far worse than
        # the storm this guards against.
        self._exit_has_been_refused = False

        # Orders already warned about in _order_filled_base, so the warning is logged once
        # each rather than on every tick.
        self._assumed_full_fills: List[str] = []

        # Whether we have already complained about a missing order book, so the warning
        # lands once per outage rather than on every tick.
        self._warned_no_touch = False

        self._current_retries = 0
        self._max_retries = max_retries

    # ------------------------------------------------------------------ properties

    @property
    def is_perpetual(self) -> bool:
        return self.is_perpetual_connector(self.config.connector_name)

    @property
    def is_trading(self) -> bool:
        return self.status == RunnableStatus.RUNNING and self.open_filled_amount > Decimal("0")

    @property
    def side(self) -> Optional[TradeType]:
        """
        The side actually taken.

        Falls back to the only side on offer when the leg is single-sided. None means the
        market has not picked a direction yet.
        """
        if self._filled_side is not None:
            return self._filled_side
        sides = self.config.sides()
        return sides[0] if len(sides) == 1 else None

    @property
    def close_order_side(self) -> Optional[TradeType]:
        if self.side is None:
            return None
        return TradeType.SELL if self.side == TradeType.BUY else TradeType.BUY

    @property
    def reference_price(self) -> Optional[Decimal]:
        return self._reference

    def _order_filled_base(self, order: Optional[TrackedOrder]) -> Decimal:
        """
        How much of an order filled, trusting the venue's terminal state when the fills
        themselves never arrived.

        executed_amount_base is populated only by trade updates, so a venue whose trade feed
        lags can report an order FILLED with zero executed amount. Every size derived from it
        then collapses to zero and no exit is armed while a real position sits on the venue.
        The completion event cannot help — it is built from the same field. A terminal FILLED
        state means the venue considers the order fully filled, so fall back to the requested
        amount and log the assumption.
        """
        if order is None:
            return Decimal("0")
        live = order.executed_amount_base or Decimal("0")
        if live > Decimal("0"):
            return live
        in_flight = order.order
        if in_flight is not None and in_flight.current_state == OrderState.FILLED \
                and in_flight.amount > Decimal("0"):
            if order.order_id not in self._assumed_full_fills:
                self._assumed_full_fills.append(order.order_id)
                self.logger().warning(
                    f"Executor ID: {self.config.id} - order {order.order_id} is FILLED but no "
                    f"fills arrived; assuming the full {in_flight.amount} at {in_flight.price}. "
                    f"Sizes and PnL for this leg are approximate.")
            return in_flight.amount
        return Decimal("0")

    def _order_avg_price(self, order: Optional[TrackedOrder]) -> Optional[Decimal]:
        """Average fill price, falling back to the order's own price when fills are missing."""
        if order is None:
            return None
        if order.executed_amount_base and order.executed_amount_base > Decimal("0"):
            price = order.average_executed_price
            if price and not price.is_nan() and price > Decimal("0"):
                return price
        in_flight = order.order
        if in_flight is not None and in_flight.current_state == OrderState.FILLED \
                and in_flight.price and in_flight.price > Decimal("0"):
            return in_flight.price
        return None

    def _entry_orders_for_side(self, side: TradeType) -> List[TrackedOrder]:
        """Everything that opened this side — the live slot plus any adopted late fill."""
        orders = []
        slot = self._entry_orders.get(side)
        if slot is not None:
            orders.append(slot)
        orders.extend(order for adopted_side, order in self._adopted_entries if adopted_side == side)
        return orders

    def _executed_amount(self, side: TradeType) -> Decimal:
        return sum((self._order_filled_base(order) for order in self._entry_orders_for_side(side)),
                   Decimal("0"))

    def _weighted_avg_price(self, orders: List[TrackedOrder]) -> Optional[Decimal]:
        filled = Decimal("0")
        notional = Decimal("0")
        for order in orders:
            amount = self._order_filled_base(order)
            price = self._order_avg_price(order)
            if amount > Decimal("0") and price is not None:
                filled += amount
                notional += amount * price
        return notional / filled if filled > Decimal("0") else None

    @property
    def open_filled_amount(self) -> Decimal:
        """
        Net entry exposure in base units.

        Normally only one side ever fills. If a cancel loses a race and both sides get
        filled, the venue nets them, so the position we actually hold is the difference.
        """
        if self._filled_side is None:
            return Decimal("0")
        opposing = TradeType.SELL if self._filled_side == TradeType.BUY else TradeType.BUY
        net = self._executed_amount(self._filled_side) - self._executed_amount(opposing)
        if net <= Decimal("0"):
            return Decimal("0")
        return self.connectors[self.config.connector_name].quantize_order_amount(
            trading_pair=self.config.trading_pair, amount=net)

    def _exit_orders(self) -> List[TrackedOrder]:
        """
        Every order that reduces the position.

        The stop loss may take several attempts before one fills — each cancelled attempt can
        carry partial fills, and they are as much a part of the exit as the one that lands.
        """
        orders: List[TrackedOrder] = []
        for order in [self._take_profit_order, self._sl_chase_order, self._close_order]:
            if order is not None and order not in orders:
                orders.append(order)
        for order in self._spent_exit_orders:
            if order not in orders:
                orders.append(order)
        return orders

    @property
    def close_filled_amount(self) -> Decimal:
        return sum([self._order_filled_base(order) for order in self._exit_orders()], Decimal("0"))

    @property
    def amount_to_close(self) -> Decimal:
        return self.open_filled_amount - self.close_filled_amount

    @property
    def entry_price(self) -> Decimal:
        """Average price actually paid, falling back to the reference before anything fills."""
        if self._filled_side is not None:
            price = self._weighted_avg_price(self._entry_orders_for_side(self._filled_side))
            if price is not None:
                return price
        return self._reference_or_entry()

    @property
    def close_price(self) -> Decimal:
        """
        Volume weighted average of everything that closed the position.

        A chased stop loss can fill across several orders at different prices, and the
        controller anchors the next leg on this number, so a single order's price will not do.
        """
        price = self._weighted_avg_price(self._exit_orders())
        return price if price is not None else self._exit_reference_price()

    @property
    def open_filled_amount_quote(self) -> Decimal:
        return self.open_filled_amount * self.entry_price

    @property
    def close_filled_amount_quote(self) -> Decimal:
        return self.close_filled_amount * self.close_price

    @property
    def filled_amount_quote(self) -> Decimal:
        return self.open_filled_amount_quote + self.close_filled_amount_quote

    @property
    def trade_pnl_pct(self) -> Decimal:
        if self.open_filled_amount == Decimal("0") or self.close_type in [CloseType.FAILED]:
            return Decimal("0")
        if self.side == TradeType.BUY:
            return (self.close_price - self.entry_price) / self.entry_price
        return (self.entry_price - self.close_price) / self.entry_price

    @property
    def trade_pnl_quote(self) -> Decimal:
        return self.trade_pnl_pct * self.open_filled_amount * self.entry_price

    def get_net_pnl_quote(self) -> Decimal:
        return self.trade_pnl_quote - self.cum_fees_quote

    def get_net_pnl_pct(self) -> Decimal:
        if self.open_filled_amount_quote == Decimal("0"):
            return Decimal("0")
        return self.net_pnl_quote / self.open_filled_amount_quote

    def get_cum_fees_quote(self) -> Decimal:
        orders = list(self._entry_orders.values()) + self._exit_orders()
        return sum([order.cum_fees_quote for order in orders if order], Decimal("0"))

    @property
    def is_expired(self) -> bool:
        return self.end_time is not None and self.end_time <= self._strategy.current_timestamp

    @property
    def end_time(self) -> Optional[float]:
        if not self.config.barriers.time_limit:
            return None
        return self.config.timestamp + self.config.barriers.time_limit

    # ------------------------------------------------------------------ prices

    def _touch_price(self, side: TradeType) -> Optional[Decimal]:
        """
        Passive price for the given side: best bid to buy, best ask to sell.

        Resting here is what makes the order a maker order — a buy at the bid joins the bid
        queue instead of lifting the ask.
        """
        price_type = PriceType.BestBid if side == TradeType.BUY else PriceType.BestAsk
        return self._usable_price(price_type, fallback=False)

    def _tick_size(self) -> Decimal:
        tick = getattr(self.trading_rules, "min_price_increment", None)
        if tick is None or not isinstance(tick, Decimal) or tick <= Decimal("0"):
            return Decimal("0")
        return tick

    def _quantize(self, price: Decimal) -> Decimal:
        return self.connectors[self.config.connector_name].quantize_order_price(
            trading_pair=self.config.trading_pair, price=price)

    def _no_cross_bound(self, side: TradeType) -> Optional[Decimal]:
        """
        The furthest a resting order of this side can go before it crosses.

        A buy may sit anywhere strictly below the best ask; a sell anywhere strictly above
        the best bid. One tick inside the opposite touch is the limit, and the most
        aggressive a maker order can be.
        """
        tick = self._tick_size()
        if tick <= Decimal("0"):
            return None
        opposite = PriceType.BestAsk if side == TradeType.BUY else PriceType.BestBid
        price = self._usable_price(opposite, fallback=False)
        if price is None:
            return None
        return price - tick if side == TradeType.BUY else price + tick

    def _maker_exit_price(self, side: TradeType) -> Optional[Decimal]:
        """
        Where a chasing exit should rest: as aggressive as maker allows.

        A resting order earns the maker fee anywhere on its own side of the spread, but where
        it sits decides whether it ever fills. Joining our own touch — a sell at the best ask
        — is the best price and the back of the queue, which is exactly the wrong trade for an
        exit: we are trying to leave while the market moves away from us, so we would sit
        unfilled, chase down, and end up taking the market price at the drift cap anyway.

        Resting one tick inside the opposite touch instead makes us the best offer in the
        book, first to fill on any flow that arrives, for the same fee. On a book whose spread
        is a single tick the two are the same price.
        """
        bound = self._no_cross_bound(side)
        if bound is None:
            # No tick size or no book: fall back to our own touch, which cannot cross.
            touch = self._touch_price(side)
            return self._quantize(touch) if touch is not None else None
        tick = self._tick_size()
        extra = tick * (max(1, self.config.barriers.stop_loss_maker_offset_ticks) - 1)
        price = bound - extra if side == TradeType.BUY else bound + extra
        if price <= Decimal("0"):
            return None
        return self._clamp_to_maker(self._quantize(price), side)

    def _clamp_to_maker(self, price: Decimal, side: TradeType) -> Optional[Decimal]:
        """
        Last guard against a 'maker' order that actually crosses.

        Quantization can round a price back onto the opposite touch when the venue reports a
        book that is not on its own tick grid. Crossing costs the taker fee and a worse fill,
        and on a post-only venue the order is rejected outright — so pull it back rather than
        send it.
        """
        bound = self._no_cross_bound(side)
        if bound is None or price is None:
            return price
        if side == TradeType.BUY and price > bound:
            return self._quantize(bound)
        if side == TradeType.SELL and price < bound:
            return self._quantize(bound)
        return price

    def _entry_rest_price(self, side: TradeType) -> Optional[Decimal]:
        """
        Where this side's opening order rests: one step from the reference, in our favour.

        Buying cheaper than the last exit, or selling dearer than it, is the whole point of
        the grid. A price a full step from the market also cannot cross by accident, so
        unlike an order sitting at the touch this one is reliably a maker fill.

        The very first leg has no reference to measure from, so it rests at the touch.
        """
        if self._reference is None:
            touch = self._touch_price(side)
            return self._quantize(touch) if touch is not None else None
        step = self.config.barriers.take_profit
        price = self._reference * (1 - step) if side == TradeType.BUY \
            else self._reference * (1 + step)
        return self._quantize(price)

    def _entry_trigger_price(self, side: TradeType) -> Optional[Decimal]:
        """
        The price at which we stop waiting and open on the wrong side of the reference.

        A buy that pays a step more than the last exit, or a sell that takes a step less.
        Without it, a grid the market walks away from never trades again.
        """
        if self._reference is None:
            return None
        step = self.config.barriers.stop_loss
        price = self._reference * (1 + step) if side == TradeType.BUY \
            else self._reference * (1 - step)
        return self._quantize(price)

    def _exit_reference_price(self) -> Decimal:
        """
        The price the stop loss is measured against.

        BestBid/BestAsk are mapped to whichever side we would actually exit into, so a
        config of BestBid behaves sensibly on a short leg too. LastTrade and MidPrice are
        used as configured.
        """
        price = self._trigger_reference_price()
        # Nothing usable: fall back to the entry price so callers get a number rather than a
        # NaN. control_stop_loss checks the reference itself and skips instead of comparing.
        return price if price is not None else self._reference_or_entry()

    def _reference_or_entry(self) -> Decimal:
        """A usable price when nothing better is available: the last exit, else the mid."""
        if self._reference is not None:
            return self._reference
        price = self._usable_price(PriceType.MidPrice)
        return price if price is not None else Decimal("0")

    def _trigger_reference_price(self) -> Optional[Decimal]:
        price_type = self.config.trigger_price_type
        if price_type in (PriceType.BestBid, PriceType.BestAsk):
            price_type = PriceType.BestBid if self.side == TradeType.BUY else PriceType.BestAsk
        return self._usable_price(price_type)

    def _usable_price(self, price_type: PriceType, fallback: bool = True) -> Optional[Decimal]:
        """
        A price we can actually compare against, or None.

        LastTrade stays at NaN until a trade prints into the order book, and some venues
        never feed trades in at all — CoinDCX perpetuals among them. A NaN Decimal raises on
        comparison rather than returning False, so this falls back to the mid price and
        reports None only when nothing is usable. Never let a NaN reach a comparison: it
        either crashes the executor or, worse, silently decides the stop loss was not hit.
        """
        candidates = (price_type, PriceType.MidPrice) if fallback else (price_type,)
        for candidate in candidates:
            try:
                price = self.get_price(self.config.connector_name, self.config.trading_pair,
                                       price_type=candidate)
            except Exception:
                continue
            if price is None:
                continue
            price = Decimal(price)
            if not price.is_nan() and price > Decimal("0"):
                return price
        return None

    @property
    def take_profit_price(self) -> Optional[Decimal]:
        """One step from the price we filled at, in our favour."""
        if self._filled_side is None or self.side is None:
            return None
        if self.side == TradeType.BUY:
            return self.entry_price * (1 + self.config.barriers.take_profit)
        return self.entry_price * (1 - self.config.barriers.take_profit)

    @property
    def stop_loss_price(self) -> Optional[Decimal]:
        """One step from the price we filled at, against us."""
        if self._filled_side is None or self.side is None:
            return None
        if self.side == TradeType.BUY:
            return self.entry_price * (1 - self.config.barriers.stop_loss)
        return self.entry_price * (1 + self.config.barriers.stop_loss)

    def _crossing_price(self, side: TradeType) -> Optional[Decimal]:
        """
        A limit priced far enough through the book that it fills like a market order.

        Used by both orders that have to happen now: an exit whose chase has given up, and an
        entry whose trigger has fired.

        It is a limit rather than a market order because CoinDCX rejects reduce_only on a
        market order — "Reduce Only Order is only applicable for Limit Order" — and the
        connector must send reduce_only on a close, or the venue demands margin for a fresh
        opposite position and refuses exactly when the position is what holds the collateral.
        A crossing limit satisfies both, and its price bounds the fill.
        """
        # Sell into the bid, buy into the ask, then push past it by the slippage budget.
        touch_type = PriceType.BestBid if side == TradeType.SELL else PriceType.BestAsk
        reference = self._usable_price(touch_type)
        if reference is None:
            reference = self._reference_or_entry()
        tick = self._tick_size()
        slippage = tick * self.config.barriers.close_slippage_ticks
        if slippage <= Decimal("0"):
            # No tick size published: fall back to a proportion of the price so the order
            # still crosses rather than resting at the touch and never filling.
            slippage = reference * Decimal("0.002")
        price = reference - slippage if side == TradeType.SELL else reference + slippage
        if price <= Decimal("0"):
            price = reference
        return self._quantize(price)

    def _maker_order_type(self) -> OrderType:
        """
        Post-only where the venue offers it, a plain limit where it does not.

        CoinDCX futures report allow_post_only == false on every instrument, so LIMIT_MAKER
        is not in supported_order_types and would be rejected before it ever left the
        process. A limit resting at the touch is still maker there in practice — it just has
        no protection against crossing if the book moves underneath it.
        """
        try:
            supported = self.connectors[self.config.connector_name].supported_order_types()
        except Exception:
            return OrderType.LIMIT
        return OrderType.LIMIT_MAKER if OrderType.LIMIT_MAKER in supported else OrderType.LIMIT

    # ------------------------------------------------------------------ control loop

    async def control_task(self):
        # Before anything else, and in every state: a late fill during shutdown is exactly
        # the case that strands a position.
        self._reap_cancelled_entries()
        if self.status == RunnableStatus.RUNNING:
            self._detect_entry_fill()
            if self._filled_side is None:
                self.control_entry_orders()
            else:
                self.control_barriers()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()
        self.evaluate_max_retries()

    async def on_start(self):
        await super().on_start()
        if self.is_expired:
            self.close_type = CloseType.EXPIRED
            self.stop()
            return
        if self.config.entry_mode == SimpleGridEntryMode.BOTH_OCO and not self.is_perpetual:
            self.logger().error(
                f"Executor ID: {self.config.id} - both_oco requires a perpetual connector; "
                f"{self.config.connector_name} cannot open a short. Stopping.")
            self.close_type = CloseType.FAILED
            self.stop()

    def evaluate_max_retries(self):
        if self._current_retries <= self._max_retries:
            return
        # A run of rejected exits is equally consistent with the exit having ALREADY happened,
        # on an order the venue told us it had cancelled — so ask the venue before calling it a
        # failure. FAILED would halt the run, send the operator after a position that is not
        # there, and drop a real stop loss out of the tally, because the controller reads it as
        # a leg that never traded.
        if (self.open_filled_amount > Decimal("0")
                and self.close_type is not None
                and self.close_type != CloseType.FAILED
                and self._venue_holds_a_position() is False):
            self.logger().warning(
                f"Executor ID: {self.config.id} - every exit order was rejected, but the venue "
                f"reports no position: it closed on an order we were told had been cancelled. "
                f"Recording this as {self.close_type} rather than FAILED. Its realised PnL is "
                f"understated — that fill was never reported to us, so the close price is not "
                f"known here; read it off the account, not off this leg.")
        else:
            self.close_type = CloseType.FAILED
        self.stop()

    # ------------------------------------------------------------------ entry

    def _reap_cancelled_entries(self):
        """
        Watch what the venue told us was gone.

        A cancel is a claim. CoinDCX has confirmed one and then filled the same order thirty
        seconds later — and because the executor had already let go of it, the fill matched
        nothing, no exits were armed, and the shutdown flatten saw a position of zero while a
        real one sat on the venue. Anything that fills while it is still under watch is ours,
        and gets its exits like any other entry.
        """
        if not self._cancelled_entries:
            return
        still_watching = []
        for side, order, cancelled_at in self._cancelled_entries:
            if self._order_filled_base(order) > Decimal("0"):
                self._adopted_entries.append((side, order))
                self.logger().warning(
                    f"Executor ID: {self.config.id} - order {order.order_id} FILLED after the "
                    f"venue confirmed it cancelled. Adopting it: this is a real position and it "
                    f"is being given its exits.")
                if self._filled_side is None:
                    self._on_entry_filled(side)
                continue
            if self._strategy.current_timestamp - cancelled_at >= \
                    self.config.cancelled_entry_watch_seconds:
                self._failed_orders.append(order)
                continue
            still_watching.append((side, order, cancelled_at))
        self._cancelled_entries = still_watching

    def _detect_entry_fill(self):
        """
        Latch the winning side.

        The fill event handler normally gets here first; this is the defensive path for a
        fill we only learn about through the in-flight order state.
        """
        if self._filled_side is not None:
            return
        for side, order in self._entry_orders.items():
            if self._order_filled_base(order) > Decimal("0"):
                self._on_entry_filled(side)
                return

    def _on_entry_filled(self, side: TradeType):
        if self._filled_side is not None:
            return
        self._filled_side = side
        self.logger().info(
            f"Executor ID: {self.config.id} - entry filled on {side.name} at "
            f"{self._weighted_avg_price(self._entry_orders_for_side(side))}; "
            f"cancelling the opposite side")
        self._cancel_losing_entries()

    def _cancel_losing_entries(self):
        """Cancel every entry order that is not the winning side."""
        for side, order in self._entry_orders.items():
            if side == self._filled_side:
                continue
            if order and order.order and order.order.is_open:
                self._cancel_order(order)

    def control_entry_orders(self):
        """
        Rest the opening order a step in our favour, and take the other side if it comes.

        Both prices come from where the previous leg ended: a resting limit one step in our
        favour, and a trigger one step against us. The resting order never moves — it is a
        grid level, not a quote that follows the market — so it cannot cross by accident and
        it fills as a maker. Only the trigger ever pays taker.

        The first leg of a run has no previous exit to measure from, so it rests at the touch
        and has no trigger.
        """
        if self.config.entry_timeout is not None and \
                self._strategy.current_timestamp - self.config.timestamp >= self.config.entry_timeout:
            self.logger().info(f"Executor ID: {self.config.id} - entry timed out unfilled")
            self._give_up_on_entry()
            return

        # A trigger whose resting order has been cancelled is waiting for the venue to let go
        # of the collateral behind it. Nothing else may be placed while that is outstanding.
        if self._entry_trigger_pending is not None:
            self._place_pending_entry_trigger()
            return

        for side in self.config.sides():
            if self._entry_trigger_reached(side):
                self._take_the_trigger(side)
                return
            if self._entry_orders[side] is not None:
                continue          # already resting; a grid level does not move
            price = self._entry_rest_price(side)
            if price is None:
                # With no reference the entry is priced off the touch, so no order book
                # means no entry at all. Silence here looks exactly like a strategy
                # patiently waiting for the market, which is the one thing it is not doing.
                if not self._warned_no_touch:
                    self._warned_no_touch = True
                    self.logger().warning(
                        f"Executor ID: {self.config.id} - no best bid/ask for "
                        f"{self.config.trading_pair}; the order book has not arrived, so no "
                        f"{side.name} entry can be priced. Nothing will be placed until it does.")
                continue
            self._warned_no_touch = False
            self.place_entry_order(side, price)

    def _entry_trigger_reached(self, side: TradeType) -> bool:
        """Has the market gone the wrong way by a full step since the last leg ended?"""
        if self._entry_triggered:
            return False
        trigger = self._entry_trigger_price(side)
        if trigger is None:
            return False
        reference = self._trigger_reference_price()
        if reference is None:
            return False
        return reference >= trigger if side == TradeType.BUY else reference <= trigger

    def _take_the_trigger(self, side: TradeType):
        """
        Open on the wrong side of the reference, crossing to get in.

        The price we hoped to open at is now a step behind the market, where a resting order
        would never fill. This is the only entry that pays taker, and the crossing price
        bounds how much worse than the trigger we accept.

        The resting order goes first, and its collateral with it: CoinDCX frees that margin
        a beat AFTER confirming the cancel, so sending the trigger in the same breath gets it
        refused for funds and the leg opens nothing at all.
        """
        resting = self._entry_orders.get(side)
        if resting and resting.order and resting.order.is_open:
            self._cancel_order(resting)
            self._entry_orders[side] = None
            self._entry_quoted_price[side] = None
            self._entry_trigger_pending = side
            # A floor, in case the confirmation never arrives; the cancel event pushes it out
            # again so the real wait is measured from the confirmation.
            self._entry_blocked_until = \
                self._strategy.current_timestamp + self.config.cancel_settle_delay
            self.logger().info(
                f"Executor ID: {self.config.id} - {side.name} entry trigger "
                f"{self._entry_trigger_price(side)} reached; cancelling the resting order "
                f"first and opening once its margin is released")
            return
        self._place_entry_trigger(side)

    def _place_pending_entry_trigger(self):
        """Send the deferred trigger once the cancelled resting order has let go of its margin."""
        if self._strategy.current_timestamp < self._entry_blocked_until:
            return
        if any(order and order.order and order.order.is_open
               for order in self._entry_orders.values()):
            return          # the cancel has not landed yet
        side = self._entry_trigger_pending
        self._entry_trigger_pending = None
        self._place_entry_trigger(side)

    def _place_entry_trigger(self, side: TradeType):
        """The crossing order itself, priced off the book as it stands now."""
        price = self._crossing_price(side)
        if price is None:
            return
        # Only now, with the order actually going out. Setting it when the trigger was merely
        # reached leaves a refused order looking like a taken one, and _entry_trigger_reached
        # never fires again — so the leg drops back to resting a full step below a market that
        # has just moved up, and sits there until it times out.
        self._entry_triggered = True
        self.logger().info(
            f"Executor ID: {self.config.id} - {side.name} entry trigger "
            f"{self._entry_trigger_price(side)} reached; opening at {price}")
        self.place_entry_order(side, price, crossing=True)

    def place_entry_order(self, side: TradeType, price: Decimal, crossing: bool = False):
        # The trigger crosses on purpose, and a post-only order priced to cross is rejected
        # outright by every venue that offers post-only. Only the resting order — a full step
        # from the market — may ask for the maker type.
        order_type = OrderType.LIMIT if crossing else self._maker_order_type()
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=order_type,
            amount=self.config.amount,
            price=price,
            side=side,
            position_action=PositionAction.OPEN,
        )
        self._entry_orders[side] = TrackedOrder(order_id=order_id)
        self._entry_quoted_price[side] = price
        placed = "crossing" if crossing else "resting"
        self.logger().debug(
            f"Executor ID: {self.config.id} - {placed} {side.name} entry {order_id} at {price} "
            f"({order_type.name})")

    def _give_up_on_entry(self):
        """No entry filled, so no position was ever opened and nothing needs unwinding."""
        for order in self._entry_orders.values():
            if order and order.order and order.order.is_open:
                self._cancel_order(order)
        self.close_type = CloseType.EXPIRED
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

    # ------------------------------------------------------------------ barriers

    def control_barriers(self):
        self._cancel_entry_remainder()
        self.control_time_limit()
        if self._status != RunnableStatus.RUNNING:
            return
        if self.open_filled_amount >= self.trading_rules.min_order_size and \
                self.open_filled_amount_quote >= self.trading_rules.min_notional_size:
            self.control_stop_loss()
            if self._status != RunnableStatus.RUNNING:
                return
            if not self._stop_loss_triggered:
                self.control_take_profit()

    def _cancel_entry_remainder(self):
        """
        Drop the unfilled part of the winning entry.

        Leaving it open would keep averaging the entry price while the position is live,
        which would move the reported PnL under our feet. Guarded by a flag because the order
        stays open until the cancel is acknowledged, and re-sending on every tick would burn
        rate limit for nothing.
        """
        if not self.config.cancel_remainder_on_partial_fill or self._filled_side is None:
            return
        if self._entry_remainder_cancelled:
            return
        order = self._entry_orders[self._filled_side]
        if order is None or order.order is None or not order.order.is_open:
            return
        # A fully filled entry can still read as open until the venue's status update lands.
        # Cancelling it would be a request the venue rejects, logged as an error every leg.
        if order.order.amount - self._order_filled_base(order) <= Decimal("0"):
            self._entry_remainder_cancelled = True
            return
        self._cancel_order(order)
        self._entry_remainder_cancelled = True

    def control_stop_loss(self):
        """
        Watch the stop level, then leave at the touch rather than crossing the spread.

        The stop cannot be a resting order — a sell placed below the market fills instantly
        at the market price — so the level is watched here. Once it is breached the exit is
        still passive: a limit at the touch on the exit side, followed down the book, which
        earns the maker fee instead of paying the taker one. Patience is bounded by
        stop_loss_max_drift_pct; past that we take the price we can get.
        """
        if not self._stop_loss_triggered:
            stop_price = self.stop_loss_price
            if stop_price is None:
                return
            reference = self._trigger_reference_price()
            if reference is None:
                # Skip rather than guess. Deciding "not breached" from missing data would
                # leave a live position unprotected without saying so.
                self.logger().warning(
                    f"Executor ID: {self.config.id} - no usable price to check the stop loss "
                    f"against; position is unprotected this tick")
                return
            breached = reference <= stop_price if self.side == TradeType.BUY else reference >= stop_price
            if not breached:
                return
            self._stop_loss_triggered = True
            self._stop_loss_trigger_price = stop_price
            self._stop_loss_trigger_reference = reference
            gap = abs(reference - stop_price) / stop_price if stop_price else Decimal("0")
            self.logger().info(
                f"Executor ID: {self.config.id} - stop loss hit ({reference} vs level "
                f"{stop_price}, already {gap:.4%} through it)")
            # Committed to leaving: the take profit must not be able to fill behind us.
            self._cancel_take_profit()

        if not self.config.barriers.stop_loss_chase:
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)
            return
        self._control_stop_loss_chase()

    def _control_stop_loss_chase(self):
        """
        Follow the touch price out of the position, and give up if it runs too far.

        Every re-post is at a worse price than the last, which is exactly the risk being
        taken to save the taker fee. The drift cap is measured from the original stop level,
        so the total cost of being patient is known in advance.
        """
        reference = self._trigger_reference_price()
        # Measured from where the market WAS when the stop fired, not from the level. A tick
        # only notices once the price is already through, and on a fast move that gap alone
        # can exceed the cap and skip the chase entirely. The gap is slippage already
        # suffered; the cap bounds what patience costs on top of it.
        anchor_for_drift = self._stop_loss_trigger_reference or self._stop_loss_trigger_price
        if reference is not None and anchor_for_drift:
            # Only movement AWAY from the stop counts. A price recovering back through the
            # level is the chase working, not it failing, and marketing out there would
            # take the worst price at the exact moment patience started paying.
            if self.side == TradeType.BUY:
                drift = (anchor_for_drift - reference) / anchor_for_drift
            else:
                drift = (reference - anchor_for_drift) / anchor_for_drift
            if drift >= self.config.barriers.stop_loss_max_drift_pct:
                self.logger().info(
                    f"Executor ID: {self.config.id} - price drifted {drift:.4%} from {anchor_for_drift} "
                    f"since the stop fired; crossing the spread to get out")
                self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)
                return

        exit_side = self.close_order_side
        if exit_side is None or self.amount_to_close < self.trading_rules.min_order_size:
            return
        # Until the venue lets go of the take profit we just cancelled, a chasing exit is a
        # second reduce-only order for the same position and is refused. Waiting costs a
        # moment; a failed order costs a control tick, which is long enough for the book to
        # turn the exit from maker into taker.
        if any(order is not self._sl_chase_order for order in self._resting_orders()):
            return
        if self._exit_placement_blocked():
            return
        desired = self._maker_exit_price(exit_side)
        if desired is None:
            return
        if self._sl_chase_order is None:
            self._place_stop_loss_chase_order(desired)
        elif self._should_requote_exit(self._sl_chase_order, desired):
            self.logger().debug(
                f"Executor ID: {self.config.id} - re-quoting the stop loss exit to {desired}")
            self._cancel_order(self._sl_chase_order)

    def _should_requote_exit(self, order: TrackedOrder, desired: Decimal) -> bool:
        """
        Whether to move the chasing exit, which may only ever move one way.

        The chase follows the price away from us and never retreats: a long's sell order
        steps down, never back up. Retreating would keep the order permanently ahead of the
        market, so a recovery would never fill it and the leg would stay open through the
        whole move it was trying to escape. Ratcheted, a rebound runs straight into our
        resting order and closes the leg — which is what a stop is for.
        """
        if order.order is None or not order.order.is_open:
            return False
        if order.order_id in self._cancel_requested:
            return False
        resting = order.order.price
        if resting is None or resting.is_nan() or resting <= Decimal("0"):
            return False
        if self.side == TradeType.BUY:
            if desired >= resting:
                return False
            moved = resting - desired
        else:
            if desired <= resting:
                return False
            moved = desired - resting
        reference = self._stop_loss_trigger_reference or self._stop_loss_trigger_price or resting
        return moved >= self.config.barriers.stop_loss_requote_pct * reference

    def _place_stop_loss_chase_order(self, price: Decimal):
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self._maker_order_type(),
            amount=self.amount_to_close,
            price=price,
            side=self.close_order_side,
            position_action=PositionAction.CLOSE,
        )
        self._sl_chase_order = TrackedOrder(order_id=order_id)
        self.logger().debug(
            f"Executor ID: {self.config.id} - resting stop loss exit {order_id} at {price}")

    def control_take_profit(self):
        if self.take_profit_price is None:
            return
        if self._take_profit_order is None:
            self.place_take_profit_order()
        elif self._take_profit_order.order and self._take_profit_order.order.is_open:
            # A partial fill that lands after the take profit is placed leaves the resting
            # amount short of the position; re-issue it at the right size.
            if self._take_profit_order.order.amount != self.amount_to_close and \
                    self.amount_to_close >= self.trading_rules.min_order_size and \
                    self._take_profit_order.order_id not in self._cancel_requested:
                self.renew_take_profit_order()

    def place_take_profit_order(self):
        if self.amount_to_close < self.trading_rules.min_order_size:
            return
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            amount=self.amount_to_close,
            price=self.take_profit_price,
            order_type=self.config.barriers.take_profit_order_type,
            position_action=PositionAction.CLOSE,
            side=self.close_order_side,
        )
        self._take_profit_order = TrackedOrder(order_id=order_id)
        self.logger().debug(
            f"Executor ID: {self.config.id} - placed take profit {order_id} at {self.take_profit_price}")

    def renew_take_profit_order(self):
        self._cancel_take_profit()

    def _cancel_take_profit(self):
        if self._take_profit_order and self._take_profit_order.order \
                and self._take_profit_order.order.is_open:
            self._cancel_order(self._take_profit_order)

    def control_time_limit(self):
        if self.is_expired:
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.TIME_LIMIT)

    # ------------------------------------------------------------------ closing

    def place_close_order_and_cancel_open_orders(self, close_type: CloseType, price: Decimal = Decimal("NaN")):
        still_resting = self.cancel_open_orders()
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

        if self.amount_to_close < self.trading_rules.min_order_size:
            return
        if still_resting:
            # Same reason as the chase above: while the take profit is still live the close
            # would be a second reduce-only order for the same position, refused on arrival
            # and costing one of the retries we may need. Send it on the acknowledgement.
            self._close_pending = True
            self._close_pending_price = price
            return
        self._place_close_order(price)

    def _place_close_order(self, price: Decimal = Decimal("NaN")):
        order_type = self.config.barriers.close_order_type
        if order_type == OrderType.MARKET:
            close_price = Decimal("NaN")
        elif price is not None and not price.is_nan():
            close_price = price
        else:
            close_price = self._crossing_price(self.close_order_side)
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=order_type,
            amount=self.amount_to_close,
            price=close_price,
            side=self.close_order_side,
            position_action=PositionAction.CLOSE,
        )
        self._close_order = TrackedOrder(order_id=order_id)
        self._close_pending = False
        self.logger().info(
            f"Executor ID: {self.config.id} - closing {self.amount_to_close} at "
            f"{close_price} ({order_type.name}) — {self.close_type}")

    def _arm_exit_placement(self, after_refusal: bool = False):
        """
        Schedule the passive exit, giving the venue time to let go of what we just cancelled.

        Not on the next control tick: that is a full second, and a second of a falling market
        is what turns a patient exit into one that crosses. A short scheduled wait puts the
        order in as soon as it can actually be accepted.
        """
        if after_refusal:
            self._exit_retry_delay = min(self._exit_retry_delay * 2,
                                         self.config.exit_retry_max_delay)
        delay = self._exit_retry_delay
        self._exit_blocked_until = self._strategy.current_timestamp + delay
        if self._exit_retry_task is not None and not self._exit_retry_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop (tests, teardown). The control tick covers it instead — check
            # for the loop before building the coroutine, or it is left orphaned.
            self._exit_retry_task = None
            return
        self._exit_retry_task = loop.create_task(self._place_exit_after(delay))

    async def _place_exit_after(self, delay: float):
        try:
            await self._sleep(delay)
            self._place_scheduled_exit()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(
                f"Executor ID: {self.config.id} - scheduled exit placement failed")

    def _place_scheduled_exit(self):
        """Whichever exit is owed — the deferred close first, then the chase."""
        self._exit_blocked_until = 0.0
        # This runs off a timer rather than the shutdown pass, so it needs the same check:
        # by the time the wait is over the order that was holding the margin may have filled,
        # which is precisely the outcome the wait was for.
        if self._settle_as_closed_if_venue_is_flat():
            return
        self._place_pending_close_if_clear()
        self._resume_stop_loss_chase_if_clear()

    def _exit_placement_blocked(self) -> bool:
        return self._strategy.current_timestamp < self._exit_blocked_until

    def _reset_exit_backoff(self):
        self._exit_retry_delay = self.config.cancel_settle_delay
        self._exit_blocked_until = 0.0
        self._consecutive_collateral_refusals = 0

    @staticmethod
    def _is_collateral_refusal(message: Optional[str]) -> bool:
        """
        Whether the venue refused this order for want of collateral rather than on its merits.

        On a reduce-only close that is a specific, diagnosable state and not a transient: the
        margin is committed to an exit we already asked the venue to cancel. CoinDCX phrases
        it "Insufficient funds"; the substring is matched rather than the code, because the
        same 400 covers several unrelated refusals.
        """
        if not message:
            return False
        lowered = str(message).lower()
        return "insufficient funds" in lowered or "insufficient balance" in lowered

    def _stand_down_after_collateral_refusal(self) -> bool:
        """
        Wait for the order that is holding the margin instead of replacing it again.

        Returns True once we have decided to wait, so the caller skips the ordinary backoff.
        The wait is long by the standards of this executor on purpose: the thing in the way
        is our own cancelled exit, and it clears when that order fills or the venue really
        does drop it — neither of which we can hurry.
        """
        self._consecutive_collateral_refusals += 1
        if self._consecutive_collateral_refusals < self.config.collateral_refusals_before_waiting:
            return False
        wait = self.config.collateral_refusal_wait
        self._exit_blocked_until = self._strategy.current_timestamp + wait
        if self._consecutive_collateral_refusals == self.config.collateral_refusals_before_waiting:
            # Once, at the point the diagnosis is made. Repeating it every refusal would bury
            # the one line that explains the whole episode.
            self.logger().warning(
                f"Executor ID: {self.config.id} - the venue has refused this close for want of "
                f"collateral {self._consecutive_collateral_refusals} times running. That margin "
                f"is held by an exit we were told had been cancelled, so a replacement cannot "
                f"be funded until that order resolves. Waiting {wait:.2f}s for it rather than "
                f"sending another.")
        return True

    def _resume_stop_loss_chase_if_clear(self):
        """
        Put the chasing exit in as soon as the take profit is out of the way.

        Left to the control loop this waits for the next tick. On a stop that is a second of
        standing still while the market moves, which is exactly how a passive exit ends up
        crossing and paying the taker fee it was placed to avoid.
        """
        if not self._stop_loss_triggered or self._status != RunnableStatus.RUNNING:
            return
        if self._close_pending or self._close_order is not None:
            return
        if self._exit_placement_blocked():
            return
        if not self.config.barriers.stop_loss_chase or self._sl_chase_order is not None:
            return
        if self._resting_orders():
            return
        if self.close_order_side is None or self.amount_to_close < self.trading_rules.min_order_size:
            return
        desired = self._maker_exit_price(self.close_order_side)
        if desired is not None:
            self._place_stop_loss_chase_order(desired)

    def _place_pending_close_if_clear(self):
        """Send the deferred close once nothing of ours is resting at the venue any more."""
        if not self._close_pending or self._close_order is not None:
            return
        if self._exit_placement_blocked() or self._has_resting_orders():
            return
        if self.amount_to_close < self.trading_rules.min_order_size:
            self._close_pending = False
            return
        self._place_close_order(self._close_pending_price)

    def _resting_orders(self) -> List[TrackedOrder]:
        """
        Orders of ours still holding room in the book.

        A fully filled order keeps reading as open until the venue's status update lands, but
        it has nothing left to cancel and no exposure beyond what already filled. Counting it
        would make every close wait on a cancel that is never coming.
        """
        resting = []
        for order in list(self._entry_orders.values()) + [self._take_profit_order, self._sl_chase_order]:
            if not (order and order.order and order.order.is_open):
                continue
            if order.order.amount - self._order_filled_base(order) <= Decimal("0"):
                continue
            resting.append(order)
        return resting

    def _has_resting_orders(self) -> bool:
        return len(self._resting_orders()) > 0

    def cancel_open_orders(self) -> bool:
        """Cancel everything of ours still in the book. True if anything was."""
        resting = self._resting_orders()
        for order in resting:
            self._cancel_order(order)
        return len(resting) > 0

    def _cancel_order(self, order: TrackedOrder):
        self._cancel_requested.add(order.order_id)
        self._strategy.cancel(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_id=order.order_id,
        )

    def all_orders_completed(self) -> bool:
        tracked = list(self._entry_orders.values()) + [self._take_profit_order, self._sl_chase_order,
                                                       self._close_order]
        return all(order is None or order.is_done for order in tracked)

    def open_and_close_volume_match(self) -> bool:
        if self.open_filled_amount == Decimal("0"):
            return True
        # is_filled reads executed_amount_base, which stays zero when a close order's trade
        # updates never arrived. Compare the amounts we trust, or shutdown never completes.
        return self.close_filled_amount >= self.open_filled_amount

    async def control_shutdown_process(self):
        self.close_timestamp = self._strategy.current_timestamp
        # Nothing to close means nothing to send. Checked before the placement below, or we
        # spend a whole pass and a retry on an order the venue has no reason to accept.
        if self._settle_as_closed_if_venue_is_flat():
            return
        # Belt and braces: normally the cancel event places this, but a cancel confirmation
        # that never arrives must not leave the position uncovered.
        self._place_pending_close_if_clear()
        if self.all_orders_completed():
            if self.open_and_close_volume_match():
                self.stop()
            else:
                await self.control_close_order()
                self._current_retries += 1
        else:
            self.cancel_open_orders()
        # Short enough that several passes fit inside the window the framework allows before
        # it tears the connectors down.
        await self._sleep(2.0)

    async def control_close_order(self):
        if self._close_order:
            in_flight_order = self.get_in_flight_order(self.config.connector_name, self._close_order.order_id) \
                if not self._close_order.order else self._close_order.order
            if in_flight_order:
                self._close_order.order = in_flight_order
                connector = self.connectors[self.config.connector_name]
                await connector._update_orders_with_error_handler(
                    orders=[in_flight_order],
                    error_handler=connector._handle_update_error_for_lost_order)
            else:
                self._failed_orders.append(self._close_order)
                self._close_order = None
        else:
            self.place_close_order_and_cancel_open_orders(close_type=self.close_type)

    def _venue_holds_a_position(self) -> Optional[bool]:
        """
        Whether the exchange says we hold anything on this pair.

        None when we cannot tell. A caller must treat that like a yes: being silent about a
        position that might be open is the wrong way to be wrong.
        """
        try:
            connector = self.connectors[self.config.connector_name]
            positions = getattr(connector, "account_positions", None)
            if positions is None:
                return None
            for position in positions.values():
                if position.trading_pair != self.config.trading_pair:
                    continue
                if position.amount and abs(position.amount) > Decimal("0"):
                    return True
            return False
        except Exception:
            return None

    def _venue_confirms_the_position_is_gone(self) -> bool:
        """
        The venue reports flat while our own books still show something to close.

        Deliberately not consulted until an exit has been refused. Before that the position
        feed cannot carry the weight: a position that has just filled takes a moment to appear
        in it, and skipping an exit on that lag would strand the very position the exit exists
        to close. After a refusal, "already gone" is the explanation that also accounts for
        the venue refusing to reduce it.
        """
        if not self._exit_has_been_refused:
            return False
        if self.open_filled_amount <= Decimal("0"):
            return False
        return self._venue_holds_a_position() is False

    def _settle_as_closed_if_venue_is_flat(self) -> bool:
        """
        Stop trying to close a position the venue says we no longer have.

        Without this the executor keeps sending reduce-only orders that cannot be accepted,
        one per pass, until it runs out of retries — twenty seconds of noise to reach a
        conclusion already visible in the position feed. True means the leg has been settled
        and the caller should do nothing further.
        """
        if not self._venue_confirms_the_position_is_gone():
            return False
        if self.close_type is None or self.close_type == CloseType.FAILED:
            return False
        self.logger().warning(
            f"Executor ID: {self.config.id} - the venue reports no position while our books "
            f"still show {self.amount_to_close} to close, and our exits are being refused: it "
            f"closed on an order we were told had been cancelled. Settling this leg as "
            f"{self.close_type} instead of sending exits the venue cannot accept. Its realised "
            f"PnL is understated — that fill was never reported to us, so the close price is "
            f"not known here; read it off the account, not off this leg.")
        self.stop()
        return True

    def early_stop(self, keep_position: bool = False):
        """
        Wind the leg down in this call, not over the next few control ticks.

        When the strategy stops, the framework calls this on every live executor, waits a
        bounded number of seconds, and then tears the connectors down whether or not we are
        finished. Leaving the unwind to control_shutdown_process meant the first pass only
        cancelled the resting orders and the market close was not sent until the pass after
        that — which can easily fall outside the window. The position then outlives the bot
        that opened it, with nothing watching it, because the stop loss only ever existed in
        this process.

        So cancel and flatten here, and leave the shutdown loop nothing to do but confirm the
        fill.
        """
        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.close_timestamp = self._strategy.current_timestamp
            self._status = RunnableStatus.SHUTTING_DOWN
            return

        if self._status != RunnableStatus.RUNNING:
            # The orchestrator calls this on anything not yet TERMINATED, so a leg whose take
            # profit has just filled lands here too. Overwriting close_type would report a win
            # as an early stop, and a second close would sell a position we no longer hold.
            # Only step in if nothing is covering the position.
            if self.close_type == CloseType.POSITION_HOLD:
                return
            if self._close_order is None and self.amount_to_close >= self.trading_rules.min_order_size:
                self.place_close_order_and_cancel_open_orders(
                    close_type=self.close_type or CloseType.EARLY_STOP)
            return

        self.place_close_order_and_cancel_open_orders(close_type=CloseType.EARLY_STOP)

    def stop(self):
        """
        Last word before this executor goes away.

        If anything is still open here it will not be closed by us or by anything else, so it
        has to be said loudly rather than left in the account for someone to find.
        """
        try:
            remaining = self.amount_to_close
            if remaining >= self.trading_rules.min_order_size \
                    and self.close_type != CloseType.POSITION_HOLD:
                if self._venue_holds_a_position() is False:
                    # Our books and the venue disagree, and the venue is the one that decides.
                    # Saying "close it by hand" about a position that does not exist spends
                    # the credibility of the one message that has to be believed.
                    self.logger().info(
                        f"Executor ID: {self.config.id} - our books show {remaining} "
                        f"{self.config.trading_pair} outstanding, but the venue reports no "
                        f"position. Treating it as already closed — most likely a fill we were "
                        f"told had been cancelled.")
                else:
                    self.logger().error(
                        f"Executor ID: {self.config.id} - stopping with {remaining} "
                        f"{self.config.trading_pair} STILL OPEN ({self.close_type}). The stop "
                        f"loss runs in this process, so nothing is watching this position now. "
                        f"Close it by hand and check the account.")
        except Exception:
            # Never let a diagnostic stop the executor from shutting down.
            pass
        if self._exit_retry_task is not None and not self._exit_retry_task.done():
            self._exit_retry_task.cancel()
        super().stop()

    async def validate_sufficient_balance(self):
        """
        Check each side we might take on its own, never the sum of them.

        In both_oco a maker order does rest on each side at the same time, so the venue locks
        collateral for both — but the leg is only viable if either side can be afforded, and
        reporting INSUFFICIENT_BALANCE for the pair would refuse legs that fit perfectly well
        once the first fill cancels the other side. The controller locks the side after the
        first fill, so this is the opening leg only.
        """
        for side in self.config.sides():
            price = self._entry_rest_price(side) or self._reference
            if price is None:
                continue
            if self.is_perpetual:
                candidate = PerpetualOrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self._maker_order_type(),
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                    leverage=Decimal(self.config.leverage),
                )
            else:
                candidate = OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=self._maker_order_type(),
                    order_side=side,
                    amount=self.config.amount,
                    price=price,
                )
            adjusted = self.adjust_order_candidates(self.config.connector_name, [candidate])
            if any(entry.amount == Decimal("0") for entry in adjusted):
                self.close_type = CloseType.INSUFFICIENT_BALANCE
                self.logger().error(
                    f"Not enough budget to open the position: one {side.name} leg of "
                    f"{self.config.amount} at {price} does not fit.")
                self.stop()
                return

    # ------------------------------------------------------------------ events

    def _tracked_orders(self) -> List[Optional[TrackedOrder]]:
        # Cancelled-but-watched orders belong here too, or their late fill never reaches us.
        watched = [order for _, order, _ in self._cancelled_entries]
        adopted = [order for _, order in self._adopted_entries]
        return list(self._entry_orders.values()) + watched + adopted + self._exit_orders()

    def update_tracked_orders_with_order_id(self, order_id: str):
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        for order in self._tracked_orders():
            if order and order.order_id == order_id:
                order.order = in_flight_order
                return

    def _entry_side_for_order_id(self, order_id: str) -> Optional[TradeType]:
        for side, order in self._entry_orders.items():
            if order and order.order_id == order_id:
                return side
        return None

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)
        # The venue accepted it, so whatever was holding the collateral has let go.
        if (self._sl_chase_order and self._sl_chase_order.order_id == event.order_id) or \
                (self._close_order and self._close_order.order_id == event.order_id):
            self._reset_exit_backoff()

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        self.update_tracked_orders_with_order_id(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._on_entry_filled(side)
            return
        # Might be one the venue said it had cancelled. Do not wait for the next tick.
        self._reap_cancelled_entries()

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self.update_tracked_orders_with_order_id(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._on_entry_filled(side)
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self.close_type = CloseType.TAKE_PROFIT
            self._close_order = self._take_profit_order
            self.close_timestamp = self._strategy.current_timestamp
            self._status = RunnableStatus.SHUTTING_DOWN
        elif self._sl_chase_order and self._sl_chase_order.order_id == event.order_id:
            # The passive exit landed: a stop loss taken at the touch instead of the spread.
            self.close_type = CloseType.STOP_LOSS
            self._close_order = self._sl_chase_order
            self.close_timestamp = self._strategy.current_timestamp
            self._status = RunnableStatus.SHUTTING_DOWN

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        self._cancel_requested.discard(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            # Cancelling an entry is routine — a re-quote, or the losing side of an OCO. The
            # tracked order only survives if it has fills, because those are the record of
            # the position we hold; otherwise the slot is freed for the next quote.
            if self._order_filled_base(self._entry_orders[side]) == Decimal("0"):
                # Freed for the next quote, but not forgotten — see _reap_cancelled_entries.
                self._cancelled_entries.append(
                    (side, self._entry_orders[side], self._strategy.current_timestamp))
                self._entry_orders[side] = None
                self._entry_quoted_price[side] = None
            if self._entry_trigger_pending is not None:
                # The collateral is released a beat after this confirmation, not on it, so the
                # wait that matters starts here rather than when the cancel was requested.
                self._entry_blocked_until = \
                    self._strategy.current_timestamp + self.config.cancel_settle_delay
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self._retire_exit_order(self._take_profit_order)
            self._take_profit_order = None
        elif self._sl_chase_order and self._sl_chase_order.order_id == event.order_id:
            # A re-quote of the chasing exit. Any fills it already took are still ours.
            self._retire_exit_order(self._sl_chase_order)
            self._sl_chase_order = None
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None
        # The cancel we were waiting on may have been the last one — but the venue needs a
        # moment to release its collateral before it will accept the replacement.
        if (self._close_pending or self._stop_loss_triggered) and not self._has_resting_orders():
            self._arm_exit_placement()

    def _reschedule_after_exit_refusal(self, event: MarketOrderFailureEvent) -> bool:
        """
        Decide how long to wait before trying this exit again.

        A collateral refusal is diagnosable and gets its own, longer wait; everything else
        goes through the ordinary doubling backoff. True means the collateral path handled it
        and logged its own line.
        """
        if self._is_collateral_refusal(event.error_message) \
                and self._stand_down_after_collateral_refusal():
            return True
        self._arm_exit_placement(after_refusal=True)
        return False

    def _retire_exit_order(self, order: TrackedOrder):
        """
        Keep every cancelled exit, whether or not it shows fills yet.

        A cancel is a claim, not a fact. CoinDCX has filled a chasing exit and reported its
        cancel as successful in the same instant — and judging by whether the order shows
        fills at that moment throws away the ones that fill a heartbeat later. The executor
        then believes it still holds a position it has already closed, and burns its retries
        being refused by a venue that has nothing left to reduce.

        An unfilled order contributes nothing to the close accounting, so keeping it costs
        nothing; keeping it is the only thing that catches the late fill.
        """
        if order not in self._spent_exit_orders:
            self._spent_exit_orders.append(order)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        self._cancel_requested.discard(event.order_id)
        side = self._entry_side_for_order_id(event.order_id)
        if side is not None:
            self._failed_orders.append(self._entry_orders[side])
            self._entry_orders[side] = None
            self._entry_quoted_price[side] = None
            self._current_retries += 1
            if self._entry_triggered:
                # Once the trigger has been taken it is the only entry order in flight, so a
                # failure here is the trigger's. Release the latch: the market is still on the
                # far side of it, and a leg that cannot re-trigger falls back to resting a full
                # step the wrong way and simply waits out its timeout.
                self._entry_triggered = False
            self.logger().error(f"Entry order failed {event.order_id}. "
                                f"Retrying {self._current_retries}/{self._max_retries}")
            return
        if self._take_profit_order and self._take_profit_order.order_id == event.order_id:
            self._failed_orders.append(self._take_profit_order)
            self._take_profit_order = None
            self.logger().error(f"Take profit order failed {event.order_id}.")
        elif self._sl_chase_order and self._sl_chase_order.order_id == event.order_id:
            self._failed_orders.append(self._sl_chase_order)
            self._sl_chase_order = None
            self._current_retries += 1
            self._exit_has_been_refused = True
            if not self._reschedule_after_exit_refusal(event):
                self.logger().error(
                    f"Stop loss exit failed {event.order_id}. Retrying in "
                    f"{self._exit_retry_delay:.2f}s ({self._current_retries}/{self._max_retries})")
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None
            self._close_pending = True
            self._current_retries += 1
            self._exit_has_been_refused = True
            if not self._reschedule_after_exit_refusal(event):
                self.logger().error(
                    f"Close order failed {event.order_id}. Retrying in "
                    f"{self._exit_retry_delay:.2f}s ({self._current_retries}/{self._max_retries})")

    # ------------------------------------------------------------------ reporting

    def get_custom_info(self) -> Dict:
        return {
            "level_id": self.config.level_id,
            "side": self.side,
            "entry_mode": self.config.entry_mode,
            "reference_price": self._reference,
            "entry_price": self.entry_price,
            # The controller re-anchors the grid on this pair of fields.
            "close_price": self.close_price,
            "close_type": self.close_type,
            "take_profit_price": self.take_profit_price,
            "stop_loss_price": self.stop_loss_price,
            "stop_loss_triggered": self._stop_loss_triggered,
            "trigger_price_type": self.config.trigger_price_type,
            "resting_entries": {side.name: price for side, price in self._entry_quoted_price.items()
                                if price is not None},
            "filled_amount": self.open_filled_amount,
            "current_retries": self._current_retries,
            "max_retries": self._max_retries,
            "order_ids": [order.order_id for order in self._tracked_orders() if order],
            "held_position_orders": self._held_position_orders,
        }

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
