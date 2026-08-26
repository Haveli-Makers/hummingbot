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

    A leg is one complete round trip: a passive entry resting at the touch price, then
    whichever exit comes first out of the take profit, the stop loss and the time limit. The
    controller reads ``close_price`` back off this executor to decide where the next leg is
    anchored.

    Three behaviours distinguish it from the other executors in this package:

    * **Maker on both ends.** The entry rests at the touch — a buy at the best bid, a sell
      at the best ask — and follows the book, so it earns the maker fee rather than paying
      the taker one. The take profit is a resting limit for the same reason.
    * **The bracket hangs off the anchor, not the fill.** A passive entry fills inside the
      bracket, so a better fill is kept as extra edge instead of dragging the exits with it.
      The entry is confined to a band around the anchor, because a fill outside it would sit
      past its own exits and close instantly for nothing.
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

        # The price the whole leg is measured from. Frozen once, so the exits cannot drift
        # with the market while the leg is live.
        self._anchor: Optional[Decimal] = config.entry_price

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
    def anchor_price(self) -> Optional[Decimal]:
        return self._anchor

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
        """Average price actually paid, falling back to the anchor before anything fills."""
        if self._filled_side is not None:
            price = self._weighted_avg_price(self._entry_orders_for_side(self._filled_side))
            if price is not None:
                return price
        return self._anchor if self._anchor is not None else self._reference_or_entry()

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
            return self._maker_entry_price(side)
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

    def _maker_entry_price(self, side: TradeType) -> Optional[Decimal]:
        """Where this side's entry should be resting right now, quantized to the tick."""
        touch = self._touch_price(side)
        if touch is None:
            return None
        improvement = self.config.entry_price_improvement_pct
        if improvement > Decimal("0"):
            # Improving means moving towards the spread: a buy bids higher, a sell offers
            # lower. It buys queue priority at the cost of a slightly worse price — but far
            # enough and it crosses, which turns a maker entry into a taker one.
            touch = touch * (1 + improvement) if side == TradeType.BUY else touch * (1 - improvement)
            return self._clamp_to_maker(self._quantize(touch), side)
        return self._quantize(touch)

    def _within_entry_band(self, price: Decimal) -> bool:
        """
        Whether an entry resting at this price could still produce a workable leg.

        The take profit and stop loss are measured from the anchor, so an entry that fills
        far from it lands already past one of them and closes instantly for nothing — or
        worse, for a loss. Outside the band we would rather hold the order back and wait for
        the price to come to us.
        """
        band = self.config.entry_band_pct
        if band is None or self._anchor is None or self._anchor <= Decimal("0"):
            return True
        return abs(price - self._anchor) / self._anchor <= band

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
        if self._anchor is not None:
            return self._anchor
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
        """One step from the anchor in our favour. Independent of where the entry filled."""
        if self._anchor is None or self.side is None:
            return None
        if self.side == TradeType.BUY:
            return self._anchor * (1 + self.config.barriers.take_profit)
        return self._anchor * (1 - self.config.barriers.take_profit)

    @property
    def stop_loss_price(self) -> Optional[Decimal]:
        """One step from the anchor against us. Independent of where the entry filled."""
        if self._anchor is None or self.side is None:
            return None
        if self.side == TradeType.BUY:
            return self._anchor * (1 - self.config.barriers.stop_loss)
        return self._anchor * (1 + self.config.barriers.stop_loss)

    def _urgent_close_price(self, side: TradeType) -> Decimal:
        """
        A limit priced far enough through the book that it fills like a market order.

        CoinDCX rejects reduce_only on a market order — "Reduce Only Order is only applicable
        for Limit Order" — and the connector must send reduce_only on a close or the venue
        demands margin for a fresh opposite position, which fails exactly when the position
        is large against the wallet. A crossing limit satisfies both: it executes immediately
        against the resting side of the book, and its price is a hard bound on the fill.
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
            self._ensure_anchor()
            self._detect_entry_fill()
            if self._filled_side is None:
                self.control_entry_orders()
            else:
                self.control_barriers()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()
        self.evaluate_max_retries()

    def _ensure_anchor(self):
        """
        Freeze the anchor on the first tick that has a price.

        The controller normally supplies it. When it does not — the very first leg of a run —
        the live mid becomes the anchor once and never moves again, because every exit price
        for this leg is measured from it.
        """
        if self._anchor is not None:
            return
        price = self._usable_price(PriceType.MidPrice)
        if price is not None:
            self._anchor = price
            self.logger().info(f"Executor ID: {self.config.id} - anchored at {price}")

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
        if self._current_retries > self._max_retries:
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
        Keep a maker order resting at the touch on every side still on offer.

        The order rests inside the bracket rather than at a level beyond it, so it earns the
        maker fee and leaves both exits reachable. It follows the touch as the book moves,
        but only within a band around the anchor: outside it a fill would land past its own
        take profit or stop loss, so we pull the order and wait for the price to come back.
        """
        if self.config.entry_timeout is not None and \
                self._strategy.current_timestamp - self.config.timestamp >= self.config.entry_timeout:
            self.logger().info(f"Executor ID: {self.config.id} - entry timed out unfilled")
            self._give_up_on_entry()
            return
        if self._anchor is None:
            return

        for side in self.config.sides():
            desired = self._maker_entry_price(side)
            order = self._entry_orders[side]
            if desired is None:
                # Every entry is priced off the touch, so no order book means no entry at
                # all. Silence here looks exactly like a strategy patiently waiting for the
                # market, which is the one thing it is not doing.
                if not self._warned_no_touch:
                    self._warned_no_touch = True
                    self.logger().warning(
                        f"Executor ID: {self.config.id} - no best bid/ask for "
                        f"{self.config.trading_pair}; the order book has not arrived, so no "
                        f"{side.name} entry can be priced. Nothing will be placed until it does.")
                continue
            self._warned_no_touch = False
            if not self._within_entry_band(desired):
                # Out of band: hold back rather than fill into a dead bracket.
                if order and order.order and order.order.is_open:
                    self.logger().info(
                        f"Executor ID: {self.config.id} - {side.name} touch {desired} left the "
                        f"band around {self._anchor}; pulling the entry")
                    self._cancel_order(order)
                continue
            if order is None:
                self.place_entry_order(side, desired)
            elif self._should_requote(order, desired):
                self.logger().debug(
                    f"Executor ID: {self.config.id} - re-quoting {side.name} entry to {desired}")
                self._cancel_order(order)

    def _should_requote(self, order: TrackedOrder, desired: Decimal) -> bool:
        """
        Whether a resting order has drifted far enough from the touch to be worth replacing.

        Re-posting on every book change would be a cancel and a create every tick — rate
        limit for nothing, and each round trip is a window where we hold no order at all.
        """
        if order.order is None or not order.order.is_open:
            return False
        if order.order_id in self._cancel_requested:
            return False
        if self._order_filled_base(order) > Decimal("0"):
            return False  # already going in; let the fill path finish it
        resting = order.order.price
        if resting is None or resting.is_nan() or resting <= Decimal("0"):
            return False
        reference = self._anchor if self._anchor and self._anchor > Decimal("0") else resting
        return abs(resting - desired) >= self.config.entry_requote_pct * reference

    def place_entry_order(self, side: TradeType, price: Decimal):
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            order_type=self._maker_order_type(),
            amount=self.config.amount,
            price=price,
            side=side,
            position_action=PositionAction.OPEN,
        )
        self._entry_orders[side] = TrackedOrder(order_id=order_id)
        self._entry_quoted_price[side] = price
        self.logger().debug(f"Executor ID: {self.config.id} - resting {side.name} entry {order_id} at {price}")

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
        # Measured from where the market WAS when the stop fired, not from the level itself.
        # A tick only notices once the price is already through the level, and on a fast move
        # that gap alone can exceed the cap — which silently skipped the chase entirely and
        # went straight to a market order every time. That gap is slippage we have already
        # suffered; the cap is meant to bound what being patient costs on top of it.
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
        # The take profit we just asked to cancel is a reduce-only order for the whole
        # position. Until the venue lets go of it, a chasing exit is a second reduce-only
        # order for the same position and CoinDCX refuses the pair with "Insufficient funds".
        # Waiting costs a moment; not waiting cost a failed order and a second of delay, and
        # a second is long enough for the book to move the exit from maker to taker.
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
            # A cancel is a request, not an instant. For the moment between asking and the
            # venue agreeing, the take profit is still live — and it is a reduce-only order
            # for the whole position. Sending the close now makes that two reduce-only orders
            # for twice what we hold, which CoinDCX refuses with "Insufficient funds": an
            # attempt guaranteed to fail, burning one of the retries we may need. Send it the
            # instant the cancel is acknowledged instead.
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
            close_price = self._urgent_close_price(self.close_order_side)
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
        self._place_pending_close_if_clear()
        self._resume_stop_loss_chase_if_clear()

    def _exit_placement_blocked(self) -> bool:
        return self._strategy.current_timestamp < self._exit_blocked_until

    def _reset_exit_backoff(self):
        self._exit_retry_delay = self.config.cancel_settle_delay
        self._exit_blocked_until = 0.0

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
            # Already winding down under its own close type. The orchestrator calls this on
            # anything not yet TERMINATED, so a leg whose take profit has just filled lands
            # here too — overwriting close_type would report a win as an early stop and cost
            # the controller its tally and its anchor, and a second close order would sell a
            # position we no longer hold. Only step in if nothing is covering the position.
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
            price = self._maker_entry_price(side) or self._anchor
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
            self._arm_exit_placement(after_refusal=True)
            self.logger().error(
                f"Stop loss exit failed {event.order_id}. Retrying in "
                f"{self._exit_retry_delay:.2f}s ({self._current_retries}/{self._max_retries})")
        elif self._close_order and self._close_order.order_id == event.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None
            self._close_pending = True
            self._current_retries += 1
            self._arm_exit_placement(after_refusal=True)
            self.logger().error(
                f"Close order failed {event.order_id}. Retrying in "
                f"{self._exit_retry_delay:.2f}s ({self._current_retries}/{self._max_retries})")

    # ------------------------------------------------------------------ reporting

    def get_custom_info(self) -> Dict:
        return {
            "level_id": self.config.level_id,
            "side": self.side,
            "entry_mode": self.config.entry_mode,
            "anchor_price": self._anchor,
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
