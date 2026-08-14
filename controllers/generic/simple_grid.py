from decimal import Decimal
from typing import List, Optional, Set

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import MarketDict, OrderType, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridBarriers,
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class SimpleGridConfig(ControllerConfigBase):
    """
    Configuration for the simple grid: a repeating trade cycle whose starting point follows
    the price where the previous cycle closed.
    """
    controller_type: str = "generic"
    controller_name: str = "simple_grid"
    candles_config: List[CandlesConfig] = []

    connector_name: str = "coindcx_perpetual"
    trading_pair: str = "BTC-USDT"
    leverage: int = Field(default=1)
    position_mode: PositionMode = PositionMode.ONEWAY

    # Sizing. total_amount_quote is inherited and caps the strategy; each leg risks
    # order_amount_quote.
    order_amount_quote: Decimal = Field(default=Decimal("100"), json_schema_extra={"is_updatable": True})

    # The one distance the whole chain is built from. Every bracket is anchor +/- this:
    # the two entry orders when flat, and the two exit orders when in a position. Because
    # it is symmetric, a winning leg and a losing leg are the same size, so the strategy
    # breaks even at a 50% hit rate rather than needing two thirds.
    take_profit: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    stop_loss: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})
    trigger_price_type: PriceType = PriceType.LastTrade

    # Entry behaviour.
    # MARKET, not LIMIT. The entry fires once the price has already reached the level, so
    # a limit order at that level would rest behind the market and never fill in the very
    # move it is meant to catch. It costs the taker fee; that is the price of entering with
    # the move rather than waiting for it.
    entry_order_type: OrderType = OrderType.MARKET
    # Set non-zero only to verify order placement on a live exchange without filling:
    # it rests the entry that far away from the touch price. Leave at 0 to trade.
    # Chasing is off by design here: the entry orders belong at fixed grid prices relative
    # to the anchor, not wherever the touch price happens to have wandered to.
    # How far past the anchor the market must move before we enter. Defaults to the take
    # profit distance, so a winning leg lands exactly on the next entry level.
    entry_step: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    entry_timeout: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})

    # Grid behaviour. Every flat state offers both sides, so the direction is re-decided
    # each time round rather than inherited. Locking is kept as an option but defaults off:
    # under this chain design it would stop the strategy re-deciding after every exit.
    initial_entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.BOTH_OCO
    lock_side_after_first_fill: bool = False
    # Spot opens the chain immediately with a passive buy at the touch price rather than
    # waiting for a step: it can only ever go long, so there is no direction to wait for,
    # and waiting would leave it out of the market until the price happened to rise.
    # Futures keeps waiting, because the wait is what tells it which side to take.
    first_leg_passive_entry: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    # How long that passive opening order may sit unfilled before it crosses the spread.
    # Without this it can wait forever in a market moving away from it.
    entry_cross_after: Optional[float] = Field(default=30.0, json_schema_extra={"is_updatable": True})
    cooldown_after_take_profit: int = Field(default=0, json_schema_extra={"is_updatable": True})
    cooldown_after_stop_loss: int = Field(default=60, json_schema_extra={"is_updatable": True})

    # Risk. Two independent halts, either one stops new legs being opened. Nothing is
    # force-closed; an open leg is left to finish on its own barriers.
    #
    # 1. Losses reach the threshold. Set as an absolute quote amount, a fraction of
    #    total_amount_quote, or both — the tighter one wins.
    max_loss_quote: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    max_loss_pct: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    # 2. Stop losses have caught up with take profits AND we are down overall. Being behind
    #    on count while still in profit is not enough on its own.
    #    OFF by default: the loss threshold above is the agreed stop condition for now, and
    #    this one is kept ready to switch on later rather than removed.
    stop_when_losses_outnumber_wins: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    #    Grace period, in closed legs, before that count check applies. Without it a single
    #    losing first leg already satisfies "losses >= wins and down", and the strategy
    #    would stop before it had a fair sample.
    min_legs_before_count_check: int = Field(default=10, json_schema_extra={"is_updatable": True})

    @field_validator("take_profit", "stop_loss", "order_amount_quote")
    @classmethod
    def validate_positive(cls, value: Decimal) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("take_profit, stop_loss and order_amount_quote must be greater than zero")
        return value

    @property
    def effective_entry_step(self) -> Decimal:
        return self.entry_step if self.entry_step is not None else self.take_profit

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class SimpleGrid(ControllerBase):
    """
    Runs one leg at a time and re-anchors on where the last leg closed.

    The first leg is offered on both sides at once and whichever fills first cancels the
    other; after that the strategy stays on the side that won. Legs stop being opened once
    the drawdown limit is reached.
    """

    def __init__(self, config: SimpleGridConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config

        self._anchor_price: Optional[Decimal] = None
        self._locked_side: Optional[TradeType] = None
        self._processed_executor_ids: Set[str] = set()

        self._realized_pnl_quote: Decimal = Decimal("0")
        self._peak_pnl_quote: Decimal = Decimal("0")
        self._last_close_timestamp: float = 0.0
        self._cooldown_seconds: int = 0

        self._legs_closed: int = 0
        self._wins: int = 0
        self._losses: int = 0
        self._last_close_type: Optional[CloseType] = None
        self._halt_reason: Optional[str] = None

    # ------------------------------------------------------------------ state

    @property
    def is_halted(self) -> bool:
        return self._halt_reason is not None

    @property
    def current_drawdown_quote(self) -> Decimal:
        return self._peak_pnl_quote - self._realized_pnl_quote

    @property
    def loss_limit_quote(self) -> Optional[Decimal]:
        """The loss threshold, tighter of the absolute and percentage forms."""
        limits = []
        if self.config.max_loss_quote is not None:
            limits.append(self.config.max_loss_quote)
        if self.config.max_loss_pct is not None:
            limits.append(self.config.total_amount_quote * self.config.max_loss_pct)
        return min(limits) if limits else None

    @property
    def break_even_win_rate(self) -> Decimal:
        """
        The share of legs that must win just to break even, before fees.

        Equal take profit and stop loss gives 50%; a stop wider than the target pushes it
        higher. Fees and stop slippage push the real figure higher still, which is why the
        status line shows this next to the rate actually being achieved.
        """
        return self.config.stop_loss / (self.config.take_profit + self.config.stop_loss)

    @property
    def actual_win_rate(self) -> Optional[Decimal]:
        if self._legs_closed == 0:
            return None
        return Decimal(self._wins) / Decimal(self._legs_closed)

    def active_executors(self) -> List[ExecutorInfo]:
        return self.filter_executors(self.executors_info, lambda e: e.is_active)

    # ------------------------------------------------------------------ main loop

    async def update_processed_data(self):
        self.processed_data = {
            "anchor_price": self._anchor_price,
            "locked_side": self._locked_side,
            "realized_pnl_quote": self._realized_pnl_quote,
            "drawdown_quote": self.current_drawdown_quote,
            "halted": self.is_halted,
        }

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self._absorb_closed_executors()

        if self.is_halted or self.config.manual_kill_switch:
            return []
        if len(self.active_executors()) > 0:
            return []
        if not self._cooldown_elapsed():
            return []

        executor_config = self._build_leg_config()
        if executor_config is None:
            return []
        return [CreateExecutorAction(controller_id=self.config.id, executor_config=executor_config)]

    def _absorb_closed_executors(self):
        """
        Fold every newly finished leg into the grid state: where it closed becomes the new
        anchor, its PnL moves the drawdown, and the side it took is the side we keep.
        """
        for executor in self.executors_info:
            if not executor.is_done or executor.id in self._processed_executor_ids:
                continue
            self._processed_executor_ids.add(executor.id)

            close_price = executor.custom_info.get("close_price")
            side = executor.custom_info.get("side")

            # A leg that never opened tells us nothing about where the grid should sit.
            # Whether a leg counts is decided by whether it actually traded, not by how it
            # closed. A leg whose level was never reached has nothing to say about where
            # the chain should sit next, and letting it move the anchor would walk the grid
            # across the market without a single fill behind it.
            opened = (executor.filled_amount_quote > Decimal("0")
                      and executor.close_type not in (CloseType.EXPIRED,
                                                      CloseType.INSUFFICIENT_BALANCE,
                                                      CloseType.FAILED))
            if opened and close_price:
                self._anchor_price = Decimal(str(close_price))
            if opened and side is not None and self.config.lock_side_after_first_fill:
                self._locked_side = side

            if opened:
                self._realized_pnl_quote += executor.net_pnl_quote
                self._peak_pnl_quote = max(self._peak_pnl_quote, self._realized_pnl_quote)
                self._legs_closed += 1
                self._last_close_type = executor.close_type
                if executor.close_type == CloseType.TAKE_PROFIT:
                    self._wins += 1
                elif executor.close_type == CloseType.STOP_LOSS:
                    self._losses += 1

            self._last_close_timestamp = executor.close_timestamp or self.market_data_provider.time()
            self._cooldown_seconds = (self.config.cooldown_after_stop_loss
                                      if executor.close_type == CloseType.STOP_LOSS
                                      else self.config.cooldown_after_take_profit)
            self._evaluate_halt_conditions()

    def _evaluate_halt_conditions(self):
        """
        Two independent reasons to stop opening legs.

        Losing more often than winning is only a problem if it is actually costing money,
        so that check needs the count and the PnL together.
        """
        limit = self.loss_limit_quote
        if limit is not None and self._realized_pnl_quote <= -limit:
            self._halt_reason = (f"loss {self._realized_pnl_quote:.4f} reached the threshold "
                                 f"-{limit:.4f} after {self._legs_closed} legs")
        elif (self.config.stop_when_losses_outnumber_wins
                and self._legs_closed >= self.config.min_legs_before_count_check
                and self._losses >= self._wins and self._realized_pnl_quote < Decimal("0")):
            self._halt_reason = (f"{self._losses} stop losses vs {self._wins} take profits "
                                 f"and down {self._realized_pnl_quote:.4f} "
                                 f"after {self._legs_closed} legs")
            self.logger().warning(f"SimpleGrid halted: {self._halt_reason}")

    def _is_opening_leg(self) -> bool:
        """True until something has actually traded, so the chain has no starting point yet."""
        return self._anchor_price is None and self._legs_closed == 0

    def _cooldown_elapsed(self) -> bool:
        if self._cooldown_seconds <= 0:
            return True
        return self.market_data_provider.time() - self._last_close_timestamp >= self._cooldown_seconds

    def _build_leg_config(self) -> Optional[SimpleGridExecutorConfig]:
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        if not mid_price or mid_price <= Decimal("0"):
            return None

        # The chain hangs off the anchor: where the previous leg closed, or the current mid
        # on the very first leg. The executor watches one step above and one step below it.
        anchor = self._anchor_price if self._anchor_price is not None else mid_price
        amount = self.config.order_amount_quote / anchor
        entry_mode = self._next_entry_mode()
        opening_passively = self._is_opening_leg() and self.config.first_leg_passive_entry \
            and not self.is_perpetual
        return SimpleGridExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=self.config.id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_mode=entry_mode,
            amount=amount,
            entry_price=None if opening_passively else anchor,
            entry_order_type=OrderType.LIMIT if opening_passively else self.config.entry_order_type,
            entry_offset_pct=Decimal("0") if opening_passively else self._entry_step_for(entry_mode),
            entry_timeout=self.config.entry_timeout,
            entry_cross_after=self.config.entry_cross_after if opening_passively else None,
            # Spot buys whichever level the market reaches, so a fall is tradeable too.
            enter_on_either_level=not self.is_perpetual,
            trigger_price_type=self.config.trigger_price_type,
            barriers=SimpleGridBarriers(
                take_profit=self.config.take_profit,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
            ),
            leverage=self.config.leverage,
        )

    def _entry_step_for(self, entry_mode: SimpleGridEntryMode) -> Decimal:
        """
        How far past the anchor the market must travel before we enter.

        The same distance either way. Spot buys whichever level is reached rather than only
        the one above, so it does not need a shortened step to stay in the market.
        """
        return self.config.effective_entry_step

    @property
    def is_perpetual(self) -> bool:
        return "perpetual" in self.config.connector_name.lower()

    def _next_entry_mode(self) -> SimpleGridEntryMode:
        """
        Which side(s) to watch.

        Spot can only ever go long. On a perpetual both levels are watched and the market
        decides: reaching the upper one means it is rising, the lower one means it is
        falling. That is what makes a take profit keep the direction and a stop loss turn
        it around, without either being coded as a rule.
        """
        if not self.is_perpetual:
            return SimpleGridEntryMode.LONG_ONLY
        if self._locked_side is not None and self.config.lock_side_after_first_fill:
            return (SimpleGridEntryMode.LONG_ONLY if self._locked_side == TradeType.BUY
                    else SimpleGridEntryMode.SHORT_ONLY)
        return self.config.initial_entry_mode

    # ------------------------------------------------------------------ status

    def to_format_status(self) -> List[str]:
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        limit = self.loss_limit_quote
        actual = self.actual_win_rate

        # Read the levels off the live executor, which freezes them when it is created.
        # Recomputing them here from the current mid would show them chasing the market.
        step = self.config.effective_entry_step
        active = [info for info in self.executors_info if info.is_active]
        live_levels = active[0].custom_info.get("entry_levels") if active else None
        if live_levels:
            long_level = live_levels.get(TradeType.BUY.name)
            short_level = live_levels.get(TradeType.SELL.name)
            anchor = "fixed when the leg opened"
            if self._anchor_price is not None:
                anchor = f"{self._anchor_price:.6f}"
        else:
            base = self._anchor_price if self._anchor_price is not None else mid_price
            long_level, short_level = base * (1 + step), base * (1 - step)
            anchor = f"{base:.6f}"
            if self._anchor_price is None:
                anchor += " (mid, no leg open yet)"

        watching = []
        if long_level:
            watching.append(f"long above {long_level:.6f}")
        if short_level:
            watching.append(f"short below {short_level:.6f}")
        gaps = [abs(level - mid_price) for level in (long_level, short_level) if level]
        if gaps:
            watching.append(f"distance to nearer level: {min(gaps) / mid_price:.4%}")

        side = self._locked_side.name if self._locked_side else "both (unlocked)"
        actual_str = f"{actual:.2%}" if actual is not None else "n/a"
        loss_str = f"{self._realized_pnl_quote:.4f}"
        loss_str += f" / threshold -{limit:.4f}" if limit is not None else " (no threshold set)"

        lines = [
            f"Simple Grid | {self.config.connector_name} | {self.config.trading_pair}",
            f"  Mid: {mid_price:.6f} | Anchor: {anchor} | Side: {side}",
            # Nothing rests in the book while we wait, so without these numbers there is no
            # way to tell a watching strategy from a stuck one.
            f"  Waiting for: {' | '.join(watching)}",
            f"  TP: {self.config.take_profit:.4%} | SL: {self.config.stop_loss:.4%} | "
            f"Entry step: {self.config.effective_entry_step:.4%} | "
            f"Amount/leg: {self.config.order_amount_quote}",
            f"  Legs closed: {self._legs_closed} | TP: {self._wins} | SL: {self._losses} | "
            f"Realised PnL: {self._realized_pnl_quote:.4f}",
            # Printed side by side deliberately: the configuration only makes money if the
            # actual rate stays above the break-even one.
            f"  Win rate needed: {self.break_even_win_rate:.2%} (before fees) | actual: {actual_str}",
            f"  Realised: {loss_str}",
        ]
        if self.is_halted:
            lines.append(f"  HALTED — {self._halt_reason}")
        elif len(self.active_executors()) == 0 and not self._cooldown_elapsed():
            remaining = self._cooldown_seconds - (self.market_data_provider.time() - self._last_close_timestamp)
            lines.append(f"  Cooling down: {remaining:.0f}s remaining")
        return lines
