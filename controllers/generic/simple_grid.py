from decimal import Decimal
from typing import List, Optional, Set

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import MarketDict, PositionMode, PriceType, TradeType
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

    # The one distance the whole chain is built from. Both exits are measured from the leg's
    # anchor — the price the previous leg closed at — never from the price we actually
    # filled at. The entry rests passively inside that bracket, so a fill better than the
    # anchor widens the take profit and narrows the stop instead of dragging both along.
    take_profit: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    stop_loss: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})
    # Mid, not LastTrade: CoinDCX perpetuals never publish a last trade, so LastTrade would
    # silently fall back to mid anyway. Saying it outright makes the trigger explicit.
    trigger_price_type: PriceType = PriceType.MidPrice

    # Entry behaviour. Every entry is a maker order resting at the touch — a buy at the best
    # bid, a sell at the best ask — so it earns the maker fee rather than paying the taker
    # one. It follows the touch as the book moves, but only inside a band around the anchor:
    # a fill outside the band would land past its own take profit or stop loss and close
    # immediately for nothing.
    # None derives it from the step. It has to stay well INSIDE the step: a band as wide as
    # the step lets an entry fill at the edge with its take profit already on top of it and
    # its stop loss two steps away, which is a losing bracket before the leg even starts.
    entry_band_pct: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    entry_band_fraction_of_step: Decimal = Field(default=Decimal("0.2"),
                                                 json_schema_extra={"is_updatable": True})
    entry_requote_pct: Decimal = Field(default=Decimal("0.0005"), json_schema_extra={"is_updatable": True})
    entry_price_improvement_pct: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    # A maker entry can rest unfilled indefinitely. Without a timeout the leg never ends, the
    # controller never opens another, and the anchor can never be refreshed.
    entry_timeout: Optional[int] = Field(default=300, json_schema_extra={"is_updatable": True})
    # When that timeout fires having traded nothing, start the next leg from wherever the
    # market is now instead of from the stale anchor. Without this the band and the anchor
    # deadlock each other: the anchor only moves on a trade, and no trade can happen while
    # the price sits outside the band around that anchor. Sitting out a trend is intended;
    # never coming back once it ends is not.
    reanchor_on_entry_timeout: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    # Re-anchoring deliberately allows an entry right after a big move, which is the shape of
    # market this strategy loses in. The pause is what stops it re-entering straight into the
    # middle of a run.
    cooldown_after_reanchor: int = Field(default=60, json_schema_extra={"is_updatable": True})

    # Stop loss behaviour. The stop level is watched, then left through a maker limit resting
    # at the touch on the exit side and followed down the book. Bounded by the drift cap,
    # past which we accept the market price.
    # The urgent exit — stop loss fallback, time limit, shutdown. A crossing LIMIT, not a
    # MARKET order: CoinDCX rejects reduce_only on market orders, and a close without
    # reduce_only has the venue demand margin for a fresh opposite position.
    # 1 = LIMIT (PriceType-style int parsing does not apply here; OrderType parses by value).
    close_slippage_ticks: int = Field(default=20, json_schema_extra={"is_updatable": True})

    stop_loss_chase: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    # Ticks inside the OPPOSITE touch: a sell one tick above the best bid, a buy one tick
    # below the best ask. The most aggressive a maker order can be — best offer in the book,
    # first to fill, same fee. Resting on our own touch instead would be a better price at
    # the back of the queue, which for an exit is the wrong way round.
    stop_loss_maker_offset_ticks: int = Field(default=1, json_schema_extra={"is_updatable": True})
    stop_loss_requote_pct: Decimal = Field(default=Decimal("0.0005"), json_schema_extra={"is_updatable": True})
    stop_loss_max_drift_pct: Decimal = Field(default=Decimal("0.001"), json_schema_extra={"is_updatable": True})

    # Direction. The opening leg of a perpetual run rests a maker order on each side of the
    # book and lets the market pick; whichever fills first is the side for the rest of the
    # run. Spot can only ever buy, so it is long from the start and stays there.
    initial_entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.BOTH_OCO
    lock_side_after_first_fill: bool = Field(default=True, json_schema_extra={"is_updatable": True})

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
    # 3. Legs keep failing before they trade. Without this the controller opens a new leg
    #    every tick against a venue that is refusing them, logging one error a second
    #    forever — which is what a stranded position looks like from the inside, because the
    #    position holds the margin the next leg needs.
    max_consecutive_failed_legs: int = Field(default=5, json_schema_extra={"is_updatable": True})
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
    def effective_entry_band(self) -> Optional[Decimal]:
        """How far from the anchor an entry may rest, defaulting to a fraction of the step."""
        if self.entry_band_pct is not None:
            return self.entry_band_pct
        return self.take_profit * self.entry_band_fraction_of_step

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class SimpleGrid(ControllerBase):
    """
    Runs one leg at a time and re-anchors on where the last leg closed.

    On a perpetual the opening leg rests a maker order on each side of the book; whichever
    fills first sets the direction for the whole run. Spot is long from the start. Legs stop
    being opened once the loss threshold is reached.
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

        self._consecutive_failed_legs: int = 0
        self._reanchor_pending: bool = False
        self._reanchors: int = 0

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
        higher. Fees and stop chase drift push the real figure higher still, which is why the
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
            # closed. A leg whose entry was never filled has nothing to say about where the
            # chain should sit next, and letting it move the anchor would walk the grid
            # across the market without a single fill behind it.
            opened = (executor.filled_amount_quote > Decimal("0")
                      and executor.close_type not in (CloseType.EXPIRED,
                                                      CloseType.INSUFFICIENT_BALANCE,
                                                      CloseType.FAILED))
            if opened and close_price:
                self._anchor_price = Decimal(str(close_price))
            if opened and side is not None and self.config.lock_side_after_first_fill \
                    and self._locked_side is None:
                self._locked_side = side
                self.logger().info(f"SimpleGrid locked to {side.name} for the rest of the run")

            if opened:
                self._realized_pnl_quote += executor.net_pnl_quote
                self._peak_pnl_quote = max(self._peak_pnl_quote, self._realized_pnl_quote)
                self._legs_closed += 1
                self._last_close_type = executor.close_type
                if executor.close_type == CloseType.TAKE_PROFIT:
                    self._wins += 1
                elif executor.close_type == CloseType.STOP_LOSS:
                    self._losses += 1

            # An entry that timed out having filled nothing is the one case where the chain
            # is allowed to start somewhere the market chose rather than somewhere a trade
            # ended. It is a fresh start, not a continuation, so it goes through here rather
            # than through close_price above — that price is just the current mid and means
            # nothing as a link in the chain.
            # A leg that failed before trading tells us the venue is refusing us, not that
            # the market moved. A leg that failed AFTER trading is worse: its position may
            # still be open, and every later leg would be stacked on top of it.
            if opened:
                self._consecutive_failed_legs = 0
            elif executor.close_type in (CloseType.INSUFFICIENT_BALANCE, CloseType.FAILED):
                self._consecutive_failed_legs += 1
            if executor.close_type == CloseType.FAILED and executor.filled_amount_quote > Decimal("0"):
                self._halt_reason = (
                    f"leg {executor.id} failed after trading {executor.filled_amount_quote:.4f} "
                    f"quote — its position may still be open. Check the account and close it by "
                    f"hand before restarting")
                self.logger().error(f"SimpleGrid halted: {self._halt_reason}")

            timed_out_unfilled = (not opened and executor.close_type == CloseType.EXPIRED)
            if timed_out_unfilled and self.config.reanchor_on_entry_timeout:
                self._reanchor_pending = True

            self._last_close_timestamp = executor.close_timestamp or self.market_data_provider.time()
            self._cooldown_seconds = self._cooldown_for(executor.close_type, timed_out_unfilled)
            self._evaluate_halt_conditions()

    def _cooldown_for(self, close_type: Optional[CloseType], timed_out_unfilled: bool) -> int:
        if timed_out_unfilled and self.config.reanchor_on_entry_timeout:
            return self.config.cooldown_after_reanchor
        if close_type == CloseType.STOP_LOSS:
            return self.config.cooldown_after_stop_loss
        return self.config.cooldown_after_take_profit

    def _evaluate_halt_conditions(self):
        """
        Two independent reasons to stop opening legs.

        Losing more often than winning is only a problem if it is actually costing money,
        so that check needs the count and the PnL together.
        """
        if self._halt_reason is not None:
            return
        if self._consecutive_failed_legs >= self.config.max_consecutive_failed_legs:
            self._halt_reason = (f"{self._consecutive_failed_legs} legs in a row failed before "
                                 f"trading. The venue is refusing our orders — usually a "
                                 f"position still holding the margin the next leg needs")
            self.logger().error(f"SimpleGrid halted: {self._halt_reason}")
            return
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
        # on the very first leg. Both exits are measured from it, and the entry may only rest
        # within a band around it.
        if self._reanchor_pending:
            previous = self._anchor_price
            self._anchor_price = mid_price
            self._reanchor_pending = False
            self._reanchors += 1
            self.logger().info(
                f"SimpleGrid re-anchored from {previous} to {mid_price} after an entry timed "
                f"out unfilled; the price had left the band around the old anchor")

        anchor = self._anchor_price if self._anchor_price is not None else mid_price
        amount = self.config.order_amount_quote / anchor
        return SimpleGridExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=self.config.id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_mode=self._next_entry_mode(),
            amount=amount,
            entry_price=anchor,
            entry_band_pct=self.config.effective_entry_band,
            entry_requote_pct=self.config.entry_requote_pct,
            entry_price_improvement_pct=self.config.entry_price_improvement_pct,
            entry_timeout=self.config.entry_timeout,
            trigger_price_type=self.config.trigger_price_type,
            barriers=SimpleGridBarriers(
                take_profit=self.config.take_profit,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
                close_slippage_ticks=self.config.close_slippage_ticks,
                stop_loss_chase=self.config.stop_loss_chase,
                stop_loss_maker_offset_ticks=self.config.stop_loss_maker_offset_ticks,
                stop_loss_requote_pct=self.config.stop_loss_requote_pct,
                stop_loss_max_drift_pct=self.config.stop_loss_max_drift_pct,
            ),
            leverage=self.config.leverage,
        )

    @property
    def is_perpetual(self) -> bool:
        return "perpetual" in self.config.connector_name.lower()

    def _next_entry_mode(self) -> SimpleGridEntryMode:
        """
        Which side(s) to offer.

        Spot can only ever go long. A perpetual opens with a maker order on each side of the
        book and lets the market decide which one gets hit; from then on the run stays on
        that side, so every later leg rests a single order.
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

        active = [info for info in self.executors_info if info.is_active]
        info = active[0].custom_info if active else {}

        base = self._anchor_price if self._anchor_price is not None else mid_price
        anchor = f"{base:.6f}"
        if self._anchor_price is None:
            anchor += " (mid, no leg closed yet)"

        # Read the bracket off the live executor, which freezes it when the leg opens.
        # Recomputing it here from the current mid would show the exits chasing the market.
        tp_price = info.get("take_profit_price") or base * (1 + self.config.take_profit)
        sl_price = info.get("stop_loss_price") or base * (1 - self.config.stop_loss)

        # Nothing sits at a level any more — the entries rest at the touch — so the useful
        # thing to show is where our orders actually are, and whether the band is holding
        # them back. Without this there is no way to tell working from stuck.
        resting = info.get("resting_entries") or {}
        band = self.config.effective_entry_band
        # How far the market has wandered from the anchor. Nothing rests in the book while
        # the price is outside the band, so without this number a strategy that has been
        # left behind by a trend looks identical to one that is simply waiting.
        drift = abs(mid_price - base) / base if base else None
        if info.get("stop_loss_triggered"):
            working = ["stop loss triggered — leaving passively at the touch"]
        elif resting:
            working = [f"{side.lower()} resting at {Decimal(str(price)):.6f}"
                       for side, price in resting.items()]
        elif active:
            working = ["nothing resting — price is outside the entry band, waiting"]
        else:
            working = ["no leg open"]
        if band is not None:
            working.append(f"band ±{band:.2%}")
        if drift is not None:
            working.append(f"price is {drift:.2%} from the anchor")

        side = self._locked_side.name if self._locked_side else (
            "both (opening leg)" if self.is_perpetual else "BUY")
        actual_str = f"{actual:.2%}" if actual is not None else "n/a"
        loss_str = f"{self._realized_pnl_quote:.4f}"
        loss_str += f" / threshold -{limit:.4f}" if limit is not None else " (no threshold set)"

        lines = [
            f"Simple Grid | {self.config.connector_name} | {self.config.trading_pair}",
            f"  Mid: {mid_price:.6f} | Anchor: {anchor} | Side: {side}",
            f"  Working: {' | '.join(working)}",
            f"  TP: {tp_price:.6f} ({self.config.take_profit:.4%}) | "
            f"SL: {sl_price:.6f} ({self.config.stop_loss:.4%}) | "
            f"Amount/leg: {self.config.order_amount_quote}",
            f"  Legs closed: {self._legs_closed} | TP: {self._wins} | SL: {self._losses} | "
            f"Re-anchors: {self._reanchors} | Realised PnL: {self._realized_pnl_quote:.4f}",
            # Printed side by side deliberately: the configuration only makes money if the
            # actual rate stays above the break-even one.
            f"  Win rate needed: {self.break_even_win_rate:.2%} (before fees) | actual: {actual_str}",
            f"  Realised: {loss_str}",
        ]
        # A mean-reverting entry only works near the anchor, and the anchor only moves when
        # something trades. Once a trend carries the price out of the band the strategy can
        # sit there indefinitely with nothing resting and nothing to report — so say so.
        if band is not None and drift is not None and drift > band * Decimal("3") \
                and not self.is_halted:
            if self.config.reanchor_on_entry_timeout and self.config.entry_timeout:
                lines.append(f"  Adrift — the price is {drift:.2%} from the anchor "
                             f"({band:.2%} band); re-anchoring in up to "
                             f"{self.config.entry_timeout}s")
            else:
                lines.append(f"  STALE — the price has been {drift:.2%} from the anchor "
                             f"({band:.2%} band); no leg can open until it comes back")
        if self.is_halted:
            lines.append(f"  HALTED — {self._halt_reason}")
        elif len(self.active_executors()) == 0 and not self._cooldown_elapsed():
            remaining = self._cooldown_seconds - (self.market_data_provider.time() - self._last_close_timestamp)
            lines.append(f"  Cooling down: {remaining:.0f}s remaining")
        return lines
