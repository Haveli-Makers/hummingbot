from decimal import Decimal
from typing import List, Optional, Set

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import (
    MarketDict,
    PositionAction,
    PositionMode,
    PositionSide,
    PriceType,
    TradeType,
)
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import (
    ExecutionStrategy,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import (
    SimpleGridBarriers,
    SimpleGridEntryMode,
    SimpleGridExecutorConfig,
)
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
)
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

    # The one distance the whole chain is built from, and it does every job: it sets where the
    # next entry rests, where its trigger sits, and how far the exits are from the fill. Both
    # exits are measured from the price the leg actually FILLED at, so a long filled at F is
    # bracketed symmetrically at F * (1 ± step).
    take_profit: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    stop_loss: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})
    # Mid, not LastTrade: CoinDCX perpetuals never publish a last trade, so LastTrade would
    # silently fall back to mid anyway. Saying it outright makes the trigger explicit.
    trigger_price_type: PriceType = PriceType.MidPrice

    # Entry behaviour. The opening order rests one step in our favour of where the previous
    # leg ended; one step against it is a trigger. If the market reaches the trigger first we
    # open there instead, crossing to do it. An order a full step from the market cannot
    # cross by accident, so the resting side is reliably a maker fill and only the trigger
    # ever pays taker.
    #
    # The very first leg has no previous exit to measure from, so it rests at the touch.
    #
    # An entry can still sit unfilled if the price stays between the two prices, so the
    # timeout is what ends a leg that is going nowhere.
    entry_timeout: Optional[int] = Field(default=300, json_schema_extra={"is_updatable": True})

    # Stop loss behaviour. The stop level is watched, then left through a maker limit resting
    # at the touch on the exit side and followed down the book. Bounded by the drift cap,
    # past which we accept the market price.
    # The urgent exit — stop loss fallback, time limit, shutdown. A crossing LIMIT, not a
    # MARKET order: CoinDCX rejects reduce_only on market orders, and a close without
    # reduce_only has the venue demand margin for a fresh opposite position.
    # 1 = LIMIT (PriceType-style int parsing does not apply here; OrderType parses by value).
    close_slippage_ticks: int = Field(default=20, json_schema_extra={"is_updatable": True})

    # CoinDCX acknowledges a cancel before it releases the collateral behind it, so an exit
    # sent on the acknowledgement is still refused as a second reduce-only order. Wait this
    # long on purpose; every refusal doubles it, capped at exit_retry_max_delay.
    cancel_settle_delay: float = Field(default=0.25, json_schema_extra={"is_updatable": True})
    exit_retry_max_delay: float = Field(default=2.0, json_schema_extra={"is_updatable": True})

    # "Insufficient funds" on a reduce-only close is not a transient: the margin is held by an
    # exit we already asked the venue to cancel and were told was gone. Replacing it faster
    # cannot help, so after this many refusals in a row the executor waits for that order to
    # resolve instead of sending another.
    collateral_refusal_wait: float = Field(default=3.0, json_schema_extra={"is_updatable": True})
    collateral_refusals_before_waiting: int = Field(default=2, json_schema_extra={"is_updatable": True})

    stop_loss_chase: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    # Ticks inside the OPPOSITE touch: a sell one tick above the best bid, a buy one tick
    # below the best ask. The most aggressive a maker order can be — best offer in the book,
    # first to fill, same fee. Resting on our own touch instead would be a better price at
    # the back of the queue, which for an exit is the wrong way round.
    stop_loss_maker_offset_ticks: int = Field(default=1, json_schema_extra={"is_updatable": True})
    stop_loss_requote_pct: Decimal = Field(default=Decimal("0.0005"), json_schema_extra={"is_updatable": True})
    stop_loss_max_drift_pct: Decimal = Field(default=Decimal("0.001"), json_schema_extra={"is_updatable": True})

    # Direction. Between legs the account is flat, so a run that opens with a buy is long or
    # flat for its whole life, and one that opens with a sell is short or flat. The first
    # order fixes the bias and there is nothing to re-decide afterwards.
    initial_entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.BOTH_OCO

    # A pause means standing flat while the grid wants to be working, so both default to none.
    cooldown_after_take_profit: int = Field(default=0, json_schema_extra={"is_updatable": True})
    cooldown_after_stop_loss: int = Field(default=0, json_schema_extra={"is_updatable": True})

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
    #
    #    Counts only legs the VENUE refused. A leg stopped by our own budget check never
    #    reached the exchange and is counted by time instead — see below.
    max_consecutive_failed_legs: int = Field(default=5, json_schema_extra={"is_updatable": True})

    # 3b. Our own budget check keeps refusing to open a leg.
    #
    #     Counted in seconds rather than in legs, because the two causes look identical per
    #     leg and differ only in how long they last. The check reads the connector's CACHED
    #     balance, and that cache is refreshed by REST every LONG_POLL_INTERVAL — 120s —
    #     whenever the private websocket is alive. So margin released just now can stay
    #     invisible for a long time, and a controller retrying every second racks up refusals
    #     at a rate that says nothing about whether the money is really there.
    #
    #     On 2026-09-02 that halted a run holding 904 INR against a 713 INR leg. Five refusals
    #     in five seconds, every one of them a stale read of a wallet that was already full.
    #
    #     A genuine shortage does not clear; a stale read does, at the next poll. Anything past
    #     one full poll interval is therefore real, and worth halting for.
    insufficient_balance_grace_seconds: float = Field(default=180.0,
                                                      json_schema_extra={"is_updatable": True})
    #     How long to wait before trying again after one. The check reads a cached wallet, so
    #     the answer cannot change until that cache is re-read and retrying every tick only
    #     produces an error line every tick.
    retry_after_insufficient_balance: int = Field(default=5, json_schema_extra={"is_updatable": True})

    # 4. The venue holds a position no leg claims. The stop loss lives in this process, so an
    #    unowned position has nothing watching it — the single most dangerous state this
    #    strategy can be in, and the one that survives every other guard.
    reconcile_positions: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    #    How long the venue and our own books must disagree before we act. An executor that
    #    has just filled takes a moment to report it, and flattening a leg that was about to
    #    announce itself would be worse than the problem.
    orphan_grace_seconds: float = Field(default=10.0, json_schema_extra={"is_updatable": True})
    #    Close it as well as halting. Only ever applies to a position that appeared while this
    #    controller was running and watching a flat account — a position that was already
    #    there when we started is somebody else's and is never touched.
    flatten_orphan_positions: bool = Field(default=True, json_schema_extra={"is_updatable": True})
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

        # Where the previous leg ended. The next leg's two entry prices hang off it.
        self._reference_price: Optional[Decimal] = None
        # The side every leg opens on. Undecided until the first one trades: an opening leg
        # may offer both, but once one has filled the account is long-or-flat (or
        # short-or-flat) for the rest of the run, so there is nothing left to choose.
        self._bias_side: Optional[TradeType] = None
        self._processed_executor_ids: Set[str] = set()

        self._realized_pnl_quote: Decimal = Decimal("0")
        self._peak_pnl_quote: Decimal = Decimal("0")
        self._last_close_timestamp: float = 0.0
        self._cooldown_seconds: int = 0

        self._consecutive_failed_legs: int = 0
        # When our own budget check first refused a leg, cleared the moment one reaches the
        # venue. A timestamp rather than a count: see insufficient_balance_grace_seconds.
        self._insufficient_balance_since: Optional[float] = None
        # Reconciliation against what the venue actually holds.
        self._seen_flat_since_start: bool = False
        self._orphan_since: Optional[float] = None
        self._orphan_flatten_sent: bool = False

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

    def unfinished_executors(self) -> List[ExecutorInfo]:
        """
        Every leg that has not reached TERMINATED — including the ones still closing.

        ``is_active`` covers RUNNING and NOT_STARTED only, so a leg that has hit its stop and
        is working its way out does not appear there. For most controllers that distinction
        does not matter. Here it decides whether the strategy works at all: this one is flat
        BETWEEN legs, and a closing leg still owns both the position and the margin behind it.

        Starting the next leg off ``is_active`` opens it while the old one still holds the
        collateral. The venue refuses it for want of funds, the executor reports a leg that
        failed before trading, and five of those in a row trip the halt — which is exactly
        what ended the 16:53 run on 2026-09-02: leg 2 spent eight seconds failing to place its
        exit, and the controller spawned five doomed legs inside that window, each one asking
        for the same price leg 2 had already taken.
        """
        return self.filter_executors(self.executors_info, lambda e: not e.is_done)

    # ------------------------------------------------------------------ main loop

    async def update_processed_data(self):
        self.processed_data = {
            "anchor_price": self._reference_price,
            "reference_price": self._reference_price,
            "realized_pnl_quote": self._realized_pnl_quote,
            "drawdown_quote": self.current_drawdown_quote,
            "halted": self.is_halted,
        }

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self._absorb_closed_executors()

        # Before anything else: does the venue agree with us about what we hold?
        reconciliation = self._reconcile_positions()
        if reconciliation:
            return reconciliation

        if self.is_halted or self.config.manual_kill_switch:
            # The account is only flat between legs, so a halt that leaves an open leg
            # running leaves a position with nothing watching it — and under this design its
            # exit would simply open the next one. Close it instead.
            return [StopExecutorAction(controller_id=self.config.id, executor_id=executor.id,
                                       keep_position=False)
                    for executor in self.active_executors()]
        # Not active_executors: a leg that is still closing has not given the margin back yet,
        # and the next leg cannot be funded until it has.
        if len(self.unfinished_executors()) > 0:
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
                self._reference_price = Decimal(str(close_price))
            if opened and side is not None and self._bias_side is None:
                self._bias_side = side
                self.logger().info(
                    f"SimpleGrid is {side.name}-or-flat for the rest of the run")
            if opened:
                self._realized_pnl_quote += executor.net_pnl_quote
                self._peak_pnl_quote = max(self._peak_pnl_quote, self._realized_pnl_quote)
                self._legs_closed += 1
                self._last_close_type = executor.close_type
                if executor.close_type == CloseType.TAKE_PROFIT:
                    self._wins += 1
                elif executor.close_type == CloseType.STOP_LOSS:
                    self._losses += 1

            # A timed-out entry does NOT move the reference either — it is covered by the
            # rule above, and deliberately so. The two prices bracket the market by a step
            # either side, so a price that reaches neither is simply a price still inside the
            # grid; re-placing the same pair is the right answer, and re-anchoring on the mid
            # would walk the grid along with a market that has not actually gone anywhere.
            # A leg that failed before trading tells us the venue is refusing us, not that
            # the market moved. A leg that failed AFTER trading is worse: its position may
            # still be open, and every later leg would be stacked on top of it.
            if opened:
                self._consecutive_failed_legs = 0
            elif executor.close_type == CloseType.FAILED:
                self._consecutive_failed_legs += 1

            # INSUFFICIENT_BALANCE is our own budget check, not the venue's answer — no order
            # was ever sent. It reads a balance cache that only refreshes every LONG_POLL_
            # INTERVAL while the websocket is up, so a leg refused a second after margin was
            # released says nothing about whether the money is there. Counting those alongside
            # real venue rejections is what halted the 17:46 run on 2026-09-02 with a full
            # wallet. Time tells the two apart where a count cannot: a stale read clears at the
            # next poll, a real shortage does not.
            if executor.close_type == CloseType.INSUFFICIENT_BALANCE:
                if self._insufficient_balance_since is None:
                    self._insufficient_balance_since = self.market_data_provider.time()
            else:
                # Any other outcome means an order did reach the venue, so funding was fine.
                self._insufficient_balance_since = None
            if executor.close_type == CloseType.FAILED and executor.filled_amount_quote > Decimal("0"):
                self._halt_reason = (
                    f"leg {executor.id} failed after trading {executor.filled_amount_quote:.4f} "
                    f"quote — its position may still be open. Check the account and close it by "
                    f"hand before restarting")
                self.logger().error(f"SimpleGrid halted: {self._halt_reason}")

            self._last_close_timestamp = executor.close_timestamp or self.market_data_provider.time()
            self._cooldown_seconds = self._cooldown_for(executor.close_type)
            self._evaluate_halt_conditions()

    # ------------------------------------------------------------------ reconciliation

    def _venue_position(self):
        """What the exchange says we are holding on our pair, or None if flat."""
        try:
            connector = self.market_data_provider.connectors.get(self.config.connector_name)
            if connector is None:
                return None
            for position in connector.account_positions.values():
                if position.trading_pair != self.config.trading_pair:
                    continue
                if position.amount and abs(position.amount) > Decimal("0"):
                    return position
        except Exception:
            # Reconciliation is a safety net; it must never be the thing that breaks a tick.
            return None
        return None

    def _claimed_amount(self) -> Decimal:
        """How much of a position the live legs believe they are holding."""
        total = Decimal("0")
        for executor in self.executors_info:
            if not executor.is_active:
                continue
            filled = executor.custom_info.get("filled_amount") or Decimal("0")
            total += abs(Decimal(str(filled)))
        return total

    def _reconcile_positions(self) -> List[ExecutorAction]:
        """
        Compare the venue's books with our own, and act when they disagree.

        Every other guard trusts our own record of what happened. This one does not — it asks
        the exchange. A position that no leg claims has no take profit, no stop loss and
        nothing that will close it at shutdown, because the stop loss only ever existed inside
        this process. It is the state that costs real money while nobody is looking.
        """
        if not self.config.reconcile_positions:
            return []
        position = self._venue_position()
        if position is None:
            self._seen_flat_since_start = True
            self._orphan_since = None
            return []
        if self._claimed_amount() > Decimal("0"):
            self._orphan_since = None
            return []

        now = self.market_data_provider.time()
        if self._orphan_since is None:
            self._orphan_since = now
            return []
        if now - self._orphan_since < self.config.orphan_grace_seconds:
            return []

        if not self.is_halted:
            self._halt_reason = (
                f"the venue holds {position.amount} {self.config.trading_pair} that no leg "
                f"claims. Nothing is watching that position — its stop loss only ever existed "
                f"in this process")
            self.logger().error(f"SimpleGrid halted: {self._halt_reason}")

        if self._orphan_flatten_sent or not self.config.flatten_orphan_positions:
            return []
        if not self._seen_flat_since_start:
            # It was already there when we started, so it is not ours to close.
            self.logger().error(
                "SimpleGrid will NOT close this position: the account was never seen flat "
                "since start, so it predates this run. Close it by hand if it is yours.")
            self._orphan_flatten_sent = True
            return []

        config = self._build_flatten_config(position)
        if config is None:
            return []
        self._orphan_flatten_sent = True
        self.logger().warning(
            f"SimpleGrid is closing the unclaimed {position.amount} "
            f"{self.config.trading_pair} at {config.price}")
        return [CreateExecutorAction(controller_id=self.config.id, executor_config=config)]

    def _build_flatten_config(self, position) -> Optional[OrderExecutorConfig]:
        """A crossing limit, for the same reason the executor's own urgent exit is one."""
        mid = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        if not mid or mid <= Decimal("0"):
            return None
        long_position = (position.position_side == PositionSide.LONG
                         if position.position_side is not None else position.amount > Decimal("0"))
        side = TradeType.SELL if long_position else TradeType.BUY
        slippage = self._tick_size() * self.config.close_slippage_ticks
        if slippage <= Decimal("0"):
            slippage = mid * Decimal("0.002")
        price = mid - slippage if side == TradeType.SELL else mid + slippage
        return OrderExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=self.config.id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=side,
            amount=abs(position.amount),
            position_action=PositionAction.CLOSE,
            execution_strategy=ExecutionStrategy.LIMIT,
            price=price,
            leverage=self.config.leverage,
        )

    def _tick_size(self) -> Decimal:
        try:
            connector = self.market_data_provider.connectors.get(self.config.connector_name)
            rule = connector.trading_rules[self.config.trading_pair]
            tick = rule.min_price_increment
            return tick if isinstance(tick, Decimal) and tick > Decimal("0") else Decimal("0")
        except Exception:
            return Decimal("0")

    def _cooldown_for(self, close_type: Optional[CloseType]) -> int:
        if close_type == CloseType.STOP_LOSS:
            return self.config.cooldown_after_stop_loss
        if close_type == CloseType.INSUFFICIENT_BALANCE:
            # Retrying this one at tick speed cannot help. The budget check reads a cached
            # wallet, so the answer is identical every tick until the cache is re-read, and
            # the only thing a new leg per second produces is one error line per second —
            # 56 of them on 2026-09-03 before the run was stopped by hand. Waiting gives the
            # refresh time to land and turns the storm into a handful of lines.
            return self.config.retry_after_insufficient_balance
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
        if self._insufficient_balance_since is not None:
            starved = self.market_data_provider.time() - self._insufficient_balance_since
            if starved >= self.config.insufficient_balance_grace_seconds:
                self._halt_reason = (
                    f"no leg has been affordable for {starved:.0f}s. That is longer than a "
                    f"balance refresh, so it is a real shortage rather than a stale cache — "
                    f"check the wallet and whether something is still holding margin")
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

        # The chain hangs off where the previous leg ended. The very first leg has nothing
        # to measure from and rests at the touch instead, so it is sized off the mid.
        reference = self._reference_price
        amount = self.config.order_amount_quote / (reference if reference is not None else mid_price)
        return SimpleGridExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=self.config.id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_mode=self._next_entry_mode(),
            amount=amount,
            entry_reference_price=reference,
            entry_timeout=self.config.entry_timeout,
            cancel_settle_delay=self.config.cancel_settle_delay,
            exit_retry_max_delay=self.config.exit_retry_max_delay,
            collateral_refusal_wait=self.config.collateral_refusal_wait,
            collateral_refusals_before_waiting=self.config.collateral_refusals_before_waiting,
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

        Every leg opens on the same side, because between legs the account is flat: a run
        that opens with a buy is long or flat for its whole life. An opening leg may offer
        both sides and let the market pick, but only until something fills. Spot can only
        ever go long.
        """
        if not self.is_perpetual:
            return SimpleGridEntryMode.LONG_ONLY
        if self._bias_side is not None:
            return (SimpleGridEntryMode.LONG_ONLY if self._bias_side == TradeType.BUY
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

        base = self._reference_price if self._reference_price is not None else mid_price
        reference = f"{base:.6f}"
        if self._reference_price is None:
            reference += " (mid, no leg closed yet)"

        step_tp, step_sl = self.config.take_profit, self.config.stop_loss
        buying = self._next_entry_mode() != SimpleGridEntryMode.SHORT_ONLY

        # What is actually in the book, and what would make it move. Nothing about this is
        # visible from the outside otherwise: one order rests at a price a step away, and the
        # other price is only watched.
        resting = info.get("resting_entries") or {}
        if info.get("stop_loss_triggered"):
            working = ["stop loss triggered — leaving passively at the touch"]
        elif info.get("take_profit_price"):
            working = [f"holding | take profit resting at {Decimal(str(info['take_profit_price'])):.6f}",
                       f"stop watched at {Decimal(str(info['stop_loss_price'])):.6f}"]
        elif resting:
            entry_rest = ", ".join(f"{s.lower()} {Decimal(str(p)):.6f}" for s, p in resting.items())
            trigger = base * (1 + step_sl) if buying else base * (1 - step_sl)
            working = [f"flat | entry resting at {entry_rest}",
                       f"trigger watched at {trigger:.6f}"]
        elif active:
            working = ["flat | entry not placed yet"]
        else:
            working = ["no leg open"]

        side = "BUY" if buying else "SELL"
        if self._bias_side is None and self.is_perpetual \
                and self.config.initial_entry_mode == SimpleGridEntryMode.BOTH_OCO:
            side = "both (opening leg)"
        actual_str = f"{actual:.2%}" if actual is not None else "n/a"
        loss_str = f"{self._realized_pnl_quote:.4f}"
        loss_str += f" / threshold -{limit:.4f}" if limit is not None else " (no threshold set)"

        lines = [
            f"Simple Grid | {self.config.connector_name} | {self.config.trading_pair}",
            f"  Mid: {mid_price:.6f} | Last exit: {reference} | Side: {side}",
            f"  Working: {' | '.join(working)}",
            f"  Step: {step_tp:.4%} up / {step_sl:.4%} down | "
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
