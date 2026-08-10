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

    # Barriers, handed straight to each leg.
    take_profit: Decimal = Field(default=Decimal("0.005"), json_schema_extra={"is_updatable": True})
    stop_loss: Decimal = Field(default=Decimal("0.01"), json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})
    trigger_price_type: PriceType = PriceType.LastTrade

    # Entry behaviour.
    entry_order_type: OrderType = OrderType.LIMIT
    # Set non-zero only to verify order placement on a live exchange without filling:
    # it rests the entry that far away from the touch price. Leave at 0 to trade.
    entry_offset_pct: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    chase_entry: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    entry_repost_threshold: Decimal = Field(default=Decimal("0.0005"), json_schema_extra={"is_updatable": True})
    min_repost_interval: float = Field(default=1.0, json_schema_extra={"is_updatable": True})
    max_entry_reposts: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})
    max_entry_drift: Optional[Decimal] = Field(default=Decimal("0.01"), json_schema_extra={"is_updatable": True})
    entry_timeout: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})

    # Grid behaviour. The first cycle offers both sides and lets the market pick; from then
    # on the side that filled is the side we keep trading.
    initial_entry_mode: SimpleGridEntryMode = SimpleGridEntryMode.BOTH_OCO
    lock_side_after_first_fill: bool = True
    cooldown_after_take_profit: int = Field(default=0, json_schema_extra={"is_updatable": True})
    cooldown_after_stop_loss: int = Field(default=60, json_schema_extra={"is_updatable": True})

    # Risk. Peak-to-trough on realised PnL; whichever limit is set and hit first halts the
    # controller. Nothing is force-closed, the strategy just stops opening new legs.
    max_drawdown_quote: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    max_drawdown_pct: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})

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
        self._halt_reason: Optional[str] = None

    # ------------------------------------------------------------------ state

    @property
    def is_halted(self) -> bool:
        return self._halt_reason is not None

    @property
    def current_drawdown_quote(self) -> Decimal:
        return self._peak_pnl_quote - self._realized_pnl_quote

    @property
    def drawdown_limit_quote(self) -> Optional[Decimal]:
        limits = []
        if self.config.max_drawdown_quote is not None:
            limits.append(self.config.max_drawdown_quote)
        if self.config.max_drawdown_pct is not None:
            limits.append(self.config.total_amount_quote * self.config.max_drawdown_pct)
        return min(limits) if limits else None

    @property
    def break_even_win_rate(self) -> Decimal:
        """
        The share of legs that must be winners just to break even, before fees.

        With a take profit smaller than the stop loss this is above 50%, and it is the
        single number that says whether the configuration can work.
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
            opened = executor.close_type not in (CloseType.EXPIRED, CloseType.INSUFFICIENT_BALANCE,
                                                 CloseType.FAILED)
            if opened and close_price:
                self._anchor_price = Decimal(str(close_price))
            if opened and side is not None and self.config.lock_side_after_first_fill:
                self._locked_side = side

            if opened:
                self._realized_pnl_quote += executor.net_pnl_quote
                self._peak_pnl_quote = max(self._peak_pnl_quote, self._realized_pnl_quote)
                self._legs_closed += 1
                if executor.close_type == CloseType.TAKE_PROFIT:
                    self._wins += 1

            self._last_close_timestamp = executor.close_timestamp or self.market_data_provider.time()
            self._cooldown_seconds = (self.config.cooldown_after_stop_loss
                                      if executor.close_type == CloseType.STOP_LOSS
                                      else self.config.cooldown_after_take_profit)
            self._evaluate_drawdown()

    def _evaluate_drawdown(self):
        limit = self.drawdown_limit_quote
        if limit is not None and self.current_drawdown_quote >= limit:
            self._halt_reason = (f"drawdown {self.current_drawdown_quote:.4f} reached the limit "
                                 f"{limit:.4f} after {self._legs_closed} legs")
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

        amount = self.config.order_amount_quote / mid_price
        return SimpleGridExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=self.config.id,
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            entry_mode=self._next_entry_mode(),
            amount=amount,
            entry_order_type=self.config.entry_order_type,
            entry_offset_pct=self.config.entry_offset_pct,
            chase_entry=self.config.chase_entry,
            entry_repost_threshold=self.config.entry_repost_threshold,
            min_repost_interval=self.config.min_repost_interval,
            max_entry_reposts=self.config.max_entry_reposts,
            max_entry_drift=self.config.max_entry_drift,
            entry_timeout=self.config.entry_timeout,
            trigger_price_type=self.config.trigger_price_type,
            barriers=SimpleGridBarriers(
                take_profit=self.config.take_profit,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
                take_profit_order_type=self.config.entry_order_type,
            ),
            leverage=self.config.leverage,
        )

    def _next_entry_mode(self) -> SimpleGridEntryMode:
        """Both sides until one fills, then whichever side won."""
        if self._locked_side is None:
            return self.config.initial_entry_mode
        return (SimpleGridEntryMode.LONG_ONLY if self._locked_side == TradeType.BUY
                else SimpleGridEntryMode.SHORT_ONLY)

    # ------------------------------------------------------------------ status

    def to_format_status(self) -> List[str]:
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        limit = self.drawdown_limit_quote
        actual = self.actual_win_rate

        anchor = f"{self._anchor_price}" if self._anchor_price is not None else "-"
        side = self._locked_side.name if self._locked_side else "both (unlocked)"
        actual_str = f"{actual:.2%}" if actual is not None else "n/a"
        drawdown_str = f"{self.current_drawdown_quote:.4f}"
        drawdown_str += f" / {limit:.4f}" if limit is not None else " (no limit set)"

        lines = [
            f"Simple Grid | {self.config.connector_name} | {self.config.trading_pair}",
            f"  Mid: {mid_price:.6f} | Anchor: {anchor} | Side: {side}",
            f"  TP: {self.config.take_profit:.4%} | SL: {self.config.stop_loss:.4%} | "
            f"Amount/leg: {self.config.order_amount_quote}",
            f"  Legs closed: {self._legs_closed} | Wins: {self._wins} | "
            f"Realised PnL: {self._realized_pnl_quote:.4f}",
            # Printed side by side deliberately: the configuration only makes money if the
            # actual rate stays above the break-even one.
            f"  Win rate needed: {self.break_even_win_rate:.2%} (before fees) | actual: {actual_str}",
            f"  Drawdown: {drawdown_str}",
        ]
        if self.is_halted:
            lines.append(f"  HALTED — {self._halt_reason}")
        elif len(self.active_executors()) == 0 and not self._cooldown_elapsed():
            remaining = self._cooldown_seconds - (self.market_data_provider.time() - self._last_close_timestamp)
            lines.append(f"  Cooling down: {remaining:.0f}s remaining")
        return lines
