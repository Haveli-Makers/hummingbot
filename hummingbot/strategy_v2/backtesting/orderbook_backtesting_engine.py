from __future__ import annotations

import bisect
import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type

import pandas as pd
from pydantic import ValidationError

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.strategy_v2.backtesting.backtesting_data_provider import BacktestingDataProvider
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig, DCAMode
from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.simple_grid_executor.data_types import SimpleGridExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

_LOG = logging.getLogger(__name__)

_DUST = 1e-12

_RECENT_TERMINATED = 500


class CandlesUnavailableError(NotImplementedError):
    """A controller needs OHLCV bars that could not be obtained from the venue."""


class UnsupportedExecutorError(NotImplementedError):
    """A controller created an executor type this engine cannot simulate."""


def _drive_coroutine(coro, what: str):
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    coro.close()
    raise RuntimeError(
        f"{what} suspended awaiting I/O. This engine replays recorded book and trade data "
        f"and cannot await a network read.")


class _Role(Enum):
    """What an order is for. Every order placed by a simulated executor has exactly one."""
    ENTRY = 1       
    TRIGGER = 2     
    TAKE_PROFIT = 3
    STOP_LOSS = 4   
    CLOSE = 5       


def _is_entry(role: _Role) -> bool:
    return role in (_Role.ENTRY, _Role.TRIGGER)


class _Market:
    """
    The prices a live executor would read, served from the replayed book and trade tape.
    """

    def __init__(self, tick_size: float):
        self.tick = tick_size
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.last_trade: Optional[float] = None
        self.now: float = 0.0

    def update(self, bt) -> None:
        self.now = bt.now
        self.best_bid = bt.best_bid
        self.best_ask = bt.best_ask

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def price(self, price_type: PriceType, fallback: bool = True) -> Optional[float]:
        """
        A price we can compare against, or None.
        """
        direct = {
            PriceType.MidPrice: self.mid,
            PriceType.BestBid: self.best_bid,
            PriceType.BestAsk: self.best_ask,
            PriceType.LastTrade: self.last_trade,
        }.get(price_type)
        if direct is not None and direct > 0:
            return direct
        if fallback and self.mid is not None and self.mid > 0:
            return self.mid
        return None

    def touch(self, side: TradeType) -> Optional[float]:
        """Passive price for a side: the best bid to buy, the best ask to sell."""
        return self.best_bid if side == TradeType.BUY else self.best_ask

    def no_cross_bound(self, side: TradeType) -> Optional[float]:
        opposite = self.best_ask if side == TradeType.BUY else self.best_bid
        if opposite is None or self.tick <= 0:
            return None
        return opposite - self.tick if side == TradeType.BUY else opposite + self.tick

    def quantize(self, price: float) -> float:
        if self.tick <= 0 or price <= 0:
            return price
        return math.floor(price / self.tick + 1e-9) * self.tick


@dataclass
class _Plan:
    side: Optional[TradeType] = None            
    entry_levels: List[Tuple[TradeType, Optional[float], float]] = field(default_factory=list)
    post_only_entries: bool = False

    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    time_limit: Optional[float] = None
    trailing_activation: Optional[float] = None
    trailing_delta: Optional[float] = None

    reference: Optional[float] = None
    rest_at_touch: bool = False
    entry_timeout: Optional[float] = None
    trigger_price_type: PriceType = PriceType.MidPrice

    # Exit behaviour.
    take_profit_crossing: bool = False         
    stop_loss_chase: bool = False
    stop_loss_maker_offset_ticks: int = 1
    stop_loss_requote_pct: float = 0.0
    stop_loss_max_drift_pct: float = 0.0
    close_slippage_ticks: int = 20


class _ExecutorSim:
    """
    A simulated strategy_v2 executor: one leg, from resting entry to flat.
    """

    def __init__(self, config, plan: _Plan, create_ts: float,
                 order_owner: Optional[Dict[int, str]] = None, key: Optional[str] = None):
        self.config = config
        self.plan = plan
        self.create_ts = create_ts

        self._order_owner = order_owner if order_owner is not None else {}
        self._key = key or config.id

        self.orders: Dict[int, _Role] = {}
        self.order_sides: Dict[int, TradeType] = {}
        self._keep_position = False

        self.filled_side: Optional[TradeType] = plan.side
        self.entry_base = 0.0
        self.entry_notional = 0.0
        self.exit_base = 0.0
        self.exit_notional = 0.0
        self.fees = 0.0

        self.status = RunnableStatus.RUNNING
        self.close_type: Optional[CloseType] = None
        self.close_ts: Optional[float] = None

        self._entry_triggered = False
        self._entry_remainder_cancelled = False
        self._open_order_last_update: Optional[float] = None
        self._sl_triggered = False
        self._sl_trigger_price: Optional[float] = None
        self._sl_trigger_reference: Optional[float] = None
        self._sl_chase_price: Optional[float] = None
        self._last_mid: Optional[float] = None
        self._last_close_ts: float = create_ts

    # ------------------------------------------------------------------ state

    @property
    def is_active(self) -> bool:
        return self.status in (RunnableStatus.RUNNING, RunnableStatus.NOT_STARTED)

    @property
    def is_done(self) -> bool:
        return self.status == RunnableStatus.TERMINATED

    @property
    def side(self) -> Optional[TradeType]:
        return self.filled_side

    @property
    def amount_to_close(self) -> float:
        return self.entry_base - self.exit_base

    @property
    def avg_entry_price(self) -> Optional[float]:
        if self.entry_base <= _DUST:
            return None
        return self.entry_notional / self.entry_base

    @property
    def avg_exit_price(self) -> Optional[float]:
        if self.exit_base <= _DUST:
            return None
        return self.exit_notional / self.exit_base

    @property
    def traded(self) -> bool:
        return self.entry_base > _DUST

    def _sign(self) -> int:
        return 1 if self.filled_side == TradeType.BUY else -1

    def net_pnl_quote(self, mid: Optional[float]) -> float:
        if self.entry_base <= _DUST:
            return -self.fees
        sign = self._sign()
        remaining = self.amount_to_close
        mark = mid if mid is not None else self._last_mid
        held = remaining * mark if mark is not None and abs(remaining) > _DUST else 0.0
        return sign * (self.exit_notional - self.entry_notional + held) - self.fees

    def close_price(self, mid: Optional[float]) -> Optional[float]:
        """
        Where the leg actually closed — the volume weighted average of its exit fills.
        """
        exit_vwap = self.avg_exit_price
        if exit_vwap is not None:
            return exit_vwap
        return mid if mid is not None else self._last_mid

    @property
    def take_profit_price(self) -> Optional[float]:
        entry = self.avg_entry_price
        if entry is None or self.plan.take_profit is None or self.filled_side is None:
            return None
        step = self.plan.take_profit
        return entry * (1 + step) if self.filled_side == TradeType.BUY else entry * (1 - step)

    @property
    def stop_loss_price(self) -> Optional[float]:
        entry = self.avg_entry_price
        if entry is None or self.plan.stop_loss is None or self.filled_side is None:
            return None
        step = self.plan.stop_loss
        return entry * (1 - step) if self.filled_side == TradeType.BUY else entry * (1 + step)

    def _entry_rest_price(self, side: TradeType, market: _Market) -> Optional[float]:
        """One step from the reference in our favour, or the touch when there is no reference."""
        if self.plan.rest_at_touch or self.plan.reference is None:
            touch = market.touch(side)
            return market.quantize(touch) if touch else None
        step = self.plan.take_profit or 0.0
        ref = self.plan.reference
        price = ref * (1 - step) if side == TradeType.BUY else ref * (1 + step)
        return market.quantize(price)

    def _entry_trigger_price(self, side: TradeType, market: _Market) -> Optional[float]:
        """Where we stop waiting and open on the wrong side of the reference."""
        if self.plan.reference is None or self.plan.stop_loss is None:
            return None
        step = self.plan.stop_loss
        ref = self.plan.reference
        price = ref * (1 + step) if side == TradeType.BUY else ref * (1 - step)
        return market.quantize(price)

    def _trigger_reference(self, market: _Market) -> Optional[float]:
        """
        The price the triggers and the stop loss are measured against.
        """
        price_type = self.plan.trigger_price_type
        if price_type in (PriceType.BestBid, PriceType.BestAsk):
            price_type = PriceType.BestBid if self.filled_side == TradeType.BUY else PriceType.BestAsk
        return market.price(price_type)

    def _maker_exit_price(self, side: TradeType, market: _Market) -> Optional[float]:
        bound = market.no_cross_bound(side)
        if bound is None:
            touch = market.touch(side)
            return market.quantize(touch) if touch else None
        extra = market.tick * (max(1, self.plan.stop_loss_maker_offset_ticks) - 1)
        price = bound - extra if side == TradeType.BUY else bound + extra
        return market.quantize(price) if price > 0 else None

    def _crossing_price(self, side: TradeType, market: _Market) -> Optional[float]:
        """A limit priced far enough through the book that it fills like a market order."""
        reference = market.price(PriceType.BestBid if side == TradeType.SELL else PriceType.BestAsk)
        if reference is None:
            reference = self.plan.reference or self.avg_entry_price or self._last_mid
        if reference is None:
            return None
        slippage = market.tick * self.plan.close_slippage_ticks
        if slippage <= 0:
            slippage = reference * 0.002
        price = reference - slippage if side == TradeType.SELL else reference + slippage
        return market.quantize(price if price > 0 else reference)

    @property
    def _exit_side(self) -> Optional[TradeType]:
        if self.filled_side is None:
            return None
        return TradeType.SELL if self.filled_side == TradeType.BUY else TradeType.BUY

    # ------------------------------------------------------------------ orders

    def _live(self, bt, role: Optional[_Role] = None) -> List[int]:
        """
        Order ids still at the venue, optionally filtered by role.

        An order whose cancel is in flight counts as live: it can still fill, so treating
        it as gone would put a second order out for the same position. It drops off this
        list when the cancel actually lands.
        """
        return [oid for oid, r in self.orders.items()
                if oid in bt.orders and (role is None or r == role)]

    def _place(self, bt, side: TradeType, price: float, amount: float, role: _Role,
               post_only: bool = False) -> Optional[int]:
        if amount <= _DUST or price is None or price <= 0:
            return None
        place = bt.buy if side == TradeType.BUY else bt.sell
        oid = place(price, amount, post_only=post_only)
        self.orders[oid] = role
        self.order_sides[oid] = side
        self._order_owner[oid] = self._key
        if _is_entry(role):
            self._open_order_last_update = bt.now
        return oid

    def _cancel(self, bt, oids: List[int]) -> None:
        for oid in oids:
            if oid in bt.orders:
                bt.cancel(oid)

    def forget(self, oid: int) -> None:
        """A rejected post-only order never rested, so the slot is free to be re-placed."""
        self.orders.pop(oid, None)
        self.order_sides.pop(oid, None)

    # ------------------------------------------------------------------ fills

    def on_fill(self, bt, oid: int, price: float, qty: float, fee: float) -> None:
        role = self.orders.get(oid)
        if role is None:
            return
        self._last_close_ts = bt.now
        self.fees += fee
        if _is_entry(role):
            side = self.order_sides[oid]
            if self.filled_side is None:
                self.filled_side = side
            elif side != self.filled_side:
                _LOG.error("executor %s filled %s after %s: OCO cancel did not land",
                           self.config.id, side.name, self.filled_side.name)
            self.entry_base += qty
            self.entry_notional += price * qty
            self._on_entry_filled(bt)
        else:
            self.exit_base += qty
            self.exit_notional += price * qty
            if self.amount_to_close <= _DUST:
                if self.close_type is None:
                    self.close_type = (CloseType.TAKE_PROFIT if role == _Role.TAKE_PROFIT
                                       else CloseType.STOP_LOSS)
                self._terminate(bt)

    def _on_entry_filled(self, bt) -> None:
        """
        First entry fill locks the direction and clears the book of every other opening order.
        """
        if self._entry_remainder_cancelled:
            return
        cancel_remainder = getattr(self.config, "cancel_remainder_on_partial_fill", True)
        entries = [oid for oid, r in self.orders.items() if _is_entry(r)]
        if not cancel_remainder:
            entries = [oid for oid in entries if self.order_sides[oid] != self.filled_side]
        self._cancel(bt, entries)
        self._entry_remainder_cancelled = True

    # ------------------------------------------------------------------ control loop

    def control(self, bt, market: _Market) -> None:
        """One tick of the executor's own control loop."""
        if market.mid is not None:
            self._last_mid = market.mid
        self._last_close_ts = market.now
        if self.status == RunnableStatus.SHUTTING_DOWN:
            self._control_closing(bt, market)
            return
        if self.status != RunnableStatus.RUNNING:
            return
        if not self.traded:
            self._control_entry(bt, market)
        else:
            self._control_barriers(bt, market)

    def _control_closing(self, bt, market: _Market) -> None:
        """
        Carry a committed exit through to flat.
        """
        if self._keep_position or self.amount_to_close <= _DUST:
            self._terminate(bt)
            return
        if self._live(bt):
            return
        price = self._crossing_price(self._exit_side, market)
        if self._place(bt, self._exit_side, price, self.amount_to_close, _Role.CLOSE) is None:
            _LOG.warning("executor %s cannot price its exit at t=%s", self.config.id, market.now)

    def _control_entry(self, bt, market: _Market) -> None:
        """SimpleGridExecutor's opening: resting entries, the timeout, and the trigger."""
        if self.plan.entry_timeout is not None and \
                market.now - self.create_ts >= self.plan.entry_timeout:
            self._cancel(bt, list(self.orders))
            self.close_type = CloseType.EXPIRED
            self.close_ts = market.now
            self.status = RunnableStatus.SHUTTING_DOWN
            self._control_closing(bt, market)
            return

        for side, level_price, amount in self.plan.entry_levels:
            if not self._entry_triggered:
                trigger = self._entry_trigger_price(side, market)
                reference = self._trigger_reference(market)
                if trigger is not None and reference is not None:
                    reached = reference >= trigger if side == TradeType.BUY else reference <= trigger
                    if reached:
                        self._entry_triggered = True
                        self._cancel(bt, self._live(bt, _Role.ENTRY))
                        price = self._crossing_price(side, market)
                        self._place(bt, side, price, amount, _Role.TRIGGER)
                        return
            if any(self.order_sides[oid] == side and _is_entry(self.orders[oid])
                   for oid in self._live(bt)):
                continue      
            if self._entry_triggered:
                continue    
            price = level_price if level_price is not None else self._entry_rest_price(side, market)
            if price is None:
                continue     
            self._place(bt, side, price, amount, _Role.ENTRY,
                        post_only=self.plan.post_only_entries)

    def _control_barriers(self, bt, market: _Market) -> None:
        exit_side = self._exit_side
        if exit_side is None or self.amount_to_close <= _DUST:
            return

        if self.plan.time_limit is not None and market.now - self.create_ts >= self.plan.time_limit:
            self._close_crossing(bt, market, CloseType.TIME_LIMIT)
            return

        if self._sl_triggered:
            self._control_stop_loss_chase(bt, market)
            return

        reference = self._trigger_reference(market)
        stop_price = self.stop_loss_price
        if stop_price is not None and reference is not None:
            breached = (reference <= stop_price if self.filled_side == TradeType.BUY
                        else reference >= stop_price)
            if breached:
                self._sl_triggered = True
                self._sl_trigger_price = stop_price
                self._sl_trigger_reference = reference
                self._cancel(bt, self._live(bt, _Role.TAKE_PROFIT))
                if not self.plan.stop_loss_chase:
                    self._close_crossing(bt, market, CloseType.STOP_LOSS)
                else:
                    self._control_stop_loss_chase(bt, market)
                return

        self._control_take_profit(bt, market, exit_side)

    def _control_take_profit(self, bt, market: _Market, exit_side: TradeType) -> None:
        target = self.take_profit_price
        if target is None:
            return

        if self.plan.take_profit_crossing:
            reference = self._trigger_reference(market)
            if reference is None:
                return
            reached = (reference >= target if self.filled_side == TradeType.BUY
                       else reference <= target)
            if reached:
                self._close_crossing(bt, market, CloseType.TAKE_PROFIT)
            return

        live = self._live(bt, _Role.TAKE_PROFIT)
        if not live:
            self._place(bt, exit_side, target, self.amount_to_close, _Role.TAKE_PROFIT)
            return
        resting = sum(bt.orders[oid].remaining for oid in live)
        if abs(resting - self.amount_to_close) > _DUST:
            self._cancel(bt, live)

    def _control_stop_loss_chase(self, bt, market: _Market) -> None:
        """
        Follow the touch out of the position, and cross if it runs too far.
        """
        exit_side = self._exit_side
        if exit_side is None or self.amount_to_close <= _DUST:
            return
        reference = self._trigger_reference(market)
        anchor = self._sl_trigger_reference or self._sl_trigger_price
        if reference is not None and anchor:
            if self.filled_side == TradeType.BUY:
                drift = (anchor - reference) / anchor
            else:
                drift = (reference - anchor) / anchor
            if drift >= self.plan.stop_loss_max_drift_pct:
                self._close_crossing(bt, market, CloseType.STOP_LOSS)
                return

        desired = self._maker_exit_price(exit_side, market)
        if desired is None:
            return
        live = self._live(bt, _Role.STOP_LOSS)
        if not live:
            self._place(bt, exit_side, desired, self.amount_to_close, _Role.STOP_LOSS)
            self._sl_chase_price = desired
            return
        resting = self._sl_chase_price
        if resting is None:
            return
        if self.filled_side == TradeType.BUY:
            if desired >= resting:
                return
            moved = resting - desired
        else:
            if desired <= resting:
                return
            moved = desired - resting
        threshold = self.plan.stop_loss_requote_pct * (anchor or resting)
        if moved >= threshold:
            self._cancel(bt, live)
            self._sl_chase_price = None

    def _close_crossing(self, bt, market: _Market, close_type: CloseType) -> None:
        """Cancel everything working and leave by crossing the book."""
        self._cancel(bt, list(self.orders))
        self.close_type = close_type
        self.close_ts = market.now
        self.status = RunnableStatus.SHUTTING_DOWN
        exit_side = self._exit_side
        if exit_side is None or self.amount_to_close <= _DUST:
            self._terminate(bt)
            return
        price = self._crossing_price(exit_side, market)
        if self._place(bt, exit_side, price, self.amount_to_close, _Role.CLOSE) is None:
            self._terminate(bt)

    def early_stop(self, bt, market: _Market, keep_position: bool) -> None:
        if self.status != RunnableStatus.RUNNING:
            return
        if keep_position:
            self._keep_position = True
            self._cancel(bt, list(self.orders))
            self.close_type = CloseType.POSITION_HOLD if self.traded else CloseType.EXPIRED
            self.close_ts = market.now
            self.status = RunnableStatus.SHUTTING_DOWN
            self._control_closing(bt, market)
            return
        if not self.traded or self.amount_to_close <= _DUST:
            self._cancel(bt, list(self.orders))
            self.close_type = CloseType.EARLY_STOP if self.traded else CloseType.EXPIRED
            self.close_ts = market.now
            self.status = RunnableStatus.SHUTTING_DOWN
            self._control_closing(bt, market)
            return
        self._close_crossing(bt, market, CloseType.EARLY_STOP)

    def _terminate(self, bt) -> None:
        self._cancel(bt, list(self.orders))
        if self.close_type is None:
            self.close_type = CloseType.EXPIRED if not self.traded else CloseType.COMPLETED
        if self.close_ts is None:
            self.close_ts = self._last_close_ts
        self.status = RunnableStatus.TERMINATED

    def force_close(self, bt, market: _Market) -> None:
        """
        End of data: stop the leg where it stands.
        """
        self._cancel(bt, list(self.orders))
        if self.close_type is None:
            self.close_type = CloseType.TIME_LIMIT if self.traded else CloseType.EXPIRED
        self.close_ts = self.close_ts or market.now
        self.status = RunnableStatus.TERMINATED

    # ------------------------------------------------------------------ reporting

    def to_executor_info(self, market: _Market) -> ExecutorInfo:
        mid = market.mid if market.mid is not None else self._last_mid
        net_pnl = self.net_pnl_quote(mid)
        entry_quote = self.entry_notional
        filled_quote = self.entry_notional + self.exit_notional
        pnl_pct = net_pnl / entry_quote if entry_quote > 0 else 0.0
        return ExecutorInfo(
            id=self.config.id,
            timestamp=self.create_ts,
            type=self.config.type,
            close_timestamp=self.close_ts,
            close_type=self.close_type,
            status=self.status,
            config=self.config,
            net_pnl_pct=Decimal(str(pnl_pct)),
            net_pnl_quote=Decimal(str(net_pnl)),
            cum_fees_quote=Decimal(str(self.fees)),
            filled_amount_quote=Decimal(str(filled_quote)),
            is_active=self.is_active,
            is_trading=self.traded and self.amount_to_close > _DUST,
            custom_info={
                "close_price": self.close_price(mid),
                "level_id": getattr(self.config, "level_id", "") or "",
                "side": self.filled_side,
                "filled_amount": Decimal(str(self.amount_to_close)),
                "current_position_average_price": self.avg_entry_price,
                "take_profit_price": self.take_profit_price,
                "stop_loss_price": self.stop_loss_price,
                "stop_loss_triggered": self._sl_triggered,
                "entry_triggered": self._entry_triggered,
                "open_filled_amount": Decimal(str(self.entry_base)),
                "close_filled_amount": Decimal(str(self.exit_base)),
                "open_order_last_update": self._open_order_last_update,
                "order_ids": [str(oid) for oid in self.orders],
                "current_retries": 0,
                "max_retries": 0,
                "held_position_orders": self._held_position_orders(),
            },
        )

    def _held_position_orders(self) -> List[dict]:
        """
        What this leg leaves on the book, in the shape ``PositionHold`` reads.

        One record per direction rather than one per fill: only the totals matter there.
        """
        if self.close_type != CloseType.POSITION_HOLD or not self.traded or self.filled_side is None:
            return []
        orders = [{"client_order_id": f"{self.config.id}-open", "trade_type": self.filled_side.name,
                   "executed_amount_base": self.entry_base, "executed_amount_quote": self.entry_notional,
                   "cumulative_fee_paid_quote": self.fees}]
        if self.exit_base > _DUST:
            orders.append({"client_order_id": f"{self.config.id}-close", "trade_type": self._exit_side.name,
                           "executed_amount_base": self.exit_base, "executed_amount_quote": self.exit_notional,
                           "cumulative_fee_paid_quote": 0.0})
        return orders


class _LiveBarrierSim(_ExecutorSim):
    def __init__(self, config, plan: _Plan, create_ts: float,
                 order_owner: Optional[Dict[int, str]] = None, key: Optional[str] = None):
        super().__init__(config, plan, create_ts, order_owner=order_owner, key=key)
        self._trailing_trigger: Optional[float] = None

    @property
    def is_active(self) -> bool:
        return self.status in (RunnableStatus.RUNNING, RunnableStatus.NOT_STARTED,
                               RunnableStatus.SHUTTING_DOWN)

    # ------------------------------------------------------------------ control

    def control(self, bt, market: _Market) -> None:
        if market.mid is not None:
            self._last_mid = market.mid
        self._last_close_ts = market.now
        if self.status == RunnableStatus.SHUTTING_DOWN:
            self._control_closing(bt, market)
        elif self.status == RunnableStatus.RUNNING:
            self._control_open(bt, market)
            if self.status == RunnableStatus.RUNNING:
                self._control_barriers(bt, market)

    def _control_open(self, bt, market: _Market) -> None:
        raise NotImplementedError

    def _net_pnl_pct(self, market: _Market) -> Optional[float]:
        """The executor's ``get_net_pnl_pct``: fees included, valued at the exit-side touch."""
        entry = self.avg_entry_price
        close = market.touch(self.filled_side)
        if entry is None or close is None:
            return None
        trade = self._sign() * (close - entry) * self.entry_base
        return (trade - self.fees) / (self.entry_base * entry)

    def _trailing_stop_hit(self, net_pct: float) -> bool:
        if self.plan.trailing_activation is None or self.plan.trailing_delta is None:
            return False
        delta = self.plan.trailing_delta
        if not self._trailing_trigger:
            if net_pct > self.plan.trailing_activation:
                self._trailing_trigger = net_pct - delta
            return False
        if net_pct < self._trailing_trigger:
            return True
        if net_pct - delta > self._trailing_trigger:
            self._trailing_trigger = net_pct - delta
        return False

    def _time_limit_reached(self, market: _Market) -> bool:
        return self.plan.time_limit is not None and market.now - self.create_ts >= self.plan.time_limit

    def _requote_crossing(self, bt, market: _Market, oids: List[int]) -> None:
        for oid in oids:
            order = bt.orders[oid]
            if not order.arrived:
                continue
            side = self.order_sides[oid]
            opposite = market.best_ask if side == TradeType.BUY else market.best_bid
            if opposite is None:
                continue
            crosses = order.price >= opposite if side == TradeType.BUY else order.price <= opposite
            if not crosses:
                bt.cancel(oid)

    # ------------------------------------------------------------------ closing

    def _control_closing(self, bt, market: _Market) -> None:
        live = self._live(bt)
        closes = [oid for oid in live if self.orders[oid] == _Role.CLOSE]
        others = [oid for oid in live if self.orders[oid] != _Role.CLOSE]
        self._cancel(bt, others)
        if self._keep_position or self.amount_to_close <= _DUST:
            self._cancel(bt, closes)
            if not live:
                self._finish(bt)
            return
        self._requote_crossing(bt, market, closes)
        if not closes:
            price = self._crossing_price(self._exit_side, market)
            if self._place(bt, self._exit_side, price, self.amount_to_close, _Role.CLOSE) is None:
                _LOG.warning("executor %s cannot price its exit at t=%s", self.config.id, market.now)

    def _finish(self, bt) -> None:
        if self.close_type == CloseType.POSITION_HOLD and not self.traded:
            self.close_type = CloseType.EARLY_STOP
        self._terminate(bt)

    def _close_crossing(self, bt, market: _Market, close_type: CloseType) -> None:
        self.close_type = close_type
        self.close_ts = market.now
        self.status = RunnableStatus.SHUTTING_DOWN
        self._control_closing(bt, market)

    def early_stop(self, bt, market: _Market, keep_position: bool) -> None:
        if self.status != RunnableStatus.RUNNING:
            return
        if keep_position:
            self._keep_position = True
            self.close_type = CloseType.POSITION_HOLD
            self.close_ts = market.now
            self.status = RunnableStatus.SHUTTING_DOWN
            self._control_closing(bt, market)
        else:
            self._close_crossing(bt, market, CloseType.EARLY_STOP)

    # ------------------------------------------------------------------ fills

    def on_fill(self, bt, oid: int, price: float, qty: float, fee: float) -> None:
        role = self.orders.get(oid)
        if role is None:
            return
        self._last_close_ts = bt.now
        self.fees += fee
        if _is_entry(role):
            self.entry_base += qty
            self.entry_notional += price * qty
            self._open_order_last_update = bt.now
            return
        self.exit_base += qty
        self.exit_notional += price * qty
        if self.amount_to_close <= _DUST and self.status == RunnableStatus.RUNNING:
            self.close_type = CloseType.TAKE_PROFIT if role == _Role.TAKE_PROFIT else CloseType.COMPLETED
            self.close_ts = bt.now
            self.status = RunnableStatus.SHUTTING_DOWN

    # ------------------------------------------------------------------ reporting

    def to_executor_info(self, market: _Market) -> ExecutorInfo:
        info = super().to_executor_info(market)
        info.is_trading = self.status == RunnableStatus.RUNNING and self.traded
        return info


class _PositionSim(_LiveBarrierSim):
    """
    A simulated ``PositionExecutor``.
    """

    @property
    def _tbc(self):
        return self.config.triple_barrier_config

    @property
    def _amount(self) -> float:
        return float(self.config.amount)

    def _within_activation_bounds(self, price: Optional[float], side: TradeType, order_type: OrderType,
                                  market: _Market) -> bool:
        bounds = self.config.activation_bounds
        mid = market.mid
        if not bounds or mid is None or price is None:
            return True
        low = float(bounds[0])
        if order_type.is_limit_type():
            return price >= mid * (1 - low) if side == TradeType.BUY else price <= mid * (1 + low)
        high = float(bounds[1])
        if side == TradeType.BUY:
            return price * (1 - low) <= mid <= price * (1 + high)
        return price * (1 - high) <= mid <= price * (1 + low)

    def _limit_entry_price(self, market: _Market) -> Optional[float]:
        side = self.filled_side
        touch = market.touch(side)
        configured = float(self.config.entry_price) if self.config.entry_price else None
        if self._tbc.open_order_type == OrderType.LIMIT_MAKER and configured is not None and touch is not None:
            configured = min(configured, touch) if side == TradeType.BUY else max(configured, touch)
        price = configured if configured is not None else touch
        return market.quantize(price) if price else None

    def _control_open(self, bt, market: _Market) -> None:
        remaining = self._amount - self.entry_base
        if remaining <= _DUST:
            return
        order_type = self._tbc.open_order_type
        side = self.filled_side
        configured = float(self.config.entry_price) if self.config.entry_price else None
        within = self._within_activation_bounds(configured, side, order_type, market)
        live = [oid for oid in self._live(bt) if _is_entry(self.orders[oid])]
        if live:
            if not order_type.is_limit_type():
                self._requote_crossing(bt, market, live)
            elif not within:
                self._cancel(bt, live)
            return
        if not within:
            return
        if order_type.is_limit_type():
            price = self._limit_entry_price(market)
        else:
            price = self._crossing_price(side, market)
        self._place(bt, side, price, remaining, _Role.ENTRY,
                    post_only=order_type == OrderType.LIMIT_MAKER)

    def _control_barriers(self, bt, market: _Market) -> None:
        if self.entry_base >= self._amount * (1 - 1e-9):
            net_pct = self._net_pnl_pct(market)
            if net_pct is not None:
                if self.plan.stop_loss and net_pct <= -self.plan.stop_loss:
                    self._close_crossing(bt, market, CloseType.STOP_LOSS)
                    return
                if self._trailing_stop_hit(net_pct):
                    self._close_crossing(bt, market, CloseType.TRAILING_STOP)
                    return
                if self._control_position_take_profit(bt, market, net_pct):
                    return
        if self._time_limit_reached(market):
            self._close_crossing(bt, market, CloseType.TIME_LIMIT)

    def _control_position_take_profit(self, bt, market: _Market, net_pct: float) -> bool:
        if not self.plan.take_profit:
            return False
        order_type = self._tbc.take_profit_order_type
        if not order_type.is_limit_type():
            if net_pct >= self.plan.take_profit:
                self._close_crossing(bt, market, CloseType.TAKE_PROFIT)
                return True
            return False
        exit_side = self._exit_side
        target = self.take_profit_price
        if order_type == OrderType.LIMIT_MAKER:
            touch = market.touch(exit_side)
            if touch is not None:
                target = max(target, touch) if exit_side == TradeType.SELL else min(target, touch)
        within = self._within_activation_bounds(target, exit_side, order_type, market)
        live = self._live(bt, _Role.TAKE_PROFIT)
        if not live:
            if within and self.amount_to_close > _DUST:
                self._place(bt, exit_side, market.quantize(target), self.amount_to_close, _Role.TAKE_PROFIT,
                            post_only=order_type == OrderType.LIMIT_MAKER)
            return False
        resting = sum(bt.orders[oid].remaining for oid in live)
        if not within or abs(resting - self.amount_to_close) > _DUST:
            self._cancel(bt, live)
        return False


class _DCASim(_LiveBarrierSim):
    """
    A simulated ``DCAExecutor``.
    """

    def __init__(self, config, plan: _Plan, create_ts: float,
                 order_owner: Optional[Dict[int, str]] = None, key: Optional[str] = None):
        super().__init__(config, plan, create_ts, order_owner=order_owner, key=key)
        self._next_level = 0
        bounds = config.activation_bounds
        if config.mode == DCAMode.TAKER and not bounds:
            bounds = [Decimal("0.0001"), Decimal("0.005")]      
        self._bounds = [float(bound) for bound in bounds] if bounds else None
        self._max_amount_quote = sum(float(amount) for amount in config.amounts_quote)

    def _level_within_bounds(self, price: float, mid: float) -> bool:
        bounds = self._bounds
        side = self.filled_side
        if self.config.mode == DCAMode.MAKER:
            if not bounds:
                return True
            return price > mid * (1 - bounds[0]) if side == TradeType.BUY else price < mid * (1 + bounds[0])
        if side == TradeType.BUY:
            return price * (1 - bounds[0]) < mid < price * (1 + bounds[1])
        return price * (1 - bounds[1]) < mid < price * (1 + bounds[0])

    def _control_open(self, bt, market: _Market) -> None:
        levels = self.plan.entry_levels
        if self._next_level >= len(levels) or market.mid is None or self._time_limit_reached(market):
            return
        side, price, amount = levels[self._next_level]
        if not self._level_within_bounds(price, market.mid):
            return
        order_price = market.quantize(price) if self.config.mode == DCAMode.MAKER else self._crossing_price(side, market)
        if self._place(bt, side, order_price, amount, _Role.ENTRY) is not None:
            self._next_level += 1

    def _all_levels_executed(self, bt) -> bool:
        return self._next_level >= len(self.plan.entry_levels) and not any(
            _is_entry(role) and oid in bt.orders for oid, role in self.orders.items())

    def _control_barriers(self, bt, market: _Market) -> None:
        net_pct = self._net_pnl_pct(market) if self.traded else None
        if net_pct is not None:
            if self.plan.stop_loss:
                if self.config.mode == DCAMode.MAKER:
                    hit = self._all_levels_executed(bt) and net_pct <= -self.plan.stop_loss
                else:
                    net_quote = net_pct * self.entry_base * self.avg_entry_price
                    hit = net_quote <= -self._max_amount_quote * self.plan.stop_loss
                if hit:
                    self._close_crossing(bt, market, CloseType.STOP_LOSS)
                    return
            if self._trailing_stop_hit(net_pct):
                self._close_crossing(bt, market, CloseType.TRAILING_STOP)
                return
            if self.plan.take_profit and net_pct > self.plan.take_profit:
                self._close_crossing(bt, market, CloseType.TAKE_PROFIT)
                return
        if self._time_limit_reached(market):
            self._close_crossing(bt, market, CloseType.TIME_LIMIT)

    def to_executor_info(self, market: _Market) -> ExecutorInfo:
        info = super().to_executor_info(market)
        prices = [level[1] for level in self.plan.entry_levels]
        info.custom_info.update({
            "n_levels": len(prices),
            "filled_amount_quote": self.entry_notional + self.exit_notional,
            "max_amount_quote": self._max_amount_quote,
            "min_price": min(prices) if prices else None,
            "max_price": max(prices) if prices else None,
            "trailing_stop_trigger_pct": self._trailing_trigger,
        })
        return info


class _OrderSim(_LiveBarrierSim):
    """
    A simulated ``OrderExecutor``: place one order, and when it has filled hand the position to
    the controller as ``POSITION_HOLD``. It has no barriers, so nothing here ever closes it.
    """

    def __init__(self, config, plan: _Plan, create_ts: float,
                 order_owner: Optional[Dict[int, str]] = None, key: Optional[str] = None):
        super().__init__(config, plan, create_ts, order_owner=order_owner, key=key)
        self._amount = float(config.amount)

    def control(self, bt, market: _Market) -> None:
        if self._amount <= _DUST and self.status == RunnableStatus.RUNNING:
            self.close_type = CloseType.EXPIRED     
            self._terminate(bt)
            return
        super().control(bt, market)

    def _order_price(self, market: _Market) -> Tuple[Optional[float], bool]:
        strategy = self.config.execution_strategy
        side = self.config.side
        touch = market.touch(side)
        if strategy == ExecutionStrategy.MARKET:
            return self._crossing_price(side, market), False
        if strategy == ExecutionStrategy.LIMIT:
            return market.quantize(float(self.config.price)), False
        if touch is None:
            return None, True
        if strategy == ExecutionStrategy.LIMIT_CHASER:
            distance = float(self.config.chaser_config.distance)
            price = touch * (1 - distance) if side == TradeType.BUY else touch * (1 + distance)
        else:
            configured = float(self.config.price)
            price = min(configured, touch) if side == TradeType.BUY else max(configured, touch)
        return market.quantize(price), True

    def _control_open(self, bt, market: _Market) -> None:
        live = self._live(bt)
        if live:
            if self.config.execution_strategy == ExecutionStrategy.MARKET:
                self._requote_crossing(bt, market, live)
            elif self.config.execution_strategy == ExecutionStrategy.LIMIT_CHASER:
                self._chase(bt, market, live)
            return
        remaining = self._amount - self.entry_base
        price, post_only = self._order_price(market)
        self._place(bt, self.config.side, price, remaining, _Role.ENTRY, post_only=post_only)

    def _chase(self, bt, market: _Market, live: List[int]) -> None:
        touch = market.touch(self.config.side)
        if touch is None:
            return
        threshold = touch * float(self.config.chaser_config.refresh_threshold)
        for oid in live:
            gap = (touch - bt.orders[oid].price if self.config.side == TradeType.BUY
                   else bt.orders[oid].price - touch)
            if gap > threshold:
                bt.cancel(oid)

    def _control_barriers(self, bt, market: _Market) -> None:
        pass

    def on_fill(self, bt, oid: int, price: float, qty: float, fee: float) -> None:
        super().on_fill(bt, oid, price, qty, fee)
        if self.entry_base >= self._amount * (1 - 1e-9) and self.status == RunnableStatus.RUNNING:
            self.close_type = CloseType.POSITION_HOLD
            self.close_ts = bt.now
            self._keep_position = True
            self.status = RunnableStatus.SHUTTING_DOWN

    def early_stop(self, bt, market: _Market, keep_position: bool) -> None:
        super().early_stop(bt, market, True)


@dataclass
class _GridLevelSim:
    """One rung of the grid: a resting buy (or sell) and the take profit that unwinds it."""
    id: str
    price: float
    amount_base: float
    take_profit: float
    open_oid: Optional[int] = None
    close_oid: Optional[int] = None
    open_base: float = 0.0         
    open_quote: float = 0.0        

    @property
    def holding(self) -> bool:
        return self.open_base > _DUST

    def reset(self) -> None:
        self.open_oid = None
        self.close_oid = None
        self.open_base = 0.0
        self.open_quote = 0.0


def _build_grid_levels(config: GridExecutorConfig, mid: float, tick: float) -> List[_GridLevelSim]:
    start, end = float(config.start_price), float(config.end_price)
    total = float(config.total_amount_quote)
    if start <= 0 or end <= 0 or total <= 0 or mid <= 0:
        return []
    low, high = min(start, end), max(start, end)
    grid_range = (high - low) / low
    if grid_range <= 0:
        return []

    min_quote = float(config.min_order_amount_quote) * 1.05   # live's safety margin
    min_step = float(config.min_spread_between_orders)
    if tick > 0:
        min_step = max(min_step, tick / mid)

    max_by_amount = int(total / min_quote) if min_quote > 0 else 1
    max_by_step = int(grid_range / min_step) if min_step > 0 else max_by_amount
    n_levels = max(1, min(max_by_amount, max_by_step))
    quote_per_level = total / n_levels

    if n_levels > 1:
        step = grid_range / (n_levels - 1)
        prices = [low + i * (high - low) / (n_levels - 1) for i in range(n_levels)]
    else:
        step = grid_range
        prices = [(low + high) / 2]

    tbc = config.triple_barrier_config
    take_profit = float(tbc.take_profit) if tbc.take_profit else 0.0
    if config.coerce_tp_to_step:
        take_profit = max(step, take_profit)
    if take_profit <= 0:
        _LOG.error("grid %s has no take profit; levels would never close", config.id)
        return []

    amount_base = quote_per_level / mid
    return [_GridLevelSim(id=f"L{i}", price=price, amount_base=amount_base,
                          take_profit=take_profit)
            for i, price in enumerate(prices)]


class _GridSim(_ExecutorSim):
    def __init__(self, config: GridExecutorConfig, plan: _Plan, create_ts: float,
                 order_owner: Optional[Dict[int, str]] = None, key: Optional[str] = None):
        super().__init__(config, plan, create_ts, order_owner=order_owner, key=key)
        self.filled_side = config.side
        self._trailing_trigger: Optional[float] = None
        self._levels: List[_GridLevelSim] = []
        self._level_of: Dict[int, _GridLevelSim] = {}
        self._last_batch_ts: float = -math.inf
        self._levels_failed = False
        self._completed_ids: List[str] = []
        self._filled_orders: List[dict] = []
        self._canceled_orders: List[int] = []

    # ------------------------------------------------------------------ setup

    def _ensure_levels(self, market: _Market) -> bool:
        if self._levels:
            return True
        if self._levels_failed:
            return False
        mid = market.mid
        if mid is None or mid <= 0:
            return False
        self._levels = _build_grid_levels(self.config, mid, market.tick)
        if not self._levels:
            self._levels_failed = True
            return False
        _LOG.info("grid %s built %s levels between %s and %s", self.config.id,
                  len(self._levels), self.config.start_price, self.config.end_price)
        return True

    # ------------------------------------------------------------------ control

    @property
    def is_active(self) -> bool:
        return self.status in (RunnableStatus.RUNNING, RunnableStatus.NOT_STARTED,
                               RunnableStatus.SHUTTING_DOWN)

    def control(self, bt, market: _Market) -> None:
        if market.mid is not None:
            self._last_mid = market.mid
        self._last_close_ts = market.now
        if self.status == RunnableStatus.SHUTTING_DOWN:
            self._control_closing(bt, market)
            return
        if self.status != RunnableStatus.RUNNING:
            return
        if not self._ensure_levels(market):
            return
        if self._check_global_barriers(bt, market):
            return
        self._work_levels(bt, market)

    def _position_pnl_pct(self, mid: float) -> float:
        """The live executor's ``position_pnl_pct``: open inventory against its cost, at the mid."""
        base = sum(level.open_base for level in self._levels)
        if base <= _DUST:
            return 0.0
        breakeven = sum(level.open_quote for level in self._levels) / base
        return self._sign() * (mid - breakeven) / breakeven

    def _trailing_hit(self, pnl_pct: float) -> bool:
        trailing = self.config.triple_barrier_config.trailing_stop
        if not trailing:
            return False
        delta = float(trailing.trailing_delta)
        if not self._trailing_trigger:
            if pnl_pct > float(trailing.activation_price):
                self._trailing_trigger = pnl_pct - delta
            return False
        if pnl_pct < self._trailing_trigger:
            return True
        if pnl_pct - delta > self._trailing_trigger:
            self._trailing_trigger = pnl_pct - delta
        return False

    def _check_global_barriers(self, bt, market: _Market) -> bool:
        """``GridExecutor.control_triple_barrier``, in its order."""
        mid = market.mid
        if mid is None:
            return False
        side = self.config.side
        pnl_pct = self._position_pnl_pct(mid)
        if self.plan.stop_loss and pnl_pct <= -self.plan.stop_loss:
            self._begin_close(bt, market, CloseType.STOP_LOSS)
            return True
        limit = float(self.config.limit_price) if self.config.limit_price else 0.0
        if limit > 0 and (mid <= limit if side == TradeType.BUY else mid >= limit):
            self._begin_close(bt, market, CloseType.POSITION_HOLD if self.config.keep_position
                              else CloseType.STOP_LOSS)
            return True
        if self.plan.time_limit is not None and market.now - self.create_ts >= self.plan.time_limit:
            self._begin_close(bt, market, CloseType.TIME_LIMIT)
            return True
        if self._trailing_hit(pnl_pct):
            self._begin_close(bt, market, CloseType.TRAILING_STOP)
            return True
        if (mid > float(self.config.end_price) if side == TradeType.BUY
                else mid < float(self.config.start_price)):
            self._begin_close(bt, market, CloseType.TAKE_PROFIT)
            return True
        return False

    def _begin_close(self, bt, market: _Market, close_type: CloseType) -> None:
        self._cancel(bt, list(self.orders))
        for level in self._levels:
            level.open_oid = None
            level.close_oid = None
        if self.close_type is None:
            self.close_type = close_type
        self.close_ts = self.close_ts or market.now
        self._keep_position = self.close_type == CloseType.POSITION_HOLD
        if self._keep_position or self.amount_to_close <= _DUST:
            self._terminate(bt)
            return
        self.status = RunnableStatus.SHUTTING_DOWN
        self._control_closing(bt, market)

    def _work_levels(self, bt, market: _Market) -> None:
        for level in self._levels:
            if level.holding:
                self._place_level_close(bt, market, level)

        budget = self._batch_budget(market)
        if budget <= 0:
            return
        open_count = sum(1 for lvl in self._levels
                         if lvl.open_oid is not None and lvl.open_oid in bt.orders)
        placed = 0
        for level in self._levels:
            if open_count >= self.config.max_open_orders or placed >= budget:
                break
            if level.holding or (level.open_oid is not None and level.open_oid in bt.orders):
                continue
            if not self._within_activation_bounds(level, market):
                continue
            if self._place_level_open(bt, market, level) is not None:
                open_count += 1
                placed += 1
        if placed:
            self._last_batch_ts = market.now

    def _batch_budget(self, market: _Market) -> int:
        frequency = float(self.config.order_frequency or 0)
        if frequency > 0 and market.now - self._last_batch_ts < frequency:
            return 0
        per_batch = self.config.max_orders_per_batch
        return int(per_batch) if per_batch else len(self._levels)

    def _within_activation_bounds(self, level: _GridLevelSim, market: _Market) -> bool:
        bounds = self.config.activation_bounds
        mid = market.mid
        if not bounds or mid is None:
            return True
        bound = float(bounds)
        if self.config.side == TradeType.BUY:
            return level.price >= mid * (1 - bound)
        return level.price <= mid * (1 + bound)

    def _level_entry_price(self, level: _GridLevelSim, market: _Market) -> Optional[float]:
        price = level.price
        extra = float(self.config.safe_extra_spread or 0)
        if self.config.side == TradeType.BUY:
            ask = market.best_ask
            if ask is not None and price >= ask:
                price = ask * (1 - extra) if extra > 0 else market.no_cross_bound(TradeType.BUY)
        else:
            bid = market.best_bid
            if bid is not None and price <= bid:
                price = bid * (1 + extra) if extra > 0 else market.no_cross_bound(TradeType.SELL)
        if price is None or price <= 0:
            return None
        return market.quantize(price)

    def _place_level_open(self, bt, market: _Market, level: _GridLevelSim) -> Optional[int]:
        price = self._level_entry_price(level, market)
        if price is None:
            return None
        oid = self._place(bt, self.config.side, price, level.amount_base, _Role.ENTRY)
        if oid is not None:
            level.open_oid = oid
            self._level_of[oid] = level
        return oid

    def _place_level_close(self, bt, market: _Market, level: _GridLevelSim) -> None:
        if level.close_oid is not None and level.close_oid in bt.orders:
            return
        exit_side = TradeType.SELL if self.config.side == TradeType.BUY else TradeType.BUY
        target = (level.price * (1 + level.take_profit) if self.config.side == TradeType.BUY
                  else level.price * (1 - level.take_profit))
        if self.plan.take_profit_crossing:
            mid = market.mid
            reached = mid is not None and (mid >= target if self.config.side == TradeType.BUY
                                           else mid <= target)
            if not reached:
                return
            price = self._crossing_price(exit_side, market)
        else:
            price = market.quantize(target)
        oid = self._place(bt, exit_side, price, level.open_base, _Role.TAKE_PROFIT)
        if oid is not None:
            level.close_oid = oid
            self._level_of[oid] = level

    # ------------------------------------------------------------------ fills

    def on_fill(self, bt, oid: int, price: float, qty: float, fee: float) -> None:
        role = self.orders.get(oid)
        if role is None:
            return
        self._last_close_ts = bt.now
        self.fees += fee
        level = self._level_of.get(oid)
        self._filled_orders.append({"ts": bt.now, "order_id": oid, "price": price, "qty": qty,
                                    "role": role.name, "level_id": level.id if level else None})

        if _is_entry(role):
            self.entry_base += qty
            self.entry_notional += price * qty
            if level is not None:
                level.open_base += qty
                level.open_quote += price * qty
                if oid not in bt.orders:
                    level.open_oid = None
            return

        self.exit_base += qty
        self.exit_notional += price * qty
        if level is not None:
            cost_per_base = level.open_quote / level.open_base if level.open_base > _DUST else 0.0
            level.open_base = max(0.0, level.open_base - qty)
            level.open_quote = level.open_base * cost_per_base
            if oid not in bt.orders:
                level.close_oid = None
            if not level.holding and level.close_oid is None:
                self._completed_ids.append(level.id)
                self._release_level(level)
                level.reset()

        if self.status == RunnableStatus.SHUTTING_DOWN and self.amount_to_close <= _DUST:
            self._terminate(bt)

    def _release_level(self, level: _GridLevelSim) -> None:
        for oid in [oid for oid, lvl in self._level_of.items() if lvl is level]:
            del self._level_of[oid]
            self.orders.pop(oid, None)
            self.order_sides.pop(oid, None)

    def forget(self, oid: int) -> None:
        level = self._level_of.pop(oid, None)
        if level is not None:
            if level.open_oid == oid:
                level.open_oid = None
            if level.close_oid == oid:
                level.close_oid = None
        self._canceled_orders.append(oid)
        super().forget(oid)

    # ------------------------------------------------------------------ reporting

    def _levels_by_state(self) -> Dict[str, List[str]]:
        states: Dict[str, List[str]] = {"NOT_ACTIVE": [], "OPEN_ORDER_PLACED": [],
                                        "OPEN_ORDER_FILLED": [], "CLOSE_ORDER_PLACED": [],
                                        "COMPLETE": list(self._completed_ids)}
        for level in self._levels:
            if level.holding:
                key = "CLOSE_ORDER_PLACED" if level.close_oid is not None else "OPEN_ORDER_FILLED"
            elif level.open_oid is not None:
                key = "OPEN_ORDER_PLACED"
            else:
                key = "NOT_ACTIVE"
            states[key].append(level.id)
        return states

    def to_executor_info(self, market: _Market) -> ExecutorInfo:
        info = super().to_executor_info(market)
        mid = market.mid if market.mid is not None else self._last_mid
        entry = self.avg_entry_price
        sign = self._sign()

        realized = 0.0
        if self.exit_base > _DUST and entry is not None:
            realized = sign * (self.avg_exit_price - entry) * self.exit_base
        holding = self.amount_to_close
        position_pnl = 0.0
        if holding > _DUST and entry is not None and mid is not None:
            position_pnl = sign * (mid - entry) * holding

        buy_quote, sell_quote = ((self.entry_notional, self.exit_notional)
                                 if self.config.side == TradeType.BUY
                                 else (self.exit_notional, self.entry_notional))
        open_liquidity = sum(lvl.amount_base * lvl.price for lvl in self._levels
                             if lvl.open_oid is not None and not lvl.holding)
        close_liquidity = sum(lvl.open_base * lvl.price for lvl in self._levels
                              if lvl.close_oid is not None)

        info.custom_info.update({
            "levels_by_state": self._levels_by_state(),
            "filled_orders": self._filled_orders,
            "failed_orders": [],        # no venue refusals are modelled
            "canceled_orders": self._canceled_orders,
            "realized_buy_size_quote": buy_quote,
            "realized_sell_size_quote": sell_quote,
            "realized_pnl_quote": realized - self.fees,
            "realized_fees_quote": self.fees,
            "position_pnl_quote": position_pnl,
            "position_size_quote": holding * (mid if mid is not None else (entry or 0.0)),
            "position_size_base": holding,
            "open_liquidity_placed": open_liquidity,
            "close_liquidity_placed": close_liquidity,
            "n_levels": len(self._levels),
            "grid_completions": len(self._completed_ids),
        })
        return info


class _OrderbookDataProvider(BacktestingDataProvider):
    def __init__(self):
        super().__init__(connectors={})
        self._market: Optional[_Market] = None
        self._key: Optional[str] = None
        self.connectors = {}
        self._closes: Dict[str, List[float]] = {}
        self.read_live_state = False
        self.bars_read: Dict[str, int] = {}

    def initialize_rate_sources(self, connector_pairs) -> None:
        """Nothing to do, and the base class would start a task polling live venues for rates."""

    def begin_update(self) -> None:
        self.read_live_state = False
        self.bars_read = {}

    def bars_changed(self) -> bool:
        return any(self._closed_count(key) != count for key, count in self.bars_read.items())

    def bind(self, market: _Market, connector_name: str, trading_pair: str) -> None:
        self._market = market
        self._key = f"{connector_name}_{trading_pair}"
        tick = Decimal(str(market.tick)) if market.tick and market.tick > 0 else None
        granular = Decimal("1e-8")
        rule = TradingRule(
            trading_pair=trading_pair,
            min_price_increment=tick if tick else granular,
            min_base_amount_increment=granular,
            min_quote_amount_increment=granular,
        )
        if not isinstance(getattr(self, "trading_rules", None), dict):
            self.trading_rules = {}
        self.trading_rules.setdefault(connector_name, {})[trading_pair] = rule

    def get_price_by_type(self, connector_name: str, trading_pair: str, price_type: PriceType):
        self.read_live_state = True
        key = f"{connector_name}_{trading_pair}"
        if self._market is None or key != self._key:
            return self.prices.get(key)
        price = self._market.price(price_type)
        return Decimal(str(price)) if price is not None else None

    async def load_candles(self, candles_configs, start_ts: int, end_ts: int) -> None:
        for config in candles_configs:
            key = self._generate_candle_feed_key(config)
            if key in self._closes:
                continue
            what = f"{config.interval} candles for {config.connector} {config.trading_pair}"
            seconds = CandlesBase.interval_to_seconds[config.interval]
            self.update_backtesting_time(start_ts - 5 * seconds, end_ts)
            try:
                frame = await super().get_candles_feed(config)
            except Exception as exc:
                raise CandlesUnavailableError(
                    f"could not fetch {what}: {exc}. This controller decides from those bars and "
                    f"they cannot be substituted, so it was not run. Check that the venue offers "
                    f"this pair and interval (candles_connector / interval in the config).") from exc
            if frame is None or frame.empty:
                raise CandlesUnavailableError(
                    f"the venue returned no {what} between {start_ts} and {end_ts}. This controller "
                    f"decides from those bars and they cannot be substituted, so it was not run.")
            frame = frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            self.candles_feeds[key] = frame
            self._closes[key] = [float(stamp) + seconds for stamp in frame["timestamp"]]

    def _closed_count(self, key: str) -> int:
        return bisect.bisect_right(self._closes[key], self._market.now if self._market else math.inf)

    def get_candles_df(self, connector_name: str, trading_pair: str, interval: str,
                       max_records: int = 500):
        key = f"{connector_name}_{trading_pair}_{interval}"
        frame = self.candles_feeds.get(key)
        if frame is None:
            raise CandlesUnavailableError(
                f"controller asked for {interval} candles on {connector_name} {trading_pair}, which "
                f"it did not declare in candles_config, so they were not fetched before the replay. "
                f"Fetched feeds: {sorted(self.candles_feeds) or 'none'}.")
        count = self._closed_count(key)
        if count == 0:
            raise CandlesUnavailableError(
                f"no {interval} candle for {connector_name} {trading_pair} had closed by "
                f"t={self._market.now}: the venue's bars start at {frame['timestamp'].iloc[0]}. "
                f"The indicators have no history to start from.")
        self.bars_read[key] = count
        return frame.iloc[max(0, count - max_records):count].copy()

    def candles_summary(self) -> List[Dict]:
        """Which bars a result was computed from, so that it can be judged."""
        summary = []
        for key, frame in self.candles_feeds.items():
            connector_pair, interval = key.rsplit("_", 1)
            summary.append({"feed": connector_pair, "interval": interval, "rows": int(len(frame)),
                            "first": float(frame["timestamp"].iloc[0]),
                            "last": float(frame["timestamp"].iloc[-1])})
        return summary

    def time(self):
        self.read_live_state = True
        return self._market.now if self._market is not None else self._time


class _PositionBook:
    def __init__(self, position_mode):
        from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
        self._hold_class = PositionHold
        self._position_mode = position_mode
        self._positions: List = []

    def _position_side(self, info: ExecutorInfo) -> Optional[TradeType]:
        if "_perpetual" not in info.connector_name:
            return None
        action = getattr(info.config, "position_action", None)
        if action is not None and self._position_mode == PositionMode.HEDGE:
            opposite = TradeType.BUY if info.config.side == TradeType.SELL else TradeType.SELL
            return opposite if action == PositionAction.CLOSE else info.config.side
        return info.config.side

    def absorb(self, info: ExecutorInfo) -> None:
        if info.close_type != CloseType.POSITION_HOLD:
            return
        side = self._position_side(info)
        for position in self._positions:
            if side is None or position.side == side:
                position.add_orders_from_executor(info)
                return
        position = self._hold_class(info.connector_name, info.trading_pair, side or info.config.side)
        position.add_orders_from_executor(info)
        self._positions.append(position)

    def summaries(self, mid: Optional[float]) -> List:
        if mid is None:
            return []
        price = Decimal(str(mid))
        return [position.get_position_summary(price) for position in self._positions]


class OrderbookBacktestingEngine:
    """
    Runs any strategy_v2 controller against recorded orderbook + trade data.

    Order matching comes from the event-driven engine in ``backtest_engine.py``; the
    executor behaviour the controller depends on is simulated by ``_ExecutorSim``.
    """

    def __init__(self):
        self.backtesting_data_provider = _OrderbookDataProvider()
        self._controller_class_cache = {}
        self._post_only_entries = False

    @staticmethod
    def _get_controller_class(config: ControllerConfigBase) -> Type[ControllerBase]:
        return config.get_controller_class()

    @staticmethod
    def _with_validated_defaults(config: ControllerConfigBase) -> ControllerConfigBase:
        values = config.model_dump()
        try:
            return type(config).model_validate(values)
        except ValidationError as exc:
            rejected = {error["loc"][0] for error in exc.errors() if error["loc"]}
        return type(config).model_validate({name: value for name, value in values.items()
                                            if name not in rejected})

    @staticmethod
    def _require_replayable(config: ControllerConfigBase, pair: str) -> None:
        """Refuse what a single recorded market cannot answer, before building the controller."""
        if config.controller_name == "ai_livestream":
            raise UnsupportedExecutorError(
                "ai_livestream takes its signals from a live MQTT stream, which is not recorded, "
                "so there is nothing to replay it against.")
        missing = [name for name in ("connector_name", "trading_pair")
                   if getattr(config, name, None) is None]
        if missing:
            raise UnsupportedExecutorError(
                f"{type(config).__name__} does not define {', '.join(missing)}. "
                f"Controllers that trade several markets at once (xemm_multiple_levels, "
                f"arbitrage_controller, stat_arb, quantum_grid_allocator) need a replay of all of "
                f"them, which this engine does not provide: it replays a single connector and pair.")
        if config.trading_pair != pair:
            raise ValueError(
                f"the controller trades {config.trading_pair} but the recording "
                f"requested is {pair}. They must match.")

    async def load_candles(self, controller_config: ControllerConfigBase, connector: str, pair: str,
                           start: str, end: Optional[str] = None, base_dir: Optional[str] = None) -> None:
        self._require_replayable(controller_config, pair)
        config = self._with_validated_defaults(controller_config)
        controller = self._get_controller_class(config)(
            config=config, market_data_provider=self.backtesting_data_provider, actions_queue=None)
        candles_configs = list(getattr(controller.config, "candles_config", None) or [])
        if not candles_configs:
            return
        span = _recording_span(connector, pair, start, end, base_dir)
        if span is not None:
            await self.backtesting_data_provider.load_candles(candles_configs, *span)

    # ------------------------------------------------------------------ run

    async def run_backtesting(
        self,
        controller_config: ControllerConfigBase,
        connector: str,
        pair: str,
        start: str,
        end: Optional[str] = None,
        maker_fee: float = 0.001,
        taker_fee: float = 0.001,
        latency_ms: float = 100,
        fill_at_touch: bool = False,
        update_interval: float = 1.0,
        tick_size: Optional[float] = None,
        post_only_entries: bool = False,
        strict_errors: bool = True,
        base_dir: Optional[str] = None,
    ) -> Dict:
        """
        Replay ``connector``/``pair`` over ``[start, end]`` and run the controller against it.
        """
        from backtest_data import DEFAULT_DIR, NoDataError, require_data
        from backtest_engine import Backtest, Order, Strategy

        base_dir = base_dir or DEFAULT_DIR
        self._require_replayable(controller_config, pair)
        require_data(connector, pair, start, end, base_dir)

        controller_config = self._with_validated_defaults(controller_config)
        controller_class = self._get_controller_class(controller_config)

        if not controller_config.id:
            import uuid
            controller_config.id = str(uuid.uuid4())[:8]

        await self.load_candles(controller_config, connector, pair, start, end, base_dir)

        bt = Backtest(connector, pair, start, end, maker_fee, taker_fee, latency_ms, fill_at_touch, base_dir)

        resolved_tick = tick_size if tick_size and tick_size > 0 else _infer_tick_size(
            connector, pair, start, end, base_dir=base_dir)
        market = _Market(resolved_tick)
        self._post_only_entries = post_only_entries

        data_provider = self.backtesting_data_provider
        data_provider.bind(market, controller_config.connector_name, controller_config.trading_pair)
        controller = controller_class(
            config=controller_config,
            market_data_provider=data_provider,
            actions_queue=None,
        )

        sims: Dict[str, _ExecutorSim] = {}
        order_owner: Dict[int, str] = {}
        finished: Dict[str, _ExecutorSim] = {} 
        retired: List[_ExecutorSim] = []
        all_sims: Dict[str, _ExecutorSim] = {}
        state = {"start_ts": None, "end_ts": None, "books": 0, "ticks": 0, "last_tick": -math.inf,
                 "activity": 0, "update_cacheable": False, "last_update_activity": -1,
                 "recent": deque(maxlen=_RECENT_TERMINATED),
                 "positions": _PositionBook(getattr(controller_config, "position_mode", PositionMode.HEDGE)),
                 "all_sims": all_sims}

        engine = self

        class _ControllerBridge(Strategy):
            """Bridges backtest engine events to the simulated executors and the controller."""

            def on_start(self_, bt_instance):
                state["start_ts"] = bt_instance.now
                _LOG.info("backtest started at %s (tick size %s)", bt_instance.now, resolved_tick)

            def on_book(self_, bt_instance, book):
                state["books"] += 1
                state["end_ts"] = bt_instance.now
                market.update(bt_instance)
                if market.mid is None:
                    return
                engine._step(controller, bt_instance, market, sims, order_owner, finished,
                             retired, controller_config, state, update_interval, strict_errors)

            def on_trade(self_, bt_instance, trade):
                market.update(bt_instance)
                market.last_trade = trade.get("px") or market.last_trade
                state["end_ts"] = bt_instance.now
                if market.mid is None:
                    return
                engine._control_sims(bt_instance, market, sims, finished)

            def on_fill(self_, bt_instance, order: Order, price: float, qty: float, maker: bool):
                owner = order_owner.get(order.id)
                sim = all_sims.get(owner) if owner else None
                if sim is None:
                    _LOG.error("fill on order %s with no owning executor", order.id)
                    return
                fee = price * qty * (bt_instance.maker_fee if maker else bt_instance.taker_fee)
                sim.on_fill(bt_instance, order.id, price, qty, fee)
                state["activity"] += 1

            def on_reject(self_, bt_instance, order: Order):
                owner = order_owner.get(order.id)
                sim = all_sims.get(owner) if owner else None
                if sim is not None:
                    sim.forget(order.id)

            def on_cancel(self_, bt_instance, order: Order):
                owner = order_owner.get(order.id)
                sim = all_sims.get(owner) if owner else None
                if sim is not None:
                    sim.forget(order.id)

        bt.run(_ControllerBridge())

        for sim in finished.values():
            state["positions"].absorb(sim.to_executor_info(market))
        retired.extend(finished.values())
        strategy_view = self._capture_strategy_view(
            controller, [sim.to_executor_info(market) for sim in retired + list(sims.values())],
            state["positions"].summaries(market.mid))
        for sim in list(sims.values()):
            sim.force_close(bt, market)
            retired.append(sim)
        sims.clear()
        finished.clear()

        reported: List[ExecutorInfo] = [sim.to_executor_info(market) for sim in retired]

        if state["books"] == 0:
            range_str = f"{start}" if not end or end == start else f"{start}..{end}"
            raise NoDataError(
                f"Found files for {connector} {pair} but no book updates were replayed over "
                f"{range_str}. The files may be empty or outside this range.")

        _LOG.info("backtest finished: %s book updates, %s controller ticks, %s executors",
                  state["books"], state["ticks"], len(reported))

        reported.sort(key=lambda info: (info.close_timestamp or info.timestamp, info.timestamp))
        results = self._summarize_results(
            reported, float(controller_config.total_amount_quote), bt._equity)
        reconciliation = _reconcile(reported, bt, market)
        results["reconciliation_residual"] = reconciliation["residual"]

        return {
            "executors": reported,
            "results": results,
            "strategy": strategy_view,
            "candles": self.backtesting_data_provider.candles_summary(),
            "fills": bt._fills,
            "equity": bt._equity,
            "fees": bt.fees,
            "final_position": bt.position,
            "reconciliation": reconciliation,
            "tick_size": resolved_tick,
        }

    @staticmethod
    def _capture_strategy_view(controller, executors: List[ExecutorInfo], positions: List) -> Dict:
        """
        The controller's own numbers, reported the way the strategy itself reports them.
        """
        view: Dict = {"status": None, "processed_data": None, "errors": [],
                      "positions": [_jsonable(position.model_dump()) for position in positions]}
        controller.executors_info = executors
        controller.positions_held = positions
        try:
            lines = controller.to_format_status()
            view["status"] = [str(line) for line in (lines or [])]
        except Exception as exc:
            view["errors"].append(f"to_format_status() raised: {exc}")
            _LOG.exception("controller.to_format_status() failed")
        try:
            view["processed_data"] = _jsonable(getattr(controller, "processed_data", None))
        except Exception as exc:
            view["errors"].append(f"processed_data could not be serialised: {exc}")
            _LOG.exception("controller.processed_data could not be serialised")
        return view

    # ------------------------------------------------------------------ per-event work

    def _control_sims(self, bt, market, sims, finished) -> None:
        """Run every live executor's control loop, and retire the ones that have finished."""
        for key, sim in list(sims.items()):
            sim.control(bt, market)
            if sim.is_done:
                finished[key] = sim
                del sims[key]

    def _step(self, controller, bt, market, sims, order_owner, finished, retired,
              config, state, update_interval, strict_errors) -> None:
        self._control_sims(bt, market, sims, finished)

        if market.now - state["last_tick"] < update_interval:
            return
        state["last_tick"] = market.now
        state["ticks"] += 1

        infos = [sim.to_executor_info(market) for sim in sims.values()]
        just_finished = [sim.to_executor_info(market) for sim in finished.values()]
        for info in just_finished:
            state["positions"].absorb(info)
        controller.executors_info = list(state["recent"]) + just_finished + infos
        controller.positions_held = state["positions"].summaries(market.mid)

        try:
            if self._processed_data_is_stale(state):
                provider = self.backtesting_data_provider
                provider.begin_update()
                state["last_update_activity"] = state["activity"]
                _drive_coroutine(controller.update_processed_data(),
                                 f"{type(controller).__name__}.update_processed_data()")
                state["update_cacheable"] = bool(provider.bars_read) and not provider.read_live_state
        except (CandlesUnavailableError, UnsupportedExecutorError):
            raise
        except Exception as exc:
            if strict_errors:
                raise RuntimeError(
                    f"controller {type(controller).__name__}.update_processed_data() raised at "
                    f"t={market.now} ({type(exc).__name__}: {exc}). Its decisions are made from what "
                    f"this writes, so continuing would backtest a controller running on empty "
                    f"state") from exc
            _LOG.exception("update_processed_data failed")
            return

        try:
            actions = controller.determine_executor_actions()
        except Exception as exc:
            if strict_errors:
                raise RuntimeError(
                    f"controller {type(controller).__name__}.determine_executor_actions() raised "
                    f"at t={market.now} ({type(exc).__name__}: {exc}); the backtest would otherwise "
                    f"report a clean zero-PnL result for a broken controller") from exc
            _LOG.exception("determine_executor_actions failed")
            actions = []

        state["recent"].extend(just_finished)
        for key, sim in list(finished.items()):
            retired.append(sim)
            del finished[key]

        for action in actions or []:
            if isinstance(action, CreateExecutorAction):
                state["activity"] += 1
                self._create_sim(bt, market, action.executor_config, sims, order_owner,
                                 state["all_sims"], strict_errors)
            elif isinstance(action, StopExecutorAction):
                sim = sims.get(action.executor_id)
                if sim is not None:
                    state["activity"] += 1
                    sim.early_stop(bt, market, bool(getattr(action, "keep_position", False)))

        self._control_sims(bt, market, sims, finished)

    def _processed_data_is_stale(self, state) -> bool:
        """
        Whether the controller's indicators need recomputing on this tick.
        """
        if not state["update_cacheable"]:
            return True
        return (state["activity"] != state["last_update_activity"]
                or self.backtesting_data_provider.bars_changed())

    def _create_sim(self, bt, market, executor_config, sims, order_owner, all_sims,
                    strict_errors: bool = True) -> None:
        plan = _build_plan(executor_config, market, post_only=self._post_only_entries)
        if plan is None:
            message = (
                f"executor type {type(executor_config).__name__} is not simulated by the "
                f"orderbook engine. Supported: SimpleGridExecutorConfig, GridExecutorConfig, "
                f"PositionExecutorConfig, DCAExecutorConfig, OrderExecutorConfig. XEMM and "
                f"Arbitrage executors quote two venues at once, which this single-market "
                f"replay cannot serve.")
            if strict_errors:
                raise UnsupportedExecutorError(message)
            _LOG.error("%s; ignored", message)
            return
        if executor_config.id in sims:
            _LOG.error("executor id %s is already live; the controller created it twice",
                       executor_config.id)
            return
        sim = _simulator_class(executor_config)(executor_config, plan, market.now,
                                                order_owner=order_owner, key=executor_config.id)
        sims[executor_config.id] = sim
        all_sims[executor_config.id] = sim
        sim.control(bt, market)

    # ------------------------------------------------------------------ summary

    @staticmethod
    def _summarize_results(executors_info: List[ExecutorInfo], total_amount_quote: float = 1000,
                           equity: Optional[List[Tuple]] = None) -> Dict:
        empty = {
            "net_pnl": 0.0, "net_pnl_quote": 0.0, "total_executors": 0,
            "total_executors_with_position": 0, "total_volume": 0.0,
            "total_long": 0, "total_short": 0, "close_types": {},
            "accuracy_long": 0.0, "accuracy_short": 0.0, "total_positions": 0,
            "accuracy": 0.0, "max_drawdown_usd": 0.0, "max_drawdown_pct": 0.0,
            "sharpe_ratio": 0.0, "profit_factor": None, "win_signals": 0, "loss_signals": 0,
        }
        if not executors_info:
            return empty

        import numpy as np

        df = pd.DataFrame([info.to_dict() for info in executors_info])
        df["net_pnl_quote"] = df["net_pnl_quote"].astype(float)             
        df["filled_amount_quote"] = df["filled_amount_quote"].astype(float)

        net_pnl_quote = float(df["net_pnl_quote"].sum()) 
        total_executors = len(df)

        traded = df[df["filled_amount_quote"] > 0]       
        total_with_pos = len(traded)
        total_volume = float(traded["filled_amount_quote"].sum()) if total_with_pos else 0.0

        longs = traded[traded["side"] == TradeType.BUY] if total_with_pos else df.iloc[:0]     # what is this do
        shorts = traded[traded["side"] == TradeType.SELL] if total_with_pos else df.iloc[:0]
        accuracy_long = float((longs["net_pnl_quote"] > 0).mean()) if len(longs) else 0.0   # how is this
        accuracy_short = float((shorts["net_pnl_quote"] > 0).mean()) if len(shorts) else 0.0

        close_types = {}
        if "close_type" in df.columns:
            names = df["close_type"].apply(lambda x: x.name if hasattr(x, "name") else str(x))
            close_types = names.value_counts().to_dict()
            close_types = {str(k): int(v) for k, v in close_types.items()}

        wins = traded[traded["net_pnl_quote"] > 0]
        losses = traded[traded["net_pnl_quote"] < 0]
        accuracy = float(len(wins) / total_with_pos) if total_with_pos else 0.0

        total_won = float(wins["net_pnl_quote"].sum()) if len(wins) else 0.0
        total_loss = float(-losses["net_pnl_quote"].sum()) if len(losses) else 0.0
        profit_factor = (total_won / total_loss) if total_loss > 0 else None

        max_drawdown = 0.0
        sharpe = 0.0
        sharpe_samples = 0
        if equity:
            pnl = pd.Series([point[1] for point in equity], dtype=float)
            ts = pd.Series([point[0] for point in equity], dtype=float)
            max_drawdown = float((pnl - pnl.cummax()).min())
            sharpe, sharpe_samples = _sharpe_from_equity(ts, pnl)
        max_drawdown_pct = max_drawdown / total_amount_quote if total_amount_quote else 0.0

        return {
            "net_pnl": net_pnl_quote / total_amount_quote if total_amount_quote else 0.0,
            "net_pnl_quote": net_pnl_quote,
            "total_executors": total_executors,
            "total_executors_with_position": total_with_pos,
            "total_volume": total_volume,
            "total_long": int(len(longs)),
            "total_short": int(len(shorts)),
            "close_types": close_types,
            "accuracy_long": accuracy_long,
            "accuracy_short": accuracy_short,
            "total_positions": total_with_pos,
            "accuracy": accuracy,
            "max_drawdown_usd": max_drawdown,
            "max_drawdown_pct": float(max_drawdown_pct),
            "sharpe_ratio": sharpe,
            "sharpe_samples": sharpe_samples,
            "profit_factor": profit_factor,
            "win_signals": int(len(wins)),
            "loss_signals": int(len(losses)),
        }


# ---------------------------------------------------------------------------- plans


def _simulator_class(config) -> Type[_ExecutorSim]:
    if isinstance(config, GridExecutorConfig):
        return _GridSim
    if isinstance(config, PositionExecutorConfig):
        return _PositionSim
    if isinstance(config, DCAExecutorConfig):
        return _DCASim
    if isinstance(config, OrderExecutorConfig):
        return _OrderSim
    return _ExecutorSim


def _build_plan(config, market: _Market, post_only: bool = False) -> Optional[_Plan]:
    """Translate an executor config into the behaviour the simulated executor should follow."""
    if isinstance(config, SimpleGridExecutorConfig):
        return _simple_grid_plan(config, post_only)
    if isinstance(config, GridExecutorConfig):
        return _grid_plan(config)
    if isinstance(config, PositionExecutorConfig):
        return _position_plan(config)
    if isinstance(config, DCAExecutorConfig):
        return _dca_plan(config)
    if isinstance(config, OrderExecutorConfig):
        return _order_plan(config)
    return None


def _simple_grid_plan(config: SimpleGridExecutorConfig, post_only: bool) -> _Plan:
    barriers = config.barriers
    reference = float(config.entry_reference_price) if config.entry_reference_price else None
    amount = float(config.amount)
    return _Plan(
        side=None,                                   # OCO: the first fill decides
        entry_levels=[(side, None, amount) for side in config.sides()],
        post_only_entries=post_only,
        take_profit=float(barriers.take_profit),
        stop_loss=float(barriers.stop_loss),
        time_limit=float(barriers.time_limit) if barriers.time_limit else None,
        reference=reference,
        rest_at_touch=reference is None,
        entry_timeout=float(config.entry_timeout) if config.entry_timeout else None,
        trigger_price_type=config.trigger_price_type,
        take_profit_crossing=barriers.take_profit_order_type == OrderType.MARKET,
        stop_loss_chase=barriers.stop_loss_chase,
        stop_loss_maker_offset_ticks=barriers.stop_loss_maker_offset_ticks,
        stop_loss_requote_pct=float(barriers.stop_loss_requote_pct),
        stop_loss_max_drift_pct=float(barriers.stop_loss_max_drift_pct),
        close_slippage_ticks=barriers.close_slippage_ticks,
    )


def _grid_plan(config: GridExecutorConfig) -> _Plan:
    tbc = config.triple_barrier_config
    return _Plan(
        side=config.side,
        take_profit=float(tbc.take_profit) if tbc.take_profit else None,
        stop_loss=float(tbc.stop_loss) if tbc.stop_loss else None,
        time_limit=float(tbc.time_limit) if tbc.time_limit else None,
        take_profit_crossing=tbc.take_profit_order_type == OrderType.MARKET,
    )


def _position_plan(config: PositionExecutorConfig) -> _Plan:
    tbc = config.triple_barrier_config
    trailing = tbc.trailing_stop
    return _Plan(
        side=config.side,
        take_profit=float(tbc.take_profit) if tbc.take_profit else None,
        stop_loss=float(tbc.stop_loss) if tbc.stop_loss else None,
        time_limit=float(tbc.time_limit) if tbc.time_limit else None,
        trailing_activation=float(trailing.activation_price) if trailing else None,
        trailing_delta=float(trailing.trailing_delta) if trailing else None,
    )


def _dca_plan(config: DCAExecutorConfig) -> _Plan:
    levels = []
    for amount_quote, price in zip(config.amounts_quote, config.prices):
        px = float(price)
        if px <= 0:
            continue
        levels.append((config.side, px, float(amount_quote) / px))
    trailing = config.trailing_stop
    return _Plan(
        side=config.side,
        entry_levels=levels,
        take_profit=float(config.take_profit) if config.take_profit else None,
        stop_loss=float(config.stop_loss) if config.stop_loss else None,
        time_limit=float(config.time_limit) if config.time_limit else None,
        trailing_activation=float(trailing.activation_price) if trailing else None,
        trailing_delta=float(trailing.trailing_delta) if trailing else None,
    )


def _order_plan(config: OrderExecutorConfig) -> _Plan:
    """A bare order with no barriers."""
    return _Plan(side=config.side)


# ---------------------------------------------------------------------------- helpers


def _jsonable(value, _depth: int = 0):
    """
    Reduce a controller's ``processed_data`` to something JSON can carry.

    Controllers put Decimals, enums, timestamps and whole DataFrames in there. A frame is
    described rather than serialised: a week of features runs to megabytes, and the caller
    asked for a result, not the dataset behind it.
    """
    if _depth > 4:
        return str(value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        converted = float(value)
        return converted if math.isfinite(converted) else None
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, pd.DataFrame):
        latest = value.iloc[-1].to_dict() if len(value) else {}
        return {"_type": "DataFrame", "rows": int(len(value)),
                "columns": [str(column) for column in value.columns],
                "latest": {str(column): _jsonable(item, _depth + 1) for column, item in latest.items()}}
    if isinstance(value, pd.Series):
        return {"_type": "Series", "rows": int(len(value))}
    if isinstance(value, dict):
        return {str(key): _jsonable(item, _depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item, _depth + 1) for item in value]
    return str(value)


def _recording_span(connector: str, pair: str, start: str, end: Optional[str],
                    base_dir: Optional[str] = None) -> Optional[Tuple[int, int]]:
    from backtest_data import DEFAULT_DIR, iter_rows

    base_dir = base_dir or DEFAULT_DIR
    first = next(iter_rows(connector, pair, "book", start, end, base_dir), None)
    if first is None:
        return None
    last_day = date.fromisoformat(end or start)
    day = last_day
    while day >= date.fromisoformat(start):
        last = None
        for last in iter_rows(connector, pair, "book", day.isoformat(), day.isoformat(), base_dir):
            pass
        if last is not None:
            return int(first["ts"]), int(last["ts"]) + 1
        day -= timedelta(days=1)
    return None


def _infer_tick_size(connector: str, pair: str, start: str, end: Optional[str],
                     books_to_scan: int = 500, base_dir: Optional[str] = None) -> float:
    from backtest_data import DEFAULT_DIR, replay

    decimals = 0
    smallest_gap = math.inf
    scanned = 0
    for kind, _ts, event in replay(connector, pair, start, end, base_dir or DEFAULT_DIR):
        if kind != "book":
            continue
        scanned += 1
        for side in ("b", "a"):
            prices = [level[0] for level in event.get(side) or []]
            for price in prices:
                text = repr(float(price))
                if "e" in text or "E" in text:
                    continue
                if "." in text:
                    places = len(text.split(".")[1].rstrip("0"))
                    decimals = max(decimals, places)
            for first, second in zip(prices, prices[1:]):
                gap = abs(first - second)
                if 0 < gap < smallest_gap:
                    smallest_gap = gap
        if scanned >= books_to_scan:
            break

    if decimals == 0 and not math.isfinite(smallest_gap):
        _LOG.warning("could not infer a tick size for %s %s; pass tick_size explicitly",
                     connector, pair)
        return 0.0
    tick = round(10.0 ** -decimals, 12)
    if math.isfinite(smallest_gap) and smallest_gap < tick * 0.999:
        _LOG.warning("inferred tick %s for %s %s is coarser than the smallest observed gap "
                     "%s; using the gap instead", tick, connector, pair, smallest_gap)
        tick = smallest_gap
    _LOG.info("inferred tick size %s for %s %s (%s decimals, smallest gap %s)",
              tick, connector, pair, decimals, smallest_gap)
    return tick


def _sharpe_from_equity(ts: pd.Series, pnl: pd.Series,
                        bucket_seconds: float = 60.0) -> Tuple[float, int]:
    if len(pnl) < 3:
        return 0.0, 0
    buckets = (ts // bucket_seconds).astype("int64")
    per_bucket = pnl.groupby(buckets).last()
    changes = per_bucket.diff().dropna()
    if len(changes) < 2:
        return 0.0, int(len(changes))
    std = float(changes.std(ddof=1))
    if not math.isfinite(std) or std <= 0:
        return 0.0, int(len(changes))
    periods_per_year = (365.0 * 24.0 * 3600.0) / bucket_seconds
    sharpe = float(changes.mean() / std * math.sqrt(periods_per_year))
    return (sharpe if math.isfinite(sharpe) else 0.0), int(len(changes))


def _reconcile(executors: List[ExecutorInfo], bt, market: _Market) -> Dict:
    """
    Check the executors' PnL against the engine's own books.
    """
    executor_pnl = float(sum(float(info.net_pnl_quote) for info in executors))
    mid = market.mid if market.mid is not None else 0.0
    engine_pnl = bt.cash + bt.position * mid
    residual = executor_pnl - engine_pnl
    scale = max(abs(engine_pnl), 1.0)
    ok = abs(residual) <= 1e-6 * scale
    if not ok:
        _LOG.error("PnL reconciliation failed: executors %.8f vs engine %.8f (residual %.8f)",
                   executor_pnl, engine_pnl, residual)
    return {
        "executor_pnl_quote": executor_pnl,
        "engine_pnl_quote": engine_pnl,
        "residual": residual,
        "ok": bool(ok),
    }
