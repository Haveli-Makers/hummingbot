"""
Backtest the simple grid (controllers/generic/simple_grid.py) on recorded data - no exchange connection, no real orders.

Mirrors the controller (one leg at a time, re-anchored on where the last leg closed) and SimpleGridExecutor
(hummingbot/strategy_v2/executors/simple_grid_executor/simple_grid_executor.py):

  FLAT, reference P   rest BUY at P * (1 - take_profit)  |  trigger: mid reaches P * (1 + stop_loss) -> cross in
  LONG at F           rest SELL at F * (1 + take_profit) |  trigger: mid reaches F * (1 - stop_loss) -> stop loss
  (mirrored for a short)

  - the first leg has no reference: it rests at the touch and has no trigger
  - the first fill latches the side (both_oco cancels the other side) and the unfilled remainder is cancelled
  - take profit / stop loss are measured from the actual average entry fill
  - stop loss: with chase, rest `maker_offset_ticks` inside the opposite touch, re-quote only away from us after
    `requote_pct`, and cross once the mid has drifted `max_drift_pct` from where the stop fired; without chase,
    cross at once. A crossing order is a limit `close_slippage_ticks` through the touch.
  - an entry that does not fill within `entry_timeout` expires; it does not move the reference
  - a leg is only finished once all its orders are gone and it is flat, so a fill on an order whose cancel was
    still in flight is closed out instead of being stranded (like control_shutdown_process)
  - the run halts on `max_loss_quote` (realised, like the controller); an open leg finishes on its own barriers

Not modelled: venue refusals for collateral / balance (the engine never refuses), reconciliation, min notional.
Decisions run on a 1 s control tick (`--update-interval`), like the executor; orders and cancels take `--latency-ms`.

    python backtest_simple_grid.py coindcx BTC-USDT 2026-10-21 2026-10-23 --order-amount-quote 100 --step 0.0015
    python backtest_simple_grid.py coindcx BTC-USDT 2026-10-21 --config conf_simple_grid_btc.yml   # conf/controllers
"""
import argparse
import math
import os
from typing import Dict, List, Optional

import pandas as pd
import yaml

from backtest_engine import Backtest, Order, Strategy

EPS = 1e-12
MAX_RETRIES = 10


class Leg:
    """One executor: an entry, then whichever exit comes first."""

    def __init__(self, n: int, start_ts: float, sides: List[str], amount: float, reference: Optional[float]):
        self.n, self.start_ts, self.sides, self.amount, self.reference = n, start_ts, sides, amount, reference
        self.status = "running"              # running -> closing -> done
        self.close_type: Optional[str] = None
        self.end_ts: Optional[float] = None

        self.entry_ids: Dict[str, Optional[int]] = {s: None for s in sides}
        self.entry_triggered = False
        self.trigger_pending: Optional[str] = None
        self.entry_blocked_until = 0.0
        self.filled_side: Optional[str] = None
        self.remainder_cancelled = False

        self.tp_id: Optional[int] = None
        self.chase_id: Optional[int] = None
        self.close_id: Optional[int] = None
        self.close_pending = False
        self.exit_blocked_until = 0.0
        self.sl_triggered = False
        self.sl_level: Optional[float] = None
        self.sl_trigger_ref: Optional[float] = None

        self.order_ids: List[int] = []       # everything this leg ever placed
        self.entry_fills: Dict[str, List[tuple]] = {"buy": [], "sell": []}   # side -> [(px, qty)]
        self.exit_fills: List[tuple] = []
        self.fees = 0.0
        self.cashflow = 0.0                  # quote in - quote out, before fees
        self.maker_fills = self.taker_fills = 0
        self.retries = 0

    @property
    def side(self) -> Optional[str]:
        if self.filled_side is not None:
            return self.filled_side
        return self.sides[0] if len(self.sides) == 1 else None

    @property
    def exit_side(self) -> Optional[str]:
        return None if self.side is None else ("sell" if self.side == "buy" else "buy")

    def _executed(self, side: str) -> float:
        return sum(q for _, q in self.entry_fills[side])

    @property
    def open_amount(self) -> float:
        if self.filled_side is None:
            return 0.0
        opposing = "sell" if self.filled_side == "buy" else "buy"
        return max(0.0, self._executed(self.filled_side) - self._executed(opposing))

    @property
    def amount_to_close(self) -> float:
        return self.open_amount - sum(q for _, q in self.exit_fills)

    @staticmethod
    def _vwap(fills: List[tuple]) -> Optional[float]:
        qty = sum(q for _, q in fills)
        return sum(p * q for p, q in fills) / qty if qty > EPS else None

    @property
    def entry_price(self) -> Optional[float]:
        return self._vwap(self.entry_fills[self.filled_side]) if self.filled_side else None

    @property
    def close_price(self) -> Optional[float]:
        return self._vwap(self.exit_fills)


class SimpleGrid(Strategy):
    def __init__(self, order_amount_quote: float = 100.0, take_profit: float = 0.005, stop_loss: float = 0.005,
                 entry_mode: str = "long_only", entry_timeout: Optional[float] = 300, time_limit: Optional[float] = None,
                 tick_size: float = 0.01, lot_size: float = 0.0, post_only: bool = False,
                 close_slippage_ticks: int = 20, stop_loss_chase: bool = True, stop_loss_maker_offset_ticks: int = 1,
                 stop_loss_requote_pct: float = 0.0005, stop_loss_max_drift_pct: float = 0.001,
                 cancel_settle_delay: float = 0.25, cooldown_after_take_profit: float = 0,
                 cooldown_after_stop_loss: float = 0, max_loss_quote: Optional[float] = None,
                 update_interval: float = 1.0):
        self.order_amount_quote = order_amount_quote
        self.tp, self.sl = take_profit, stop_loss
        self.entry_mode = entry_mode
        self.entry_timeout, self.time_limit = entry_timeout, time_limit
        self.tick, self.lot = tick_size, lot_size
        self.post_only = post_only          # LIMIT_MAKER for resting orders (CoinDCX futures has none)
        self.slippage_ticks = close_slippage_ticks
        self.chase = stop_loss_chase
        self.offset_ticks = max(1, stop_loss_maker_offset_ticks)
        self.requote_pct, self.max_drift = stop_loss_requote_pct, stop_loss_max_drift_pct
        self.settle = cancel_settle_delay
        self.cooldown_tp, self.cooldown_sl = cooldown_after_take_profit, cooldown_after_stop_loss
        self.max_loss_quote = max_loss_quote
        self.update_interval = update_interval

        # controller state
        self.reference: Optional[float] = None
        self.bias_side: Optional[str] = None
        self.realized_pnl = 0.0
        self.halt_reason: Optional[str] = None
        self.last_close_ts = 0.0
        self.cooldown = 0.0
        self.leg: Optional[Leg] = None
        self.legs: List[Leg] = []
        self._roles: Dict[int, tuple] = {}   # order id -> (leg, role, side)
        self._last_tick = -math.inf

    # ---- prices ----------------------------------------------------------------------------------------------

    def _q(self, price: float) -> float:
        """quantize_order_price: floor to the tick."""
        return round(math.floor(price / self.tick + 1e-9) * self.tick, 10)

    def _crossing_price(self, bt: Backtest, side: str) -> float:
        slip = self.tick * self.slippage_ticks
        return self._q(bt.best_ask + slip) if side == "buy" else self._q(bt.best_bid - slip)

    def _maker_exit_price(self, bt: Backtest, side: str) -> float:
        """One tick inside the opposite touch: the most aggressive price that is still maker."""
        extra = self.tick * (self.offset_ticks - 1)
        if side == "buy":
            return self._q(bt.best_ask - self.tick - extra)
        bound = bt.best_bid + self.tick
        return max(self._q(bound + extra), round(bound, 10))

    # ---- orders ----------------------------------------------------------------------------------------------

    def _place(self, bt: Backtest, leg: Leg, role: str, side: str, price: float, qty: float, maker: bool) -> int:
        place = bt.buy if side == "buy" else bt.sell
        oid = place(price, qty, post_only=self.post_only and maker)
        self._roles[oid] = (leg, role, side)
        leg.order_ids.append(oid)
        return oid

    @staticmethod
    def _alive(bt: Backtest, oid: Optional[int]) -> bool:
        return oid is not None and oid in bt.orders

    @staticmethod
    def _resting(bt: Backtest, oid: Optional[int]) -> bool:
        return oid is not None and oid in bt.orders and bt.orders[oid].cancel_ts is None

    def _resting_ids(self, bt: Backtest, leg: Leg) -> List[int]:
        ids = list(leg.entry_ids.values()) + [leg.tp_id, leg.chase_id]
        return [oid for oid in ids if self._resting(bt, oid)]

    # ---- controller ------------------------------------------------------------------------------------------

    def _tick(self, bt: Backtest):
        if bt.now - self._last_tick < self.update_interval or bt.mid is None:
            return
        self._last_tick = bt.now
        if self.leg is not None:
            self._control_leg(bt, self.leg)
            if self.leg.status == "done":
                self._absorb(bt, self.leg)
                self.leg = None
        if self.leg is None and self.halt_reason is None and bt.now - self.last_close_ts >= self.cooldown:
            self._start_leg(bt)

    def _start_leg(self, bt: Backtest):
        if self.bias_side is not None:
            sides = [self.bias_side]
        else:
            sides = {"long_only": ["buy"], "short_only": ["sell"], "both_oco": ["buy", "sell"]}[self.entry_mode]
        amount = self.order_amount_quote / (self.reference if self.reference is not None else bt.mid)
        if self.lot > 0:
            amount = round(math.floor(amount / self.lot + 1e-9) * self.lot, 10)
        if amount <= 0:
            self.halt_reason = "order_amount_quote is below one lot"
            return
        self.leg = Leg(len(self.legs) + 1, bt.now, sides, amount, self.reference)

    def _absorb(self, bt: Backtest, leg: Leg):
        self.legs.append(leg)
        opened = bool(leg.entry_fills["buy"] or leg.entry_fills["sell"]) and leg.close_type not in ("EXPIRED", "FAILED")
        if opened:
            if leg.close_price is not None:
                self.reference = leg.close_price
            if self.bias_side is None:
                self.bias_side = leg.filled_side
            self.realized_pnl += leg.cashflow - leg.fees
        if leg.close_type == "FAILED" and leg.open_amount > EPS:
            self.halt_reason = f"leg {leg.n} failed after trading"
        self.last_close_ts = bt.now
        self.cooldown = self.cooldown_sl if leg.close_type == "STOP_LOSS" else self.cooldown_tp
        if self.max_loss_quote is not None and self.realized_pnl <= -self.max_loss_quote and self.halt_reason is None:
            self.halt_reason = f"loss {self.realized_pnl:.4f} reached -{self.max_loss_quote} after {len(self.legs)} legs"

    # ---- executor control loop -------------------------------------------------------------------------------

    def _control_leg(self, bt: Backtest, leg: Leg):
        if leg.status == "running":
            if leg.retries > MAX_RETRIES:
                self._close(bt, leg, "FAILED")
            elif leg.filled_side is None:
                self._control_entry(bt, leg)
            else:
                self._control_barriers(bt, leg)
        if leg.status == "closing":
            self._control_shutdown(bt, leg)

    def _control_entry(self, bt: Backtest, leg: Leg):
        if self.entry_timeout is not None and bt.now - leg.start_ts >= self.entry_timeout:
            for oid in self._resting_ids(bt, leg):
                bt.cancel(oid)
            leg.close_type, leg.status = "EXPIRED", "closing"
            return
        if leg.trigger_pending is not None:
            if bt.now >= leg.entry_blocked_until and not any(self._alive(bt, o) for o in leg.entry_ids.values()):
                side, leg.trigger_pending = leg.trigger_pending, None
                self._place_trigger(bt, leg, side)
            return
        for side in leg.sides:
            if self._trigger_reached(bt, leg, side):
                if self._resting(bt, leg.entry_ids[side]):
                    # cancel the resting order first; the trigger goes once its margin is released
                    bt.cancel(leg.entry_ids[side])
                    leg.entry_ids[side] = None
                    leg.trigger_pending = side
                    leg.entry_blocked_until = bt.now + self.settle
                else:
                    self._place_trigger(bt, leg, side)
                return
            if leg.entry_ids[side] is not None:
                continue                      # a grid level does not move
            if leg.reference is None:
                price = bt.best_bid if side == "buy" else bt.best_ask
            else:
                price = leg.reference * (1 - self.tp) if side == "buy" else leg.reference * (1 + self.tp)
            leg.entry_ids[side] = self._place(bt, leg, "entry", side, self._q(price), leg.amount, maker=True)

    def _trigger_reached(self, bt: Backtest, leg: Leg, side: str) -> bool:
        if leg.entry_triggered or leg.reference is None:
            return False
        if side == "buy":
            return bt.mid >= self._q(leg.reference * (1 + self.sl))
        return bt.mid <= self._q(leg.reference * (1 - self.sl))

    def _place_trigger(self, bt: Backtest, leg: Leg, side: str):
        leg.entry_triggered = True
        leg.entry_ids[side] = self._place(bt, leg, "entry", side, self._crossing_price(bt, side), leg.amount,
                                          maker=False)

    def _control_barriers(self, bt: Backtest, leg: Leg):
        own = leg.entry_ids.get(leg.filled_side)
        if not leg.remainder_cancelled and self._resting(bt, own):
            bt.cancel(own)                    # cancel_remainder_on_partial_fill
        leg.remainder_cancelled = True

        if self.time_limit is not None and bt.now - leg.start_ts >= self.time_limit:
            self._close(bt, leg, "TIME_LIMIT")
            return
        if leg.open_amount <= EPS:
            return
        long = leg.side == "buy"
        entry = leg.entry_price

        if not leg.sl_triggered:
            level = entry * (1 - self.sl) if long else entry * (1 + self.sl)
            if (bt.mid <= level) if long else (bt.mid >= level):
                leg.sl_triggered, leg.sl_level, leg.sl_trigger_ref = True, level, bt.mid
                if self._resting(bt, leg.tp_id):
                    bt.cancel(leg.tp_id)
        if leg.sl_triggered:
            if not self.chase:
                self._close(bt, leg, "STOP_LOSS")
            else:
                self._control_chase(bt, leg)
            return

        # take profit
        if leg.tp_id is None:
            if leg.amount_to_close > EPS:
                price = self._q(entry * (1 + self.tp) if long else entry * (1 - self.tp))
                leg.tp_id = self._place(bt, leg, "tp", leg.exit_side, price, leg.amount_to_close, maker=True)
        elif self._resting(bt, leg.tp_id):
            if abs(bt.orders[leg.tp_id].remaining - leg.amount_to_close) > EPS:
                bt.cancel(leg.tp_id)          # re-sized on the cancel

    def _control_chase(self, bt: Backtest, leg: Leg):
        long = leg.side == "buy"
        anchor = leg.sl_trigger_ref or leg.sl_level
        drift = (anchor - bt.mid) / anchor if long else (bt.mid - anchor) / anchor
        if drift >= self.max_drift:
            self._close(bt, leg, "STOP_LOSS")
            return
        if leg.amount_to_close <= EPS:
            return
        if any(oid != leg.chase_id for oid in self._resting_ids(bt, leg)) or bt.now < leg.exit_blocked_until:
            return                            # the take profit's cancel has not landed yet
        if any(self._alive(bt, oid) for oid in (leg.tp_id,) if oid != leg.chase_id):
            return
        desired = self._maker_exit_price(bt, leg.exit_side)
        if leg.chase_id is None:
            leg.chase_id = self._place(bt, leg, "chase", leg.exit_side, desired, leg.amount_to_close, maker=True)
        elif self._resting(bt, leg.chase_id):
            resting = bt.orders[leg.chase_id].price
            moved = (resting - desired) if long else (desired - resting)   # only ever away from us
            if moved > 0 and moved >= self.requote_pct * anchor:
                bt.cancel(leg.chase_id)

    def _close(self, bt: Backtest, leg: Leg, close_type: str):
        resting = self._resting_ids(bt, leg)
        for oid in resting:
            bt.cancel(oid)
        leg.close_type, leg.status = close_type, "closing"
        if leg.amount_to_close <= EPS:
            return
        if resting:
            leg.close_pending = True
        else:
            self._place_close(bt, leg)

    def _place_close(self, bt: Backtest, leg: Leg):
        leg.close_pending = False
        leg.close_id = self._place(bt, leg, "close", leg.exit_side, self._crossing_price(bt, leg.exit_side),
                                   leg.amount_to_close, maker=False)

    def _control_shutdown(self, bt: Backtest, leg: Leg):
        if leg.close_pending and not self._resting_ids(bt, leg) and bt.now >= leg.exit_blocked_until:
            self._place_close(bt, leg)
        if any(self._alive(bt, oid) for oid in leg.order_ids):
            for oid in self._resting_ids(bt, leg):
                bt.cancel(oid)
            return
        if leg.amount_to_close > EPS and leg.exit_side is not None:
            # a fill landed while its cancel was in flight: close it out rather than strand it
            leg.retries += 1
            self._place_close(bt, leg)
            return
        leg.status, leg.end_ts = "done", bt.now

    # ---- engine callbacks ------------------------------------------------------------------------------------

    def on_book(self, bt, book):
        self._tick(bt)

    def on_trade(self, bt, trade):
        self._tick(bt)

    def on_fill(self, bt, order: Order, price: float, qty: float, maker: bool):
        leg, role, side = self._roles[order.id]
        fee = price * qty * (bt.maker_fee if maker else bt.taker_fee)
        leg.fees += fee
        leg.cashflow += price * qty if side == "sell" else -price * qty
        leg.maker_fills += maker
        leg.taker_fills += not maker
        done = order.remaining <= EPS
        if role == "entry":
            leg.entry_fills[side].append((price, qty))
            if leg.filled_side is None:
                leg.filled_side = side    # first fill wins: cancel the other side
                for other, oid in leg.entry_ids.items():
                    if other != side and self._resting(bt, oid):
                        bt.cancel(oid)
            return
        leg.exit_fills.append((price, qty))
        if done and leg.status == "running":
            if role == "tp":
                leg.close_type, leg.status = "TAKE_PROFIT", "closing"
            elif role == "chase":
                leg.close_type, leg.status = "STOP_LOSS", "closing"

    def on_cancel(self, bt, order: Order):
        leg, role, side = self._roles[order.id]
        if role == "entry":
            if leg.entry_ids.get(side) == order.id and order.filled <= EPS:
                leg.entry_ids[side] = None
            if leg.trigger_pending is not None:
                leg.entry_blocked_until = bt.now + self.settle
        elif role == "tp" and leg.tp_id == order.id:
            leg.tp_id = None
            leg.exit_blocked_until = bt.now + self.settle
        elif role == "chase" and leg.chase_id == order.id:
            leg.chase_id = None
            leg.exit_blocked_until = bt.now + self.settle

    def on_reject(self, bt, order: Order):
        """Post-only order that would have crossed: free the slot and retry on the next tick."""
        leg, role, side = self._roles[order.id]
        leg.retries += 1
        if role == "entry" and leg.entry_ids.get(side) == order.id:
            leg.entry_ids[side] = None
            leg.entry_triggered = False
        elif role == "tp" and leg.tp_id == order.id:
            leg.tp_id = None
        elif role == "chase" and leg.chase_id == order.id:
            leg.chase_id = None

    # ---- report ----------------------------------------------------------------------------------------------

    def report(self, bt: Backtest) -> pd.DataFrame:
        legs = self.legs + ([self.leg] if self.leg is not None else [])
        rows = []
        for leg in legs:
            rows.append({
                "leg": leg.n, "start_ts": leg.start_ts, "end_ts": leg.end_ts if leg.end_ts else bt.now,
                "side": leg.side, "reference": leg.reference, "amount": leg.amount,
                "entry_price": leg.entry_price, "close_price": leg.close_price, "filled": leg.open_amount,
                "close_type": leg.close_type if leg.status == "done" else f"OPEN ({leg.status})",
                "pnl_quote": leg.cashflow - leg.fees if leg.amount_to_close <= EPS else None,
                "fees": leg.fees, "maker_fills": leg.maker_fills, "taker_fills": leg.taker_fills,
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            df["start"] = pd.to_datetime(df["start_ts"], unit="s", utc=True)
            df["duration_s"] = df["end_ts"] - df["start_ts"]
        return df


def summarize(df: pd.DataFrame, grid: SimpleGrid) -> str:
    if df.empty:
        return "  no legs (no book data in range?)"
    traded = df[df["close_type"].isin(["TAKE_PROFIT", "STOP_LOSS", "TIME_LIMIT"])]
    counts = "  ".join(f"{k}={v}" for k, v in df["close_type"].value_counts().items())
    wins = (traded["close_type"] == "TAKE_PROFIT").sum()
    lines = [f"  legs:           {len(df)}  {counts}"]
    if len(traded):
        lines += [
            f"  win rate:       {wins / len(traded):.2%}   needed (before fees) {grid.sl / (grid.tp + grid.sl):.2%}",
            f"  realised PnL:   {traded['pnl_quote'].sum():+,.4f}   fees {df['fees'].sum():,.4f}",
            f"  fills:          maker {int(df['maker_fills'].sum())}  taker {int(df['taker_fills'].sum())}",
            f"  leg time:       median {traded['duration_s'].median():.0f}s   p90 {traded['duration_s'].quantile(0.9):.0f}s",
        ]
    if grid.halt_reason:
        lines.append(f"  HALTED:         {grid.halt_reason}")
    return "\n".join(lines)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest the simple grid on recorded order book + trades")
    ap.add_argument("connector")
    ap.add_argument("pair")
    ap.add_argument("start", help="YYYY-MM-DD (UTC)")
    ap.add_argument("end", nargs="?", help="YYYY-MM-DD (UTC), inclusive")
    ap.add_argument("--config", help="simple_grid controller config in conf/controllers (sets the strategy settings)")
    ap.add_argument("--order-amount-quote", type=float, default=100.0)
    ap.add_argument("--step", type=float, help="sets both --take-profit and --stop-loss")
    ap.add_argument("--take-profit", type=float, default=0.005)
    ap.add_argument("--stop-loss", type=float, default=0.005)
    ap.add_argument("--entry-mode", default="long_only", choices=["long_only", "short_only", "both_oco"])
    ap.add_argument("--entry-timeout", type=float, default=300)
    ap.add_argument("--time-limit", type=float)
    ap.add_argument("--close-slippage-ticks", type=int, default=20)
    ap.add_argument("--no-chase", action="store_true", help="cross at once when the stop loss fires")
    ap.add_argument("--maker-offset-ticks", type=int, default=1)
    ap.add_argument("--requote-pct", type=float, default=0.0005)
    ap.add_argument("--max-drift-pct", type=float, default=0.001)
    ap.add_argument("--cancel-settle-delay", type=float, default=0.25)
    ap.add_argument("--cooldown-tp", type=float, default=0)
    ap.add_argument("--cooldown-sl", type=float, default=0)
    ap.add_argument("--max-loss-quote", type=float)
    ap.add_argument("--post-only", action="store_true", help="resting orders are LIMIT_MAKER (not on CoinDCX futures)")
    ap.add_argument("--tick-size", type=float, default=0.01)
    ap.add_argument("--lot-size", type=float, default=0.0, help="floor the leg amount to this; 0 = no rounding")
    ap.add_argument("--update-interval", type=float, default=1.0)
    ap.add_argument("--maker-fee", type=float, default=0.001)
    ap.add_argument("--taker-fee", type=float, default=0.001)
    ap.add_argument("--latency-ms", type=float, default=100)
    ap.add_argument("--fill-at-touch", action="store_true", help="also fill when a trade prints AT our price")
    ap.add_argument("--out", help="write legs.csv, fills.csv and equity.csv here")
    a = ap.parse_args()

    if a.step is not None:
        a.take_profit = a.stop_loss = a.step
    if a.config:
        path = a.config if os.path.isabs(a.config) else os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                     "conf", "controllers", a.config)
        with open(path) as f:
            c = yaml.safe_load(f)
        a.order_amount_quote = float(c.get("order_amount_quote", a.order_amount_quote))
        a.take_profit = float(c.get("take_profit", a.take_profit))
        a.stop_loss = float(c.get("stop_loss", a.stop_loss))
        a.entry_mode = c.get("initial_entry_mode", a.entry_mode)
        a.entry_timeout = c.get("entry_timeout", a.entry_timeout)
        a.time_limit = c.get("time_limit", a.time_limit)
        a.close_slippage_ticks = int(c.get("close_slippage_ticks", a.close_slippage_ticks))
        a.no_chase = not c.get("stop_loss_chase", not a.no_chase)
        a.maker_offset_ticks = int(c.get("stop_loss_maker_offset_ticks", a.maker_offset_ticks))
        a.requote_pct = float(c.get("stop_loss_requote_pct", a.requote_pct))
        a.max_drift_pct = float(c.get("stop_loss_max_drift_pct", a.max_drift_pct))
        a.cancel_settle_delay = float(c.get("cancel_settle_delay", a.cancel_settle_delay))
        a.cooldown_tp = float(c.get("cooldown_after_take_profit", a.cooldown_tp))
        a.cooldown_sl = float(c.get("cooldown_after_stop_loss", a.cooldown_sl))
        if c.get("max_loss_quote") is not None:
            a.max_loss_quote = float(c["max_loss_quote"])
        if c.get("max_loss_pct") is not None and c.get("total_amount_quote") is not None:
            pct_limit = float(c["max_loss_pct"]) * float(c["total_amount_quote"])
            a.max_loss_quote = pct_limit if a.max_loss_quote is None else min(a.max_loss_quote, pct_limit)
        if "perpetual" not in str(c.get("connector_name", "perpetual")):
            a.entry_mode = "long_only"

    grid = SimpleGrid(a.order_amount_quote, a.take_profit, a.stop_loss, a.entry_mode, a.entry_timeout, a.time_limit,
                      a.tick_size, a.lot_size, a.post_only, a.close_slippage_ticks, not a.no_chase,
                      a.maker_offset_ticks, a.requote_pct, a.max_drift_pct, a.cancel_settle_delay,
                      a.cooldown_tp, a.cooldown_sl, a.max_loss_quote, a.update_interval)
    bt = Backtest(a.connector, a.pair, a.start, a.end, a.maker_fee, a.taker_fee, a.latency_ms, a.fill_at_touch)
    result = bt.run(grid)
    legs = grid.report(bt)

    print(f"simple grid {a.pair} on {a.connector} {a.start}..{a.end or a.start}  "
          f"tp={a.take_profit} sl={a.stop_loss} amount={a.order_amount_quote} mode={a.entry_mode}")
    print(summarize(legs, grid))
    print(result.summary())
    if a.out:
        os.makedirs(a.out, exist_ok=True)
        legs.to_csv(os.path.join(a.out, "legs.csv"), index=False)
        result.save(a.out)
        print(f"  saved legs.csv, fills.csv, equity.csv to {a.out}")
