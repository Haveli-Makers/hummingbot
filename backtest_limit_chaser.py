"""
Backtest scripts/limit_chaser.py on recorded data - no exchange connection, no real orders.

Mirrors OrderExecutor's LIMIT_CHASER logic (hummingbot/strategy_v2/executors/order_executor/order_executor.py):
  - every `update_interval` s: if there is no order, place a LIMIT_MAKER at ref * (1 -/+ distance),
    where ref = best bid for BUY / best ask for SELL (price floored to the tick, like the connector does)
  - if the order is open and the market moved AWAY by more than ref * refresh_threshold
    (BUY: ref - order_price > ref * threshold; SELL: order_price - ref > ref * threshold): cancel + place a new one
  - a post-only order that would cross is rejected -> retried on the next tick, stop after 10 retries
  - the chase ends when an order is completely filled
Like the live executor, a renewed order is again for the full `amount` even if the old one part-filled while its
cancel was in flight, so an episode can overfill - the report counts those.

One chase = one "episode". By default a new episode starts `--restart-sec` after the previous one ends, until the
data runs out, so you get statistics over many chases. Use --once for exactly one, like the live script.

    python backtest_limit_chaser.py coindcx BTC-USDT 2026-10-21 [2026-10-27] --side BUY --amount 0.001 \
        --distance 0.001 --refresh-threshold 0.002
    python backtest_limit_chaser.py coindcx BTC-USDT 2026-10-21 --config conf_limit_chaser.yml   # from conf/scripts
"""
import argparse
import math
import os
from typing import List, Optional

import pandas as pd
import yaml

from backtest_engine import Backtest, Order, Strategy

MAX_RETRIES = 10


class LimitChaser(Strategy):
    def __init__(self, side: str, amount: float, distance: float, refresh_threshold: float,
                 update_interval: float = 1.0, tick_size: float = 0.01, restart_sec: Optional[float] = 10.0,
                 timeout_sec: Optional[float] = None):
        self.side = side.lower()
        self.amount = amount
        self.distance = distance
        self.threshold = refresh_threshold
        self.update_interval = update_interval
        self.tick = tick_size
        self.restart_sec = restart_sec      # None = run a single episode
        self.timeout_sec = timeout_sec      # None = chase until filled (live script behaviour)

        self.episodes: List[dict] = []
        self._ep: Optional[dict] = None
        self._order_id: Optional[int] = None
        self._last_tick = -math.inf
        self._next_start: Optional[float] = None
        self._done = False

    # ---- helpers ---------------------------------------------------------------------------------------------

    def _ref_price(self, bt: Backtest) -> Optional[float]:
        return bt.best_bid if self.side == "buy" else bt.best_ask

    def _order_price(self, ref: float) -> float:
        raw = ref * (1 - self.distance) if self.side == "buy" else ref * (1 + self.distance)
        return round(math.floor(raw / self.tick + 1e-9) * self.tick, 10)

    def _place(self, bt: Backtest):
        price = self._order_price(self._ref_price(bt))
        place = bt.buy if self.side == "buy" else bt.sell
        self._order_id = place(price, self.amount, post_only=True)
        self._ep["orders"] += 1

    def _start_episode(self, bt: Backtest):
        self._ep = {"start_ts": bt.now, "side": self.side, "arrival_mid": bt.mid, "arrival_touch": self._ref_price(bt),
                    "filled": 0.0, "notional": 0.0, "fees": 0.0, "orders": 0, "renews": 0, "rejects": 0,
                    "retries": 0, "status": None, "end_ts": None}
        self._order_id = None

    def _end_episode(self, bt: Backtest, status: str):
        ep = self._ep
        ep["status"], ep["end_ts"] = status, bt.now
        self.episodes.append(ep)
        self._ep = None
        bt.cancel_all()
        if self.restart_sec is None:
            self._done = True
        else:
            self._next_start = bt.now + self.restart_sec

    # ---- control loop (runs every update_interval, like the executor's control_task) --------------------------

    def _tick(self, bt: Backtest):
        if bt.now - self._last_tick < self.update_interval or bt.mid is None:
            return
        self._last_tick = bt.now

        if self._ep is None:
            if self._next_start is None or bt.now >= self._next_start:
                self._start_episode(bt)
            else:
                return

        if self.timeout_sec is not None and bt.now - self._ep["start_ts"] >= self.timeout_sec:
            self._end_episode(bt, "timeout")
            return
        if self._ep["retries"] > MAX_RETRIES:
            self._end_episode(bt, "max_retries")
            return

        order = bt.orders.get(self._order_id) if self._order_id is not None else None
        if self._order_id is None:
            self._place(bt)
        elif order is not None and order.arrived and order.cancel_ts is None:   # "is_open"
            ref = self._ref_price(bt)
            moved_away = (ref - order.price) if self.side == "buy" else (order.price - ref)
            if moved_away > ref * self.threshold:
                bt.cancel(order.id)   # renew_order(): cancel then immediately place a fresh full-size order
                self._place(bt)
                self._ep["renews"] += 1

    def on_book(self, bt, book):
        self._tick(bt)

    def on_trade(self, bt, trade):
        self._tick(bt)

    # ---- order events ----------------------------------------------------------------------------------------

    def on_fill(self, bt, order: Order, price: float, qty: float, maker: bool):
        if self._ep is None:
            return  # a cancelled order from a finished episode filled while its cancel was in flight
        fee = price * qty * (bt.maker_fee if maker else bt.taker_fee)
        self._ep["filled"] += qty
        self._ep["notional"] += price * qty
        self._ep["fees"] += fee
        if order.id == self._order_id and order.remaining <= 1e-12:
            self._end_episode(bt, "filled")   # process_order_completed_event -> POSITION_HOLD

    def on_reject(self, bt, order: Order):
        if self._ep is not None and order.id == self._order_id:   # process_order_failed_event
            self._order_id = None
            self._ep["rejects"] += 1
            self._ep["retries"] += 1

    def on_cancel(self, bt, order: Order):
        if self._ep is not None and order.id == self._order_id:   # only happens outside renew (renew swaps id first)
            self._order_id = None

    def is_done(self, bt) -> bool:
        return self._done

    # ---- report ----------------------------------------------------------------------------------------------

    def report(self, bt: Backtest) -> pd.DataFrame:
        if self._ep is not None:
            self._ep["status"], self._ep["end_ts"] = "data_end", bt.now
            self.episodes.append(self._ep)
            self._ep = None
        df = pd.DataFrame(self.episodes)
        if df.empty:
            return df
        df["start"] = pd.to_datetime(df["start_ts"], unit="s", utc=True)
        df["duration_s"] = df["end_ts"] - df["start_ts"]
        df["avg_price"] = df["notional"] / df["filled"].where(df["filled"] > 0)
        sign = 1 if self.side == "buy" else -1
        # Positive = worse than arrival (paid more on a buy / received less on a sell), in basis points
        df["slippage_vs_mid_bps"] = sign * (df["avg_price"] - df["arrival_mid"]) / df["arrival_mid"] * 1e4
        df["slippage_vs_touch_bps"] = sign * (df["avg_price"] - df["arrival_touch"]) / df["arrival_touch"] * 1e4
        df["overfill"] = df["filled"] - self.amount
        return df


def summarize(df: pd.DataFrame, amount: float) -> str:
    if df.empty:
        return "  no episodes (no book data in range?)"
    filled = df[df["status"] == "filled"]
    lines = [f"  episodes:       {len(df)}  " + "  ".join(f"{k}={v}" for k, v in df["status"].value_counts().items())]
    if len(filled):
        d = filled["duration_s"]
        lines += [
            f"  time to fill:   median {d.median():.1f}s   mean {d.mean():.1f}s   p90 {d.quantile(0.9):.1f}s   max {d.max():.1f}s",
            f"  vs arrival mid: median {filled['slippage_vs_mid_bps'].median():+.2f} bps   "
            f"mean {filled['slippage_vs_mid_bps'].mean():+.2f} bps   (+ = worse)",
            f"  vs arrival touch: median {filled['slippage_vs_touch_bps'].median():+.2f} bps",
            f"  renews/episode: mean {filled['renews'].mean():.1f}   max {filled['renews'].max()}",
            f"  post-only rejects: {int(df['rejects'].sum())}",
            f"  fees (quote):   {df['fees'].sum():,.4f}",
        ]
    over = df[df["overfill"] > 1e-12]
    if len(over):
        lines.append(f"  overfilled:     {len(over)} episodes (max {over['filled'].max():.8g} vs amount {amount})")
    return "\n".join(lines)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest the limit chaser on recorded order book + trades")
    ap.add_argument("connector")
    ap.add_argument("pair")
    ap.add_argument("start", help="YYYY-MM-DD (UTC)")
    ap.add_argument("end", nargs="?", help="YYYY-MM-DD (UTC), inclusive")
    ap.add_argument("--config", help="limit_chaser config in conf/scripts (sets side/amount/distance/threshold/interval)")
    ap.add_argument("--side", default="BUY", choices=["BUY", "SELL", "buy", "sell"])
    ap.add_argument("--amount", type=float, default=0.001)
    ap.add_argument("--distance", type=float, default=0.001, help="0.001 = 0.1%% behind the top of book")
    ap.add_argument("--refresh-threshold", type=float, default=0.002)
    ap.add_argument("--update-interval", type=float, default=1.0)
    ap.add_argument("--tick-size", type=float, default=0.01)
    ap.add_argument("--maker-fee", type=float, default=0.001)
    ap.add_argument("--taker-fee", type=float, default=0.001)
    ap.add_argument("--latency-ms", type=float, default=100)
    ap.add_argument("--fill-at-touch", action="store_true", help="also fill when a trade prints AT our price")
    ap.add_argument("--once", action="store_true", help="run a single chase, like the live script")
    ap.add_argument("--restart-sec", type=float, default=10.0, help="pause between episodes")
    ap.add_argument("--timeout-sec", type=float, help="give up on a chase after this long")
    ap.add_argument("--out", help="write episodes.csv and fills.csv here")
    a = ap.parse_args()

    if a.config:
        path = a.config if os.path.isabs(a.config) else os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                     "conf", "scripts", a.config)
        with open(path) as f:
            c = yaml.safe_load(f)
        a.side = c.get("trade_type", a.side)
        a.amount = float(c.get("amount", a.amount))
        a.distance = float(c.get("chaser_distance", a.distance))
        a.refresh_threshold = float(c.get("refresh_threshold", a.refresh_threshold))
        a.update_interval = float(c.get("update_interval", a.update_interval))

    chaser = LimitChaser(a.side, a.amount, a.distance, a.refresh_threshold, a.update_interval, a.tick_size,
                         None if a.once else a.restart_sec, a.timeout_sec)
    bt = Backtest(a.connector, a.pair, a.start, a.end, a.maker_fee, a.taker_fee, a.latency_ms, a.fill_at_touch)
    result = bt.run(chaser)
    episodes = chaser.report(bt)

    print(f"limit chaser {a.side.upper()} {a.amount} {a.pair} on {a.connector} {a.start}..{a.end or a.start}  "
          f"distance={a.distance} refresh_threshold={a.refresh_threshold} interval={a.update_interval}s")
    print(summarize(episodes, a.amount))
    if a.out:
        os.makedirs(a.out, exist_ok=True)
        episodes.to_csv(os.path.join(a.out, "episodes.csv"), index=False)
        result.fills.to_csv(os.path.join(a.out, "fills.csv"), index=False)
        print(f"  saved episodes.csv, fills.csv to {a.out}")
