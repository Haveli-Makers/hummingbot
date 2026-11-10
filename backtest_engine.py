"""
Event-driven backtester for data recorded by scripts/orderbook_trade_recorder.py.

Replays book changes and public trades in time order, lets a strategy place/cancel limit orders, and simulates fills:
  - Resting (maker) orders fill against recorded trades on the opposite side that trade THROUGH the order price
    (or AT the price too, with fill_at_touch=True - optimistic, ignores queue position). Fill size is capped by the
    trade size. A resting order is also filled if a later book shows the opposite side crossing it.
  - An order priced through the current book when it becomes active fills immediately as taker, walking the levels.
  - Placing and cancelling both take `latency_ms` to reach the exchange; an order can still fill while its cancel
    is in flight.

Write a strategy by subclassing Strategy (see SimpleMarketMaker below), then:

    python backtest_engine.py coindcx BTC-USDT 2026-10-21 [2026-10-27] --spread-bps 2 --size 0.001 --out results

or from Python:

    from backtest_engine import Backtest, SimpleMarketMaker
    result = Backtest("coindcx", "BTC-USDT", "2026-10-21", maker_fee=0.001).run(SimpleMarketMaker(spread_bps=2))
    print(result.summary()); result.fills  # DataFrame
"""
import argparse
import itertools
import math
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from backtest_data import DEFAULT_DIR, replay


@dataclass
class Order:
    id: int
    side: str            # "buy" | "sell"
    price: float
    qty: float
    placed_ts: float
    active_ts: float     # when it reaches the exchange (placed_ts + latency)
    post_only: bool = False            # LIMIT_MAKER: rejected on arrival instead of crossing the book
    filled: float = 0.0
    arrived: bool = False             # reached the exchange (took any crossing liquidity, now resting)
    cancel_ts: Optional[float] = None  # when the cancel takes effect

    @property
    def remaining(self) -> float:
        return self.qty - self.filled


class Strategy:
    """Override any of these. `bt` is the running Backtest: use bt.buy / bt.sell / bt.cancel / bt.cancel_all,
    and read bt.now, bt.book, bt.best_bid, bt.best_ask, bt.mid, bt.position, bt.cash, bt.open_orders."""

    def on_start(self, bt: "Backtest"):
        pass

    def on_book(self, bt: "Backtest", book: dict):
        pass

    def on_trade(self, bt: "Backtest", trade: dict):
        pass

    def on_fill(self, bt: "Backtest", order: Order, price: float, qty: float, maker: bool):
        pass

    def on_reject(self, bt: "Backtest", order: Order):
        """A post-only order would have crossed the book when it arrived."""
        pass

    def on_cancel(self, bt: "Backtest", order: Order):
        """A cancel took effect (the order may have been partially filled before that)."""
        pass

    def is_done(self, bt: "Backtest") -> bool:
        """Return True to end the backtest early."""
        return False


class Backtest:
    def __init__(self, connector: str, pair: str, start: str, end: Optional[str] = None,
                 maker_fee: float = 0.001, taker_fee: float = 0.001, latency_ms: float = 100,
                 fill_at_touch: bool = False, base_dir: str = DEFAULT_DIR):
        self.connector, self.pair, self.start, self.end = connector, pair, start, end
        self.maker_fee, self.taker_fee = maker_fee, taker_fee
        self.latency = latency_ms / 1000
        self.fill_at_touch = fill_at_touch
        self.base_dir = base_dir

        self.now: float = 0.0
        self.book: Optional[dict] = None
        self.position: float = 0.0   # base asset, relative to the start
        self.cash: float = 0.0       # quote asset, relative to the start (fees included)
        self.fees: float = 0.0
        self.orders: Dict[int, Order] = {}
        self._ids = itertools.count(1)
        self._fills: List[dict] = []
        self._equity: List[tuple] = []

    # ---- strategy API ----------------------------------------------------------------------------------------

    @property
    def best_bid(self) -> Optional[float]:
        return self.book["b"][0][0] if self.book and self.book["b"] else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.book["a"][0][0] if self.book and self.book["a"] else None

    @property
    def mid(self) -> Optional[float]:
        bid, ask = self.best_bid, self.best_ask
        return (bid + ask) / 2 if bid is not None and ask is not None else None

    @property
    def open_orders(self) -> List[Order]:
        return [o for o in self.orders.values() if o.cancel_ts is None]

    def buy(self, price: float, qty: float, post_only: bool = False) -> int:
        return self._place("buy", price, qty, post_only)

    def sell(self, price: float, qty: float, post_only: bool = False) -> int:
        return self._place("sell", price, qty, post_only)

    def cancel(self, order_id: int):
        order = self.orders.get(order_id)
        if order is not None and order.cancel_ts is None:
            order.cancel_ts = self.now + self.latency

    def cancel_all(self):
        for order in self.open_orders:
            self.cancel(order.id)

    # ---- engine ----------------------------------------------------------------------------------------------

    def _place(self, side: str, price: float, qty: float, post_only: bool = False) -> int:
        order = Order(next(self._ids), side, price, qty, self.now, self.now + self.latency, post_only)
        self.orders[order.id] = order
        return order.id

    def _fill(self, strategy: Strategy, order: Order, price: float, qty: float, maker: bool):
        qty = min(qty, order.remaining)
        if qty <= 0:
            return
        fee = price * qty * (self.maker_fee if maker else self.taker_fee)
        sign = 1 if order.side == "buy" else -1
        order.filled += qty
        self.position += sign * qty
        self.cash -= sign * price * qty + fee
        self.fees += fee
        self._fills.append({"ts": self.now, "order_id": order.id, "side": order.side, "price": price, "qty": qty,
                            "maker": maker, "fee": fee, "position": self.position})
        if order.remaining <= 1e-12:
            del self.orders[order.id]
        strategy.on_fill(self, order, price, qty, maker)

    def _process_order_lifecycle(self, strategy: Strategy, ts: float):
        """Apply cancels that have landed and take liquidity for orders that just went live through the book."""
        for order in list(self.orders.values()):
            if order.cancel_ts is not None and order.cancel_ts <= ts:
                del self.orders[order.id]
                strategy.on_cancel(self, order)
            elif not order.arrived and order.active_ts <= ts and self.book:
                order.arrived = True
                if order.post_only and self._crosses_book(order):
                    del self.orders[order.id]
                    strategy.on_reject(self, order)
                else:
                    self._take_liquidity(strategy, order)

    def _crosses_book(self, order: Order) -> bool:
        if order.side == "buy":
            return self.best_ask is not None and order.price >= self.best_ask
        return self.best_bid is not None and order.price <= self.best_bid

    def _take_liquidity(self, strategy: Strategy, order: Order):
        levels = self.book["a"] if order.side == "buy" else self.book["b"]
        for px, qty in levels:
            crosses = px <= order.price if order.side == "buy" else px >= order.price
            if not crosses or order.id not in self.orders:
                break
            self._fill(strategy, order, px, qty, maker=False)

    def _match_trade(self, strategy: Strategy, trade: dict):
        """A taker sell hits resting buys priced >= trade price (best first); a taker buy lifts resting sells."""
        side = "buy" if trade["side"] == "sell" else "sell"
        px, left = trade["px"], trade["qty"]
        resting = [o for o in self.orders.values() if o.side == side and o.arrived]
        resting.sort(key=lambda o: -o.price if side == "buy" else o.price)
        for order in resting:
            through = order.price > px if side == "buy" else order.price < px
            if not (through or (self.fill_at_touch and order.price == px)) or left <= 0:
                break
            qty = min(left, order.remaining)
            self._fill(strategy, order, order.price, qty, maker=True)
            left -= qty

    def _match_book(self, strategy: Strategy):
        """If the opposite side of a new book crosses a resting order, that liquidity would have traded with it."""
        for order in list(self.orders.values()):
            if not order.arrived:
                continue
            levels = self.book["a"] if order.side == "buy" else self.book["b"]
            crossing = sum(q for p, q in levels if (p <= order.price if order.side == "buy" else p >= order.price))
            if crossing > 0:
                self._fill(strategy, order, order.price, crossing, maker=True)

    def run(self, strategy: Strategy) -> "BacktestResult":
        started = False
        for kind, ts, event in replay(self.connector, self.pair, self.start, self.end, self.base_dir):
            self.now = ts
            self._process_order_lifecycle(strategy, ts)
            if kind == "book":
                self.book = event
                if not started:
                    started = True
                    strategy.on_start(self)
                self._match_book(strategy)
                strategy.on_book(self, event)
                if self.mid is not None:
                    self._equity.append((ts, self.cash + self.position * self.mid, self.position))
            elif self.book is not None:
                self._match_trade(strategy, event)
                strategy.on_trade(self, event)
            if started and strategy.is_done(self):
                break
        return BacktestResult(self)


class BacktestResult:
    def __init__(self, bt: Backtest):
        self.fills = pd.DataFrame(bt._fills)
        self.equity = pd.DataFrame(bt._equity, columns=["ts", "pnl", "position"])
        for df in (self.fills, self.equity):
            if not df.empty:
                df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        self.fees = bt.fees
        self.final_position = bt.position

    def summary(self) -> str:
        if self.equity.empty:
            return "No book data in range."
        pnl = self.equity["pnl"]
        drawdown = (pnl.cummax() - pnl).max()
        f = self.fills
        n_maker = int(f["maker"].sum()) if not f.empty else 0
        volume = float((f["price"] * f["qty"]).sum()) if not f.empty else 0.0
        hours = (self.equity["ts"].iloc[-1] - self.equity["ts"].iloc[0]) / 3600
        return "\n".join([
            f"  period:         {self.equity['time'].iloc[0]} -> {self.equity['time'].iloc[-1]} ({hours:.2f} h)",
            f"  fills:          {len(f)} (maker {n_maker}, taker {len(f) - n_maker})",
            f"  volume (quote): {volume:,.2f}",
            f"  fees (quote):   {self.fees:,.4f}",
            f"  final position: {self.final_position:+.8g} (base)",
            f"  PnL (quote):    {pnl.iloc[-1]:+,.4f}   (marked to mid, after fees)",
            f"  max drawdown:   {drawdown:,.4f}",
        ])

    def save(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        self.fills.to_csv(os.path.join(out_dir, "fills.csv"), index=False)
        self.equity.to_csv(os.path.join(out_dir, "equity.csv"), index=False)


# ---- example strategy ----------------------------------------------------------------------------------------

class SimpleMarketMaker(Strategy):
    """Quotes one bid and one ask `spread_bps` around mid, refreshing every `refresh_sec`;
    stops quoting the side that would push |position| beyond `max_position`."""

    def __init__(self, spread_bps: float = 2.0, order_size: float = 0.001, refresh_sec: float = 5.0,
                 max_position: float = 0.01, tick_size: float = 0.01):
        self.spread = spread_bps / 10_000
        self.order_size = order_size
        self.refresh_sec = refresh_sec
        self.max_position = max_position
        self.tick = tick_size
        self._last_refresh = -math.inf

    def on_book(self, bt: Backtest, book: dict):
        if bt.now - self._last_refresh < self.refresh_sec or bt.mid is None:
            return
        self._last_refresh = bt.now
        bt.cancel_all()
        bid = math.floor(bt.mid * (1 - self.spread) / self.tick) * self.tick
        ask = math.ceil(bt.mid * (1 + self.spread) / self.tick) * self.tick
        if bt.position + self.order_size <= self.max_position + 1e-12:
            bt.buy(round(bid, 10), self.order_size)
        if bt.position - self.order_size >= -self.max_position - 1e-12:
            bt.sell(round(ask, 10), self.order_size)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest SimpleMarketMaker on recorded order book + trades")
    ap.add_argument("connector")
    ap.add_argument("pair")
    ap.add_argument("start", help="YYYY-MM-DD (UTC)")
    ap.add_argument("end", nargs="?", help="YYYY-MM-DD (UTC), inclusive")
    ap.add_argument("--spread-bps", type=float, default=2.0)
    ap.add_argument("--size", type=float, default=0.001, help="order size in base asset")
    ap.add_argument("--refresh-sec", type=float, default=5.0)
    ap.add_argument("--max-position", type=float, default=0.01)
    ap.add_argument("--tick-size", type=float, default=0.01)
    ap.add_argument("--maker-fee", type=float, default=0.001, help="fraction, 0.001 = 0.1%%")
    ap.add_argument("--taker-fee", type=float, default=0.001)
    ap.add_argument("--latency-ms", type=float, default=100)
    ap.add_argument("--fill-at-touch", action="store_true", help="also fill when a trade prints AT our price")
    ap.add_argument("--out", help="directory to write fills.csv and equity.csv")
    a = ap.parse_args()

    strategy = SimpleMarketMaker(a.spread_bps, a.size, a.refresh_sec, a.max_position, a.tick_size)
    result = Backtest(a.connector, a.pair, a.start, a.end, a.maker_fee, a.taker_fee, a.latency_ms,
                      a.fill_at_touch).run(strategy)
    print(f"{a.connector} {a.pair} {a.start}..{a.end or a.start}  spread={a.spread_bps}bps size={a.size}")
    print(result.summary())
    if a.out:
        result.save(a.out)
        print(f"  saved fills.csv, equity.csv to {a.out}")
