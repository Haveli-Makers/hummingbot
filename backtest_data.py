"""
Load data recorded by scripts/orderbook_trade_recorder.py for backtesting.

    from backtest_data import load_trades, load_books, replay

    trades = load_trades("coindcx", "BTC-USDT", "2026-10-19", "2026-10-20")   # pandas DataFrame
    books  = load_books("coindcx", "BTC-USDT", "2026-10-19", levels=5)        # flattened top-5 columns

    for kind, ts, event in replay("coindcx", "BTC-USDT", "2026-10-19"):
        if kind == "book":   # event = {"ts", "uid", "b": [[px, qty], ...], "a": [[px, qty], ...]}
            ...
        else:                # kind == "trade"; event = {"ts", "ets", "id", "px", "qty", "side", "bb", "ba"}
            ...

Run directly for a quick summary:  python backtest_data.py coindcx BTC-USDT 2026-10-19
"""
import gzip
import heapq
import json
import os
import sys
import zlib
from datetime import date, timedelta
from typing import Iterator, List, Optional, Tuple

import pandas as pd

DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "market_data")


def _days(start: str, end: Optional[str]) -> List[str]:
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end) if end else d0
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _iter_file(path: str) -> Iterator[dict]:
    if not os.path.exists(path):
        return
    try:
        with gzip.open(path, "rt") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    except (EOFError, zlib.error, gzip.BadGzipFile, json.JSONDecodeError):
        # A truncated last member (recorder killed mid-write) - keep everything read so far.
        return


def iter_rows(connector: str, pair: str, kind: str, start: str, end: Optional[str] = None,
              base_dir: str = DEFAULT_DIR) -> Iterator[dict]:
    for day in _days(start, end):
        yield from _iter_file(os.path.join(base_dir, connector, pair, f"{day}_{kind}.jsonl.gz"))


def load_trades(connector: str, pair: str, start: str, end: Optional[str] = None,
                base_dir: str = DEFAULT_DIR) -> pd.DataFrame:
    df = pd.DataFrame(iter_rows(connector, pair, "trades", start, end, base_dir))
    if not df.empty:
        df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        # No dedupe on "id": some exchanges (e.g. CoinDCX) use the ms timestamp as id, shared by fills in one sweep.
        df = df.sort_values("ts", kind="stable").reset_index(drop=True)
    return df


def load_books(connector: str, pair: str, start: str, end: Optional[str] = None, levels: int = 5,
               base_dir: str = DEFAULT_DIR) -> pd.DataFrame:
    """One row per book change with columns bid_px_0, bid_qty_0, ..., ask_px_{levels-1}, plus mid and spread."""
    records = []
    for row in iter_rows(connector, pair, "book", start, end, base_dir):
        rec = {"ts": row["ts"], "uid": row["uid"]}
        for side, key in (("bid", "b"), ("ask", "a")):
            book_side = row[key]
            for i in range(levels):
                px, qty = book_side[i] if i < len(book_side) else (None, None)
                rec[f"{side}_px_{i}"] = px
                rec[f"{side}_qty_{i}"] = qty
        records.append(rec)
    df = pd.DataFrame(records)
    if not df.empty:
        df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        df["mid"] = (df["bid_px_0"] + df["ask_px_0"]) / 2
        df["spread"] = df["ask_px_0"] - df["bid_px_0"]
    return df


def replay(connector: str, pair: str, start: str, end: Optional[str] = None,
           base_dir: str = DEFAULT_DIR) -> Iterator[Tuple[str, float, dict]]:
    """Merged, time-ordered stream of ("book" | "trade", ts, event). Streams from disk, so works for long ranges."""
    books = (("book", r["ts"], r) for r in iter_rows(connector, pair, "book", start, end, base_dir))
    trades = (("trade", r["ts"], r) for r in iter_rows(connector, pair, "trades", start, end, base_dir))
    yield from heapq.merge(books, trades, key=lambda e: e[1])


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("usage: python backtest_data.py <connector> <PAIR> <start YYYY-MM-DD> [end YYYY-MM-DD]")
        sys.exit(1)
    connector, pair, start = sys.argv[1:4]
    end = sys.argv[4] if len(sys.argv) > 4 else None
    t = load_trades(connector, pair, start, end)
    b = load_books(connector, pair, start, end, levels=1)
    print(f"{connector} {pair} {start}..{end or start}")
    print(f"  book updates: {len(b):,}" + (f"  ({b['time'].iloc[0]} -> {b['time'].iloc[-1]})" if len(b) else ""))
    print(f"  trades:       {len(t):,}" + (f"  ({t['time'].iloc[0]} -> {t['time'].iloc[-1]})" if len(t) else ""))
    if len(t):
        print(f"  volume:       {t['qty'].sum():,.6f}  buy/sell: {(t['side'] == 'buy').sum()}/{(t['side'] == 'sell').sum()}")
    if len(b):
        print(f"  median spread: {b['spread'].median():.8g}")
