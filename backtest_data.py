"""
Load data recorded by standalone_recorder.py for backtesting.

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


class NoDataError(FileNotFoundError):
    """No recorded data exists for the requested connector/pair/date range."""


def available_pairs(connector: str, base_dir: str = DEFAULT_DIR) -> List[str]:
    """Pairs that have ANY recorded file under base_dir/connector/. Empty list if the connector dir is missing."""
    d = os.path.join(base_dir, connector)
    if not os.path.isdir(d):
        return []
    return sorted(name for name in os.listdir(d) if os.path.isdir(os.path.join(d, name)))


def has_data(connector: str, pair: str, kind: str, start: str, end: Optional[str] = None,
             base_dir: str = DEFAULT_DIR) -> bool:
    """True if at least one data file exists for this pair/kind in [start, end]."""
    for day in _days(start, end):
        if os.path.exists(os.path.join(base_dir, connector, pair, f"{day}_{kind}.jsonl.gz")):
            return True
    return False


def require_data(connector: str, pair: str, start: str, end: Optional[str] = None,
                 base_dir: str = DEFAULT_DIR) -> None:
    """Raise NoDataError with a helpful message if no book data covers the range."""
    if has_data(connector, pair, "book", start, end, base_dir):
        return
    pairs = available_pairs(connector, base_dir)
    range_str = f"{start}" if not end or end == start else f"{start}..{end}"
    if not pairs:
        hint = (f"No data recorded for connector '{connector}'. "
                f"Check base_dir={base_dir!r} or record data first.")
    elif pair not in pairs:
        hint = f"'{pair}' is not recorded on '{connector}'. Recorded pairs: {', '.join(pairs)}"
    else:
        hint = (f"'{pair}' is recorded on '{connector}' but no book files cover {range_str}. "
                f"Try a different date range.")
    raise NoDataError(hint)


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


def iter_raw_rows(connector: str, pair: str, kind: str, start: str, end: Optional[str] = None,
                  base_dir: str = DEFAULT_DIR) -> Iterator[dict]:
    """Rows exactly as stored on disk."""
    for day in _days(start, end):
        yield from _iter_file(os.path.join(base_dir, connector, pair, f"{day}_{kind}.jsonl.gz"))


def _apply_book_row(book: Tuple[dict, dict], row: dict) -> None:
    """Apply a stored snapshot (replaces the book) or difference (qty 0 removes the level) row."""
    is_diff = row.get("t") == "d"
    for side, levels in zip(book, (row["b"], row["a"])):
        if not is_diff:
            side.clear()
        for px, qty in levels:
            if qty > 0:
                side[px] = qty
            else:
                side.pop(px, None)


def _book_levels(book: Tuple[dict, dict]) -> Tuple[list, list]:
    return ([[px, qty] for px, qty in sorted(book[0].items(), reverse=True)],
            [[px, qty] for px, qty in sorted(book[1].items())])


def iter_rows(connector: str, pair: str, kind: str, start: str, end: Optional[str] = None,
              base_dir: str = DEFAULT_DIR) -> Iterator[dict]:
    """
    Stored rows; for kind "book" every row is the full recorded book {"ts", "uid", "b", "a"} at that moment.
    """
    if kind != "book":
        yield from iter_raw_rows(connector, pair, kind, start, end, base_dir)
        return
    book: Tuple[dict, dict] = ({}, {})
    have_snapshot = False
    last_levels = None
    for row in iter_raw_rows(connector, pair, kind, start, end, base_dir):
        if "t" not in row:
            yield row
            continue
        if row["t"] == "d" and not have_snapshot:
            continue        # nothing to apply a difference to
        have_snapshot = True
        _apply_book_row(book, row)
        levels = _book_levels(book)
        if row["t"] == "s" and levels == last_levels:
            continue        # a checkpoint (midnight / passed verification) of the book we already yielded
        last_levels = levels
        yield {"ts": row["ts"], "uid": row["uid"], "b": levels[0], "a": levels[1]}


def verify_books(connector: str, pair: str, start: str, end: Optional[str] = None,
                 base_dir: str = DEFAULT_DIR) -> pd.DataFrame:
    """
    Re-check every verification snapshot against the book rebuilt from the rows stored before it.
    """
    book: Tuple[dict, dict] = ({}, {})
    have_snapshot = False
    records = []
    for row in iter_raw_rows(connector, pair, "book", start, end, base_dir):
        if "t" not in row:
            continue
        if row["t"] == "s" and row.get("src") == "verify" and have_snapshot:
            bids, asks = _book_levels(book)
            rebuilt_matches = bids == [list(x) for x in row["b"]] and asks == [list(x) for x in row["a"]]
            records.append({"ts": row["ts"], "live_ok": bool(row.get("ok")), "bad": row.get("bad", 0),
                            "stored_ok": rebuilt_matches if row.get("ok") else True})
        if row["t"] == "s":
            have_snapshot = True
        if have_snapshot:
            _apply_book_row(book, row)
    df = pd.DataFrame(records, columns=["ts", "live_ok", "bad", "stored_ok"])
    if not df.empty:
        df.insert(0, "time", pd.to_datetime(df["ts"], unit="s", utc=True))
    return df


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


def find_gaps(df: pd.DataFrame, min_gap_sec: float = 60) -> pd.DataFrame:
    """Periods longer than min_gap_sec with no rows, from a load_books/load_trades frame: from, to, minutes."""
    if df.empty:
        return pd.DataFrame(columns=["from", "to", "minutes"])
    gap = df["ts"].diff()
    mask = gap > min_gap_sec
    return pd.DataFrame({"from": df["time"].shift()[mask], "to": df["time"][mask],
                         "minutes": gap[mask] / 60}).reset_index(drop=True)


def replay(connector: str, pair: str, start: str, end: Optional[str] = None,
           base_dir: str = DEFAULT_DIR) -> Iterator[Tuple[str, float, dict]]:
    """Merged, time-ordered stream of ("book" | "trade", ts, event). Streams from disk, so works for long ranges."""
    books = (("book", r["ts"], r) for r in iter_rows(connector, pair, "book", start, end, base_dir))
    trades = (("trade", r["ts"], r) for r in iter_rows(connector, pair, "trades", start, end, base_dir))
    yield from heapq.merge(books, trades, key=lambda e: e[1])


if __name__ == "__main__":
    args = sys.argv[1:]
    min_gap = 60.0
    if "--gap-sec" in args:
        i = args.index("--gap-sec")
        min_gap = float(args[i + 1])
        del args[i:i + 2]
    if len(args) < 3:
        print("usage: python backtest_data.py <connector> <PAIR> <start YYYY-MM-DD> [end YYYY-MM-DD] [--gap-sec 60]")
        sys.exit(1)
    connector, pair, start = args[:3]
    end = args[3] if len(args) > 3 else None
    t = load_trades(connector, pair, start, end)
    b = load_books(connector, pair, start, end, levels=1)
    print(f"{connector} {pair} {start}..{end or start}")
    print(f"  book updates: {len(b):,}" + (f"  ({b['time'].iloc[0]} -> {b['time'].iloc[-1]})" if len(b) else ""))
    print(f"  trades:       {len(t):,}" + (f"  ({t['time'].iloc[0]} -> {t['time'].iloc[-1]})" if len(t) else ""))
    if len(t):
        print(f"  volume:       {t['qty'].sum():,.6f}  buy/sell: {(t['side'] == 'buy').sum()}/{(t['side'] == 'sell').sum()}")
    if len(b):
        print(f"  median spread: {b['spread'].median():.8g}")
    v = verify_books(connector, pair, start, end)
    if len(v):
        print(f"  verifications: {len(v)}   matched exchange: {int(v['live_ok'].sum())}   "
              f"failed (book resynced): {int((~v['live_ok']).sum())}   "
              f"stored rows damaged: {int((~v['stored_ok']).sum())}")
        for r in v[~(v["live_ok"] & v["stored_ok"])].itertuples():
            reason = "stored rows do not rebuild to this snapshot" if not r.stored_ok else f"{r.bad} level(s) differed"
            print(f"    {r.time:%Y-%m-%d %H:%M:%S} UTC  {reason}")
    # Book and trades normally stop together (recorder down, sleep, network). A gap in only one of them points at
    # that feed. A long gap in BTC is never a quiet market - legs spanning one get made-up results.
    for name, df in (("book", b), ("trades", t)):
        if df.empty:
            continue
        gaps = find_gaps(df, min_gap)
        span = (df["ts"].iloc[-1] - df["ts"].iloc[0]) / 3600
        missing = gaps["minutes"].sum() / 60
        print(f"  {name} gaps > {min_gap:g}s: {len(gaps)}   missing {missing:.1f} h of {span:.1f} h "
              f"-> covered {span - missing:.1f} h ({(span - missing) / span:.0%})" if span else "")
        for g in gaps.itertuples():
            print(f"    {g._1:%Y-%m-%d %H:%M:%S} -> {g.to:%Y-%m-%d %H:%M:%S} UTC  {g.minutes:8.1f} min")
