"""
Standalone order book + public trades recorder for every exchange hummingbot has a connector for.

Usage:
    python standalone_recorder.py --connector coindcx --pairs BTC-USDT,ETH-USDT
    python standalone_recorder.py --connector binance --pairs BTC-USDT --depth 50 --verify-sec 900
    python standalone_recorder.py --list                      # connectors hummingbot knows
    python standalone_recorder.py --connector coinswitch --list-keys   # API key settings a connector takes

How the book is stored:
    Whatever the exchange sends (differences, full snapshots, or both) is applied to a local book exactly as
    hummingbot's own order book tracker does. A row is written only when the top `depth` levels really changed,
    and it holds just the levels that changed. So a repeated snapshot, or an update that changes nothing we
    record, writes nothing. A full snapshot row is written at start, at UTC midnight (every daily file stands on
    its own) and at every verification.

Verification (every --verify-sec, default one hour):
    A fresh order book is requested from the exchange and compared with the book rebuilt from the stored
    snapshot + differences. Live updates keep arriving while the request is in flight, so both books keep
    receiving them for --verify-grace-sec; if they agree at any moment in that window the stored data is good
    ("ok": true). If they never agree, the local book is replaced by the exchange's ("ok": false, "bad": n) so an
    error cannot outlive one verification interval.
"""

import argparse
import asyncio
import gzip
import heapq
import importlib
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

Levels = Tuple[Tuple[float, float], ...]
View = Tuple[Levels, Levels]
Book = Tuple[Dict[float, float], Dict[float, float]]   # ({bid px: qty}, {ask px: qty})

logger = logging.getLogger("recorder")

PAST_DIFF_WINDOW_SIZE = 32


def top_levels(book: Book, depth: int) -> View:
    """Best `depth` levels per side, best first. depth 0 keeps the whole book."""
    bids, asks = book
    if depth > 0:
        return (tuple(heapq.nlargest(depth, bids.items())), tuple(heapq.nsmallest(depth, asks.items())))
    return (tuple(sorted(bids.items(), reverse=True)), tuple(sorted(asks.items())))


def side_changes(old: Levels, new: Levels) -> List[List[float]]:
    """Levels to set (and, with qty 0, to remove) that turn `old` into `new`."""
    old_levels, new_levels = dict(old), dict(new)
    changes = [[px, qty] for px, qty in new if old_levels.get(px) != qty]
    changes += [[px, 0.0] for px, _ in old if px not in new_levels]
    return changes


def levels_differing(a: View, b: View) -> int:
    """Number of price levels that are missing from one side or carry a different quantity."""
    return sum(len({px for px, _ in set(x) ^ set(y)}) for x, y in zip(a, b))


def apply_rows(book: Book, bids, asks):
    for side, rows in zip(book, (bids, asks)):
        for row in rows:
            px, qty = float(row.price), float(row.amount)
            if qty > 0:
                side[px] = qty
            else:
                side.pop(px, None)


# ---- connector + API keys ----------------------------------------------------------------------------------------

def connector_settings(name: str):
    from hummingbot.client.settings import AllConnectorSettings
    settings = AllConnectorSettings.get_connector_settings().get(name.removesuffix("_paper_trade"))
    if settings is None:
        raise ValueError(f"hummingbot has no connector named '{name}'. Run with --list to see them.")
    return settings


def key_fields(settings) -> List[str]:
    """Names of the API key settings a connector takes, e.g. ['coinswitch_api_key', 'coinswitch_api_secret']."""
    config = settings.config_keys
    if config is None:
        return []
    if isinstance(config, dict):   # legacy config maps
        return [name for name, var in config.items() if getattr(var, "is_connect_key", False)]
    from hummingbot.client.config.config_helpers import ClientConfigAdapter
    return [item.attr for item in ClientConfigAdapter(config).traverse()
            if item.client_field_data is not None and item.client_field_data.is_connect_key]


def common_key_field(fields: List[str], kind: str) -> str:
    matches = [f for f in fields if kind in f and (kind == "api_key" or "api_key" not in f)]
    if len(matches) != 1:
        found = "none" if not matches else "several: " + ", ".join(matches)
        raise ValueError(f"Cannot tell which key setting is the {kind.replace('_', ' ')} ({found}); "
                         f"use --key NAME=VALUE with one of: {', '.join(fields) or 'none'}")
    return matches[0]


def resolve_api_keys(settings, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                     passphrase: Optional[str] = None, overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    fields = key_fields(settings)
    keys: Dict[str, str] = {f: os.environ[f.upper()] for f in fields if os.environ.get(f.upper())}
    for kind, value in (("api_key", api_key), ("secret", api_secret), ("passphrase", passphrase)):
        if value:
            keys[common_key_field(fields, kind)] = value
    for name, value in (overrides or {}).items():
        if name not in fields:
            raise ValueError(f"{settings.name} has no key setting '{name}'; it takes: {', '.join(fields) or 'none'}")
        keys[name] = value
    return keys


def build_connector(settings, trading_pairs: List[str], api_keys: Optional[Dict[str, str]] = None):
    """The connector as paper trading builds it (non_trading_connector_instance_with_default_configuration),
    with the given API keys in place of the empty defaults. It is never asked to trade."""
    if not api_keys:
        return settings.non_trading_connector_instance_with_default_configuration(trading_pairs=trading_pairs)
    from decimal import Decimal

    from pydantic import SecretStr

    from hummingbot.client.config.config_helpers import ClientConfigAdapter

    config = settings.config_keys
    if isinstance(config, dict):  
        kwargs = {key: (var.value or "") for key, var in config.items()}
    elif config is not None:
        kwargs = {item.attr: item.value.get_secret_value() if isinstance(item.value, SecretStr) else item.value or ""
                  for item in ClientConfigAdapter(config).traverse() if item.attr != "connector"}
    else:
        kwargs = {}
    kwargs.update(api_keys)
    params = settings.conn_init_parameters(trading_pairs=trading_pairs, trading_required=False, api_keys=kwargs,
                                           rate_limits_share_pct=Decimal("100"), balance_asset_limit={})
    params = settings.add_domain_parameter(params)
    connector_class = getattr(importlib.import_module(settings.module_path()), settings.class_name())
    return connector_class(**params)


class PairState:
    def __init__(self):
        self.book: Optional[Book] = None      
        self.view: View = ((), ())              
        self.day: str = ""                        
        self.past_diffs: Deque = deque(maxlen=PAST_DIFF_WINDOW_SIZE)
        self.pending: List = []                   
        self.shadow: Optional[Tuple[Book, int]] = None
        self.verify_ok: bool = False
        self.counts = {"snapshots": 0, "diffs": 0, "trades": 0, "skipped": 0, "verify_ok": 0, "verify_bad": 0}


class StandaloneRecorder:
    def __init__(self, connector: str, pairs: List[str], depth: int = 20, verify_sec: float = 3600,
                 verify_grace_sec: float = 5, flush_sec: float = 5, output_dir: str = "",
                 api_keys: Optional[Dict[str, str]] = None):
        self.connector_name = connector.removesuffix("_paper_trade")
        self.api_keys = api_keys or {}
        self.pairs = [p.strip().upper() for p in pairs if p.strip()]
        self.depth = depth
        self.verify_sec = verify_sec
        self.verify_grace_sec = verify_grace_sec
        self.flush_sec = flush_sec
        self.output_dir = output_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "market_data")
        self._states: Dict[str, PairState] = {p: PairState() for p in self.pairs}
        self._buffers: Dict[Tuple[str, str, str], List[str]] = {}    # (pair, day, kind) -> rows
        self._data_source = None

    # ---- persistence -----------------------------------------------------------------------------------------

    @staticmethod
    def _day(ts: float) -> str:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

    def _buffer(self, pair: str, kind: str, row: dict):
        key = (pair, self._day(row["ts"]), kind)
        self._buffers.setdefault(key, []).append(json.dumps(row, separators=(",", ":")))

    def _flush(self):
        buffers, self._buffers = self._buffers, {}
        for (pair, day, kind), rows in buffers.items():
            try:
                folder = os.path.join(self.output_dir, self.connector_name, pair)
                os.makedirs(folder, exist_ok=True)
                with gzip.open(os.path.join(folder, f"{day}_{kind}.jsonl.gz"), "ab", compresslevel=6) as f:
                    f.write(("\n".join(rows) + "\n").encode())
            except Exception:
                logger.exception(f"Failed writing {kind} rows for {pair}; {len(rows)} rows dropped")

    # ---- order book ------------------------------------------------------------------------------------------

    def _snapshot_row(self, ts: float, uid: int, view: View, src: str, **extra) -> dict:
        return {"t": "s", "ts": round(ts, 6), "uid": uid, "b": view[0], "a": view[1], "src": src, **extra}

    def _record_book(self, pair: str, uid: int, snapshot_src: Optional[str] = None, **extra):
        """Write what changed in the recorded levels since the last row (or a full snapshot if snapshot_src)."""
        state = self._states[pair]
        now = time.time()
        view = top_levels(state.book, self.depth)
        day = self._day(now)
        if snapshot_src is None and day != state.day and state.day:
            self._buffer(pair, "book", self._snapshot_row(now, uid, state.view, "rotate"))
            state.counts["snapshots"] += 1
        state.day = day
        if snapshot_src is not None:
            self._buffer(pair, "book", self._snapshot_row(now, uid, view, snapshot_src, **extra))
            state.counts["snapshots"] += 1
        else:
            bids, asks = side_changes(state.view[0], view[0]), side_changes(state.view[1], view[1])
            if not bids and not asks:
                state.counts["skipped"] += 1
                return
            self._buffer(pair, "book", {"t": "d", "ts": round(now, 6), "uid": uid, "b": bids, "a": asks})
            state.counts["diffs"] += 1
        state.view = view

    @staticmethod
    def _restore(message, past_diffs) -> Book:
        """Snapshot + the recent differences newer than it (OrderBook.restore_from_snapshot_and_diffs)."""
        book: Book = ({}, {})
        apply_rows(book, message.bids, message.asks)
        for diff in past_diffs:
            if diff.update_id > message.update_id:
                apply_rows(book, diff.bids, diff.asks)
        return book

    def _on_book_message(self, message, is_snapshot: bool):
        pair = message.trading_pair
        state = self._states.get(pair)
        if state is None:
            return
        if state.book is None:
            state.pending.append((message, is_snapshot))
            return
        if is_snapshot:
            state.book = self._restore(message, state.past_diffs)
            if state.shadow is not None:
                state.shadow = (self._restore(message, state.past_diffs), state.shadow[1])
        else:
            apply_rows(state.book, message.bids, message.asks)
            state.past_diffs.append(message)
            if state.shadow is not None:
                apply_rows(state.shadow[0], message.bids, message.asks)
        self._record_book(pair, message.update_id)
        self._check_shadow(state)

    def _check_shadow(self, state: PairState):
        if state.shadow is not None and not state.verify_ok:
            state.verify_ok = top_levels(state.shadow[0], self.depth) == state.view

    async def _fetch_book(self, pair: str):
        """A fresh order book straight from the exchange, as a snapshot message."""
        order_book = await self._data_source.get_new_order_book(pair)
        from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
        bids = [(row.price, row.amount) for row in order_book.bid_entries()]
        asks = [(row.price, row.amount) for row in order_book.ask_entries()]
        if not bids and not asks:
            raise IOError(f"the exchange returned an empty order book for {pair} (the request most likely failed; "
                          f"see the connector's warnings above)")
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": pair, "update_id": order_book.snapshot_uid, "bids": bids, "asks": asks,
        }, time.time())

    async def _init_pair(self, pair: str):
        state = self._states[pair]
        while True:
            try:
                snapshot = await self._fetch_book(pair)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"Could not get the initial order book for {pair}; retrying in 5s")
                await asyncio.sleep(5)
        state.book = self._restore(snapshot, [])
        self._record_book(pair, snapshot.update_id, snapshot_src="start")
        pending, state.pending = state.pending, []
        for message, is_snapshot in pending:
            if is_snapshot or message.update_id > snapshot.update_id:
                self._on_book_message(message, is_snapshot)
        logger.info(f"{pair}: recording {len(state.view[0])} bid / {len(state.view[1])} ask levels")

    # ---- verification ----------------------------------------------------------------------------------------

    async def _verify_pair(self, pair: str):
        state = self._states[pair]
        if state.book is None:
            return
        snapshot = await self._fetch_book(pair)
        state.shadow = (self._restore(snapshot, state.past_diffs), snapshot.update_id)
        state.verify_ok = False
        self._check_shadow(state)
        deadline = time.time() + self.verify_grace_sec
        while not state.verify_ok and time.time() < deadline:
            await asyncio.sleep(0.05)      # live updates feed both books meanwhile (see _on_book_message)
        shadow, uid = state.shadow
        state.shadow = None
        if state.verify_ok:
            state.counts["verify_ok"] += 1
            self._record_book(pair, uid, snapshot_src="verify", ok=True)
            logger.info(f"{pair}: verification OK - stored snapshot + differences match the exchange book")
        else:
            bad = levels_differing(top_levels(shadow, self.depth), state.view)
            state.counts["verify_bad"] += 1
            state.book = shadow
            self._record_book(pair, uid, snapshot_src="verify", ok=False, bad=bad)
            logger.warning(f"{pair}: verification FAILED - {bad} level(s) differed from the exchange book; "
                           f"local book replaced with the exchange's")

    async def _verify_loop(self):
        while True:
            await asyncio.sleep(self.verify_sec)
            for pair in self.pairs:
                try:
                    await self._verify_pair(pair)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._states[pair].shadow = None
                    logger.exception(f"{pair}: verification could not run; will retry next interval")

    # ---- trades ----------------------------------------------------------------------------------------------

    def _on_trade_message(self, message):
        pair = message.trading_pair
        state = self._states.get(pair)
        if state is None:
            return
        content = message.content
        bids, asks = state.view
        self._buffer(pair, "trades", {
            "ts": round(time.time(), 6),
            "ets": message.timestamp,
            "id": str(content.get("trade_id", "")),
            "px": float(content["price"]),
            "qty": float(content["amount"]),
            "side": "sell" if int(float(content.get("trade_type", 1))) == 2 else "buy",   # TradeType.SELL == 2
            "bb": bids[0][0] if bids else None,
            "ba": asks[0][0] if asks else None,
        })
        state.counts["trades"] += 1

    # ---- loops -----------------------------------------------------------------------------------------------

    async def _consume(self, queue: asyncio.Queue, handler, *args):
        while True:
            message = await queue.get()
            try:
                handler(message, *args)
            except Exception:
                logger.exception("Error handling a market data message")

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(self.flush_sec)
            self._flush()

    async def _status_loop(self):
        while True:
            await asyncio.sleep(60)
            for pair, state in self._states.items():
                c = state.counts
                bids, asks = state.view
                top = f"{bids[0][0] if bids else '-'} / {asks[0][0] if asks else '-'}"
                logger.info(f"{pair}: snapshots={c['snapshots']} differences={c['diffs']} unchanged-skipped={c['skipped']} "
                            f"trades={c['trades']} verify ok/failed={c['verify_ok']}/{c['verify_bad']}  bid/ask={top}")

    async def run(self):
        settings = connector_settings(self.connector_name)
        connector = build_connector(settings, self.pairs, self.api_keys)
        self._data_source = connector.order_book_tracker.data_source
        keys = f"with API keys ({', '.join(sorted(self.api_keys))})" if self.api_keys else "without API keys"
        logger.info(f"Recording {self.pairs} on {self.connector_name} {keys} (depth={self.depth or 'full'}, "
                    f"verify every {self.verify_sec:g}s) to {self.output_dir}")

        loop = asyncio.get_running_loop()
        diffs, snapshots, trades = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        source = self._data_source
        tasks = [asyncio.ensure_future(coro) for coro in (
            source.listen_for_subscriptions(),
            source.listen_for_order_book_diffs(loop, diffs),
            source.listen_for_order_book_snapshots(loop, snapshots),
            source.listen_for_trades(loop, trades),
            self._consume(diffs, self._on_book_message, False),
            self._consume(snapshots, self._on_book_message, True),
            self._consume(trades, self._on_trade_message),
            self._flush_loop(),
            self._status_loop(),
            self._verify_loop(),
        )]
        try:
            await asyncio.gather(*(self._init_pair(pair) for pair in self.pairs))
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._flush()
            for pair, state in self._states.items():
                logger.info(f"{pair}: final counts {state.counts}")


def main():
    parser = argparse.ArgumentParser(description="Standalone order book + trades recorder for any hummingbot connector")
    parser.add_argument("--connector", default="coindcx", help="hummingbot connector name (default: coindcx)")
    parser.add_argument("--pairs", default="BTC-USDT", help="Comma-separated trading pairs (default: BTC-USDT)")
    parser.add_argument("--depth", type=int, default=20,
                        help="Order book levels per side to record; 0 = the whole book (default: 20)")
    parser.add_argument("--verify-sec", type=float, default=3600,
                        help="Seconds between verifications against a fresh exchange order book (default: 3600)")
    parser.add_argument("--verify-grace-sec", type=float, default=5,
                        help="How long a verification waits for the two books to agree (default: 5)")
    parser.add_argument("--flush-sec", type=float, default=5, help="Flush to disk every N seconds (default: 5)")
    parser.add_argument("--output-dir", default="", help="Output directory (default: ./data/market_data)")
    parser.add_argument("--list", action="store_true", help="List the connectors hummingbot knows and exit")
    parser.add_argument("--list-keys", action="store_true",
                        help="List the API key settings --connector takes (for --key and environment variables) and exit")
    parser.add_argument("--api-key", default=None, help="API key, for exchanges that need one to serve market data")
    parser.add_argument("--api-secret", default=None, help="API secret, for exchanges that need one")
    parser.add_argument("--passphrase", default=None, help="API passphrase, for exchanges that take one (e.g. kucoin)")
    parser.add_argument("--key", action="append", default=[], metavar="NAME=VALUE",
                        help="Set any other API key setting by its name (see --list-keys); repeatable")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    if args.list:
        from hummingbot.client.settings import AllConnectorSettings
        names = sorted(name for name, s in AllConnectorSettings.get_connector_settings().items()
                       if not name.endswith("_paper_trade") and not s.uses_gateway_generic_connector())
        print("\n".join(names))
        return
    if args.verify_sec <= 0 or args.depth < 0:
        parser.error("--verify-sec must be > 0 and --depth must be >= 0")
    overrides = {}
    for item in args.key:
        name, sep, value = item.partition("=")
        if not sep or not name.strip():
            parser.error(f"--key needs NAME=VALUE, got '{item}'")
        overrides[name.strip()] = value
    try:
        settings = connector_settings(args.connector)
        if args.list_keys:
            fields = key_fields(settings)
            print("\n".join(f"{f}    (env {f.upper()})" for f in fields) if fields
                  else f"{settings.name} takes no API keys")
            return
        api_keys = resolve_api_keys(settings, api_key=args.api_key, api_secret=args.api_secret,
                                    passphrase=args.passphrase, overrides=overrides)
    except ValueError as e:
        parser.error(str(e))

    recorder = StandaloneRecorder(args.connector, args.pairs.split(","), depth=args.depth,
                                  verify_sec=args.verify_sec, verify_grace_sec=args.verify_grace_sec,
                                  flush_sec=args.flush_sec, output_dir=args.output_dir, api_keys=api_keys)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(recorder.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
