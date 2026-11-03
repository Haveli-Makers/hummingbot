"""
Standalone orderbook + public trades recorder.

Records L2 order book snapshots and public trades to gzipped JSONL files,
compatible with the backtest engine's replay format. No hummingbot instance
required — connects directly to the exchange's public WebSocket/Socket.IO feed.

Usage:
    python standalone_recorder.py                          # defaults
    python standalone_recorder.py --pairs BTC-USDT,ETH-USDT --depth 20
    python standalone_recorder.py --connector binance --pairs BTC-USDT

Output layout (same as orderbook_trade_recorder.py):
    <output_dir>/<connector>/<PAIR>/<YYYY-MM-DD>_book.jsonl.gz
    <output_dir>/<connector>/<PAIR>/<YYYY-MM-DD>_trades.jsonl.gz
"""

import argparse
import asyncio
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import socketio
except ImportError:
    print("Missing dependency: python-socketio[asyncio_client]")
    print("Install with: pip install python-socketio[asyncio_client] aiohttp")
    sys.exit(1)

try:
    import aiohttp  # noqa: F401  (needed by socketio async client)
except ImportError:
    print("Missing dependency: aiohttp")
    print("Install with: pip install aiohttp")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("recorder")

Levels = Tuple[Tuple[float, float], ...]

# ── Exchange adapters ──────────────────────────────────────────────────

EXCHANGE_CONFIGS = {
    "coindcx": {
        "type": "socketio",
        "url": "wss://stream.coindcx.com",
        "pair_format": lambda pair: f"B-{pair.replace('-', '_')}",
        "orderbook_channel": lambda cdx_pair: f"{cdx_pair}@orderbook@10",
        "trades_channel": lambda cdx_pair: f"{cdx_pair}@trades",
        "ob_event": "depth-update",
        "ob_snapshot_event": "depth-snapshot",
        "trade_event": "new-trade",
        "parse_book": "_parse_coindcx_book",
        "parse_trade": "_parse_coindcx_trade",
    },
}


def _unwrap(message: Any) -> Optional[Dict[str, Any]]:
    if isinstance(message, dict) and "data" in message:
        message = message["data"]
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except ValueError:
            return None
    return message if isinstance(message, dict) else None


def _parse_coindcx_book(payload: dict, depth: int) -> Optional[dict]:
    bids_raw = payload.get("bids", {})
    asks_raw = payload.get("asks", {})
    if not bids_raw and not asks_raw:
        return None
    bids = sorted(
        [(float(px), float(qty)) for px, qty in bids_raw.items()],
        key=lambda x: -x[0],
    )[:depth]
    asks = sorted(
        [(float(px), float(qty)) for px, qty in asks_raw.items()],
        key=lambda x: x[0],
    )[:depth]
    return {
        "ts": round(time.time(), 6),
        "uid": payload.get("vs", 0),
        "b": bids,
        "a": asks,
    }


def _parse_coindcx_trade(payload: dict, best_bid: Optional[float], best_ask: Optional[float]) -> Optional[dict]:
    if "p" not in payload or "q" not in payload:
        return None
    return {
        "ts": round(time.time(), 6),
        "ets": payload.get("T", payload.get("t", time.time())),
        "id": str(payload.get("t", "")),
        "px": float(payload["p"]),
        "qty": float(payload["q"]),
        "side": "sell" if payload.get("m", False) else "buy",
        "bb": best_bid,
        "ba": best_ask,
    }


# ── Recorder ───────────────────────────────────────────────────────────

class StandaloneRecorder:
    def __init__(
        self,
        connector: str,
        pairs: List[str],
        depth: int = 20,
        book_sample_ms: int = 100,
        flush_sec: int = 5,
        output_dir: str = "",
    ):
        self.connector = connector
        self.pairs = [p.strip().upper() for p in pairs]
        self.depth = depth
        self.book_sample_sec = book_sample_ms / 1000
        self.flush_sec = flush_sec

        if output_dir:
            self.output_dir = output_dir
        else:
            base = os.path.dirname(os.path.abspath(__file__))
            self.output_dir = os.path.join(base, "data", "market_data")

        self._exchange = EXCHANGE_CONFIGS.get(connector)
        if self._exchange is None:
            raise ValueError(f"Unsupported connector: {connector}. Supported: {list(EXCHANGE_CONFIGS.keys())}")

        self._book_buffers: Dict[str, List[str]] = {p: [] for p in self.pairs}
        self._trade_buffers: Dict[str, List[str]] = {p: [] for p in self.pairs}
        self._last_books: Dict[str, Tuple[Levels, Levels]] = {}
        self._counts: Dict[str, List[int]] = {p: [0, 0] for p in self.pairs}
        self._running = False
        self._client: Optional[socketio.AsyncClient] = None

        self._cdx_pair_map: Dict[str, str] = {}
        self._stream_to_hb: Dict[str, str] = {}
        for p in self.pairs:
            cdx = self._exchange["pair_format"](p)
            self._cdx_pair_map[p] = cdx
            self._stream_to_hb[p.replace("-", "")] = p

    # ── file I/O ───────────────────────────────────────────────────────

    def _file_path(self, trading_pair: str, kind: str) -> str:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        folder = os.path.join(self.output_dir, self.connector, trading_pair)
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, f"{day}_{kind}.jsonl.gz")

    def _write(self, trading_pair: str, kind: str, rows: List[str]):
        if not rows:
            return
        with gzip.open(self._file_path(trading_pair, kind), "ab", compresslevel=6) as f:
            f.write(("\n".join(rows) + "\n").encode())

    def _flush(self):
        for pair in self.pairs:
            book_rows, self._book_buffers[pair] = self._book_buffers[pair], []
            trade_rows, self._trade_buffers[pair] = self._trade_buffers[pair], []
            try:
                self._write(pair, "book", book_rows)
                self._write(pair, "trades", trade_rows)
            except Exception:
                logger.exception(f"Failed writing data for {pair}; rows dropped")

    async def _flush_loop(self):
        while self._running:
            await asyncio.sleep(self.flush_sec)
            self._flush()

    # ── status ─────────────────────────────────────────────────────────

    async def _status_loop(self):
        while self._running:
            await asyncio.sleep(30)
            lines = [f"Recording to {self.output_dir}"]
            for pair in self.pairs:
                bc, tc = self._counts[pair]
                last = self._last_books.get(pair)
                bb = last[0][0][0] if last and last[0] else "-"
                ba = last[1][0][0] if last and last[1] else "-"
                lines.append(f"  {pair:<14} book={bc:>8}  trades={tc:>6}  bid/ask={bb}/{ba}")
            logger.info("\n".join(lines))

    # ── Socket.IO connection ───────────────────────────────────────────

    def _resolve_pair(self, symbol: str) -> Optional[str]:
        hb = self._stream_to_hb.get(symbol)
        if hb:
            return hb
        for pair in self.pairs:
            if symbol in (pair, pair.replace("-", ""), self._cdx_pair_map.get(pair, "")):
                return pair
        return None

    def _on_book(self, payload: dict):
        symbol = payload.get("s", payload.get("symbol", ""))
        pair = self._resolve_pair(symbol)
        if not pair:
            return
        row = _parse_coindcx_book(payload, self.depth)
        if row is None:
            return
        bids = tuple(tuple(x) for x in row["b"])
        asks = tuple(tuple(x) for x in row["a"])
        if self._last_books.get(pair) == (bids, asks):
            return
        self._last_books[pair] = (bids, asks)
        self._book_buffers[pair].append(json.dumps(row, separators=(",", ":")))
        self._counts[pair][0] += 1

    def _on_trade(self, payload: dict):
        symbol = payload.get("s", payload.get("symbol", ""))
        pair = self._resolve_pair(symbol)
        if not pair:
            return
        last = self._last_books.get(pair)
        bb = last[0][0][0] if last and last[0] else None
        ba = last[1][0][0] if last and last[1] else None
        row = _parse_coindcx_trade(payload, bb, ba)
        if row is None:
            return
        self._trade_buffers[pair].append(json.dumps(row, separators=(",", ":")))
        self._counts[pair][1] += 1

    async def _connect_socketio(self):
        cfg = self._exchange
        client = socketio.AsyncClient(logger=False, reconnection=True, reconnection_delay=5)
        self._client = client

        @client.event
        async def connect():
            logger.info(f"Connected to {cfg['url']}")
            for pair in self.pairs:
                cdx = self._cdx_pair_map[pair]
                ob_ch = cfg["orderbook_channel"](cdx)
                tr_ch = cfg["trades_channel"](cdx)
                await client.emit("join", {"channelName": ob_ch})
                await asyncio.sleep(0.05)
                await client.emit("join", {"channelName": tr_ch})
                await asyncio.sleep(0.05)
                logger.info(f"  Subscribed: {ob_ch}, {tr_ch}")

        @client.event
        async def disconnect():
            logger.warning("Disconnected from exchange stream")

        @client.on(cfg["ob_event"])
        async def on_depth_update(message):
            payload = _unwrap(message)
            if payload:
                self._on_book(payload)

        @client.on(cfg["ob_snapshot_event"])
        async def on_depth_snapshot(message):
            payload = _unwrap(message)
            if payload:
                self._on_book(payload)

        @client.on(cfg["trade_event"])
        async def on_new_trade(message):
            payload = _unwrap(message)
            if payload:
                self._on_trade(payload)

        await client.connect(cfg["url"], transports=["websocket"])

    # ── main loop ──────────────────────────────────────────────────────

    async def run(self):
        self._running = True
        logger.info(f"Starting recorder: connector={self.connector} pairs={self.pairs} depth={self.depth}")
        logger.info(f"Output: {self.output_dir}")

        flush_task = asyncio.create_task(self._flush_loop())
        status_task = asyncio.create_task(self._status_loop())

        try:
            await self._connect_socketio()
            await self._client.wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutting down...")
        finally:
            self._running = False
            flush_task.cancel()
            status_task.cancel()
            self._flush()
            if self._client:
                await self._client.disconnect()
            logger.info("Recorder stopped. Final counts:")
            for pair in self.pairs:
                bc, tc = self._counts[pair]
                logger.info(f"  {pair}: {bc} book snapshots, {tc} trades")


def main():
    parser = argparse.ArgumentParser(
        description="Standalone orderbook + trades recorder for backtesting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--connector", default="coindcx", help="Exchange connector (default: coindcx)")
    parser.add_argument("--pairs", default="BTC-USDT", help="Comma-separated trading pairs (default: BTC-USDT)")
    parser.add_argument("--depth", type=int, default=20, help="Order book levels per side (default: 20)")
    parser.add_argument("--book-sample-ms", type=int, default=100, help="Book sampling interval in ms (default: 100)")
    parser.add_argument("--flush-sec", type=int, default=5, help="Flush to disk every N seconds (default: 5)")
    parser.add_argument("--output-dir", default="", help="Output directory (default: ./data/market_data)")
    args = parser.parse_args()

    pairs = [p.strip() for p in args.pairs.split(",") if p.strip()]
    recorder = StandaloneRecorder(
        connector=args.connector,
        pairs=pairs,
        depth=args.depth,
        book_sample_ms=args.book_sample_ms,
        flush_sec=args.flush_sec,
        output_dir=args.output_dir,
    )

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(recorder.run())


if __name__ == "__main__":
    main()
