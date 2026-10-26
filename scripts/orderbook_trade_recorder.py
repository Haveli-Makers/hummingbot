import asyncio
import gzip
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot import data_path
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

Levels = Tuple[Tuple[float, float], ...]


class OrderBookTradeRecorderConfig(BaseClientModel):
    """
    Records L2 order book (top N levels, written only when it changes) and every public trade,
    for replay in an order book backtester.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field(
        default="coindcx",
        json_schema_extra={
            "prompt": lambda mi: "Enter the connector name (a *_paper_trade connector needs no API keys): ",
            "prompt_on_new": True,
        },
    )
    trading_pairs: str = Field(
        default="BTC-USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter trading pairs to record (comma-separated, e.g. BTC-USDT,ETH-USDT): ",
            "prompt_on_new": True,
        },
    )
    depth: int = Field(
        default=20,
        gt=0,
        json_schema_extra={"prompt": lambda mi: "Order book levels per side to record: ", "prompt_on_new": True},
    )
    book_sample_ms: int = Field(
        default=100,
        ge=10,
        json_schema_extra={
            "prompt": lambda mi: "How often to check the book for changes, in ms (e.g. 100): ",
            "prompt_on_new": True,
        },
    )
    flush_sec: int = Field(
        default=5,
        gt=0,
        json_schema_extra={"prompt": lambda mi: "Flush buffered rows to disk every N seconds: ", "prompt_on_new": True},
    )
    output_dir: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Output directory (leave empty for <hummingbot>/data/market_data): ",
            "prompt_on_new": True,
        },
    )


class OrderBookTradeRecorder(ScriptStrategyBase):
    """
    Output layout (one gzip member appended per flush, so a crash loses at most `flush_sec` of data):

        <output_dir>/<connector>/<PAIR>/<YYYY-MM-DD>_book.jsonl.gz
            {"ts": <local recv epoch sec>, "uid": <book update id>, "b": [[px, qty], ...], "a": [[px, qty], ...]}
        <output_dir>/<connector>/<PAIR>/<YYYY-MM-DD>_trades.jsonl.gz
            {"ts": <local recv epoch sec>, "ets": <exchange ts>, "id": <trade id>, "px": .., "qty": ..,
             "side": "buy"|"sell" (taker side), "bb": <best bid at receipt>, "ba": <best ask at receipt>}

    Files rotate at UTC midnight. Run headless in the background with run_recorder.ps1.
    """

    markets: Dict[str, Set[str]] = {}

    @classmethod
    def init_markets(cls, config: OrderBookTradeRecorderConfig):
        cls.markets = {config.connector_name: set(cls._parse_pairs(config.trading_pairs))}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[OrderBookTradeRecorderConfig] = None):
        if config is None:
            config = OrderBookTradeRecorderConfig()
        super().__init__(connectors, config)
        self.connector_name = config.connector_name
        self.trading_pairs = self._parse_pairs(config.trading_pairs)
        self.depth = config.depth
        self.book_sample_sec = config.book_sample_ms / 1000
        self.flush_sec = config.flush_sec
        self.output_dir = config.output_dir or os.path.join(data_path(), "market_data")

        self._book_buffers: Dict[str, List[str]] = {p: [] for p in self.trading_pairs}
        self._trade_buffers: Dict[str, List[str]] = {p: [] for p in self.trading_pairs}
        self._last_books: Dict[str, Tuple[Levels, Levels]] = {}
        self._counts: Dict[str, List[int]] = {p: [0, 0] for p in self.trading_pairs}  # [book rows, trades]
        self._trade_forwarder = SourceInfoEventForwarder(self._on_public_trade)
        # The connector rebuilds its OrderBook objects after a network reconnect, so track which one we listen to
        self._listened_books: Dict[str, object] = {}
        self._subscribed = False
        self._sampler_task: Optional[asyncio.Task] = None
        self._flusher_task: Optional[asyncio.Task] = None

    @staticmethod
    def _parse_pairs(pairs: str) -> List[str]:
        return [p.strip().upper() for p in pairs.split(",") if p.strip()]

    @property
    def connector(self) -> ConnectorBase:
        return self.connectors[self.connector_name]

    # ---- lifecycle -------------------------------------------------------------------------------------------

    def _ensure_trade_listeners(self):
        for trading_pair in self.trading_pairs:
            order_book = self.connector.get_order_book(trading_pair)
            previous = self._listened_books.get(trading_pair)
            if order_book is previous:
                continue
            if previous is not None:
                previous.remove_listener(OrderBookEvent.TradeEvent, self._trade_forwarder)
                self.logger().info(f"Order book for {trading_pair} was rebuilt (reconnect); re-attached trade listener")
            order_book.add_listener(OrderBookEvent.TradeEvent, self._trade_forwarder)
            self._listened_books[trading_pair] = order_book

    def on_tick(self):
        self._ensure_trade_listeners()
        if not self._subscribed:
            self._sampler_task = safe_ensure_future(self._sample_books_loop())
            self._flusher_task = safe_ensure_future(self._flush_loop())
            self._subscribed = True
            self.logger().info(f"Recording {self.trading_pairs} on {self.connector_name} to {self.output_dir}")

    async def on_stop(self):
        for task in (self._sampler_task, self._flusher_task):
            if task is not None:
                task.cancel()
        for order_book in self._listened_books.values():
            order_book.remove_listener(OrderBookEvent.TradeEvent, self._trade_forwarder)
        self._listened_books.clear()
        self._flush()

    # ---- capture ---------------------------------------------------------------------------------------------

    def _top_levels(self, entries) -> Levels:
        levels = []
        for row in entries:
            levels.append((row.price, row.amount))
            if len(levels) >= self.depth:
                break
        return tuple(levels)

    def _sample_books(self):
        now = time.time()
        for trading_pair in self.trading_pairs:
            order_book = self.connector.get_order_book(trading_pair)
            bids = self._top_levels(order_book.bid_entries())
            asks = self._top_levels(order_book.ask_entries())
            if not bids and not asks:
                continue
            if self._last_books.get(trading_pair) == (bids, asks):
                continue
            self._last_books[trading_pair] = (bids, asks)
            self._book_buffers[trading_pair].append(json.dumps({
                "ts": round(now, 6),
                "uid": max(order_book.snapshot_uid, order_book.last_diff_uid),
                "b": bids,
                "a": asks,
            }, separators=(",", ":")))
            self._counts[trading_pair][0] += 1

    async def _sample_books_loop(self):
        while True:
            try:
                self._sample_books()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error sampling order books")
            await asyncio.sleep(self.book_sample_sec)

    def _on_public_trade(self, event_tag: int, order_book, event: OrderBookTradeEvent):
        trading_pair = event.trading_pair
        if trading_pair not in self._trade_buffers:
            return
        last_book = self._last_books.get(trading_pair)
        self._trade_buffers[trading_pair].append(json.dumps({
            "ts": round(time.time(), 6),
            "ets": event.timestamp,
            "id": event.trade_id,
            "px": float(event.price),
            "qty": float(event.amount),
            "side": event.type.name.lower(),
            "bb": last_book[0][0][0] if last_book and last_book[0] else None,
            "ba": last_book[1][0][0] if last_book and last_book[1] else None,
        }, separators=(",", ":")))
        self._counts[trading_pair][1] += 1

    # ---- persistence -----------------------------------------------------------------------------------------

    def _file_path(self, trading_pair: str, kind: str) -> str:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # coindcx_paper_trade streams the same public data as coindcx, so keep one folder per exchange
        exchange = self.connector_name.removesuffix("_paper_trade")
        folder = os.path.join(self.output_dir, exchange, trading_pair)
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, f"{day}_{kind}.jsonl.gz")

    def _write(self, trading_pair: str, kind: str, rows: List[str]):
        if not rows:
            return
        # Each append is a complete gzip member; gzip readers transparently concatenate them.
        with gzip.open(self._file_path(trading_pair, kind), "ab", compresslevel=6) as f:
            f.write(("\n".join(rows) + "\n").encode())

    def _flush(self):
        for trading_pair in self.trading_pairs:
            book_rows, self._book_buffers[trading_pair] = self._book_buffers[trading_pair], []
            trade_rows, self._trade_buffers[trading_pair] = self._trade_buffers[trading_pair], []
            try:
                self._write(trading_pair, "book", book_rows)
                self._write(trading_pair, "trades", trade_rows)
            except Exception:
                self.logger().exception(f"Failed writing data for {trading_pair}; rows dropped")

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(self.flush_sec)
            self._flush()

    # ---- status ----------------------------------------------------------------------------------------------

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Waiting for connector to be ready..."
        lines = [f"Recording to {self.output_dir}", f"  {'pair':<14}{'book rows':>12}{'trades':>10}  best bid / ask"]
        for trading_pair in self.trading_pairs:
            book_rows, trades = self._counts[trading_pair]
            last = self._last_books.get(trading_pair)
            top = f"{last[0][0][0] if last and last[0] else '-'} / {last[1][0][0] if last and last[1] else '-'}"
            lines.append(f"  {trading_pair:<14}{book_rows:>12}{trades:>10}  {top}")
        return "\n".join(lines)
