# Architecture: Order Book & Public Trades Backtesting

This doc explains how we record CoinDCX's order book and public trades, store them, and replay them to backtest strategies with realistic fills. It is for engineers who will run, extend or debug the system.

For how this compares with Hummingbot's built-in candle backtesting, see `BACKTESTING_CANDLES_VS_ORDERBOOK.md`.

---

## 1. Overview

The system has two parts that are joined only by files on disk:

1. **Capture (live):** Hummingbot runs headless, keeps an L2 order book in sync from the exchange websocket, and a recorder script writes book changes and public trades to disk.
2. **Backtest (offline):** a loader streams those files back in time order. An event-driven engine feeds them to a strategy, simulates its orders and reports results.

```
 ┌───────────────────────────── CAPTURE (live, Hummingbot headless) ─────────────────────────────┐
 │                                                                                                 │
 │  CoinDCX socket.io            CoinDCX connector                 Hummingbot core                 │
 │  wss://stream.coindcx.com     (order book data source)          OrderBookTracker                │
 │  ┌──────────────────┐        ┌──────────────────────┐          ┌───────────────────────┐        │
 │  │ depth-update     │──────► │ unwrap envelope      │─ diff ──►│ diff router           │        │
 │  │ depth-snapshot   │──────► │ map symbol → pair    │─ snap ──►│ snapshot router       │──┐     │
 │  │ new-trade        │──────► │ queue raw payloads   │─ trade ─►│ trade loop            │  │     │
 │  └──────────────────┘        └──────────────────────┘          └───────────────────────┘  │     │
 │  REST /market_data/orderbook ─► initial snapshot (uid 0)                                   ▼     │
 │                                                                           ┌──────────────────┐  │
 │                                                                           │ OrderBook (per   │  │
 │                                                                           │ pair, in memory) │  │
 │                                                                           └──────┬─────┬─────┘  │
 │                                                     every 100 ms: top 20 levels  │     │ TradeEvent│
 │                                                                                  ▼     ▼          │
 │                                                          scripts/orderbook_trade_recorder.py      │
 │                                                          buffer → gzip append every 5 s           │
 └──────────────────────────────────────────────────────────────────────┬──────────────────────────┘
                                                                        ▼
                              data/market_data/<connector>/<PAIR>/<YYYY-MM-DD>_{book,trades}.jsonl.gz
                                                                        │
 ┌──────────────────────────────── BACKTEST (offline, plain Python) ───┴──────────────────────────┐
 │                                                                                                 │
 │  backtest_data.py              backtest_engine.py                         strategy              │
 │  ┌──────────────────┐         ┌──────────────────────────────┐          ┌──────────────────┐   │
 │  │ replay():        │ events  │ Backtest.run()               │ on_book  │ SimpleMarketMaker│   │
 │  │ heap-merge book  │───────► │  order lifecycle (latency)   │ on_trade │ LimitChaser      │   │
 │  │ + trades by ts   │         │  matching (trades & book)    │ on_fill  │ your Strategy    │   │
 │  └──────────────────┘         │  accounting (pos, cash, fees)│ ◄──────► │                  │   │
 │  load_books/trades()          └──────────────┬───────────────┘ buy/sell/│                  │   │
 │  → pandas (analysis)                         ▼                 cancel   └──────────────────┘   │
 │                                 BacktestResult: fills, equity, summary → CSV                   │
 └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Components**

| Component | File | Role |
|---|---|---|
| Supervisor | `run_recorder.ps1` | Starts Hummingbot headless in the background and restarts it 10 s after any exit |
| Exchange adapter | `hummingbot/connector/exchange/coindcx/coindcx_api_order_book_data_source.py`, `coindcx_order_book.py` | Websocket subscription, message parsing, symbol mapping |
| Book maintenance | `hummingbot/core/data_type/order_book_tracker.py`, `order_book.pyx` | Applies snapshots and diffs; emits trade events |
| Recorder | `scripts/orderbook_trade_recorder.py` (config `conf/scripts/conf_orderbook_trade_recorder.yml`) | Samples the book, captures trades, writes files |
| Loader | `backtest_data.py` | Reads files; time-ordered replay; pandas loaders |
| Engine | `backtest_engine.py` | Simulated exchange plus strategy driver |
| Strategies | `backtest_engine.SimpleMarketMaker`, `backtest_limit_chaser.LimitChaser` | Example and ported strategies |

---

## 2. Capture

### 2.1 Exchange feed (CoinDCX)

On connect, the data source joins two socket.io channels per pair. The market prefix differs by pair (`B-` for USDT pairs, `I-` for INR pairs), so it is read from `/exchange/v1/markets_details` rather than hard-coded.

- `B-BTC_USDT@orderbook@10`
- `B-BTC_USDT@trades`

It receives three event types, each wrapped as `{"event": ..., "data": "<JSON string>"}`:

| Event | Payload (after unwrapping) | Used as |
|---|---|---|
| `depth-update` | `{"vs": seq, "bids": {px: qty}, "asks": {...}, "s": "BTCUSDT"}`; qty `0` means remove | Diff |
| `depth-snapshot` | Same shape, full top of book, about 1/s | Snapshot (resync) |
| `new-trade` | `{"T": ms, "p": px, "q": qty, "m": 1/0, "s": "B-BTC_USDT"}` | Trade |

- **`m` is "buyer is maker".** `m=1` means the aggressor **sold**.
- **`vs` is the exchange sequence number** that orders snapshots and diffs.
- **The REST snapshot has no `vs`, so it gets uid `0`.** That way the first streamed diffs are not rejected as stale, and the first streamed snapshot takes over.

### 2.2 Book maintenance (Hummingbot core)

The data source only queues raw payloads. Hummingbot's base class parses them and routes them to `OrderBookTracker`:

- **Diff:** rejected if older than the book's `snapshot_uid`, otherwise applied with `apply_diffs`. Recent diffs are kept in a short window.
- **Snapshot:** applied with `restore_from_snapshot_and_diffs`, which replays any newer diffs from that window. This is what keeps the local book from drifting.
- **Trade:** `apply_trade` updates the last price and fires `OrderBookEvent.TradeEvent` to listeners.

When the network drops and comes back, the connector **rebuilds its `OrderBook` objects**. Anything holding a reference to the old object stops receiving updates; see 2.3.

### 2.3 Recorder (`orderbook_trade_recorder.py`)

| Concern | Design |
|---|---|
| Book capture | An async task every `book_sample_ms` (100) reads the top `depth` (20) levels per side. A row is written **only if the levels changed**, so a quiet market produces no rows. |
| Trade capture | A `TradeEvent` listener on each pair's `OrderBook`. Each trade is stamped with the receive time and the best bid/ask from the last sample. |
| Reconnects | Each tick checks whether `get_order_book(pair)` is a new object and re-attaches the trade listener if so. Without this, trades stop after the first reconnect. |
| Buffering | Rows go to in-memory lists that are flushed every `flush_sec` (5) and on stop. |
| Crash safety | Each flush appends one complete gzip block to the file. A crash loses at most the unflushed 5 s, plus a possibly truncated last block, which the loader ignores. |
| Rotation | The file name carries the UTC date, so files roll over at UTC midnight. |
| Supervision | `run_recorder.ps1` restarts the process after any exit. `HEADLESS_MQTT=false` lets headless mode run without an MQTT broker. |

**What a book row records:** the top-20 levels as Hummingbot's book held them at the sampling moment, not every exchange diff. CoinDCX's 10-level channel gives about 1–2 changes per second, so a 100 ms sample captures essentially all of them.

---

## 3. Storage

```
data/market_data/coindcx/BTC-USDT/
    2026-10-21_book.jsonl.gz
    2026-10-21_trades.jsonl.gz
```

**Book row**
```json
{"ts": 1792563135.004549, "uid": 165896, "b": [[82717.88, 4.41131], ...], "a": [[82717.89, 3.08458], ...]}
```
| Field | Meaning |
|---|---|
| `ts` | Local receive time, epoch seconds. Used as the replay clock. |
| `uid` | `max(snapshot_uid, last_diff_uid)`, i.e. the exchange `vs` |
| `b`, `a` | `[price, qty]` levels, best price first, up to `depth` |

**Trade row**
```json
{"ts": 1792563135.03287, "ets": 1792563135.022, "id": 1792563135022, "px": 82717.89, "qty": 0.00009, "side": "buy", "bb": 82717.88, "ba": 82717.89}
```
| Field | Meaning |
|---|---|
| `ts` | Local receive time (same clock as book rows) |
| `ets` | Exchange trade time |
| `id` | CoinDCX trade id. It is the ms timestamp, so it is **not unique**: fills from one sweep share it. |
| `side` | Aggressor (taker) side |
| `bb`, `ba` | Best bid/ask the recorder held when the trade arrived |

**Why gzip JSON Lines:** writes are append-only and survive crashes, files are 5–10× smaller than CSV, and rows can be read one at a time. Plain JSON would need the whole file in memory and can't be appended. Convert to CSV or Parquet for analysis; section 7 has the commands.

**Volume (measured):** BTC-USDT gives about 1 book row/s and about 2–4 trades/s, roughly 10–20 MB per pair per day.

---

## 4. Loading & replay (`backtest_data.py`)

| Function | Returns | Use |
|---|---|---|
| `iter_rows(connector, pair, kind, start, end)` | Generator of dicts, day by day | Low-level access |
| `replay(connector, pair, start, end)` | Generator of `(kind, ts, event)` in `ts` order (`heapq.merge` of book and trades) | Backtest input; memory stays flat over long ranges |
| `load_books(..., levels=N)` | DataFrame: `bid_px_i`, `bid_qty_i`, `ask_px_i`, `ask_qty_i`, `mid`, `spread` | Analysis |
| `load_trades(...)` | DataFrame of trades | Analysis |

Both streams use the same local clock (`ts`), so merging them reproduces what a live strategy on this machine would have seen, in the order it would have seen it.

---

## 5. Backtest engine (`backtest_engine.py`)

### 5.1 Event loop

For each `(kind, ts, event)` from `replay()`:

```
now = ts
1. order lifecycle      cancels whose (cancel_ts <= now) take effect           → strategy.on_cancel
                        orders whose (active_ts <= now) arrive at the exchange:
                          post-only & would cross  → removed                   → strategy.on_reject
                          otherwise                → take crossing liquidity   → strategy.on_fill (taker)
2a. kind == "book"      book = event
                        resting orders crossed by the new book are filled      → strategy.on_fill (maker)
                        strategy.on_book(bt, book)
                        record equity point (cash + position × mid)
2b. kind == "trade"     trade matched against resting orders                   → strategy.on_fill (maker)
                        strategy.on_trade(bt, trade)
3. stop if strategy.is_done(bt)
```

Strategy actions (`buy`, `sell`, `cancel`) are time-stamped `now` and only take effect after `latency_ms`. This reproduces the round trip a live strategy pays.

### 5.2 Order model

```
            place (now)                   active_ts = now + latency
   strategy ────────────► PENDING ───────────────────────────────► ARRIVED / RESTING ──► FILLED
                                │ post-only & crosses                 │      ▲    │
                                ▼                                     │      └────┘ partial fills
                             REJECTED                     cancel(now) │
                                                                      ▼
                                                     CANCEL PENDING (can still fill)
                                                                      │ cancel_ts = now + latency
                                                                      ▼
                                                                  CANCELLED
```

### 5.3 Fill rules

| Situation | Rule | Price | Fee |
|---|---|---|---|
| Order arrives priced through the book | Works through the recorded opposite levels up to the limit price | Each level's price | Taker |
| Order arrives as post-only and would cross | Rejected | — | — |
| Resting buy, recorded **taker sell** at `px < order price` | Fill `min(remaining, trade qty)`; the trade's quantity is shared across our orders best price first | Order price | Maker |
| Same, but trade `px == order price` | Fills only with `fill_at_touch=True` (assumes we were first in the queue) | Order price | Maker |
| Resting order, new book shows the opposite side at or through its price | Fill up to the crossing quantity | Order price | Maker |
| Taker **buy** trades | Never fill resting buys (and taker sells never fill resting sells) | — | — |

The default (fill only on trades *through* our price) is **conservative**. `fill_at_touch` is **optimistic**. Running both gives a range for the true result.

### 5.4 Accounting

- `position` (base) and `cash` (quote) start at 0, relative to the starting balance. Spot balance limits are **not** enforced.
- Fees are `price × qty × maker_fee | taker_fee`, deducted from cash.
- PnL is `cash + position × mid`, recorded at every book event, which gives the equity curve and max drawdown.

### 5.5 Strategy interface

```python
class Strategy:
    def on_start(self, bt): ...
    def on_book(self, bt, book): ...          # {"ts","uid","b","a"}
    def on_trade(self, bt, trade): ...        # {"ts","ets","id","px","qty","side","bb","ba"}
    def on_fill(self, bt, order, price, qty, maker): ...
    def on_reject(self, bt, order): ...
    def on_cancel(self, bt, order): ...
    def is_done(self, bt) -> bool: ...
```

- **Read:** `bt.now`, `bt.book`, `bt.best_bid`, `bt.best_ask`, `bt.mid`, `bt.position`, `bt.cash`, `bt.orders`, `bt.open_orders`
- **Act:** `bt.buy(px, qty, post_only=False)`, `bt.sell(...)`, `bt.cancel(id)`, `bt.cancel_all()`

### 5.6 Outputs

`BacktestResult` contains:
- `fills` (DataFrame): ts, order_id, side, price, qty, maker, fee, position
- `equity` (DataFrame): ts, pnl, position
- `summary()`: fills, volume, fees, final position, PnL, max drawdown
- `save(dir)`: writes the two DataFrames as CSV

Strategies can add their own reports. `LimitChaser.report()` gives one row per chase with time to fill, slippage vs arrival mid and best price, renews, rejects and status.

---

## 6. Strategies

Live Hummingbot scripts call the exchange connector directly, so they can't run inside the engine unchanged. Each one is **ported**: its decision logic is copied into a `Strategy` subclass that the engine drives.

| Port | Mirrors | Notes |
|---|---|---|
| `SimpleMarketMaker` (`backtest_engine.py`) | Symmetric quoting around mid | Example and template |
| `LimitChaser` (`backtest_limit_chaser.py`) | `OrderExecutor` with `LIMIT_CHASER` as used by `scripts/limit_chaser.py` | Post-only orders, one-way repricing on `refresh_threshold`, 10 retries, full-size renew (it can overfill like the live executor). Repeats chases for statistics. |

**Planned: replay connector.** A fake connector would feed the recorded data into a real `OrderBook`, apply the fill rules in 5.3, emit Hummingbot's standard order events, and run the Hummingbot clock in backtest mode. Then any script or v2 controller would run **unchanged** and per-script ports would no longer be needed.

---

## 7. Operations

```powershell
# Record
$env:CONFIG_PASSWORD = "<password>"; .\run_recorder.ps1          # start (background, auto-restart)
.\run_recorder.ps1 -Stop                                          # stop
Get-Content logs\logs_conf_orderbook_trade_recorder.log -Tail 20 -Wait

# Inspect
.\.venv\Scripts\python.exe backtest_data.py coindcx BTC-USDT 2026-10-21 2026-10-28

# Backtest
.\.venv\Scripts\python.exe backtest_engine.py coindcx BTC-USDT 2026-10-21 2026-10-28 --spread-bps 2 --out results_mm
.\.venv\Scripts\python.exe backtest_limit_chaser.py coindcx BTC-USDT 2026-10-21 2026-10-28 --distance 0.0001 --refresh-threshold 0.0002 --out results_chaser
```

**Health check:** while recording, both files' last-write times should be seconds old, and the end times printed by `backtest_data.py` for the book and for trades should match. If trades lag behind the book, check the log for a reconnect.

---

## 8. Assumptions & limits

| Area | Limitation | Effect |
|---|---|---|
| Queue position | Unknown | Fill rate is uncertain between the conservative default and `fill_at_touch` |
| Market impact | Our orders don't change recorded trades or book | Fine for small size, optimistic for large |
| Depth | Top 20 levels only; book sampled, not every diff | Large taker sweeps beyond level 20 are not priced |
| Clock | Local receive time, so it includes network delay | Matches a live bot on the same machine; true exchange latency is not modelled separately |
| Gaps | No data while the recorder or network is down; no history before recording started | Backtest only over continuous periods; check timestamps |
| Balances | Spot balances and short selling are not enforced | Position can go negative |
| Fees | Flat maker/taker rates passed in | Set to your actual CoinDCX tier |
| Exchange | Capture is CoinDCX-specific; engine and file format are exchange-agnostic | Other exchanges need only a working connector feed |
