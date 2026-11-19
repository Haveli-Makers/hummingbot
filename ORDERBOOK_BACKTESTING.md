# Order Book and Public Trades Backtesting

How recorded order book and public trade data flows from an exchange to a backtest result, where it is stored and
why, and what is needed to run it in production on AWS.

Status as of 2026-11-16. Items marked **Not built yet** are designed but not implemented.

---

## 1. Summary

- **What it does.** A recorder saves the live order book and every public trade for chosen exchange pairs. A backtest
  later replays that data and runs a strategy controller against it, simulating order fills from the real book and
  trades instead of from candles.
- **Three codebases.** The dashboard (frontend) collects the settings and shows results. The API server (backend)
  exposes one endpoint that runs the backtest. The hummingbot repo holds the recorder, the data loader and the
  backtesting engine.
- **One recorder for every exchange.** `standalone_recorder.py` reuses hummingbot's own connectors, so any exchange
  hummingbot supports can be recorded with no extra code and no API keys.
- **Storage is compressed files, not a database.** One file per pair per day, kept on local disk and, in production,
  archived to Amazon S3. Section 7 compares the alternatives.
- **The data checks itself.** Every hour (configurable) the recorder compares what it has stored with a fresh order
  book from the exchange, and the result is written into the data.

---

## 2. End-to-end flow

There are two independent flows. Recording runs all the time. Backtesting runs on demand and only reads what the
recorder has already written.

```
RECORDING (runs 24/7, one process per exchange)

  Exchange public feed ──► hummingbot connector ──► standalone_recorder.py ──► daily .jsonl.gz files
  (WebSocket / REST)       (no API keys)            (snapshots, differences,    data/market_data/
                                                     trades, verification)      <connector>/<PAIR>/


BACKTESTING (runs when a user clicks Run)

  Dashboard page                API server                         hummingbot repo
  ───────────────               ───────────────────────────        ─────────────────────────────────
  Orderbook Backtest   POST     /backtesting/                      OrderbookBacktestingEngine
  page: connector,   ───────►   run-controller-orderbook-   ────►    │
  pair, dates, fees,            backtest                             ├─ backtest_data.py
  latency, controller           (routers/backtesting.py)             │    reads the daily files and
  config                                                             │    rebuilds whole order books
                                                                     ├─ backtest_engine.py
  charts and tables   ◄───────  JSON: executors, results,   ◄────    │    matches orders against the
  (PnL, position,               fills, equity, fees,                 │    replayed books and trades
  fills)                        candles, reconciliation              └─ the strategy controller
```

### The pieces and where they live

| Layer | File | Role |
|---|---|---|
| Frontend | `hm-dashboard/dashboard/frontend/pages/config/orderbook_backtest/app.py` | The Orderbook Backtest page: inputs and results |
| Frontend | `hm-dashboard/dashboard/frontend/visualization/orderbook_backtest.py` | PnL and position chart with fill markers, strategy report table |
| Frontend | `hm-dashboard/dashboard/api_client/routers/backtesting.py` | Sends the request to the API server |
| Backend | `hm-api/api-server/routers/backtesting.py` | `POST /backtesting/run-controller-orderbook-backtest` |
| Backend | `hm-api/api-server/models/backtesting.py` | Request shape (`ControllerOrderbookBacktestConfig`) |
| Hummingbot | `standalone_recorder.py` | The recorder |
| Hummingbot | `backtest_data.py` | Reads the files, rebuilds books, re-checks verifications |
| Hummingbot | `backtest_engine.py` | Event-driven order matching against replayed books and trades |
| Hummingbot | `hummingbot/strategy_v2/backtesting/orderbook_backtesting_engine.py` | Runs a strategy_v2 controller on top of the matching engine |

### What the API request carries

| Field | Meaning | Default |
|---|---|---|
| `connector`, `pair` | Which recording to replay | required |
| `start`, `end` | Date range, `YYYY-MM-DD` in UTC | `end` optional |
| `maker_fee`, `taker_fee` | Fee rates applied to simulated fills | 0.001 each |
| `latency_ms` | Delay before a placed or cancelled order reaches the simulated exchange | 100 |
| `fill_at_touch` | Fill resting orders when the price touches them, not only when it trades through | false |
| `config` | The controller configuration, as a dictionary or the name of a saved config file | required |

The response contains the executors, summary results, the fill list, the equity curve (time, PnL, position), fees,
final position, a reconciliation report, the tick size and the candles used.

---

## 3. Recording

### How it starts

```
python standalone_recorder.py --connector coindcx --pairs BTC-USDT,ETH-USDT --depth 10 --verify-sec 3600
```

| Option | Meaning | Default |
|---|---|---|
| `--connector` | hummingbot connector name; `--list` prints all of them | `coindcx` |
| `--pairs` | Comma-separated pairs, all recorded over one connection | `BTC-USDT` |
| `--depth` | Order book levels per side to record; 0 records the whole book | 20 |
| `--verify-sec` | Seconds between verifications | 3600 |
| `--verify-grace-sec` | How long a verification waits for the two books to agree | 5 |
| `--flush-sec` | How often buffered rows are written to disk | 5 |
| `--output-dir` | Where files go | `data/market_data` |

### What happens after the command

```
1. Build the exchange's hummingbot connector (no API keys, no trading)
   and take its public order book data source
                │
                ▼
2. The data source connects to the exchange
   ├─ live stream ──► differences, pushed snapshots, public trades
   └─ REST request ─► one full order book to start from
                │
                ▼
3. Write the starting book as a snapshot row
                │
                ▼
4. For every message:
   ├─ difference ─► apply it to the local book
   ├─ snapshot ───► replace the local book
   │      then: did the top N levels change?
   │        ├─ no ──► write nothing
   │        └─ yes ─► write a difference row holding only the changed levels
   └─ trade ──────► write a trade row
                │
                ▼
5. Every 5 s append the buffered rows to the day's files
   (at UTC midnight a new file starts with a full snapshot)
                │
                ▼
6. Every --verify-sec compare the local book with a fresh one from the exchange (section 5)
```

### Why this works for every exchange

Exchanges differ in what they send: some send only differences, some only full snapshots, some both. The recorder
does not need to know. It applies whatever arrives to a local book, exactly as hummingbot's own order book tracker
does, and then stores the change in the top N levels. The consequences:

- A repeated snapshot that changes nothing writes nothing.
- A difference that only touches levels deeper than N writes nothing.
- An exchange that only sends snapshots still produces small difference rows.

What each exchange was observed to send in live tests:

| Exchange | Feed | Observed |
|---|---|---|
| CoinDCX | Differences about once a second, plus a full snapshot every 2–3 s | Every difference changed the book; the snapshots were redundant |
| Binance | Differences several times a second | About 660 stored changes per pair in 95 s at depth 20 |
| WazirX | REST poll every 5 s (no stream in the hummingbot connector) | Most polls were identical to the previous one |

---

## 4. File format

### Layout

```
data/market_data/<connector>/<PAIR>/<YYYY-MM-DD>_book.jsonl.gz
data/market_data/<connector>/<PAIR>/<YYYY-MM-DD>_trades.jsonl.gz
```

Each file is gzip-compressed text with one JSON record per line. Days are UTC. A file is only appended to during its
own day and never changes afterwards.

### Book rows

A **snapshot** is the whole recorded book:

```json
{"t":"s","ts":1794773047.609,"uid":0,"b":[[64820.0,4.45795],[64819.99,0.00008]],"a":[[64820.01,0.09606]],"src":"start"}
```

A **difference** lists only the levels that changed:

```json
{"t":"d","ts":1794773048.345,"uid":99134,"b":[[64822.99,1.91436],[64820.0,0.0]],"a":[[64823.0,0.4872]]}
```

| Field | Meaning |
|---|---|
| `t` | `"s"` snapshot, `"d"` difference |
| `ts` | Time the recorder received the message, Unix seconds on the recorder's clock |
| `uid` | The exchange's update number for that message, passed through by the connector |
| `b`, `a` | Bids and asks as `[price, quantity]`. In a difference, quantity 0 means remove that level |
| `src` | Snapshots only: `start` (recorder started), `rotate` (UTC midnight), `verify` (a verification) |
| `ok`, `bad` | Verification snapshots only: whether it matched the exchange, and how many levels differed if not |

Files written by the older recorders have no `t` field; every row is a whole book. The loader still reads them.

### Trade rows

```json
{"ts":1794770633.76,"ets":1794770633.52,"id":"6865450220","px":64938.36,"qty":0.00008,"side":"sell","bb":64938.36,"ba":64938.37}
```

| Field | Meaning |
|---|---|
| `ts` | Time the recorder received the trade |
| `ets` | The exchange's own trade time |
| `id` | The exchange's trade ID (CoinDCX has none, so the trade time in milliseconds is used) |
| `px`, `qty` | Price and quantity |
| `side` | The taker's side: `buy` or `sell` |
| `bb`, `ba` | Best bid and ask in the recorded book when the trade arrived |

Book rows use the recorder's receive time because the backtest merges books and trades into one timeline, which needs
one clock, and hummingbot does not pass through an exchange timestamp for book updates on every exchange.

---

## 5. Verification

### Live, in the recorder

Every `--verify-sec` the recorder asks the exchange for a fresh order book and compares its top N levels with the
book rebuilt from the stored snapshot and differences.

Live updates keep arriving while that request is in flight, so an instant comparison would often fail for no real
reason. Instead both books keep receiving the same updates for a few seconds (`--verify-grace-sec`):

- **They agree at some moment in that window.** The stored data is good. A snapshot row with `"ok": true` is written.
- **They never agree.** The local book is replaced by the exchange's, and a snapshot row with `"ok": false` and
  `"bad": <levels that differed>` is written. An error therefore cannot last longer than one interval.

### Offline, from the files

```
python backtest_data.py coindcx BTC-USDT 2026-11-16
```

The summary includes a line such as:

```
verifications: 5   matched exchange: 5   failed (book resynced): 0   stored rows damaged: 0
```

- **matched exchange / failed** is what the recorder found live.
- **stored rows damaged** re-reads the files and checks that each snapshot plus the differences after it rebuilds to
  the next verification snapshot. Anything above 0 means rows are missing or corrupt on disk.

---

## 6. Backtesting

1. **Request.** The dashboard posts the settings to the API server. The API server builds the controller
   configuration and calls `OrderbookBacktestingEngine.run_backtesting`.
2. **Checks.** The engine refuses runs it cannot answer: no recorded data for that pair and range, a controller whose
   pair differs from the recording, controllers that trade several markets at once, and controllers that need a live
   signal stream.
3. **Loading.** `backtest_data.py` reads the day files in order. A snapshot row replaces the book it is holding; a
   difference row is applied on top. After each row it hands over one complete book, so the engine never sees a raw
   difference. Checkpoint snapshots identical to the current book are skipped, so nothing is replayed twice.
4. **Replay.** Books and trades are merged into one time-ordered stream.
5. **Matching** (`backtest_engine.py`):
   - A resting order fills when a recorded trade on the opposite side trades through its price, capped by the trade
     size. With `fill_at_touch` it also fills at its price, which is optimistic because queue position is ignored.
   - A resting order also fills if a later book crosses it.
   - An order priced through the current book fills immediately as a taker, walking the levels.
   - Placing and cancelling both take `latency_ms`; an order can still fill while its cancel is in flight.
6. **Result.** Executors, fills, equity curve, fees and a reconciliation report go back to the dashboard, which draws
   the PnL and position chart with fill markers.

---

## 7. Storage choice

### The decision

**Compressed daily files, on local disk while the day is being recorded, archived to Amazon S3.** No database.

### What the data needs

| Need | Detail |
|---|---|
| Write pattern | Append-only. One writer per exchange. Nothing is ever updated or deleted |
| Read pattern | A backtest reads whole days for one pair, in time order, start to end. It never looks up single rows |
| Volume | Grows without limit: every pair on every exchange, every day |
| Durability | Lost data cannot be re-downloaded later; exchanges do not serve historical order books |
| Cost | Should stay small as history accumulates |

### Options compared

| Option | Fits the read pattern | Storage cost | Things to operate | Verdict |
|---|---|---|---|---|
| **Daily gzip files on S3 (chosen)** | Yes: one object is exactly one pair-day | Lowest, about $0.023 per GB-month | None | Chosen |
| Files on EBS only | Yes | About $0.08 per GB-month, and the volume must be resized as it fills | Disk sizing, backups | Good as the short-term buffer, not as the archive |
| Parquet files on S3 | Yes, and faster to load into tables | Similar to gzip files | A conversion job | A possible later optimisation, not needed now |
| TimescaleDB / PostgreSQL | Works, but rows must be queried back out in order | Database storage costs several times S3, plus the instance | A database server, backups, upgrades | Rejected |
| ClickHouse | Excellent for analytics across many pairs | Good compression, but needs a server or a paid cloud service | A cluster or a subscription | Rejected for now |
| QuestDB / InfluxDB | Built for time series and fast ingestion | A server with attached disk | A database server | Rejected |
| DynamoDB | Poor: reading a whole day means paging through millions of items | Charged per read and write, expensive at this volume | None | Rejected |

Prices are approximate public list prices and vary by region.

### Why files on S3

- **The data is already in the shape the backtest reads.** One pair-day is one file, read once from start to end. A
  database would store the same rows and hand them back in the same order, with a query layer in between that adds
  nothing.
- **Cheapest storage that does not need managing.** S3 has no capacity to plan, no server to patch and no backups to
  schedule, and it stores each object redundantly.
- **Finished files never change.** That makes upload a simple copy, and lets old days move automatically to a cheaper
  S3 tier.
- **Failure stays small.** A corrupt file affects one pair on one day. A database problem can affect everything.
- **No lock-in.** The files are plain compressed JSON that any tool can read.

### When to revisit

A database or Parquet becomes worth it if the usage changes to one of these:

- Queries across many pairs and days at once (for example, average spread per exchange per hour over a year).
  ClickHouse, or Parquet queried with Amazon Athena, suits that.
- Backtests that need only a short time slice out of very large days, where reading the whole day is wasteful.
- Load time of the JSON files becoming the slowest part of a backtest. Parquet would help.

In each case the files on S3 remain the source of truth and the other store is built from them.

### Expected volume

Measured from short live samples at 20 levels and extrapolated, so treat these as order of magnitude:

| Market | Book data per pair per day |
|---|---|
| Binance BTC-USDT | about 30 MB |
| CoinDCX BTC-USDT | about 5–10 MB |
| WazirX BTC-USDT | under 1 MB |

Trades add a similar amount on busy pairs. Example: 10 exchanges with 5 pairs each at 30 MB is about 1.5 GB a day,
or 45 GB a month, which is roughly $1 a month on S3 for each month of history kept.

---

## 8. Moving to production on AWS

### Target layout

```
EC2 instance (recorders)                         S3 bucket
  systemd: recorder@binance   ─┐                 market_data/<connector>/<PAIR>/<day>_*.jsonl.gz
  systemd: recorder@coindcx   ─┼─► EBS volume ──────► daily (or hourly) sync
  systemd: recorder@wazirx    ─┘   data/market_data        │
                                                           ▼
EC2 instance (API server + dashboard)            download the days a backtest needs
  backtest reads a local cache of the files  ◄──────────────┘
```

### Recorders: one systemd service per exchange

One process records one exchange with all of its pairs. A single template unit covers every exchange.

`/etc/systemd/system/recorder@.service`:

```ini
[Unit]
Description=Order book recorder for %i
After=network-online.target
Wants=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/hummingbot
EnvironmentFile=/etc/recorder/%i.env
ExecStart=/home/ubuntu/hummingbot/.venv/bin/python standalone_recorder.py --connector %i --pairs ${PAIRS} --depth ${DEPTH} --verify-sec ${VERIFY_SEC}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

One settings file per exchange, for example `/etc/recorder/binance.env`:

```
PAIRS=BTC-USDT,ETH-USDT,SOL-USDT
DEPTH=20
VERIFY_SEC=3600
```

```bash
sudo systemctl enable --now recorder@binance recorder@coindcx recorder@wazirx   # start, and start on boot
systemctl list-units 'recorder@*'                                               # what is running
journalctl -u recorder@binance -f                                               # one exchange's log
```

This unit file has been written but **not yet run on a Linux machine**.

### Storage steps

| Step | What | Status |
|---|---|---|
| 1 | Recorders write to an EBS volume of 20–50 GB | Works today |
| 2 | A systemd timer runs `aws s3 sync data/market_data s3://<bucket>/market_data`, then deletes local files older than a few days | **Not built yet** (no code change needed, only the timer) |
| 3 | An S3 lifecycle rule moves files older than about 90 days to the Infrequent Access tier | **Not built yet** (bucket setting) |
| 4 | Before a backtest, the API server downloads the needed days from S3 into a local cache | **Not built yet** (`backtest_data.py` reads local files only) |

Until step 4 exists, the API server must run on a machine where the recorded files are present under
`<hummingbot repo>/data/market_data`.

If the recorder instance is lost, everything recorded since the last upload is lost. Syncing hourly instead of daily
reduces that to an hour.

### How the API server finds the engine

The API server imports `backtest_data.py` and `backtest_engine.py` from the root of the hummingbot repo, which it
locates from the installed `hummingbot` package. So on the API server, hummingbot must be installed from a checkout of
this repo (not from a published package), with those two files present at its root.

### What to deploy and what not to

| Deploy | Do not copy to the server |
|---|---|
| `standalone_recorder.py` | `.env`, `conf/connectors/*.yml`, `conf/.password_verification`, `certs/` (keys and passwords; the recorder needs none) |
| `backtest_data.py`, `backtest_engine.py` | `logs/`, `build/`, `.venv/`, `.pytest_cache/` (local artifacts) |
| The `hummingbot/` package | |
| The API server and dashboard | |

Git already ignores everything in the right-hand column, so a `git clone` on the server will not bring them.

### Scaling notes

- **Rate limits on polled exchanges.** WazirX is fetched over REST every 5 s per pair against a limit of 2 requests
  per second. Errors appeared at start-up with only two pairs, so many WazirX pairs need a slower poll.
- **Memory.** Each recorder process loads hummingbot. Memory per process has not been measured; check with a few
  running before choosing an instance size.
- **Clock.** Row times come from the recorder's clock, so keep the instance time-synchronised (the AWS default).

---

## 9. Known limitations and open decisions

| Item | Effect | What would fix it |
|---|---|---|
| CoinDCX records only 10 levels | The hummingbot connector subscribes to the 10-level channel. Use `--depth 10` | Change the connector to the 20-level channel. This also changes what live CoinDCX trading sees, so it is an open decision |
| CoinDCX book is wrong for the first few seconds after a start | Its REST order book lags the stream, so the first rows can be crossed until the first pushed snapshot arrives 2–3 s later | Hold back writing until the book is no longer crossed |
| WazirX public trades are not recorded | The hummingbot connector has no trade stream | Add a trade stream to the WazirX connector |
| Recordings from 2–12 Nov 2026 for CoinDCX are corrupt | An older recorder stored differences as whole books. About 60% of rows are wrong | Do not backtest on those days; re-record |
| Only the top N levels are stored | Depth beyond `--depth` cannot be reconstructed later | Record with a larger `--depth`, or 0 for the whole book, at the cost of larger files |
| One market per backtest | Controllers that trade several markets at once are refused | Replay of several recordings together |
| Gaps while a recorder is down | No data exists for that period | The loader's summary lists gaps longer than 60 s; avoid backtesting across them |

Changes made alongside the recorder that affect more than recording:

- The WazirX connector was discarding its own polled order books, so its book refreshed only once an hour. It now
  refreshes every 5 s, which also applies to live WazirX trading.

---

## 10. FAQs

**Do I need API keys to record?**
No. The recorder only reads public data.

**Does the recorder need the hummingbot app running?**
No. It uses hummingbot's connector code as a library, in its own process.

**Why are snapshots and differences in the same file? Will the backtest read both?**
Yes, deliberately. The loader replaces its book on a snapshot and applies a difference on top, then hands the engine a
whole book each time. A difference is never treated as a book.

**Why store differences at all instead of a full book every time?**
Size. On Binance a difference averaged about 10 levels against 40 for a full top-20 book, and on quiet markets most
messages change nothing and are not written.

**Is the `uid` from the exchange?**
Yes, it is the exchange's update number passed through by the connector. Where the exchange gives none, the connector
fills one in (0 for CoinDCX's REST book, the time in milliseconds for WazirX).

**Is `ts` from the exchange?**
No. It is the recorder's clock when the message arrived. Trades also carry `ets`, the exchange's trade time.

**What does a failed verification mean?**
The stored book had drifted from the exchange's, usually because an update was missed during a reconnect. The recorder
corrects itself at that moment. An occasional failure is expected; repeated failures point to a problem with that
exchange's feed or connector.

**How do I change how often it verifies?**
`--verify-sec`, in seconds. For example 900 for every 15 minutes.

**How do I record several pairs or several exchanges?**
Several pairs: list them in `--pairs`. Several exchanges: run one recorder per exchange.

**Can two recorders record the same exchange and pair?**
No. Both would append to the same files and the data would be unusable.

**What happens if the recorder crashes or restarts?**
At most the last 5 seconds of buffered rows are lost. On restart it writes a new starting snapshot into the same day's
file and continues; the loader resets its book on that snapshot. The time in between is a gap.

**How do I check what is being recorded?**
The recorder prints a status line per pair every minute. `python backtest_data.py <connector> <PAIR> <date>` gives a
day's summary, gaps and verification results.

**Which dates do I use?**
UTC dates, both for file names and for the backtest's start and end.

**Why not just use candles?**
Candles cannot say whether a resting limit order would have been filled, or at what size. The recorded book and trades
can, which matters most for market-making and other strategies that rest orders near the touch.

**Can I backtest a period I did not record?**
No. Exchanges do not provide historical order books, so recording has to be running beforehand.
