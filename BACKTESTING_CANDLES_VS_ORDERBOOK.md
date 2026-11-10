# Backtesting: Candles vs Order Book

Hummingbot can be backtested two ways:

- **Candles:** Hummingbot's built-in v2 backtesting engine, run on OHLCV candles.
- **Order book:** our recorder and replay backtester, run on recorded L2 order book data and every public trade.

This doc compares how each one gets its data, stores it, processes it and simulates fills, and when to use which.

## At a glance

| | Candles (built-in) | Order book (ours) |
|---|---|---|
| **Data** | OHLCV bars (open, high, low, close, volume), e.g. 1m | Top 20 book levels per side, plus every public trade |
| **Source** | Exchange REST API (historical candles) | Exchange websocket (live stream) |
| **When collected** | On demand, at backtest time; any past date | Only while the recorder runs; nothing before it started |
| **Stored** | Not stored. Fetched into memory for each run | Gzipped JSON Lines on disk, one file per day |
| **Time resolution** | One row per candle (1m default) | Book about 1/s; trades individually (BTC-USDT about 4/s) |
| **Fill model** | Price touched → filled | Fills only from real trades that go through the order price, capped by trade size |
| **Sees** | Price path | Spread, depth, queue, trade flow, latency |
| **What it backtests** | v2 controllers (position and DCA executors) | Any strategy written as Python callbacks; limit chaser included |
| **Output** | PnL, win rate, drawdown, Sharpe, profit factor, close types | Fills, PnL/equity curve, fees, time to fill, price vs arrival |
| **Code** | `hummingbot/strategy_v2/backtesting/` | `scripts/orderbook_trade_recorder.py`, `backtest_data.py`, `backtest_engine.py`, `backtest_limit_chaser.py` |

## 1. Candle backtesting (built-in)

**How data comes in.** `BacktestingDataProvider.get_candles_feed()` calls `CandlesFactory.get_candle(config).get_historical_candles(...)`. That downloads OHLCV bars for the requested window from the exchange's REST API (CoinDCX is supported via `CoinDCXSpotCandles`). It also pulls a warm-up buffer of `max_records` candles before the start, so indicators have history.

**How it is stored.** In memory only, as a pandas DataFrame keyed `connector_trading-pair_interval`. Nothing is written to disk, so every new run downloads again.

**How it is processed** (`BacktestingEngineBase.run_backtesting`):
1. The controller computes its features (indicators, signals) over the candles.
2. The engine walks the candles one row at a time (`backtesting_resolution`, default `1m`). For each row it sets price = candle close and asks the controller for actions.
3. Each "create executor" action is simulated over the rest of the candles (`PositionExecutorSimulator` or `DCAExecutorSimulator`):
   - **Limit entry:** filled at the first candle whose close reaches the entry price.
   - **Take-profit / trailing stop:** evaluated on close-to-close returns.
   - **Stop-loss:** evaluated on the candle's low (buy) or high (sell).
   - **Fees:** a flat `trade_cost` (default 0.06%).
4. `summarize_results()` reports net PnL, volume, accuracy, max drawdown, Sharpe, profit factor and close types.

**Limits.**
- Inside a candle, the order in which prices happened is unknown.
- There is no spread, depth or queue, so a limit order counts as filled whenever price touches it.
- Results for maker and short-term strategies (market making, chasers) are optimistic, often by a lot.
- Only v2 controllers can be backtested. Scripts such as `limit_chaser.py` cannot.

## 2. Order book backtesting (ours)

**How data comes in.** `run_recorder.ps1` runs Hummingbot headless with `scripts/orderbook_trade_recorder.py`:
- The CoinDCX connector subscribes to the websocket channels `B-BTC_USDT@orderbook` and `@trades`.
- It applies `depth-update` diffs to its local book, and resyncs from `depth-snapshot` events that arrive about once a second.
- It emits every trade as an event.

The recorder then:
- samples the top 20 levels every 100 ms and writes a row only when they changed;
- writes every trade immediately, together with the best bid and ask at that moment.

Several connector bugs were fixed to make this work: message unwrapping, the per-market `B-`/`I-` pair prefix, snapshot sequence IDs, double parsing, and the buy/sell side mapping.

**How it is stored.**

```
data/market_data/<connector>/<PAIR>/<YYYY-MM-DD>_book.jsonl.gz
    {"ts": recv time, "uid": exchange sequence, "b": [[px, qty] x20], "a": [[px, qty] x20]}
data/market_data/<connector>/<PAIR>/<YYYY-MM-DD>_trades.jsonl.gz
    {"ts", "ets": exchange time, "id", "px", "qty", "side": taker side, "bb": best bid, "ba": best ask}
```

- **Appends:** data is added every 5 s as a new gzip block, so a crash loses at most 5 s.
- **Rotation:** files roll over at UTC midnight (5:30 AM IST).
- **Size:** roughly 10–20 MB per day for BTC-USDT.
- **Gaps:** there is no history from before the recorder started, and any downtime leaves a gap.

**How it is processed.**
1. `backtest_data.replay()` streams book rows and trades from disk, merged in time order. Memory stays flat over long ranges.
2. `backtest_engine.Backtest` feeds every event to the strategy (`on_book`, `on_trade`, `on_fill`).
3. Orders are simulated as follows:
   - **Latency:** placing and cancelling each take `latency_ms` (100 ms by default) to reach the exchange.
   - **Post-only orders:** rejected if they would cross the book on arrival.
   - **Crossing orders:** fill as taker, working through the recorded levels.
   - **Resting orders:** fill only when a recorded trade from the opposite side goes **through** their price, capped by that trade's size. With `--fill-at-touch`, a trade **at** the price also fills them.
   - **Fees:** separate maker and taker rates.
4. Reports:
   - fills and PnL marked to mid;
   - for the limit chaser, also fill rate, time to fill, price vs arrival, renews and rejects per chase.

**Limits.**
- Our position in the queue is unknown. Fills strictly through our price are cautious; `--fill-at-touch` is optimistic. Run both to get a range.
- Our orders are assumed not to move the market, which is fine for small sizes.
- Only 20 levels are recorded.

## When to use which

| Question | Use |
|---|---|
| Does a directional or indicator signal work over months? | **Candles.** Long history is available immediately. |
| Will my limit or maker orders actually fill, how fast, and at what price? | **Order book** |
| Market making, the limit chaser, spread capture, execution quality | **Order book** |
| Quick first screening of many configs | **Candles**, then confirm the best configs on **order book** data |

## Commands

```powershell
# Record (runs in the background; stop with -Stop)
$env:CONFIG_PASSWORD = "<password>"; .\run_recorder.ps1

# Check the recorded data
.\.venv\Scripts\python.exe backtest_data.py coindcx BTC-USDT 2026-10-21 2026-10-28

# Backtest on order book data
.\.venv\Scripts\python.exe backtest_engine.py coindcx BTC-USDT 2026-10-21 2026-10-28 --spread-bps 2 --out results
.\.venv\Scripts\python.exe backtest_limit_chaser.py coindcx BTC-USDT 2026-10-21 2026-10-28 --distance 0.0001 --refresh-threshold 0.0002
```

Candle backtests are run from Python: `BacktestingEngineBase().run_backtesting(controller_config, start, end, "1m")`.
