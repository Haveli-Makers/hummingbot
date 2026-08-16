# Simple Grid

A trend-following strategy that trades one position at a time and re-centres itself on
wherever the last trade closed.

---

## What it does

Pick a starting price — the **anchor**. Simple Grid then watches two levels, one step above
and one step below it, and does nothing until the market reaches one of them.

When a level is reached, it enters **in that direction**: a step up means buy, a step down
means sell. It is joining the move, not betting against it. Once in, it sets a take profit
one step further along and a stop loss one step back. Whichever is hit closes the position,
and the closing price becomes the new anchor. Then it starts watching again.

That is the whole cycle:

```
watch  ->  a level is reached  ->  enter at market  ->  take profit or stop loss
   ^                                                              |
   +---------------- re-anchor at the closing price --------------+
```

Only one position is open at a time. This is not a classic grid that ladders many orders
into the book — it is a single leg that keeps moving with the market.

### Why nothing rests in the book

The entry levels sit on the far side of the market by design. A limit order left at a level
the market has already reached would fill instantly at the wrong price, so the strategy
watches instead and sends a **market order** once the price is genuinely there.

This means `status` will often show no open orders. That is the strategy waiting, not a
hang — the `Waiting for:` line tells you which prices it wants.

---

## A worked example

Anchor at **100**, step **0.5%**, on futures.

| | |
|---|---|
| Long level | 100.50 |
| Short level | 99.50 |

**The market rises to 100.50.** It buys at market. Take profit goes at 101.00, stop loss at
100.00.

- Price reaches **101.00** — take profit fills, +0.5%. New anchor: **101.00**, now watching
  101.505 and 100.495.
- Or price falls to **100.00** — stop loss fires, −0.5%. New anchor: **100.00**, watching
  100.50 and 99.50 again.

**The market falls to 99.50 instead.** On futures it sells short, take profit at 99.00, stop
loss at 100.00. On spot it cannot short — see below.

### Spot vs futures

| | Futures | Spot |
|---|---|---|
| Rise to the upper level | buy | buy |
| Fall to the lower level | sell short | **buy the dip** |
| Sides watched | both | both, but both mean buy |

Spot can only ever hold the base asset, so a fall is treated as a cheaper entry rather than
a chance to go short. Set `initial_entry_mode: long_only` on spot; `both_oco` is rejected.

### The three market shapes

- **Trending** — the best case. Each move triggers an entry in the direction of the trend
  and the take profit is reached.
- **Sideways** — the worst case. Price nudges past a level, triggers an entry, then reverses
  into the stop loss. Repeatedly. This is what `max_loss_quote` exists to stop.
- **Sharp reversal** — one stop loss, then it re-anchors lower and follows the new direction.

---

## Fees decide whether this works

A win and a loss are **not** symmetric, because the two sides pay different fees.

Entries and stop losses are market orders and pay the **taker** fee. Only take profits rest
in the book and pay **maker**. So every round costs one taker fee plus either a maker fee (on
a win) or a second taker fee (on a loss):

```
win  = +step − taker − maker
loss = −step − taker − taker
```

Work it out for your venue's schedule before choosing a step. As an illustration, at 0.02%
maker and 0.06% taker with a 0.5% step, a win is +0.42% and a loss is −0.62% — so you need
roughly **60%** of legs to win, not 50%.

> The `Win rate needed` line in `status` shows the **pre-fee** figure of 50%. The real number
> is always higher. Treat the panel as a floor, not an answer.

There is also a step below which no win rate is profitable, because `step` no longer covers
the round-trip fee. Compute that floor first; it sets the minimum step your venue allows you
to trade at all.

---

## Choosing a pair and a step

The step has to clear fees *and* be reachable. Those pull in opposite directions.

- Too tight and fees eat every round.
- Too wide and nothing ever triggers. A 0.5% step on a pair that moves 0.15% a day will sit
  and watch for hours — correctly, but pointlessly.

Check the pair's typical daily range before committing to a step. Also check the pair's
**minimum notional** — it varies widely between pairs on the same exchange, and a
high-priced pair can require ten times the order size of a cheap one.

Two floors apply at once — a minimum notional *and* a minimum size increment. Quantisation
rounds **down**, so an order sized exactly at the limit can land just under it. Leave
headroom.

---

## Configuration

Two files are needed, with the **same base name** in different directories. `--conf` names
the script one.

| File | Holds |
|---|---|
| `conf/scripts/<name>.yml` | the `controllers_config:` list and global drawdown guards |
| `conf/controllers/<name>.yml` | the strategy settings |

If only the controller file exists you get a short run that does nothing, with this in the
log and no connector created:

```
Failed to load config file ...: No such file or directory: '.../conf/scripts/<name>.yml'
```

### Script file

```yaml
script_file_name: v2_with_controllers.py
markets: {}
candles_config: []
controllers_config:
  - my_simple_grid.yml

max_global_drawdown_quote: 15
max_controller_drawdown_quote: 10
```

### Controller file

```yaml
id: my_simple_grid
controller_name: simple_grid
controller_type: generic

connector_name: binance_perpetual   # your connector
trading_pair: ETH-USDT              # your pair
leverage: 1
position_mode: ONEWAY

total_amount_quote: '9'      # ceiling on capital in use
order_amount_quote: '7'      # per leg

take_profit: '0.005'         # 0.5%
stop_loss: '0.005'
entry_step: null             # null = same as take_profit

trigger_price_type: 1        # 1 = MidPrice
entry_order_type: 1          # 1 = MARKET

initial_entry_mode: both_oco
lock_side_after_first_fill: false

cooldown_after_take_profit: 0
cooldown_after_stop_loss: 60

max_loss_quote: '2'          # the stop condition — always set it
max_loss_pct: null
stop_when_losses_outnumber_wins: false
min_legs_before_count_check: 10

manual_kill_switch: false
candles_config: []
initial_positions: []
```

### What to decide

| Setting | Guidance |
|---|---|
| `trading_pair` | Check its minimum notional and daily range |
| `order_amount_quote` | Per leg. Must clear the minimum notional with headroom |
| `total_amount_quote` | Caps capital in use. Less than 2× the leg size means one leg at a time |
| `take_profit` / `stop_loss` | The step. Must clear fees — see above |
| `max_loss_quote` | **The stop condition.** Always set it |
| `leverage` | Leave at 1 unless you have a specific reason |

### Settings that are easy to get wrong

| Setting | Note |
|---|---|
| `entry_order_type` | Must be **MARKET**. A limit at a level the market has already reached never fills, and every leg times out with no position |
| `trigger_price_type` | `LastTrade` needs the venue to publish trades. Where it does not, the value silently falls back to mid — prefer **MidPrice** so the config says what it does |
| `initial_entry_mode` | `both_oco` needs a non-zero step, and is futures-only |
| `entry_step: '0'` | Enters immediately at the anchor. Useful for testing, not for trading |

---

## Running it

```
connect <your connector>
start --script v2_with_controllers.py --conf my_simple_grid.yml
status
```

Reading `status`:

```
Mid: 1.001050 | Anchor: 1.001000 | Side: both (unlocked)
Waiting for: long above 1.006005 | short below 0.995995 | distance to nearer level: 0.4988%
Legs closed: 3 | TP: 2 | SL: 1 | Realised PnL: -0.0109
Realised: -0.0109 / threshold -0.3000
```

- **`Waiting for`** — the levels it wants, and how far away the nearer one is. If that
  distance moves with price, the strategy is working. Nothing rests in the book while it
  waits.
- **`Anchor`** — `(mid, no leg open yet)` until the first leg closes, then the last closing
  price. It should move after every close.
- **`Legs closed` / `TP` / `SL`** — the running tally.
- **`Realised / threshold`** — how close you are to the halt.

To stop: `stop`, then **check your positions**. Orders and positions are different things;
"no open orders" does not mean you are flat.

---

## When it stops

The strategy halts when peak-to-trough realised losses reach `max_loss_quote` (or
`max_loss_pct`). It stops opening new legs; it does not abandon an open one.

A second condition — halting when losses outnumber wins — exists behind
`stop_when_losses_outnumber_wins`, with `min_legs_before_count_check` as a grace period so a
bad opening run does not end it early. It is **off by default**.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| No orders, ever | Usually correct. Check `distance to nearer level` — if it moves, the strategy is watching. The step may just be wider than the pair's range |
| `Not enough budget to open the position` | The leg does not fit. Lower `order_amount_quote`, but not below the minimum notional. A single occurrence right after a close is a position still settling and clears itself |
| `is not ready. Please wait...` repeating | The connector has not finished starting. The log names the check it is waiting on |
| Every leg times out with no position | `entry_order_type` is not MARKET |
| Nothing runs, log mentions a missing file | The `conf/scripts/` half of the config is missing |

---

## Related

- Executor: `hummingbot/strategy_v2/executors/simple_grid_executor/`
- Controller: `controllers/generic/simple_grid.py`
- Tests: `test/hummingbot/strategy_v2/executors/simple_grid_executor/`,
  `test/controllers/generic/test_simple_grid.py`
