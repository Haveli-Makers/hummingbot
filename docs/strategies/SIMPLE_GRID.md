# Simple Grid

A mean-reversion strategy that holds one position or none, alternates buy and sell, and
measures every price from where the previous leg ended.

---

## What it does

The strategy is always in one of two states, and both work the same way.

When it is **flat**, it rests a limit order one step *below* the price the last leg ended at,
waiting to buy cheaper. When it is **holding**, it rests a limit order one step *above* what it
paid, waiting to sell dearer.

In each state there is also a price one step the *other* way that it watches but does not
place. Reach that first and it acts there anyway, crossing the spread to do it.

```
FLAT, last exit at P              HOLDING, filled at F

  rest    BUY  at P − step          rest    SELL at F + step
  trigger      P + step             trigger      F − step
```

Whichever of the two happens first ends the state and begins the next. Every order is the same
size, and they alternate buy, sell, buy, sell for the life of the run. Between legs the account
is flat, so the position is never doubled.

The very first order of a run has nothing to measure from, so it rests at the touch and has no
trigger.

It makes money when the price keeps coming back. It loses when the price walks away in the
direction it is not facing.

### Why the resting order never moves

It is a grid level, not a quote. It is never re-priced to follow the market.

That is the point rather than an omission. An order sitting *at* the touch can be crossed by a
book that moves between reading it and the order landing, and CoinDCX has no post-only flag to
prevent that — in one live run it turned 9 of 13 fills into taker fills. An order a full step
away cannot be crossed by accident, so **only a trigger ever pays taker**.

### The bracket comes off the fill

Take profit and stop loss are measured from the price the entry actually filled at, not from
what it was aiming at. A long filled at `F` is bracketed symmetrically at `F × (1 ± step)`, so
the risk and the reward are the ones the position really has.

---

## A worked example

Step **0.15%**, first order rests at the touch of **100.00**.

| # | state | action | result |
|---|---|---|---|
| 1 | flat | rest BUY 100.00 | fills — long at 100.00 |
| 2 | long | rest SELL 100.15, watch 99.85 | 100.15 fills — **+0.15%**, flat |
| 3 | flat | rest BUY 100.00, watch 100.30 | fills — long at 100.00 |
| 4 | long | rest SELL 100.15, watch 99.85 | 99.85 reached — stop, cross out |

Legs 1–2 are the winning shape: price came back. Legs 3–4 are the losing shape: it did not.

### The first fill fixes the direction

On futures the opening leg may offer both sides at once (`both_oco`); the first to fill cancels
the other. From then on the account is **long-or-flat** (or short-or-flat) for the whole run —
there is nothing left to choose, because the alternation is forced by the position itself. Spot
can only ever be long-or-flat.

### The three market shapes

- **Chop** — the intended case. Price crosses the grid repeatedly, each crossing a completed leg.
- **Trend with the bias** — take profits fill, but the entry keeps having to reach further.
- **Trend against the bias** — the losing case, and it is structural. Every leg buys on the way
  past and stops out. `max_loss_quote` is what ends it.

---

## Choosing a pair and a step

The step must be large in **ticks** and small in **time**.

- Large in ticks, because all four prices are quantized. Under ~15 ticks per step they start to
  collide and the drift cap stops being expressible.
- Small in time, because leg rate is what produces a sample.

Measure both from public 1-minute candles before committing to a pair: ticks per step is
`price x step / price_increment`, and leg time is how long the price takes to travel one step
from a standing start. On CoinDCX at a 0.15% step, ZEC-USDT gives ~125 ticks and a ~2 minute
median leg; XRP-USDT gives ~20 ticks and ~4 minutes.

Beware the **minimum notional**. If one leg is only just above it, any partial fill leaves a
position too small for the venue to close (see *Partial fills* below).

---

## Configuration

Two files: a script config and a controller config.

### Script file — `conf/scripts/conf_simple_grid_<name>.yml`

```yaml
script_file_name: v2_with_controllers.py
markets: {}
candles_config: []
controllers_config:
  - conf_simple_grid_<name>.yml
max_global_drawdown_quote: 0.4
max_controller_drawdown_quote: 0.4
```

### Controller file — `conf/controllers/conf_simple_grid_<name>.yml`

| setting | default | what it does |
|---|---|---|
| `connector_name` | `coindcx_perpetual` | |
| `trading_pair` | `BTC-USDT` | |
| `leverage` / `position_mode` | `1` / `ONEWAY` | |
| `total_amount_quote` | `100` | caps the strategy |
| `order_amount_quote` | `100` | size of one leg, in quote |
| `take_profit` / `stop_loss` | `0.005` | **the step.** Both ends of every bracket |
| `time_limit` | `None` | optional per-leg deadline |
| `trigger_price_type` | `MidPrice` | which price arms the stop and the entry trigger |
| `entry_timeout` | `300` | give up if neither entry price is reached |
| `initial_entry_mode` | `both_oco` | `long_only`, `short_only`, or both sides at once |

**Exits**

| setting | default | what it does |
|---|---|---|
| `close_slippage_ticks` | `20` | how far through the book an urgent exit is priced |
| `stop_loss_chase` | `True` | leave passively at the touch instead of crossing at once |
| `stop_loss_maker_offset_ticks` | `1` | ticks *inside* the opposite touch |
| `stop_loss_requote_pct` | `0.0005` | re-post the chase after this much movement |
| `stop_loss_max_drift_pct` | `0.001` | how far past the stop before it gives up and crosses |

**Venue timing** — these exist because CoinDCX releases collateral *after* it confirms a cancel.

| setting | default | what it does |
|---|---|---|
| `cancel_settle_delay` | `0.25` | wait before replacing a cancelled reduce-only order |
| `exit_retry_max_delay` | `2.0` | ceiling on the doubling backoff |
| `collateral_refusal_wait` | `3.0` | stand-down after repeated "Insufficient funds" |
| `collateral_refusals_before_waiting` | `2` | how many in a row before that longer wait |
| `retry_after_insufficient_balance` | `5` | pause before retrying a leg our budget check refused |

**Risk** — any one of these stops new legs being opened.

| setting | default | what it does |
|---|---|---|
| `max_loss_quote` / `max_loss_pct` | `None` | loss threshold; the tighter one wins |
| `max_consecutive_failed_legs` | `5` | legs the **venue** refused, in a row |
| `insufficient_balance_grace_seconds` | `180.0` | how long our own budget check may keep refusing |
| `reconcile_positions` | `True` | halt on a position no leg claims |
| `orphan_grace_seconds` | `10.0` | how long the venue and our books must disagree first |
| `flatten_orphan_positions` | `True` | close an unclaimed position the run itself opened |
| `stop_when_losses_outnumber_wins` | `False` | off by default; `max_loss_quote` is the agreed stop |
| `min_legs_before_count_check` | `10` | minimum sample before that check can fire |
| `cooldown_after_take_profit`, `cooldown_after_stop_loss` | `0` | a pause is time spent flat, so both default to none |

### Settings that are easy to get wrong

- **`take_profit` below ~3× the round-trip fee** guarantees a loss. See the fee table.
- **`stop_loss_max_drift_pct` too small for the pair's tick** skips the chase entirely — on a
  coarse tick 0.03% can be under 4 ticks, so the exit crosses every time.
- **`close_slippage_ticks` scaled from another pair.** 20 ticks is 0.024% on ZEC but 0.148% on
  XRP — a whole step of slippage allowance.
- **`both_oco` on a small wallet.** It rests an order on each side, so margin is locked twice.

---

## Running it

```bash
./start
```

```
start --script v2_with_controllers.py --conf conf_simple_grid_<name>.yml
```

Check the config loads before starting the bot:

```bash
python -c "import sys; sys.path.insert(0,'.'); \
from controllers.generic.simple_grid import SimpleGridConfig; import yaml; \
print(SimpleGridConfig(**yaml.safe_load(open('conf/controllers/conf_simple_grid_<name>.yml'))).trading_pair)"
```

Before a first live run, check by hand that one leg clears the venue's minimum size and
minimum notional, that its margin fits the wallet, and that the step is worth more than the
round-trip fee. Those four are what a misconfigured run gets wrong.

---

## Partial fills (Need Confirmation)

The side latches on the **first** fill, however small, and the unfilled remainder of the entry
is cancelled — leaving it open would keep moving the average entry price, and the bracket hangs
off that price. The take profit is re-sized if the position changes underneath it, and a stop
that fills across several chased orders is accounted at their weighted average.

**The one gap:** a partial fill below the venue's minimum notional leaves a position that
cannot be closed. No take profit or stop loss is armed for it, the leg exhausts its retries,
and the run halts telling you to close it by hand. Size legs comfortably above the minimum
notional so a partial fill still clears it.

---

## When it stops

New legs stop on any halt condition above; an open leg is left to finish on its own barriers.
`stop` cancels resting orders and flattens any open position in the same call.

The stop loss exists **only inside the running process**. A position that outlives the bot has
nothing watching it — always confirm the account is flat after stopping.

---

## Troubleshooting

| symptom | cause |
|---|---|
| `Insufficient funds` on a close | a cancelled exit still holds the margin; the executor waits it out |
| `Cannot place reduce only order` | there is no position left — it already closed |
| `Not enough budget` repeatedly | our budget check is reading a stale wallet; it re-reads after a cancel |
| entry never fills | price sat between the two grid prices; `entry_timeout` ends the leg |
| every leg stops out | trend against the bias — structural, not a bug |
| `STILL OPEN` at shutdown | a position we could not close. Check the account by hand |

---

## Related

- `hummingbot/strategy_v2/executors/simple_grid_executor/` — one leg
- `controllers/generic/simple_grid.py` — the chain of legs
- `test/controllers/generic/test_simple_grid.py` and
  `test/hummingbot/strategy_v2/executors/simple_grid_executor/` — the test names are the
  specification for the behaviour above
