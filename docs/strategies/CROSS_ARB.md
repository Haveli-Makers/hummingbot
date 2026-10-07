# Cross-exchange arbitrage (`cross_arb`)

Buys a coin on the exchange where it is cheaper and sells it on the exchange where it is dearer, at
the same time, at the best price on each side. It has two parts: a **controller** that decides when
to trade, and an **executor** that carries out one trade from start to finish.

## How it works

1. Every tick, the controller reads the best bid and best ask of each pair on both exchanges and works
   out the gap in both directions:
   - **gross** = (best bid on the selling exchange − best ask on the buying exchange) / best ask
   - **net** = the same after both fees and the TDS withheld from the sale
2. If the gap reaches the trigger and every check passes, it starts an executor with the plan already
   decided: where to buy, where to sell, how much, and the price limit on each side.
3. The executor sends both orders, watches them fill, cancels whatever did not fill, and fixes any
   difference between what was bought and what was sold.
4. It reports the result. The controller counts it, waits out the cooldown, and looks again.

At most one trade runs per pair at a time.

## Files

| File | Role |
|---|---|
| `controllers/generic/cross_arb.py` | `CrossArbController` and `CrossArbConfig`: prices, checks, sizing, limits, status |
| `hummingbot/strategy_v2/executors/cross_arb_executor/cross_arb_executor.py` | `CrossArbExecutor`: one trade |
| `hummingbot/strategy_v2/executors/cross_arb_executor/data_types.py` | `CrossArbExecutorConfig`, `CrossArbPhase`, `MismatchPolicy`, `LegOrder` |
| `hummingbot/strategy_v2/executors/executor_orchestrator.py`, `hummingbot/strategy_v2/models/executors_info.py` | register the executor |
| `test/controllers/generic/test_cross_arb.py` | controller tests |
| `test/hummingbot/strategy_v2/executors/cross_arb_executor/` | executor tests, including the bot's own executor-creation path |

It runs under `scripts/v2_with_controllers.py`, like any other controller.

## The controller

### Checks before a trade

Each check that fails is counted and named, so `status` shows why the bot is not trading.

| Check | Shown as |
|---|---|
| Both order books have sent a new snapshot within `max_book_age` | `no fresh book: <exchange>` |
| The pair is not paused after a kept mismatch (see `hold` below) | `paused: holding a mismatch` |
| The gap reaches `min_profitability` (gross or net, per `trigger_on`) | `gap below threshold` |
| A trade size exists that both exchanges accept and both balances cover | `not enough <INR or coin> on <exchange>`, `total budget in use`, `nothing on offer`, `rounds to zero`, `below min order value (100)`, `below a venue minimum` |
| No trade already running for the pair | `executor already running` |
| The cooldown since the last trade on the pair has passed | `cooldown` |

A book counts as stale when the exchange stops sending snapshots, not when prices stay the same:
quiet books are normal on these exchanges.

### Sizing

```
amount = min(order_amount_quote / ask,
             (total_amount_quote − money already in running trades) / ask,
             size at the best ask, size at the best bid,
             quote balance on the buying exchange / ask,
             coin balance on the selling exchange)
```

The amount is rounded to the buying exchange's step, then the selling exchange's, and is used only if
it survives both unchanged, so the two orders are for exactly the same amount. It must also clear
both exchanges' minimum order size and value and `min_order_amount_quote`. If it does not, nothing is
sent. The exchanges' real minimums (₹60 on CSX, ₹50 on WazirX) are higher than their published rules
say, which is what `min_order_amount_quote` guards against.

### Limits that stop all trading

| Limit | Shown as |
|---|---|
| `manual_kill_switch` | `kill switch on` |
| Today's realised loss reaches `max_loss_quote` | `daily loss limit reached` |
| `max_consecutive_failures` failed trades in a row | `N failed attempts in a row` |
| `max_trades_per_hour` reached | `trades per hour reached` |

These show as `HALTED` in the status. In `v2_with_controllers.py` the kill switch also stops the
controller.

### Low balances

Trades usually run one way, so one side's money runs down. When a direction has no money left it
simply stops (`not enough … on …`) while the other direction keeps trading. Below
`rebalance_below_quote` the status shows a `REBALANCE:` line saying which balance is low.

### Status

`status` shows the trigger and size limits; profit realised today, failures in a row and trades this
hour; any `HALTED` or `PAUSED` line; a table of every pair and direction (ask, bid, gross %, net %,
size, and what blocked it); the most common reasons for not trading; any `REBALANCE` lines; and how
many trades are running.

## The executor

### Orders: limit orders priced to fill immediately

Each order is a `LIMIT` order at the other side's best price: buy at the best ask, sell at the best
bid (optionally `slippage_ticks` further). It fills like a market order, but cannot fill at a worse
price, and both orders can be for exactly the same coin amount. Market orders cannot do this here:
WazirX has none, and CSX's take an amount in rupees, not in coins.

`leg_order: simultaneous` (default) sends both orders together. `buy_first` / `sell_first` send one,
then the other sized to what the first actually filled.

### Phases

| Phase | What happens | Ends when |
|---|---|---|
| (start) | Check both balances | short of money: ends `INSUFFICIENT_BALANCE`, nothing sent |
| `placing` | Send the orders | at once |
| `waiting` | Watch both orders fill | both are finished, or `fill_timeout` |
| `cleanup` | Cancel what did not fill, then wait `cancel_settle_delay` before believing the cancel | all orders final, or `cleanup_timeout` |
| `reconcile` | Compare bought and sold; fix any difference | matched, kept on purpose, or `flatten_timeout` |

Every phase has a deadline, so a trade can never hang.

How a trade ends:

| Close type | Meaning |
|---|---|
| `COMPLETED` | Both sides matched (or the leftover is too small to trade and is reported) |
| `EXPIRED` | Nothing filled in time |
| `FAILED` | A mismatch could not be fixed, or an order would not cancel in time |
| `POSITION_HOLD` | The leftover was kept on purpose (`mismatch_policy: hold`) |
| `EARLY_STOP` | The bot was stopped during the trade |
| `INSUFFICIENT_BALANCE` | Not enough money at the start |

### Fills, cancels and refused orders

- Amounts filled are read only from the exchange's order state.
- A cancel confirmation is not trusted: an exchange can confirm a cancel and still fill the order
  afterwards. Cancelled orders stay watched for `cancelled_order_watch_seconds`; a late fill is
  counted and the two sides are compared again.
- A refused order is noticed from its own state, not only from the failure event.

### A mismatch between the two sides

`mismatch = bought − sold`. A leftover worth no more than `dust_threshold_quote` (default: the
exchanges' own minimums) is reported, not traded, because no exchange would accept the order.
Otherwise `mismatch_policy` decides:

| Policy | What happens |
|---|---|
| `flatten` (default) | Trade the difference away at once at the better of the two exchanges, up to `max_retries` attempts, each re-priced. An exchange that refused the unwind is not used again; one that just refused that side's order is tried last |
| `hold` | Keep it. The trade ends `POSITION_HOLD`, only the matched part counts as profit, the leftover is passed to the framework as a held position, and the controller pauses that pair: clear the leftover by hand, then restart the bot |

### Stopping the bot mid-trade

The framework allows about 20 seconds after a stop. `early_stop()` acts at once: it cancels open
orders and, under `flatten`, sends the unwind in the same call. If the sides still do not match after
15 seconds, it logs an error naming the unmatched amount, which then needs clearing by hand, and stops.

### The result

Each trade reports both exchanges and prices, planned and filled amounts, average prices, fees, TDS,
the mismatch and how it was handled, late fills, the net result and why it ended.

**Net = money from the sale − money paid for the buy − fees − TDS**, and the percentage is taken
against the money paid. TDS is not reported by the connectors, so it is computed from `tds_pct`.

## Settings

### Controller

| Setting | Default | Meaning |
|---|---|---|
| `exchange_a`, `exchange_b` | `csx`, `wazirx` | the two exchanges |
| `trading_pairs` | `[USDT-INR]` | pairs to trade |
| `min_profitability` | `0.01` | the trigger, as a fraction (0.01 = 1%) |
| `trigger_on` | `gross` | compare the gross gap or the net |
| `taker_fee_pct` | `{}` | fee per exchange in %; an exchange left out counts as free |
| `gst_pct` | `18` | GST added on top of the fee (0 when the fee already includes it) |
| `tds_pct` | `1` | TDS withheld from every sale, in % |
| `order_amount_quote` | `10000` | most money per trade |
| `total_amount_quote` | `10000` | most money in running trades at once, across all pairs |
| `min_order_amount_quote` | `100` | smallest trade |
| `max_book_age` | `10` | seconds without a new snapshot before a book counts as stale |
| `cooldown` | `5` | seconds between trades on a pair |
| `max_trades_per_hour` | `60` | |
| `max_loss_quote` | off | realised loss that stops trading for the day |
| `max_consecutive_failures` | `5` | |
| `manual_kill_switch` | `false` | |
| `rebalance_below_quote` | `500` | balance below which a `REBALANCE` line appears |
| `rebalance_alerts` | `true` | |

The controller also passes these to each executor: `fill_timeout`, `cleanup_timeout`,
`flatten_timeout`, `cancel_settle_delay`, `mismatch_policy`, `leg_order`, `slippage_ticks`,
`dust_threshold_quote`.

### Executor

| Setting | Default | Meaning |
|---|---|---|
| `buying_market`, `selling_market` | — | exchange and pair for each side (same coin, same quote currency) |
| `order_amount` | — | coin amount, already valid on both exchanges |
| `buy_price_cap`, `sell_price_floor` | — | the price limit on each side |
| `leg_order` | `simultaneous` | or `buy_first` / `sell_first` |
| `fill_timeout` | `15` | seconds |
| `cleanup_timeout` | `20` | seconds |
| `flatten_timeout` | `20` | seconds |
| `cancel_settle_delay` | `0.25` | seconds before believing a cancel |
| `cancelled_order_watch_seconds` | `30` | how long a cancelled order is watched for late fills |
| `mismatch_policy` | `flatten` | or `hold` |
| `dust_threshold_quote` | `0` (the exchanges' minimums) | smaller leftovers are reported, not traded |
| `max_retries` | `2` | unwind attempts; the original orders are never re-sent |
| `slippage_ticks` | `0` | price steps beyond the best price |
| `tds_pct` | from the controller | for the reported net |

## Running it

A controller config, in `conf/controllers/`:

```yaml
id: cross_arb_sol
controller_type: generic
controller_name: cross_arb
exchange_a: csx
exchange_b: wazirx
trading_pairs:
  - SOL-INR
min_profitability: 0.0125
trigger_on: gross
taker_fee_pct:
  csx: 0.05
  wazirx: 0
gst_pct: 0
tds_pct: 1
order_amount_quote: 500
total_amount_quote: 500
min_order_amount_quote: 100
max_book_age: 15
cooldown: 10
max_trades_per_hour: 10
max_loss_quote: 50
max_consecutive_failures: 3
```

And a script config with the same name in `conf/scripts/`:

```yaml
script_file_name: v2_with_controllers.py
markets: {}
candles_config: []
controllers_config:
  - conf_cross_arb_sol.yml
```

Then, in the bot: `start --script v2_with_controllers.py --conf conf_cross_arb_sol.yml`.

Both exchanges' keys must be saved with `connect`. CSX accepts calls only from whitelisted IPs, so its
connector goes through a proxy (see `docs/PROXY_SERVER_GUIDE.md`).
