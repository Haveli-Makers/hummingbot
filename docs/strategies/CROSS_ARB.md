# Cross-exchange arbitrage (`cross_arb`)

Buy a coin on the exchange where it is cheap and sell it on the exchange where it is dear, at the
same moment, using only the best price on each side.

The
pairs, the trigger percentage, the fee and tax rates and the sizes are all settings. This document
describes the machinery that has to be right whatever those settings are.

---

## 1. What it does, in one cycle

1. Watch the best bid and best ask of the same trading pair on two exchanges.
2. When (best bid on B − best ask on A) / best ask on A is at or above `min_profitability`, and every
   guard passes, start one executor for that opportunity.
3. The executor buys on A and sells on B at those prices, for the amount both top levels can absorb.
4. It confirms what actually filled, cancels whatever did not, and fixes any mismatch between the two
   sides.
5. It records the result: prices, amounts, fees, tax withheld, and the net outcome in the quote
   currency.
6. The controller updates its inventory picture, waits out the cooldown, and looks again.

Both directions are watched: A→B and B→A.

---

## 2. Why this is new code

The upstream `arbitrage_controller` + `arbitrage_executor` cannot be made to do this safely:

| Upstream behaviour | Why it fails here |
|---|---|
| Sends `MARKET` orders | **WazirX has no market order type at all**: its docs allow only `limit` and `stop_limit`, and a live attempt was refused with code 1999 "type does not have a valid value" (2026-09-23, `temp/arbitrage/data/order_probe_*.json`). CSX does document `"type": "MARKET"`, and our CSX/CoinSwitch/Zebpay *connectors* separately only ever send `LIMIT`. So a market order cannot be the common execution path, whatever we do to the connectors |
| No price cap | Our books are REST snapshots up to 5 s old, so a market order can fill far from the quoted price |
| Waits for both orders to be *fully* filled, forever | A partial fill or a cancel leaves the executor stuck, and its direction is then blocked for good |
| No clean-up when one side fails | Leaves a one-sided position nobody unwinds |
| Counts executors, not balances | Failed and skipped attempts count as trades; inventory is never actually checked |
| Profit % divides quote by base | Reports ~1300% for a 1.3% trade |

We leave that code untouched (other strategies use it) and add our own under new names.

---

## 3. Pieces and files

| File | Role |
|---|---|
| `hummingbot/strategy_v2/executors/cross_arb_executor/data_types.py` | `CrossArbExecutorConfig`, `CrossArbSide`, `MismatchPolicy` |
| `hummingbot/strategy_v2/executors/cross_arb_executor/cross_arb_executor.py` | `CrossArbExecutor`: one opportunity, start to finish |
| `controllers/generic/cross_arb.py` | `CrossArbController` + `CrossArbConfig`: scanning, guards, inventory, rebalance proposals |
| `hummingbot/strategy_v2/executors/executor_orchestrator.py` | register `"cross_arb_executor"` (one import + one registry line) |
| `hummingbot/strategy_v2/models/executors_info.py` | add the config to `AnyExecutorConfig` |
| `test/hummingbot/strategy_v2/executors/cross_arb_executor/test_cross_arb_executor.py` | executor tests |
| `test/controllers/generic/test_cross_arb.py` | controller tests |
| `conf/controllers/conf_cross_arb_*.yml`, `conf/scripts/conf_cross_arb_*.yml` | run configuration |

Same shape as `simple_grid`, so the dashboard, the api-server and `v2_with_controllers.py` pick it up
with no special handling.

---

## 4. The executor: one opportunity, start to finish

One executor handles exactly one attempt and then terminates. It is created with the plan already
decided (which exchange to buy on, at what price cap, how much), so it never re-decides the trade.

### 4.1 States

```
CREATED ──► PLACING ──► WAITING ──► CLEANUP ──► RECONCILE ──► TERMINATED
                │           │           │            │
                └───────────┴───────────┴────────────┴──► (failure paths, all time-bounded)
```

| State | What happens | Leaves when |
|---|---|---|
| **PLACING** | Re-check balances, size the trade, send both orders | both orders acknowledged, or a send fails |
| **WAITING** | Watch fills on both sides | both fully filled, or `fill_timeout` (default 15 s) |
| **CLEANUP** | Cancel whatever is unfilled, then *verify* the cancel really happened | both orders terminal, or `cleanup_timeout` (default 20 s) |
| **RECONCILE** | Compare filled amounts; fix any difference | flattened, held on purpose, or `flatten_timeout` (default 20 s) |
| **TERMINATED** | Write the result | — |

Every state has a timeout. No state can wait forever, which is the specific way the upstream executor
hangs.

### 4.2 Orders: a crossing limit, never a market order

Each leg is a `LIMIT` order priced at the other side's quote, optionally `slippage_ticks` (default 0)
through it:

- buy at the seller's best ask, sell at the buyer's best bid.

At the top of the book this fills exactly like a market order, but:

- it is the only order type that can hedge an exact quantity on both venues. WazirX has no market
  order at all, and CSX's takes a **whole number of rupees**, not a coin amount: asking it for ₹60
  sold 0.60 USDT when we held 0.61, stranding the remainder below the ₹60 floor (verified live
  2026-09-23). Two legs of an arbitrage have to match in coins, which only a limit order can express;
- it cannot fill at a worse price than we decided, which matters because WazirX's book is up to 5 s
  old when we act on it;
- if the price has moved away, it simply rests unfilled and we cancel it. An unfilled order costs
  nothing; a bad fill costs money.

This is the same reasoning as the CoinDCX closing orders in `simple_grid`.

### 4.3 Sizing

```
amount = min(order_amount_quote / ask,        # what the operator allows per trade
             ask_qty,                         # what the seller is offering
             bid_qty,                         # what the buyer is asking for
             quote_balance_on_buy_side / ask, # what we can pay with
             base_balance_on_sell_side)       # what we can deliver
```

then:

1. Quantize to **both** exchanges' quantity steps and take the coarser result, so the same number is
   valid on both sides. Quantizing per exchange separately is what leaves dust behind.
2. Check the result against **both** exchanges' minimum order size and minimum order value, plus the
   operator's `min_order_amount_quote`.
3. If it fails any minimum, **do not send anything**. A rejected order is the beginning of a one-sided
   position, so it is avoided before it happens, not handled afterwards.

The minimums are large and undocumented, found by being rejected live: **₹60 on CSX, ₹50 on WazirX**,
neither of them published in the venue's market list (which implies about ₹1). The practical
consequence is that **a leftover worth less than the floor cannot be traded away on that venue at
all**, so `dust_threshold_quote` defaults to the venue minimums and a smaller mismatch is reported
for a human to clear rather than chased with an order that would be refused.

Balances are read at this moment, not when the executor was created.

### 4.4 Both legs at once

Both orders are sent in the same pass, without waiting for the first to fill. Sequential legging is a
config option (`leg_order: simultaneous | risky_side_first`) but simultaneous is the default: the
price cap already bounds the loss on each leg, while waiting for leg one doubles the time the gap has
to disappear.

### 4.5 Fills, cancels and the mismatch

- A fill is only believed from the connector's order state (`executed_amount_base`), never from the
  placement acknowledgement.
- After a cancel, the order is re-read. **A cancel acknowledgement is not proof**: CoinDCX has
  confirmed a cancel for an order that filled 30 seconds later, and WazirX's cancel response returns
  the order still `"status": "wait"` — only the next read shows `"cancel"` (verified live 2026-09-23). The executor waits
  `cancel_settle_delay` (default 0.25 s), re-reads, and only then treats the leg as final. Late fills
  found in this window are added to the filled amount, not ignored.
- Then the two sides are compared:

```
matched   = min(bought, sold)
mismatch  = bought − sold        # positive: we hold extra coin; negative: we are short
```

If `|mismatch| × price` is above `dust_threshold_quote` (default: the larger exchange minimum), the
executor acts on `mismatch_policy`:

| Policy | Behaviour |
|---|---|
| `flatten` (default) | Immediately trade the difference away with a crossing limit on the exchange with the better price, accepting a small loss |
| `hold` | Keep it, mark the executor `MISMATCH_HELD`, and raise an alert; the controller pauses that pair |

Both are recorded. "Do nothing quietly" is not an option.

### 4.6 Shutdown

`early_stop()` must leave nothing in the air, and the framework only allows about 20 seconds:

1. Cancel both legs (no waiting for confirmation first).
2. If one side is filled and the other is not, flatten immediately with a crossing limit.
3. Write the result with `CloseType.EARLY_STOP`.

This is the `simple_grid` shutdown lesson: a bot stopped mid-trade must not leave a leg behind.

### 4.7 What the executor reports

`get_custom_info()` carries, per attempt: both exchanges and prices, intended and actual amounts,
which side filled first, fees per side, tax withheld, the mismatch and what was done about it, the
net result in the quote currency, and the reason it ended. This is what makes a live run auditable.

**Profit** is `sold_value − bought_value − fees − tax`, all converted to one currency, with the
percentage taken against the amount spent (not against the coin amount, which is the upstream bug).

Fees and tax have to be read from **different places per venue**, both verified live: CSX puts
`takerFee`, `makerFee` and `tdsPerc` on the order itself and has no trades endpoint at all, while
WazirX puts `fee` and `tdsAmount` on each fill in `myTrades` and nothing on the order. Measured
rates: CSX taker 0.05%, WazirX 0%, and 1% TDS on the sell side at both.

---

## 5. The controller: when to start an executor

Each tick:

1. Read both books for every configured pair.
2. For each direction, compute the gross gap, and the net after fees, GST and tax (all settings).
3. Take the best direction and check every guard below. Any failure is **counted and named**, so the
   status display answers "why is it not trading?".

| Guard | Default | Why |
|---|---|---|
| gap at or above `min_profitability` | 1% | the operator's trigger; `trigger_on: gross \| net` decides which number is compared |
| both books fresher than `max_book_age` | 10 s | a frozen feed shows a gap that is not there |
| both connectors ready and trading enabled | — | never act on a half-started connector |
| size passes both exchanges' minimums | — | see §4.3 |
| balances sufficient on both sides | — | read now, not at startup |
| no executor already running for this pair | 1 | keeps the inventory picture simple |
| cooldown since the last attempt on this pair | 5 s | stops a tight loop against a refusing venue |
| `max_trades_per_hour` not reached | 60 | blunt rate limiter |
| daily loss under `max_loss_quote` / `max_loss_pct` | off | stops the day |
| consecutive failures under `max_consecutive_failures` | 5 | a venue refusing everything stops the strategy instead of grinding |
| `manual_kill_switch` off | off | one flag to stand down |

### 5.1 Inventory and rebalancing

The controller tracks, per pair and per exchange, the base and quote balance, and compares them with
the operator's targets (`target_base_per_exchange`, `target_quote_per_exchange`).

Because a coin's gap usually runs one way, one side drains. So:

- When the side that must deliver falls below `min_base_balance` / `min_quote_balance`, that
  **direction** is paused (the opposite one keeps trading) and a rebalance need is raised.
- A rebalance proposal says what to move, from where to where, and the expected fee: e.g. "move 8,000
  GALA from CSX to WazirX; WazirX withdrawal fee 725 GALA".
- `rebalance_mode`:

| Mode | Behaviour |
|---|---|
| `alert` (default) | Log + alert only. A person moves the funds |
| `propose` | Prepare the transfer through the wallet-transfer framework and wait for an explicit approval before sending |
| `auto` | Send it, within `max_transfer_quote` per transfer and per day, to whitelisted addresses only |

Only crypto legs can be automated at all, and only where the connector supports withdrawal (WazirX
yes, CSX built but untested, CoinDCX not at all). INR always needs a person.

### 5.2 Status display

`to_format_status()` shows, per pair: the current gap both ways (gross and net), the amount available
at the top, balances on both exchanges against their targets, the last few attempts with their
outcome, the guard-rejection counters, and any open rebalance need.

---

## 6. Settings

### Controller (`conf/controllers/conf_cross_arb_*.yml`)

| Setting | Default | Meaning |
|---|---|---|
| `exchange_a`, `exchange_b` | — | any two spot connectors |
| `trading_pairs` | — | list, e.g. `[SOL-INR, GALA-INR]` |
| `min_profitability` | `0.01` | trigger, as a fraction |
| `trigger_on` | `gross` | compare the raw gap or the after-cost number |
| `taker_fee_pct` | per exchange | used for the net number |
| `gst_pct` | `18` | tax on the fee |
| `tds_pct` | `1` | withheld from every sale |
| `order_amount_quote` | `10000` | maximum per trade |
| `min_order_amount_quote` | `2000` | skip anything smaller |
| `max_book_age` | `10` | seconds |
| `cooldown` | `5` | seconds between attempts on a pair |
| `max_trades_per_hour` | `60` | |
| `max_loss_quote` / `max_loss_pct` | off | daily stop |
| `max_consecutive_failures` | `5` | |
| `target_base_per_exchange`, `target_quote_per_exchange` | — | inventory targets per pair |
| `min_base_balance`, `min_quote_balance` | — | pause level per direction |
| `rebalance_mode` | `alert` | `alert` / `propose` / `auto` |
| `manual_kill_switch` | `false` | |

### Executor (set by the controller)

| Setting | Default | Meaning |
|---|---|---|
| `buying_market`, `selling_market` | — | connector + pair for each leg |
| `order_amount` | — | base units, already valid on both venues |
| `buy_price_cap`, `sell_price_floor` | — | the crossing limit prices |
| `slippage_ticks` | `0` | ticks through the touch |
| `leg_order` | `simultaneous` | or `risky_side_first` |
| `fill_timeout` | `15` | seconds |
| `cleanup_timeout` | `20` | seconds |
| `flatten_timeout` | `20` | seconds |
| `cancel_settle_delay` | `0.25` | seconds before believing a cancel |
| `mismatch_policy` | `flatten` | or `hold` |
| `dust_threshold_quote` | venue minimum | below this, a mismatch is ignored |
| `max_retries` | `2` | per leg, re-priced each time, never blind |

---

## 7. What can go wrong, and what happens

| Situation | Behaviour |
|---|---|
| One leg rejected (size, balance, venue error) | Other leg is cancelled at once; if it already filled, flatten; attempt recorded as `ONE_LEG_FAILED` |
| One leg partly filled | Cancel the remainder, then match the two sides and flatten the difference |
| Both partly filled, different amounts | Keep the matched part, flatten the difference |
| Cancel acknowledged but the order fills later | Detected by re-reading after `cancel_settle_delay`; the late fill is counted and flattened if needed |
| Price moves before the orders land | Orders rest unfilled at our cap and are cancelled; no loss beyond fees |
| A book goes stale or a connector disconnects | Guard blocks new attempts; running executors finish on their timeouts |
| Exchange returns 429 / 5xx | Connector throttling handles it; repeated failures trip `max_consecutive_failures` |
| Bot stopped mid-trade | `early_stop`: cancel, flatten, record, inside the ~20 s window |
| Inventory exhausted on one side | That direction pauses; rebalance need raised; the other direction keeps trading |

---

## 8. Tests

Executor: sizing against both venues' rules; below-minimum is skipped, not sent; both legs fill;
one leg rejected; one leg times out; partial fill on one side; partial on both; cancel that lies;
late fill after cleanup; mismatch flattened; mismatch held; shutdown mid-flight; profit and
percentage arithmetic including fees and tax; retries bounded.

Controller: trigger on gross and on net; every guard blocks and is counted; the best direction wins;
one executor per pair; cooldown; daily loss stop; inventory pause on one direction only; rebalance
proposal contents; status output.

Plus an offline dry-run harness (scripted books, no network) in the style of
`temp/strategies/simple_grid_dry_run.py`, so the whole cycle can be watched end to end before any
money moves.

---

## 9. Build order

1. `data_types.py` + executor skeleton with the state machine and timeouts — with tests.
2. Sizing, order placement, fills and cancel verification — with tests.
3. Mismatch handling and shutdown — with tests.
4. Controller: scan, guards, executor creation, status — with tests.
5. Inventory tracking and rebalance proposals (`alert` mode) — with tests.
6. Dry-run harness on scripted books.
7. Config files, then a paper run, then a live run at the smallest size the venues allow.
8. Later: `propose`/`auto` rebalancing, dashboard page, api-server wiring.

Each step is offline and tested before the next. Nothing touches an exchange until step 7, which
needs funded accounts and an explicit go-ahead.
