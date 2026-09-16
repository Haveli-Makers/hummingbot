# Monitoring & Alerts — User Guide

This feature watches your market-making bot **every second** and messages your team's
Google Chat space when something goes wrong — an error, a missing order, too little
depth on the book — and keeps a **daily scorecard** of how well the bot met its
commitment (its "SLA": e.g. *orders on both sides, big enough, close enough to the
market price, at least 96% of the day*).

You do not need to understand the code. Setup is: **create a chat webhook → fill one
config file → start the bot as usual.**

---

## Step 1 — One-time setup: connect Google Chat (5 minutes)

1. Open the Google Chat **space** where alerts should arrive (or create one, e.g. "Alerts").
2. Click the space name (top) → **Apps & integrations** → **Webhooks** → **Add webhook**.
3. Name it (e.g. `hb-alerts`) → **Save** → **copy the URL** it shows you.
4. Before starting the bot, paste the URL into the terminal you launch the bot from:
   ```bash
   export GCHAT_WEBHOOK_URL='PASTE-THE-URL-HERE'
   ```
   ⚠️ This must be done in **every new terminal** before launching the bot (or add the
   line to your `~/.bashrc` once). Treat the URL like a password — anyone who has it
   can post to your space.

> No webhook? The bot still measures and keeps the daily scorecard — you just won't
> receive chat messages ("log-only mode").

---

## Step 2 — Which type of PMM are you running?

Open your strategy config (`conf/strategies/conf_*.yml`) and check:

| If your strategy has... | Your type | Use template |
|---|---|---|
| One `bid_spread` / `ask_spread` (one buy + one sell order), or several levels but **one single commitment** ("₹X within Y%") | **Simple** | Template A |
| Several levels **and** a tiered commitment ("₹10k within 1.35% AND ₹40k within 1.75% AND ...", each with its own uptime %) | **Tiered** | Template B |

---

## Step 3 — Fill in the monitoring config

Create/edit the file **`conf/monitoring.yml`** (same `conf/` folder as your strategy
configs). Put **exactly one** of the two templates in it.

### Template A — Simple PMM

```yaml
pmm_sla_monitor:
  enabled: true
  connector_name: wazirx           # your exchange, as named in Hummingbot
  trading_pair: USDT-INR           # your pair
  spread_band_pct: 1.5             # orders within this % of market price count
  min_depth_quote: 20000           # required order value on EACH side (in INR/quote)
  required_uptime_pct: 96          # % of the day the above must hold
```

### Template B — Tiered / multi-level PMM

```yaml
multilevel_pmm_sla_monitor:
  enabled: true
  connector_name: wazirx
  trading_pair: USDT-INR
  tiers:                                    # one line per tier of your commitment
    - {name: tier1, spread_band_pct: 1.35, min_depth_quote: 10000, required_uptime_pct: 99}
    - {name: tier2, spread_band_pct: 1.75, min_depth_quote: 40000, required_uptime_pct: 97}
    - {name: tier3, spread_band_pct: 2.10, min_depth_quote: 90000, required_uptime_pct: 96}
  required_uptime_pct: 96                   # target for the combined "overall" score
```
Tiers are **cumulative**: `min_depth_quote` for tier2 means the *total* value within
1.75%, including tier1's orders.

### How to choose your numbers (rules of thumb)

- **`spread_band_pct`** = the band from your SLA agreement. Then make sure your
  strategy **quotes tighter than it** (e.g. quote at 1.4% if the band is 1.5%) —
  orders placed *exactly at* the edge randomly fall outside due to price rounding.
- **`min_depth_quote`** = the required value per side from your agreement. If you're
  just testing, set it to ~85% of what your orders actually add up to.
- **`required_uptime_pct`** = from your agreement (e.g. 96 = allowed to be "down" at
  most ~58 minutes per day).
- Everything else has sensible defaults you can add only if needed:
  `grace_period_sec: 2` (ignore blips shorter than this), `alert_warmup_sec: 10`
  (no alerts right after start), `day_reset_timezone: Asia/Kolkata` (when the daily
  score resets), `heartbeat_log_interval_sec: 300` (how often a status line is logged).

---

## Step 4 — Start and verify (2 minutes)

Start the bot and your strategy exactly as you always do. Then check three things:

1. The log shows: `Google Chat alerting enabled: forwarding ERROR+ logs...`
2. The log shows: `SLA monitor started for <exchange>:<pair> (..., alerts on).`
   — if it says **log-only** instead, your webhook variable wasn't set (Step 1.4).
3. Every few minutes a **heartbeat** line appears with your live numbers:
   `SLA monitor heartbeat: uptime 99.83% ...; currently in spec ... Day 2026-07-19 uptime 99.79%`

Optional live drill: temporarily raise `min_depth_quote` above what you quote,
restart the strategy, and a 🟠 alert should reach the chat within ~15 seconds.
Revert afterwards.

---

## Step 5 — Understand the messages you'll receive

| Message looks like | Meaning | What to do |
|---|---|---|
| 🔴 / 🟠 followed by an error text and `logger: ...` | The bot itself hit an error (exchange unreachable, order rejected...) | Check the bot; if it repeats every 5 min, it's still broken |
| 🟠 *Depth below the SLA minimum* / *Tier 'x' depth below...* `Down for 12s...` | Your resting orders are too small / too far / partly missing | Check balance and the strategy — often insufficient funds |
| 🔴 *One side has no standing orders* | You're quoting only one side — inventory risk | Usually a fill or failed placement; check balances |
| 🟠 *Order book stale or connector disconnected* | The bot can't trust its market data (network/exchange issue) | Check connectivity; alerts queue up and arrive after reconnect |
| ✅ *Resolved: ...* `Recovered after 2m 04s` | The problem above fixed itself | Nothing — informational |
| 🔴 *Daily SLA breached (tierX)* | Yesterday's score was below that tier's target | Report per your desk's process; see the CSV for details |

Noise control is built in: a persisting problem = **one message + one reminder every
5 minutes**, never a flood. Short blips (< 2s, e.g. normal order refreshes) never alert.

---

## Step 6 — The daily scorecard

Every midnight (IST) the finished day is written to:
```
data/sla/<monitor>_<exchange>_<pair>_sla_daily.csv     ← one row per day (per tier)
```
Columns in plain words: date · exchange · pair · which tier ("overall" = combined) ·
uptime % · seconds in spec · total seconds · downtime minutes · main cause ·
**sla_met (yes/no)** · complete_day (no = bot was down at midnight).

The live counters survive bot restarts — stopping/starting the bot mid-day does not
reset the day's score.

---

## Switching off / changing things

- **Pause monitoring:** set `enabled: false` in `conf/monitoring.yml` (or delete the
  file), then restart the strategy.
- **Chat messages only off:** launch the bot without `GCHAT_WEBHOOK_URL`.
- **Change any number:** edit `conf/monitoring.yml` → `stop` → `start` (the file is
  re-read at every strategy start; no bot relaunch needed).

## Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| No `SLA monitor started` line | `conf/monitoring.yml` missing, `enabled: false`, or `connector_name` doesn't match your strategy's exchange |
| Says `log-only` | `GCHAT_WEBHOOK_URL` not exported in the launching terminal |
| Bot start fails after editing the file | A typo/invalid value in monitoring.yml — the error message names the bad field |
| Alert fires immediately and never stops | Your `min_depth_quote`/band doesn't match what you actually quote — re-check Step 3's rules of thumb |
| Constant one-side alerts while using ping-pong / price floor–ceiling modes | Those modes quote one side on purpose — this SLA type doesn't fit them; talk to the team |
| Nothing at all in chat during a network outage | Expected — alerts queue and arrive right after reconnection |

## Three golden rules

1. **Quote tighter than the band** you're measured against.
2. **One monitor section** in `conf/monitoring.yml` at a time (the first enabled one wins).
3. The monitor **never touches your orders** — it only watches. Turning it on/off can
   never change how the strategy trades.

---

*See also: [MONITORING_DEVELOPER_GUIDE.md](MONITORING_DEVELOPER_GUIDE.md) to add monitoring for a new strategy, and [MONITORING_INTERNALS.md](MONITORING_INTERNALS.md) for how the framework works inside.*
