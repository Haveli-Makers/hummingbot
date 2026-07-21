# Monitoring & Alerting — Internals and Design Decisions

## 1. The mental model

The feature answers one question continuously: **"is my strategy doing what we promised,
and if not, who finds out and how fast?"**

Failures come in two kinds, and each needs a different detection mechanism:

| | Loud failures | Silent breaches |
|---|---|---|
| Examples | crash, exchange disconnect, order rejected | depth below SLA, one side unquoted, stale book |
| Evidence | the bot already writes an ERROR log | **nothing** — verified: PMM's `c_apply_budget_constraint` (pure_market_making.pyx) drops unaffordable orders with zero log lines; order rejections log at NETWORK level (16) which is *below INFO (20)* |
| Our detection | **Layer A**: forward ERROR+ log records | **Layer B**: measure the condition every second |

Plus **Layer C**: accumulate Layer B's measurements into per-day uptime figures
(SLAs are *daily* promises), persist them, and record/alert at end of day.

All three layers deliver through one shared pipeline: `Alert → AlertDispatcher →
GChatNotifier → Google Chat webhook`.

---

## 2. The delivery pipeline (bottom-up)

### 2.1 PRE-EXISTING: `hummingbot/notifier/notifier_base.py` (`NotifierBase`)
What Hummingbot already had: an abstract notifier with an `asyncio.Queue`.
`add_message_to_queue(msg)` enqueues without blocking; a background task
(`send_message_from_queue`, started by `start()`) pulls one message, calls the abstract
`_send_message(msg)`, catches any exception, and sleeps 1s — i.e. **max 1 message/sec,
failures never propagate**. `MQTTNotifier` already implemented it; registration happens
via `TradingCore.add_notifier()` and `TradingCore.notify()` fans every notification out
to all registered notifiers (that's why `status` output appears in Gchat too).
**We reused all of this** — our channel is just another subclass.

### 2.2 NEW: `hummingbot/notifier/gchat_notifier.py` (`GChatNotifier`)
- Constructor takes the webhook URL (must be non-empty) — comes from the
  `GCHAT_WEBHOOK_URL` environment variable (a secret; never in YAML).
- `_send_message`: lazily creates one shared `aiohttp.ClientSession`, POSTs
  `{"text": message}` with a 10s total timeout.
- Retry logic: on 429/5xx or a connection exception → one retry after
  `RETRY_DELAY = 2.0`s. Other HTTP statuses (e.g. 404 bad webhook) → log + drop
  (retrying a rejected payload is pointless).
- **Requeue on outage** (`_requeue_for_later_delivery`): if the final failure was a
  *connection* error (network down), the message goes back on the queue — capped at
  `MAX_PENDING_MESSAGES = 50` — so alerts raised *during* an outage deliver when
  connectivity returns. This exists because the first live Wi-Fi test proved the alert
  about the outage was itself dropped by the outage.
- `stop()` closes the HTTP session via `safe_ensure_future` (async close from sync).

### 2.3 NEW: `hummingbot/monitoring/alert.py`
`Severity` (INFO=1 / WARNING=2 / CRITICAL=3 — ints so `<` comparisons work),
`AlertStatus` (FIRING / RESOLVED), and the `Alert` dataclass:
`source` (which instance: "pmm_ml.wazirx.USDT-INR"), `check` (which invariant:
"tier2_depth_below_min"), `severity`, `title`, `message`, `metrics` (dict of key
figures), `status`, `timestamp`. **`(source, check)` is the alert's identity** — dedup
and recovery matching key.

### 2.4 NEW: `hummingbot/monitoring/alert_dispatcher.py` (`AlertDispatcher`)
The noise-control chokepoint every alert passes through. `dispatch(alert)`:
1. RESOLVED path: only delivered if `(source, check)` is in `_active_alerts` (we never
   send an all-clear for something we never announced); removes it from the dict.
2. FIRING path: drop if below `min_severity` (default WARNING) → drop if the same
   `(source, check)` was delivered less than `renotify_interval` (300s) ago → consume
   a token → record delivery time → format → deliver.
3. **Token bucket**: `rate_limit_per_min = 20` tokens, refilled continuously at
   20/60 per second (`_take_token` computes elapsed×rate, capped at max). A global
   circuit breaker against alert storms.
4. `_format` produces the structured chat layout: line 1 = emoji + *bold title*
   (`✅ *Resolved: ...*` for recoveries), line 2 = pretty source
   (`wazirx · USDT-INR (pmm_ml)`), line 3 = message, line 4 = compact
   `key: value | key: value` context. Delivery = `notifier.add_message_to_queue(text)`
   for every registered notifier.
5. The whole `dispatch` is wrapped in try/except — **alerting can never throw into the
   caller** (which is ultimately the trading loop).
- `time_fn` injection makes every timing rule unit-testable with a fake clock (a pattern
  used across the feature: dispatcher, FSM, day tracker).

---

## 3. Layer A — errors → Gchat

### 3.1 Background: Python logging + Hummingbot specifics
Every logger has a name (`hummingbot.connector.exchange.wazirx...`), a level, handlers,
and a `propagate` flag. Hummingbot adds custom levels — crucially **NETWORK = DEBUG+6
= 16**, used by `logger().network(...)` for connector problems — *below* INFO. The file
`conf/hummingbot_logs.yml` (from `hummingbot/templates/hummingbot_logs_TEMPLATE.yml`)
configures subtrees (`hummingbot.strategy`, `.connector`, `.client`, ...) with
**`propagate: false`** — their records go to their own handlers and never bubble to the
root logger.

### 3.2 NEW: `hummingbot/monitoring/gchat_log_handler.py` (`GChatLogHandler`)
A `logging.Handler` with `level=ERROR`. Its `emit(record)`:
- skips records whose logger name starts with `EXCLUDED_LOGGER_PREFIXES`
  (`hummingbot.monitoring`, `hummingbot.notifier`) — otherwise a failing notifier's own
  error logs would loop back into the pipeline forever;
- if called from a non-main thread, re-schedules itself onto the event loop via
  `call_soon_threadsafe` (same trick as the existing `MQTTLogHandler`);
- converts the record to an Alert (`_to_alert`): severity **CRITICAL if
  `record.levelno >= CRITICAL` or the record carries `exc_info`** (a traceback implies
  an unexpected exception), else WARNING; title = first line truncated to 120 chars;
  message = text + `ExcType: value` truncated to 600; `check = log.<logger name>` so
  dedup is per-component (a reconnect loop erroring every second = 1 Gchat message per
  5 min);
- everything try/excepted into `handleError` (never raises into the logging call).

### 3.3 The two logging landmines (both hit in live testing)
1. **`dictConfig` disables unlisted loggers**: `init_logging` (hummingbot/__init__.py)
   re-runs `logging.config.dictConfig` on *every* `start` command; by Python default
   this sets `disabled=True` on every existing logger not named (directly or via an
   ancestor) in the config. Our modules and even `hummingbot.core.trading_core` went
   silent from the 2nd start of a session. **Fix**: template v13 adds
   `hummingbot.core`, `hummingbot.monitoring`, `hummingbot.notifier` subtrees.
2. **`propagate: false`**: a handler attached only to the root logger never sees
   strategy/connector/client records. **Fix** (in `trading_core._gchat_target_loggers`):
   attach the handler to the root *and* to every subtree named in the logging config
   (excluding our own pipeline's subtrees), mirroring how the MQTT bridge patches
   loggers.

---

## 4. Layer B — measuring the SLA every second

### 4.1 NEW: `hummingbot/monitoring/sla_sampler.py` — the pure math
`evaluate_sample(mid, orders, band_pct, min_depth_quote)`:
- `mid` None/NaN/≤0 → out of spec with reason `orderbook_stale` (can't prove anything).
- band = `band_pct/100`. Bid is in band when `(mid − price)/mid <= band` (note: a bid
  *above* mid gives a negative distance → still counts); ask mirror-image. The edge is
  **inclusive** (`<=`).
- Per side: depth = Σ `price × amount_remaining` of in-band orders (**cumulative** —
  multi-level quoting counts). If depth < minimum, the reason explains *why*:
  no orders at all → `one_side_missing`; orders but none in band → `spread_too_wide`;
  in-band but small → `depth_below_min`.
- In spec ⇔ both sides ≥ minimum, i.e. reasons empty.
Pure function, no I/O, no clocks — unit tests pin the boundary semantics (band edge
inclusive, depth exactly at minimum passes, partial fills shrink depth, ...).
Lesson encoded here from live testing: PMM price quantization rounds the *bid* away
from mid, so an order *configured* at exactly the band edge measures at 1.5023% and
counts as zero → **quote tighter than the band** (1.4% vs 1.5%).

### 4.2 NEW: `hummingbot/monitoring/sampler_base.py` — the reuse contract
- `SLASample(in_spec, reasons, metrics, slo_results)` — the generic per-second result.
  `metrics` is a `Dict[str, str]` of key figures for logs/alerts; `slo_results` is an
  optional `Dict[str, bool]` of named sub-objectives (e.g. depth tiers) that each
  accrue their own daily uptime; single-objective monitors leave it None.
- `MonitorIdentity(monitor_type, connector_name, trading_pair)` — derives
  `source` ("pmm_ml.wazirx.USDT-INR", alert identity) and `instance_id`
  ("pmm_ml_wazirx_USDT-INR", file naming for state/CSV; includes the monitor type so
  different monitor types on the same pair can never collide).
- `SLASamplerBase` (ABC) — the *only* thing a new strategy implements:
  `take_sample()`, `check_alerts` (reason → (Severity, headline)), `config_summary()`
  (startup banner), optional `describe(sample)` (general detail line) and
  `describe_check(check, sample)` (per-alert detail; defaults to `describe`).

### 4.3 NEW: the PMM samplers
**`pmm_sampler.py` (`PMMDepthSampler`)** — single-objective:
- Reads the connector from `trading_core.markets[connector_name]`.
- **Staleness guard**: if `connector.network_status is not NetworkStatus.CONNECTED`,
  return a stale sample immediately. Why: a disconnected connector's *local* order book
  is frozen — `get_price_by_type` happily serves the last-known mid, which looks valid
  and proves nothing (live testing showed a 2-minute outage counted as healthy before
  this guard; after it, the monitor flagged the outage *faster than the error stream*).
- Orders come from `connector.in_flight_orders` (the client-side `InFlightOrder`
  registry): filter `trading_pair` match + `is_open`, map to
  `OpenOrder(is_buy, price, amount_remaining = amount − executed_amount_base)` —
  partial fills reduce measured depth.
- `CHECK_ALERTS`: `one_side_missing` → **CRITICAL** (one-sided = unhedged inventory
  risk); `spread_too_wide` / `depth_below_min` / `orderbook_stale` → WARNING.

**`multilevel_pmm_sampler.py` (`MultiLevelPMMSampler`)** — multi-objective (tiered):
- Config carries a `tiers:` list (`SLATierConfig`: name, band, min depth, uptime
  target). Tiers are **nested cumulative**: the pure math is evaluated once per tier
  band, so inner-band orders count toward every outer tier.
- Emits tier-scoped reasons (`tier2_depth_below_min`) → each tier has its own alert
  lifecycle; a globally missing side raises only the CRITICAL `one_side_missing`
  (suppressing redundant per-tier noise); `slo_results` carries per-tier pass/fail.
- `describe_check` shows only the failing tier's figures in that tier's alert.

### 4.4 NEW: `hummingbot/monitoring/breach_fsm.py` (`BreachStateMachine`)
Turns per-second breach booleans into *sustained-breach* alerts:
```
OK --breach--> PENDING --(now − started ≥ grace_period_sec)--> FIRING --recover--> OK
                  |__ recover before grace → OK (silent — blips never alert)
```
- One instance per (source, check); `update(breached, message, metrics)` fed every
  sample.
- While FIRING it dispatches on *every* update — the dispatcher's dedup turns that into
  1 delivery + a reminder each `renotify_interval`. Messages lead with a human-readable
  duration ("Down for 2m 04s. ...") via `format_duration`.
- Recovery from FIRING dispatches a RESOLVED alert ("Recovered after ...") carrying the
  current values; recovery from PENDING is silent (that's the whole point of grace).
- Injected `time_fn`; tested for blip/sustain/flap/zero-grace.

### 4.5 NEW: `hummingbot/monitoring/sla_monitor.py` (`SLAMonitor`) — the engine
Strategy-agnostic; owns the loop and glues everything:
- `start()`: first `_record_interrupted_day()` (see §5), then
  `safe_ensure_future(monitor_loop())`.
- `monitor_loop()`: logs the banner (identity + `sampler.config_summary()` + target +
  "alerts on"/"log-only"), then forever: `sampler.take_sample()` →
  `_process_sample()`, everything try/excepted (a monitoring bug can never leak out),
  `asyncio.sleep(sample_interval_sec)`.
- `_process_sample`: session counters → `_record_daily` (tracker + rollover→recorder)
  → `_log_transitions` (logs on in-spec flips *and* reason-set changes) →
  `_log_heartbeat` (every `heartbeat_log_interval_sec`: session uptime, current state,
  describe line, and the Day figure) → `_update_breach_alerts`.
- `_update_breach_alerts`: skipped during the first `alert_warmup_sec` (10s) after
  start — the strategy needs 1–2s to place its first orders and every start would
  otherwise raise a spurious `one_side_missing`; the *tally* still counts those seconds
  (honesty), only alerting waits. Then feeds every FSM
  `breached = check in sample.reasons` with `describe_check(check, sample)` as the
  message and slim metrics (session + day uptime only).
- FSMs are built in the constructor from `sampler.check_alerts` — only if a dispatcher
  was provided; otherwise the monitor is log-only (no webhook → still measures).
- `stop()`: cancels the task and `flush()`es the day tracker.

---

## 5. Layer C — the daily accounting

### 5.1 NEW: `hummingbot/monitoring/sla_day_tracker.py` (`SLADayTracker`)
- Day key: `datetime.fromtimestamp(ts, tz=ZoneInfo(day_reset_timezone)).date()` —
  the configured timezone's calendar day (00:30 IST belongs to the IST date even though
  UTC says yesterday; unit-tested).
- `record(sample)`: if the day key changed → build the finished day's `DaySummary`,
  reset counters, **return the summary exactly once** (the engine hands it to the
  recorder). Then increment `total`, `in_spec`, per-reason downtime counts, and — when
  the sample carries `slo_results` — the per-objective in-spec counters.
- Persistence: JSON to `data/sla/<instance_id>_sla_state.json`, throttled to one write
  per 5s, **atomic** (write `.tmp`, then `Path.replace`) so a kill mid-write can't
  corrupt it. Restart same day → counters restored (`Restored SLA day state...`).
  Restart on a *later* day → the stale file becomes `pending_summary`
  (`complete=False`) instead of being lost; corrupt/empty file → warn and start fresh.
- `DaySummary`: day, connector, pair, totals, `downtime_by_reason`, `slo_in_spec`,
  `complete`; computed `uptime_pct`, per-objective `slo_uptime_pct(name)` and
  `main_cause` (argmax of downtime reasons).

### 5.2 NEW: `hummingbot/monitoring/sla_recorder.py` (`SLARecorder`)
`record(summary)` writes and alerts **per objective**, each step individually
try/excepted:
1. Append to `data/sla/<instance_id>_sla_daily.csv` (header auto-written once):
   `date, connector, trading_pair, slo, uptime_pct, in_spec_seconds, total_seconds,
   downtime_minutes, main_cause, sla_met, complete_day`. One row for `overall` (judged
   against the base `required_uptime_pct`) plus one row per named objective (judged
   against its own target from `slo_targets`); per-objective `main_cause` filters the
   downtime reasons by the objective's prefix.
2. For every row below its target (and a non-empty day): dispatch 🔴
   `daily_sla_breach` (overall) or `daily_sla_breach_<name>` with uptime, target,
   downtime minutes, main cause, and a "Partial day" note when `complete=False`.
The engine calls it at rollover; `_record_interrupted_day` calls it at startup for a
pending (bot-was-down-at-midnight) day.

---

## 6. Configuration & wiring

### 6.1 NEW: `hummingbot/monitoring/config.py`
- `MonitoringConfigBase` (pydantic, validated): `enabled`, `required_uptime_pct` (96),
  `sample_interval_sec` (1), `grace_period_sec` (2), `alert_warmup_sec` (10),
  `day_reset_timezone` (Asia/Kolkata), `heartbeat_log_interval_sec` (300), and the
  `slo_targets()` hook (empty for single-objective monitors).
- `PMMSLAMonitorConfig(MonitoringConfigBase)`: `connector_name`, `trading_pair`,
  `spread_band_pct` (1.5), `min_depth_quote` (20000).
- `MultiLevelPMMSLAMonitorConfig(MonitoringConfigBase)`: `connector_name`,
  `trading_pair`, `tiers: List[SLATierConfig]` (unique names enforced);
  `slo_targets()` maps tier name → its uptime target.
- `MONITOR_CONFIG_SECTIONS = {"pmm_sla_monitor": ..., "multilevel_pmm_sla_monitor":
  ...}` — the YAML section registry. `load_monitoring_config` reads
  `conf/monitoring.yml` (path from `CONF_DIR_PATH`), returns the first enabled known
  section, None when the file is absent / no section / `enabled: false`, and **raises
  on invalid values** (misconfiguration must be loud).

### 6.2 NEW: `hummingbot/monitoring/factory.py`
`SAMPLER_FACTORIES = {PMMSLAMonitorConfig: PMMDepthSampler,
MultiLevelPMMSLAMonitorConfig: MultiLevelPMMSampler}`.
`create_sla_monitor(trading_core, config, dispatcher)`: look up the sampler by config
type → build it → verify `identity.connector_name in trading_core.markets` (else warn +
None) → assemble `SLADayTracker` + `SLARecorder` (with `config.slo_targets()`) +
`SLAMonitor`. **Adding a strategy = config class + sampler class + one entry in each
registry.**

### 6.3 MODIFIED: `hummingbot/core/trading_core.py` — the lifecycle
Modeled exactly on the pre-existing **KillSwitch pattern** (component created from
config at strategy start, started via `_wait_till_ready` so it only runs once all
markets report `ready`, stopped in `stop_strategy`):
- `_start_strategy_execution()` calls, right after the kill-switch block:
  `_start_gchat_alerts()` (env var present → notifier + dispatcher + log handler
  attached to root + configured subtrees + `add_notifier`) then
  `await _start_sla_monitor()` (config present → `create_sla_monitor` →
  `_wait_till_ready(monitor.start)`).
- `stop_strategy()` calls `_stop_sla_monitor()` (cancel + flush) and
  `_stop_gchat_alerts()` (detach handler from every patched logger, remove + stop
  notifier).
- Both wrapped so any failure logs and disables monitoring **without blocking the
  strategy**.
- New fields: `alert_dispatcher` (public — shared by handler, FSMs, recorder),
  `sla_monitor`, private `_gchat_notifier`, `_gchat_log_handler`,
  `_gchat_patched_loggers`.

### 6.4 Local runtime config: `conf/monitoring.yml` (not committed)
The live switchboard. Delete the file or `enabled: false` to turn measurement off;
unset `GCHAT_WEBHOOK_URL` to turn alert delivery off (monitor then runs log-only).

---

## 7. Reused vs new — the complete inventory

| Pre-existing Hummingbot piece | How the feature uses it |
|---|---|
| `NotifierBase` queue/drain/error-isolation | `GChatNotifier` subclasses it |
| `TradingCore.add_notifier()` / `notify()` fan-out | Gchat gets `status`/kill-switch messages free |
| KillSwitch lifecycle (`_wait_till_ready`, start/stop hooks) | exact template for monitor wiring |
| `MQTTLogHandler` (thread hop, logger patching idea) | template for `GChatLogHandler` + target-logger attach |
| `connector.in_flight_orders` / `InFlightOrder` | order source for depth measurement |
| `connector.get_price_by_type(pair, MidPrice)` | live mid |
| `connector.network_status` (`NetworkIterator`) | staleness guard |
| `safe_ensure_future` | all background tasks |
| `HummingbotLogger` / class-level logger pattern | every new class follows it |
| `data_path()`, `CONF_DIR_PATH` | file locations |
| logging template + `get_logging_conf()` | handler targets; template extended (v13) |
| pydantic config-model conventions | config classes |
| aioresponses/IsolatedAsyncioWrapperTestCase test infra | notifier tests |

New: everything under `hummingbot/monitoring/` (12 modules), `gchat_notifier.py`,
the trading_core wiring, the template logger additions, 12 test files (~143 tests).

## 8. Key values and their reasoning
`grace 2s` — longer than a refresh cancel→replace gap (~1s), far shorter than real
incidents; `warmup 10s` — first order placement takes 1–2s, margin for slow starts;
`renotify 300s` / `rate 20/min` — reminder cadence vs storm breaker; `1s sampling` —
the SLA's own definition, and reads are all local (no API weight); `IST days` — INR
market; `persist 5s` — ≤5 lost samples per restart; `ERROR threshold` — WARNINGs are
too noisy, silent things are Layer B's job; `one_side_missing CRITICAL` — inventory
risk, the rest WARNING; `quote 1.4% vs 1.5% band` — tick quantization; `webhook via
env` — secret + no template migration; `CSV before Sheets` — no service account
dependency; alerts *during* outage requeue (cap 50) because they're usually *about*
the outage.

## 9. Three end-to-end walkthroughs

**A. Connector error → Gchat.** WazirX user-stream raises → connector logs ERROR →
record hits `GChatLogHandler` (attached to `hummingbot.connector` subtree) → not
excluded, main thread → `_to_alert` (WARNING, check=`log.hummingbot.connector...`) →
dispatcher: severity ok, not a dup, token ok → format 🟠 → queue → `GChatNotifier`
drains → POST → message in space. Same error 5s later → dedup drops it; 5 min later →
reminder.

**B. Depth breach → alert → recovery.** Sampler: orders in band sum below the minimum
→ `SLASample(False, [depth_below_min], {...})` → engine tallies (session+day),
logs `SLA OUT of spec (depth_below_min): ...`, FSM OK→PENDING; 2s later still breached
→ FIRING → Alert("Depth below the SLA minimum", "Down for 2s. ...") → dispatcher →
Gchat 🟠. Condition persists → re-dispatch each second, dedup delivers one reminder per
5 min. Depth recovers → FSM FIRING→OK → RESOLVED alert → dispatcher matches active
entry → ✅ "Recovered after 1m 14s".

**C. Midnight.** 00:00:00 IST sample: tracker sees new day key → returns yesterday's
`DaySummary`, resets counters, persists → engine logs `SLA day closed: ...` → recorder
appends one CSV row per objective; any objective under its target → 🔴
`daily_sla_breach(_<name>)`. If the bot was *down* at midnight: next start finds the
stale state file → `pending_summary` → recorded with `complete_day: no`.

## 10. The live-testing lessons baked into the code
1. Wi-Fi test #1: outage alerts were dropped → **requeue-on-connection-failure**.
2. Restart test: `dictConfig` silently disabled our loggers → **template v13** (also
   fixed pre-existing trading_core silence).
3. Band-edge test: bid at *configured* 1.5% measured 1.5023% (tick rounding) → ops
   guidance **quote inside the band**; monitor was correct.
4. Wi-Fi test #2: frozen order book counted as uptime → **network_status guard**.
5. Real incident caught: a `stop` during a network blip failed to cancel orders →
   orphan orders on the exchange → the monitor's depth jump (2× ask depth) was the only
   visible signal. Also revealed: failed cancels log at NETWORK level (invisible to
   Layer A) — measured checks are the safety net.

## 11. File & test map
```
hummingbot/monitoring/
  alert.py                  alert_dispatcher.py       gchat_log_handler.py
  sampler_base.py           sla_sampler.py            pmm_sampler.py
  multilevel_pmm_sampler.py breach_fsm.py             sla_monitor.py
  sla_day_tracker.py        sla_recorder.py           config.py
  factory.py
hummingbot/notifier/gchat_notifier.py
hummingbot/core/trading_core.py                       (wiring)
hummingbot/templates/hummingbot_logs_TEMPLATE.yml     (v13)
conf/monitoring.yml (local), data/sla/*.json|*.csv    (runtime artifacts)

test/hummingbot/monitoring/   one test file per module (~143 tests total)
test/hummingbot/notifier/test_gchat_notifier.py
test/hummingbot/core/test_trading_core.py             (wiring tests)
```
Run: `python -m pytest test/hummingbot/monitoring/ test/hummingbot/notifier/
test/hummingbot/core/test_trading_core.py -q` and
`python -m flake8 hummingbot/monitoring/`.

## 12. Operations quick-reference
Enable: `export GCHAT_WEBHOOK_URL=...` + `conf/monitoring.yml` present → start strategy.
Verify: banner lines `Google Chat alerting enabled...` and `SLA monitor started ...
(..., alerts on)`. Watch: `grep -E "SLA|Google Chat|ERROR"` on the strategy log.
Artifacts: `data/sla/<monitor>_<connector>_<pair>_sla_state.json` (live counters,
updates ≤5s) and `..._sla_daily.csv` (one row per objective per finished day). Disable
measurement: remove monitoring.yml; disable delivery only: unset the env var (log-only
mode). Troubleshooting: no alerts at all → check the two banner lines; alerts but no
SLA lines → monitoring.yml missing/disabled; SLA lines but no alerts → env var unset
(banner says "log-only"); everything silent after a restart → check
conf/hummingbot_logs.yml is template v13.

---

*See also: [MONITORING_USER_GUIDE.md](MONITORING_USER_GUIDE.md) for setup and daily use,
and [MONITORING_DEVELOPER_GUIDE.md](MONITORING_DEVELOPER_GUIDE.md) to add monitoring
for a new strategy.*
