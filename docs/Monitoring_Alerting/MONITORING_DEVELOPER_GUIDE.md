# How to Add SLA Monitoring & Alerting for a New Strategy

## 0. What you get for free vs. what you build

The monitoring engine is strategy-agnostic. By implementing one small class you inherit
**all** of this with zero extra code:

| Inherited automatically | Where it lives |
|---|---|
| 1-second sampling loop, exception isolation, startup warmup | `monitoring/sla_monitor.py` |
| Grace-period alerting (blips never alert), reminders every 5 min, ✅ recovery messages | `monitoring/breach_fsm.py` + `alert_dispatcher.py` |
| Dedup, severity filter, 20/min rate limit | `monitoring/alert_dispatcher.py` |
| Google Chat delivery with outage requeue | `notifier/gchat_notifier.py` |
| Transition + heartbeat logging | `monitoring/sla_monitor.py` |
| Per-day uptime tally (per objective), timezone-aware reset, restart persistence | `monitoring/sla_day_tracker.py` |
| Daily CSV rows + per-objective `daily_sla_breach` alerts | `monitoring/sla_recorder.py` |
| Strategy lifecycle wiring (starts/stops with the strategy) | `core/trading_core.py` (untouched) |
| ERROR-log alerts (Layer A) | already bot-wide, nothing to do |

**You build exactly three things:**
1. A **config class** (what is tunable for your SLA),
2. A **sampler class** (how to measure "in spec right now"),
3. **Two registry entries** (so the factory can assemble your monitor).

Files you must NOT touch: `sla_monitor.py`, `breach_fsm.py`, `alert_dispatcher.py`,
`gchat_notifier.py`, `sla_day_tracker.py`, `sla_recorder.py`, `trading_core.py`.

Two in-tree reference implementations to copy from:
- `monitoring/pmm_sampler.py` — single-objective (one band, one depth, one target).
- `monitoring/multilevel_pmm_sampler.py` — multi-objective (tiered SLAs, per-tier
  targets); see §7 below.

---

## 1. Before coding: design your SLA on paper

Answer these five questions first (worth a 10-minute review with a senior):

1. **What is the condition?** One sentence, measurable every second from data the bot
   already has. *Example: "an open order exists within `max_distance_pct` of the top
   of book on the configured side."*
2. **What are the failure reasons?** Each distinct *why* becomes a reason code (a
   stable snake_case string) and its own alert with its own dedup/recovery lifecycle.
   Reuse `orderbook_stale` from `sla_sampler.py` for "can't trust the data".
   *Example: `order_missing`, `too_far_from_top`, `orderbook_stale`.*
3. **Severity per reason?** CRITICAL = money/risk exposure (missing order, unhedged
   leg); WARNING = degraded quality. *Example: `order_missing` CRITICAL, others WARNING.*
4. **What thresholds must be tunable?** Those become config fields. Generic knobs
   (grace, warmup, uptime target, timezone, intervals) are inherited — do not redefine.
5. **What key figures should appear in logs/alerts?** Those become the sample's
   `metrics` dict. *Example: order price, top-of-book, distance %.*

---

## 2. Step 1 — the config class (`hummingbot/monitoring/config.py`)

Add a subclass of `MonitoringConfigBase` containing **only** your strategy-specific
fields, and register its YAML section name. Worked example: a **Limit Chaser** monitor
("the order must always rest within X% of the top of book on its side"):

```python
class LimitChaserSLAMonitorConfig(MonitoringConfigBase):
    """Limit-chaser SLA: an order must rest within max_distance_pct of the top of book."""
    connector_name: str
    trading_pair: str
    side: str = Field(default="BUY", pattern="^(BUY|SELL)$")
    max_distance_pct: Decimal = Field(default=Decimal("0.5"), gt=0)


MONITOR_CONFIG_SECTIONS: Dict[str, Type[MonitoringConfigBase]] = {
    "pmm_sla_monitor": PMMSLAMonitorConfig,
    "multilevel_pmm_sla_monitor": MultiLevelPMMSLAMonitorConfig,
    "limit_chaser_sla_monitor": LimitChaserSLAMonitorConfig,   # <-- add this line
}
```

Notes:
- Inherited generic fields (do not redeclare): `enabled`, `required_uptime_pct`,
  `sample_interval_sec`, `grace_period_sec`, `alert_warmup_sec`, `day_reset_timezone`,
  `heartbeat_log_interval_sec`.
- Use pydantic validation (`gt`, `le`, `pattern`) — `load_monitoring_config` raises on
  bad values *on purpose* so misconfiguration is loud at startup.

---

## 3. Step 2 — the sampler class (new file, e.g. `monitoring/limit_chaser_sampler.py`)

Implement `SLASamplerBase`. This is the entire strategy-specific runtime:

```python
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.alert import Severity
from hummingbot.monitoring.config import LimitChaserSLAMonitorConfig
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample, SLASamplerBase
from hummingbot.monitoring.sla_sampler import ORDER_BOOK_STALE  # reuse shared reason

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore

ORDER_MISSING = "order_missing"
TOO_FAR_FROM_TOP = "too_far_from_top"

CHECK_ALERTS = {
    ORDER_MISSING: (Severity.CRITICAL, "No standing chaser order"),
    TOO_FAR_FROM_TOP: (Severity.WARNING, "Chaser order too far from the top of book"),
    ORDER_BOOK_STALE: (Severity.WARNING, "Order book stale or connector disconnected"),
}


class LimitChaserSampler(SLASamplerBase):
    def __init__(self, trading_core: "TradingCore", config: LimitChaserSLAMonitorConfig):
        super().__init__(MonitorIdentity(
            monitor_type="limit_chaser",
            connector_name=config.connector_name,
            trading_pair=config.trading_pair,
        ))
        self._trading_core = trading_core
        self._config = config

    @property
    def check_alerts(self) -> Dict[str, Tuple[Severity, str]]:
        return CHECK_ALERTS

    def config_summary(self) -> str:
        return f"{self._config.side} within {self._config.max_distance_pct}% of top"

    def take_sample(self) -> SLASample:
        connector = self._trading_core.markets.get(self._config.connector_name)
        connected = (connector is not None
                     and getattr(connector, "network_status", NetworkStatus.CONNECTED)
                     is NetworkStatus.CONNECTED)
        if not connected:
            return SLASample(in_spec=False, reasons=[ORDER_BOOK_STALE],
                             metrics={"top": "n/a", "distance_pct": "n/a"})

        is_buy = self._config.side == "BUY"
        try:
            top = connector.get_price_by_type(
                self._config.trading_pair,
                PriceType.BestBid if is_buy else PriceType.BestAsk)
        except Exception:
            top = None
        if top is None or top.is_nan() or top <= 0:
            return SLASample(in_spec=False, reasons=[ORDER_BOOK_STALE],
                             metrics={"top": "n/a", "distance_pct": "n/a"})

        orders = [o for o in connector.in_flight_orders.values()
                  if o.trading_pair == self._config.trading_pair and o.is_open
                  and (o.trade_type.name == "BUY") is is_buy]
        if not orders:
            return SLASample(in_spec=False, reasons=[ORDER_MISSING],
                             metrics={"top": str(top), "distance_pct": "n/a"})

        best_distance_pct = min(abs(top - o.price) / top for o in orders) * Decimal("100")
        in_spec = best_distance_pct <= self._config.max_distance_pct
        return SLASample(
            in_spec=in_spec,
            reasons=[] if in_spec else [TOO_FAR_FROM_TOP],
            metrics={"top": str(top), "distance_pct": f"{best_distance_pct:.3f}"},
        )

    def describe(self, sample: SLASample) -> str:
        return (f"distance {sample.metrics['distance_pct']}% from top {sample.metrics['top']}, "
                f"allowed {self._config.max_distance_pct}%")
```

### The rules every sampler must follow

1. **Read-only.** Never place, cancel, or modify anything. Only read connector state.
2. **Never trust a disconnected connector.** Copy the `network_status` guard verbatim —
   a disconnected connector serves a *frozen* local order book whose last-known prices
   look valid. Without the guard, outages count as healthy uptime (proven live).
3. **Reason codes are stable identifiers.** They key the breach FSMs, alert dedup and
   the per-reason downtime accounting in the daily CSV. Never rename casually; reuse
   `ORDER_BOOK_STALE` from `sla_sampler.py` rather than inventing a synonym.
4. **`metrics` values are strings** — they go straight into log lines and the alert's
   context footer. Format numbers yourself (`f"{x:.2f}"`).
5. **`reasons` empty ⇔ `in_spec=True`.** The engine and tracker rely on that invariant.
6. **Don't raise from `take_sample()`** if you can classify the situation — a raised
   exception is caught by the engine but logged as a monitor error rather than scored
   as a sample. "Data unavailable" should be an `orderbook_stale` sample, not a crash.
7. **Nontrivial math goes into a pure function** in its own module (like
   `sla_sampler.evaluate_sample`) so boundary cases get direct unit tests. Inline math
   is fine only when it is a few lines (as above).
8. **Data sources available** through `trading_core.markets[connector_name]`:
   `in_flight_orders` (open orders: `price`, `amount`, `executed_amount_base`,
   `trade_type`, `is_open`, `trading_pair`), `get_price_by_type(pair, PriceType.X)`
   (MidPrice/BestBid/BestAsk/LastTrade), `get_order_book(pair)`, `get_balance(asset)`,
   `get_available_balance(asset)`, `network_status`.

Optional hooks with sensible defaults: `describe_check(check, sample)` — the detail
line for one specific check's alert (override to show only that check's figures, see
the multilevel sampler); `describe(sample)` — the general log/heartbeat detail line.

---

## 4. Step 3 — register in the factory (`hummingbot/monitoring/factory.py`)

```python
SAMPLER_FACTORIES: Dict[...] = {
    PMMSLAMonitorConfig: PMMDepthSampler,
    MultiLevelPMMSLAMonitorConfig: MultiLevelPMMSampler,
    LimitChaserSLAMonitorConfig: LimitChaserSampler,   # <-- add this line
}
```

That's all the wiring. `trading_core` already calls
`create_sla_monitor(trading_core, config, dispatcher)`, which looks up your sampler by
config type, verifies the connector is part of the running strategy, and assembles the
tracker + recorder + engine around it. State and CSV files are automatically named
`data/sla/<monitor_type>_<connector>_<pair>_...` from your `MonitorIdentity`, so
monitor types can never collide.

**Known limit — one monitor at a time:** `load_monitoring_config` returns the *first*
enabled known section in `conf/monitoring.yml`. Running multiple monitors concurrently
is a designed-for but deferred feature (the factory/registry is ready; the loader and
`trading_core.sla_monitor` field are single-instance today).

---

## 5. Step 4 — tests

You test **only your own two components**; the engine, FSM, dispatcher, tracker and
recorder already have their own suites.

| What | Template to copy | Typical cases |
|---|---|---|
| Pure math (if extracted) | `test/hummingbot/monitoring/test_sla_sampler.py` | boundary inclusive/exclusive, zero/None inputs, cumulative behavior |
| Sampler | `test/hummingbot/monitoring/test_pmm_sampler.py` (or `test_multilevel_pmm_sampler.py`) | in-spec sample + metrics; each reason code; disconnected connector → stale; missing connector; price fetch raising; orders filtered by pair/side/open; `describe()` content; identity values |
| Config | `test/hummingbot/monitoring/test_monitoring_config.py` | your section loads; defaults; invalid values raise |

Mock pattern: `MagicMock()` connector with `get_price_by_type.return_value`,
`network_status = NetworkStatus.CONNECTED`, and `in_flight_orders` as a dict of `Mock`
orders carrying the real attribute types (`Decimal` prices/amounts, `TradeType` enum).

Run everything:
```bash
python -m pytest test/hummingbot/monitoring/ -q
python -m flake8 hummingbot/monitoring/
```

---

## 6. Step 5 — configure and run

`conf/monitoring.yml` (replace the existing section — one monitor at a time):

```yaml
limit_chaser_sla_monitor:
  enabled: true
  connector_name: wazirx
  trading_pair: USDT-INR
  side: BUY
  max_distance_pct: 0.5
  required_uptime_pct: 96
  grace_period_sec: 2
  alert_warmup_sec: 10
```

Then: `export GCHAT_WEBHOOK_URL=...` → start the bot → start the strategy.

---

## 7. Multi-objective (tiered) SLAs

When one monitor must track **several uptime targets at once** (e.g. depth tiers with
99/97/96% targets), use the multi-objective support — `monitoring/multilevel_pmm_sampler.py`
is the complete in-tree example:

- Your sampler sets `SLASample.slo_results = {"tier1": True, "tier2": False, ...}` —
  each named objective accrues its **own daily uptime figure** in the tracker.
- Your config overrides `slo_targets()` returning `{name: required_uptime_pct}` —
  the recorder writes **one daily CSV row per objective** (plus "overall") and raises
  a per-objective `daily_sla_breach_<name>` alert against each target.
- Give each objective its own reason code (e.g. `tier2_depth_below_min`) so real-time
  alerts, dedup and downtime attribution stay per-objective, and override
  `describe_check` so each alert shows only its own figures.

---

## 8. Step 6 — verification checklist (every new monitor, before calling it done)

| # | Check | Pass looks like |
|---|---|---|
| 1 | Startup banner | `SLA monitor started for <conn>:<pair> (<your config_summary>, target uptime 96%, alerts on).` |
| 2 | Heartbeat | periodic line with session uptime, your `describe()` output, and the `Day ... uptime` figure |
| 3 | Each reason code, forced live | flip a config threshold (or break the condition) → `SLA OUT of spec (<reason>): ...` in the log |
| 4 | Alert + reminder | 🟠/🔴 in Gchat ~`warmup+grace` seconds after a sustained breach; one reminder at +5 min |
| 5 | Recovery | fix the condition → `SLA back IN spec` + ✅ `Resolved ... Recovered after Xs` |
| 6 | Quiet startup | no alert during the first ~12s of any start |
| 7 | Restart persistence | `stop`/`start` mid-day → `Restored SLA day state...`, Day counter continues |
| 8 | Outage honesty | disconnect the network ~40s → `orderbook_stale` samples, uptime drops, queued alerts deliver after reconnect |
| 9 | Day close | edit the state file's `"day"` to yesterday, restart → CSV row(s) + breach alert(s) if below target |

---

## 9. Quick reference — the complete diff for a new strategy

```
hummingbot/monitoring/config.py                 ~10 lines (config class + 1 registry line)
hummingbot/monitoring/<strategy>_sampler.py     ~80–120 lines (the sampler)
hummingbot/monitoring/factory.py                1 registry line
test/hummingbot/monitoring/test_<strategy>_sampler.py   copy/adapt from PMM's
conf/monitoring.yml                             your section (local, not committed)
```

Nothing else changes. If you find yourself editing `sla_monitor.py`,
`trading_core.py`, or the dispatcher to make a strategy work — stop; either the
sampler contract is being misused, or you've found a genuine engine gap that should be
its own reviewed change.

---

*See also: [MONITORING_USER_GUIDE.md](MONITORING_USER_GUIDE.md) for setup and daily use,
and [MONITORING_INTERNALS.md](MONITORING_INTERNALS.md) for the framework's internals
and design decisions.*
