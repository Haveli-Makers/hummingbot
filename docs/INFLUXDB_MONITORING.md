# InfluxDB monitoring: hummingbot

Hummingbot writes monitoring data to InfluxDB 3. The api-server reads it back and serves it under `/metrics`, and
the dashboard draws it. This file covers the hummingbot side: what it writes, how to turn it on, and the account
monitor. The api-server and dashboard repos each have their own `INFLUXDB_MONITORING.md`.

## Why InfluxDB

Before, a bot's data reached the api-server over MQTT, and the api-server kept only the latest values: the last
heartbeat and the latest performance. Market data was sampled into Postgres every 15 minutes. Nothing could answer
"was the bot up all night?", "when did it hang?" or "where were its orders against the price?".

A proof of concept in tried InfluxDB 3 Core (free, open source, queried with SQL). Four days of three
bots came to 2.7 million rows in 78 MB, queries took 23–65 ms, and InfluxDB used about 0.5 GB of memory. The we
decided:

- InfluxDB 3 Core runs next to the api-server, one per environment, reachable only from that machine. Data is kept
  14 days.
- Hummingbot writes straight to InfluxDB. MQTT stays for bot commands only.
- The api-server serves the data and the dashboard draws it.
- Monitoring is per exchange account first: an account monitor (a bot that never trades) records an account's
  balances, open orders, fills and market data. Bot-level data is still written; its dashboard page is hidden for now.

## How the pieces fit

```
bot (any strategy) ──┐
                     ├──> InfluxDB 3 (api-server's compose, 127.0.0.1:8181) ──> api-server /metrics ──> dashboard
account monitor ─────┘
```

## Turning it on

Monitoring is on only when both `INFLUXDB_URL` and `INFLUXDB_TOKEN` are set in the bot's environment.
`INFLUXDB_DATABASE` is optional (default `hbot`). Bots deployed by the api-server get all three automatically when
the api-server has monitoring on. To run a bot by hand on the api-server's machine:

```bash
export INFLUXDB_URL=http://localhost:8181 INFLUXDB_DATABASE=hbot INFLUXDB_TOKEN=$(docker exec hummingbot-influxdb sh -c 'cat "$INFLUXDB3_ADMIN_TOKEN_FILE"' | python -c "import json,sys; print(json.load(sys.stdin)['token'])")
./start
```

When a strategy starts, the log says `Bot monitoring: writing to InfluxDB at http://localhost:8181 (database hbot)`.

## What every bot writes (the bot sink)

`InfluxDBSink` in `hummingbot/remote_iface/influxdb.py` starts and stops with the strategy (`TradingCore`). Every row
is tagged `bot=<instance_id>` from `conf_client.yml`.

| Table | When | What |
|---|---|---|
| `bot_heartbeat` | strategy loop every 1 s, a background thread every 5 s | The loop's beat stops when it freezes while the thread's keeps going: that is how a hang is told from a crash |
| `bot_lifecycle` | at start, and at a clean stop | `started`, `stopped` |
| `bot_price` | every 1 s, per market | best bid, best ask, mid |
| `open_order` | every 5 s | one row per open order of the bot |
| `order_event` | on each order event | created, partially_filled, filled, cancelled, failed, expired |
| `controller_performance` | every 5 s (V2 strategies) | PnL and volume per controller |
| `grid_state`, `grid_level` | every 5 s (grid controllers) | written when a controller's `get_custom_info()` returns `{"reference_price": ..., "levels": [{"id", "side", "price", "amount", "state", "order_id"}]}` |

The sink can't block or crash the bot. Rows are buffered and written in 1-second batches. While InfluxDB is
unreachable, the newest 50,000 rows are kept and a warning is logged at most once a minute.

## The account monitor

`scripts/account_monitor.py` is a bot that never places or cancels an order. It holds one exchange account's API keys
and records the whole account, whatever acts on it: any bot on that account, or a person. Stopping it cancels
nothing, because hummingbot only cancels orders a strategy placed itself.

Its config, in `conf/scripts/<name>.yml`:

| Field | Default | Meaning |
|---|---|---|
| `account_name` | `coindcx-main` | The account's name on the dashboard (letters, digits, `.`, `_`, `-`) |
| `exchange` | `coindcx` | The connector |
| `trading_pairs` | `ETH-INR` | Comma-separated pairs for open orders and market data |
| `update_interval` | `5` | Seconds between reads of balances, open orders and fills |
| `market_data_interval` | `1` | Seconds between order book samples |
| `fills_lookback_hours` | `24` | How far back it reads fills when it starts |

To run it:
1. `connect <exchange>` once. For an IP-bound CoinDCX key, also enter the proxy URL it asks for.
2. Set the `INFLUXDB_*` variables as above, then `./start`.
3. `start --script account_monitor.py --conf <name>.yml`. `status` then shows its last read and any errors.

What it writes (`hummingbot/remote_iface/account_monitor.py`), every row tagged `account` and `exchange`:

| Table | What |
|---|---|
| `account_balance` | total and available per asset. An asset that empties gets one zero row. |
| `account_open_order` | every open order on the account, read from the exchange |
| `account_fill` | each new trade from the exchange's trade history. A restart rewrites the same rows, so nothing is counted twice. |
| `account_update` | one row per read: what worked, counts, errors |
| `market` | best bid and ask, mid, spread, depth within 1% of mid, seconds since the book last changed (`source=account_monitor`) |

Exchange support:
- Open orders need the connector's `get_open_orders()`, which only CoinDCX has so far.
- Fills need `get_all_account_trades()`: CoinDCX, WazirX, CSX, CoinSwitch, Zebpay and CoinEx have it.
- Balances and market data work with any connector.

## Things to know

- **Reads share the key's rate limits.** The monitor's reads count against the same key as anything else using it.
  Use a read-only key if the exchange offers one, and keep `update_interval` at 5 s or more.
- **Never trade with a monitoring account's keys.** While `conf/connectors/<exchange>.yml` holds a production
  account's keys, any strategy started on that exchange trades on that account.
- **CoinDCX market data.** Until the CoinDCX order book fix (PR #67) is merged, CoinDCX's book freezes at its first
  snapshot, so the monitor's CoinDCX market data goes flat. Balances, orders and fills are not affected.
- **The token is an admin token.** InfluxDB 3 Core has no scoped tokens. Keep InfluxDB on the private machine or
  network, and the token out of shared files.
- **One name per bot.** Rows are tagged with `instance_id`, so two bots with the same one mix their data.

## Files and tests

- `hummingbot/remote_iface/influxdb.py`: settings, line protocol, the writer and the bot sink.
- `hummingbot/remote_iface/account_monitor.py` and `scripts/account_monitor.py`: the account monitor.
- `hummingbot/core/trading_core.py`: starts and stops the sink with the strategy.
- `hummingbot/connector/exchange/coindcx/coindcx_exchange.py`: `get_open_orders()`.
- Tests:
  - `test/hummingbot/remote_iface/test_influxdb.py`
  - `test/hummingbot/remote_iface/test_account_monitor.py`
  - `test/hummingbot/core/test_trading_core.py`
  - `test/hummingbot/connector/exchange/coindcx/test_coindcx_open_orders.py`

## Troubleshooting

| You see | Why, and what to do |
|---|---|
| No "Bot monitoring: writing to InfluxDB" line | `INFLUXDB_URL` or `INFLUXDB_TOKEN` isn't set in that terminal |
| "InfluxDB at … is unreachable" | Wrong URL, or the api-server's InfluxDB isn't running (`make monitoring` in the api-server) |
| "InfluxDB answered HTTP 401" | Wrong token. Wiping InfluxDB's volume makes a new one, so export it again. |
| Errors in the account monitor's `status` | The exchange refused a read (keys, proxy, rate limit). The dashboard shows the same error. |
