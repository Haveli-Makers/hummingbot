# Hummingbot Connector E2E Tests

End-to-end tests that run **directly against live exchange connectors** using the
Hummingbot connector stack — no API-server, no dashboard, no intermediary.

Each test instantiates a real connector, calls into it the same way a strategy
would, and asserts on real exchange state (order book, in-flight orders, balances,
trade history).

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Hummingbot conda environment | `conda activate hummingbot` before running |
| Real exchange accounts | Tests place and immediately cancel real (tiny) orders |
| API keys with **spot trading** permission | Read + Trade; no withdraw permission needed |
| `python-dotenv` | Already in hummingbot deps; provides `dotenv_values()` |
| `pytest-asyncio >= 1.0` | `asyncio_mode = auto` used in `pytest.ini` |

---

## Configuration

### 1. Create your `.env` file

```bash
cp test/e2e_tests/.env.example test/e2e_tests/.env
```

The `.env` file is **never committed** (it contains real API keys). The test loader
uses `dotenv_values()` which reads **only** the `.env` file — OS/shell environment
variables are never consulted.

### 2. Enable an exchange

```ini
TEST_BINANCE=true
```

Set to `false` (or omit the line entirely) to skip that exchange.

### 3. Add credentials

Credentials use the format:

```
{KEY}_CRED_{exact_connector_param_name}=value
```

The `{exact_connector_param_name}` must match the parameter name in the connector's
`ConfigMap` class exactly (case-sensitive). Find them in:

```
hummingbot/connector/exchange/{connector_name}/{connector_name}_utils.py
```

**Example — Binance:**

```ini
BINANCE_CRED_binance_api_key=abc123
BINANCE_CRED_binance_api_secret=xyz789
```

**Example — KuCoin (three credential fields):**

```ini
KUCOIN_CRED_kucoin_api_key=abc123
KUCOIN_CRED_kucoin_secret_key=xyz789
KUCOIN_CRED_kucoin_passphrase=mypassphrase
```

### 4. Optional overrides per exchange

```ini
BINANCE_TRADING_PAIR=BTC-USDT          # default: ETH-USDT
BINANCE_LIMIT_BUY_AMOUNT=0.001         # order size in base asset
BINANCE_LIMIT_SELL_AMOUNT=0.001
BINANCE_BUY_PRICE_OFFSET=0.80          # fraction of mid-price (0.80 = 20% below)
BINANCE_SELL_PRICE_OFFSET=1.20         # fraction of mid-price (1.20 = 20% above)
BINANCE_EDIT_PRICE_MULTIPLIER=0.78     # applied to buy price in edit test

# Per-exchange timing (override global defaults)
BINANCE_ORDER_PROPAGATION_WAIT=10      # seconds to wait for order in in_flight_orders
BINANCE_CANCEL_PROPAGATION_WAIT=15     # seconds to wait for cancelled order to leave
```

### Global timing defaults

```ini
ORDER_PROPAGATION_WAIT=10
CANCEL_PROPAGATION_WAIT=15
CONNECTOR_READY_TIMEOUT=60
```

---

## Running the Tests

From the **repository root**, with the hummingbot conda env activated:

```bash
# All enabled exchanges, all 8 tests
pytest test/e2e_tests/ -v

# One specific exchange only
pytest test/e2e_tests/ -v -k binance

# One specific test across all exchanges
pytest test/e2e_tests/ -v -k test_01_orderbook

# One test on one exchange
pytest test/e2e_tests/ -v -k "binance and test_02"

# Stop at first failure
pytest test/e2e_tests/ -v -x

# Show print/log output
pytest test/e2e_tests/ -v -s
```

> **Important**: Run from the repo root so Python finds the `hummingbot` package.
> The `test/e2e_tests/pytest.ini` is picked up automatically as the rootdir config
> for that subdirectory, overriding the repo-level `pyproject.toml`.

---

## The 8 Tests

| # | Name | What it checks |
|---|---|---|
| 01 | `test_01_orderbook_matches_in_memory` | Order book has bids below asks; spread is positive |
| 02 | `test_02_create_order_then_fetch_by_id` | `buy()` returns a client ID that appears in `in_flight_orders` |
| 03 | `test_03_active_orders_contain_created_order` | Created order is listed in `in_flight_orders` |
| 04 | `test_04_cancel_order_verify_cancelled` | `cancel()` removes order from `in_flight_orders` (or marks done) |
| 05 | `test_05_cancel_all_orders` | `cancel_all()` clears all open orders |
| 06 | `test_06_edit_order_price_changed` | `cancel()` + new `buy()` at different price reflects changed price |
| 07 | `test_07_balance_then_limit_sell_check_balance` | `get_available_balance()` decreases after a limit sell is placed |
| 08 | `test_08_fetch_trades_match_in_memory` | `OrderFilledEvent`s collected during the session are internally consistent |

All orders are placed **well away from the market** (configurable `BUY_PRICE_OFFSET` /
`SELL_PRICE_OFFSET`) so they will not fill during the test run. Teardown cancels
everything regardless.

---

## Adding a New Exchange

Only `.env` changes are needed — no Python code changes:

```ini
# 1. Enable it
TEST_MYEXCHANGE=true

# 2. Set the hummingbot connector ID (must be registered in AllConnectorSettings)
MYEXCHANGE_CONNECTOR_NAME=my_exchange

# 3. Trading pair and order sizes
MYEXCHANGE_TRADING_PAIR=ETH-USDT
MYEXCHANGE_LIMIT_BUY_AMOUNT=0.001
MYEXCHANGE_LIMIT_SELL_AMOUNT=0.001
MYEXCHANGE_BUY_PRICE_OFFSET=0.80
MYEXCHANGE_SELL_PRICE_OFFSET=1.20
MYEXCHANGE_EDIT_PRICE_MULTIPLIER=0.78

# 4. Credentials (param names from hummingbot/connector/exchange/my_exchange/my_exchange_utils.py)
MYEXCHANGE_CRED_my_exchange_api_key=YOUR_KEY
MYEXCHANGE_CRED_my_exchange_secret_key=YOUR_SECRET
```

The test parametrization picks up every block where `TEST_{KEY}=true`
automatically on the next run.

---

## How It Works Internally

### Connector lifecycle (per test session)

```
start_network()
    │
    ▼  poll connector.ready (up to CONNECTOR_READY_TIMEOUT seconds)
    │
    ├── test_01 … test_08 (share the same connector instance)
    │
    ▼
teardown: cancel() all in_flight_orders → cancel_all() → stop_network()
```

The `cx` pytest fixture is **module-scoped** so a single connector instance is
shared across all 8 tests for one exchange. This mirrors real strategy usage and
avoids re-authenticating for every test.

### Key hummingbot APIs used

| API | Notes |
|---|---|
| `connector.get_order_book(trading_pair)` | Returns `OrderBook` with `bid_entries()` / `ask_entries()` |
| `connector.buy(pair, amount, type, price)` | Synchronous; returns `client_order_id` immediately |
| `connector.sell(pair, amount, type, price)` | Same as `buy` |
| `connector.cancel(trading_pair, client_id)` | Synchronous cancel request |
| `connector.cancel_all(timeout_seconds)` | Async; returns `List[CancellationResult]` |
| `connector.in_flight_orders` | `Dict[client_id, InFlightOrder]` — live active orders |
| `connector.get_available_balance(currency)` | `Decimal` from `_account_available_balances` |
| `MarketEvent.OrderFilled` + `OrderFilledEvent` | Event system; `_FillCollector` listener attached at fixture setup |

---

## Troubleshooting

### Connector not ready within timeout

```
AssertionError: connector not ready within 60 s
```

- Verify your API key/secret are correct and have trade permissions
- Check network connectivity to the exchange
- Increase `CONNECTOR_READY_TIMEOUT` in `.env` (e.g. `120`)

### Order never appears in `in_flight_orders`

```
AssertionError: order <id> never appeared in in_flight_orders
```

- The connector may have rejected the order (insufficient balance, invalid pair)
- Increase `ORDER_PROPAGATION_WAIT` (e.g. `20`)
- Check that `TRADING_PAIR` is a valid pair on that exchange
- Ensure account has enough balance to place even a tiny order

### `KeyError: 'my_connector_name'`

The connector name in `{KEY}_CONNECTOR_NAME=` is not registered in hummingbot's
`AllConnectorSettings`. Run:

```python
from hummingbot.client.settings import AllConnectorSettings
print(list(AllConnectorSettings.get_connector_settings().keys()))
```

to see all valid connector IDs.

### `ValueError: missing credential param 'xyz'`

The `{KEY}_CRED_{param}` key names don't match what the connector expects. Check
the connector's `ConfigMap` class:

```
hummingbot/connector/exchange/{name}/{name}_utils.py
```

Look for the `ConnectorConfigMap` fields — the field names are the `{param}` values.

### `pytest-asyncio` loop errors

If you see `RuntimeError: Timeout context manager should be used inside a task` or
similar async loop errors, ensure:

1. You are running `pytest` from the **repo root** (so `test/e2e_tests/pytest.ini`
   is picked up, which sets `asyncio_mode = auto` and
   `asyncio_default_fixture_loop_scope = module`)
2. You have not added a custom `event_loop` fixture anywhere — that pattern is
   deprecated in pytest-asyncio ≥ 0.21

---

## CI Integration

Store credentials as repository secrets, then write the `.env` file in CI before
running:

```yaml
- name: Write E2E .env
  run: |
    cat > test/e2e_tests/.env << 'EOF'
    TEST_BINANCE=true
    BINANCE_CONNECTOR_NAME=binance
    BINANCE_TRADING_PAIR=ETH-USDT
    BINANCE_LIMIT_BUY_AMOUNT=0.001
    BINANCE_LIMIT_SELL_AMOUNT=0.001
    BINANCE_BUY_PRICE_OFFSET=0.80
    BINANCE_SELL_PRICE_OFFSET=1.20
    BINANCE_EDIT_PRICE_MULTIPLIER=0.78
    BINANCE_CRED_binance_api_key=${{ secrets.BINANCE_API_KEY }}
    BINANCE_CRED_binance_api_secret=${{ secrets.BINANCE_API_SECRET }}
    EOF

- name: Run E2E tests
  run: |
    conda run -n hummingbot pytest test/e2e_tests/ -v
```

Credentials exist **only** in the ephemeral `.env` file for the duration of the
job — they are never exported into the shell environment.
