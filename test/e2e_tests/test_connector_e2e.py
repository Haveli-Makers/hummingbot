"""
End-to-End tests for Hummingbot exchange connectors.

Tests interact directly with live Hummingbot connector instances — no
api-server or any other intermediary is involved.  Each connector is
instantiated, started (start_network), exercised, and stopped inside the
module-scoped pytest fixture.

Tests covered:
  1. Order book        — structure, sort order, no-crossed-book, snapshot consistency
  2. Create order      — limit buy appears in in_flight_orders, exchange-acknowledged
  3. Active orders     — order listed among open in_flight_orders
  4. Cancel order      — order leaves in_flight_orders / reaches terminal state
  5. Cancel all        — 3 orders placed; cancel_all() removes every one
  6. Edit order        — cancel + re-create at a new price; verify price and new id
  7. Balance + sell    — available balance decreases after sell, recovers on cancel
  8. Trade fills       — OrderFilledEvent structure and field validation

Configuration
─────────────
Copy  test/e2e_tests/.env.example → test/e2e_tests/.env  and fill in values.
Enable an exchange:  TEST_{NAME}=true
Credentials:         {NAME}_CRED_{exact_connector_param}=value

Run all enabled exchanges:
    pytest test/e2e_tests/ -v

Filter to one exchange:
    pytest test/e2e_tests/ -v -k binance
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import pytest
import pytest_asyncio
from dotenv import dotenv_values

from hummingbot.client.config.config_helpers import get_connector_class
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import MarketEvent, MarketOrderFailureEvent, OrderFilledEvent

# ─── Environment loading ──────────────────────────────────────────────────────
# All configuration (including credentials) is read exclusively from the .env
# file.  Shell environment variables are never consulted.

_TEST_DIR = Path(__file__).parent
_ENV_FILE: Optional[Path] = None
for _candidate in (_TEST_DIR / ".env", _TEST_DIR.parent / ".env"):
    if _candidate.exists():
        _ENV_FILE = _candidate
        break

if _ENV_FILE is None:
    raise FileNotFoundError(
        "No .env file found. "
        "Copy test/e2e_tests/.env.example → test/e2e_tests/.env and fill in your values."
    )

_env: Dict[str, Optional[str]] = dotenv_values(_ENV_FILE)


def _require(key: str) -> str:
    val = (_env.get(key) or "").strip()
    if not val:
        raise ValueError(
            f"Required key '{key}' is missing or empty in {_ENV_FILE}. "
            "See test/e2e_tests/.env.example for reference."
        )
    return val


# ─── Global timing defaults ───────────────────────────────────────────────────

_DEFAULT_ORDER_WAIT = int(_env.get("ORDER_PROPAGATION_WAIT") or "10")
_DEFAULT_CANCEL_WAIT = int(_env.get("CANCEL_PROPAGATION_WAIT") or "15")
_DEFAULT_READY_WAIT = int(_env.get("CONNECTOR_READY_TIMEOUT") or "60")


# ─── Test logger ──────────────────────────────────────────────────────────────
# Every test logs actual comparison values to a timestamped file so you can
# verify what was asserted without rerunning.  Log files are written to
# test/e2e_tests/logs/ and never overwrite each other.

_LOG_DIR = _TEST_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / f"e2e_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

_log = logging.getLogger("hb_e2e")
_log.setLevel(logging.DEBUG)
if not _log.handlers:
    _fh = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S"))
    _log.addHandler(_fh)

_log.info(f"Log file: {_LOG_FILE}")
_log.info(f".env file: {_ENV_FILE}")


# ─── Exchange config ──────────────────────────────────────────────────────────


@dataclass
class ExchangeConfig:
    key: str
    connector_name: str
    trading_pair: str
    limit_buy_amount: Decimal
    limit_sell_amount: Decimal
    buy_price_offset: float
    sell_price_offset: float
    edit_price_multiplier: float
    credentials: Dict[str, str]
    order_propagation_wait: int
    cancel_propagation_wait: int
    ready_timeout: int


def _get_configured_exchanges() -> List[ExchangeConfig]:
    configs: List[ExchangeConfig] = []
    for env_key, env_val in _env.items():
        if not env_key.startswith("TEST_"):
            continue
        if (env_val or "").strip().lower() != "true":
            continue
        key = env_key[5:]
        pfx = f"{key}_"
        cred_pfx = f"{key}_CRED_"
        credentials: Dict[str, str] = {}
        for k, v in _env.items():
            if k.startswith(cred_pfx) and v and v.strip():
                credentials[k[len(cred_pfx):]] = v.strip()
        configs.append(ExchangeConfig(
            key=key,
            connector_name=_env.get(f"{pfx}CONNECTOR_NAME") or key.lower(),
            trading_pair=_env.get(f"{pfx}TRADING_PAIR") or "ETH-USDT",
            limit_buy_amount=Decimal(_env.get(f"{pfx}LIMIT_BUY_AMOUNT") or "0.001"),
            limit_sell_amount=Decimal(_env.get(f"{pfx}LIMIT_SELL_AMOUNT") or "0.001"),
            buy_price_offset=float(_env.get(f"{pfx}BUY_PRICE_OFFSET") or "0.80"),
            sell_price_offset=float(_env.get(f"{pfx}SELL_PRICE_OFFSET") or "1.20"),
            edit_price_multiplier=float(_env.get(f"{pfx}EDIT_PRICE_MULTIPLIER") or "0.78"),
            credentials=credentials,
            order_propagation_wait=int(_env.get(f"{pfx}ORDER_PROPAGATION_WAIT") or str(_DEFAULT_ORDER_WAIT)),
            cancel_propagation_wait=int(_env.get(f"{pfx}CANCEL_PROPAGATION_WAIT") or str(_DEFAULT_CANCEL_WAIT)),
            ready_timeout=int(_env.get(f"{pfx}READY_TIMEOUT") or str(_DEFAULT_READY_WAIT)),
        ))
    return sorted(configs, key=lambda c: c.key)


CONFIGURED_EXCHANGES: List[ExchangeConfig] = _get_configured_exchanges()


# ─── Fill event collector ─────────────────────────────────────────────────────


class _FillCollector:
    def __init__(self) -> None:
        self._fills: List[OrderFilledEvent] = []

    def __call__(self, event_tag, event: OrderFilledEvent) -> None:
        self._fills.append(event)

    @property
    def fills(self) -> List[OrderFilledEvent]:
        return list(self._fills)


class _FailCollector:
    """
    Listens for MarketEvent.OrderFailure so we can detect fast-rejected orders.

    When an order is rejected by the exchange (wrong credentials, insufficient
    balance, trading rule violation), the connector fires OrderFailure and then
    moves the order to FAILED state, removing it from in_flight_orders.  This
    can happen within milliseconds — faster than our 200 ms polling interval —
    so without this listener the order appears to have "never existed."
    """

    def __init__(self) -> None:
        self._failed_ids: set = set()

    def __call__(self, event_tag, event: MarketOrderFailureEvent) -> None:
        oid = getattr(event, "order_id", None)
        if oid:
            self._failed_ids.add(oid)

    def has_failed(self, client_id: str) -> bool:
        return client_id in self._failed_ids


# ─── Connector wrapper ────────────────────────────────────────────────────────


class ConnectorWrapper:
    def __init__(self, connector, cfg: ExchangeConfig) -> None:
        self.connector = connector
        self.cfg = cfg
        self.order_wait = cfg.order_propagation_wait
        self.cancel_wait = cfg.cancel_propagation_wait
        self._fill_collector = _FillCollector()
        self._failure_collector = _FailCollector()
        try:
            connector.add_listener(MarketEvent.OrderFilled, self._fill_collector)
        except Exception:
            pass
        try:
            connector.add_listener(MarketEvent.OrderFailure, self._failure_collector)
        except Exception:
            pass

    def remove_listeners(self) -> None:
        try:
            self.connector.remove_listener(MarketEvent.OrderFilled, self._fill_collector)
        except Exception:
            pass
        try:
            self.connector.remove_listener(MarketEvent.OrderFailure, self._failure_collector)
        except Exception:
            pass

    @property
    def collected_fills(self) -> List[OrderFilledEvent]:
        return self._fill_collector.fills

    # ── Logging helpers ────────────────────────────────────────────────────────

    def log(self, msg: str, level: str = "info") -> None:
        """Write a line prefixed with [connector_name] to the log file."""
        getattr(_log, level)(f"[{self.cfg.connector_name}] {msg}")

    def log_value(self, name: str, actual, *, note: str = "") -> None:
        """Log a single measured value (no assertion yet)."""
        suffix = f"  ({note})" if note else ""
        _log.info(f"[{self.cfg.connector_name}]   {name} = {actual!r}{suffix}")

    def log_check(self, name: str, actual, expected, *, note: str = "") -> None:
        """Log actual vs expected for an upcoming assertion."""
        suffix = f"  ({note})" if note else ""
        _log.info(
            f"[{self.cfg.connector_name}]   CHECK {name}: "
            f"actual={actual!r}  expected={expected!r}{suffix}"
        )

    def log_pass(self, test_name: str) -> None:
        _log.info(f"[{self.cfg.connector_name}] ✓ {test_name} PASSED")

    def log_section(self, title: str) -> None:
        bar = "═" * (50 - len(title))
        _log.info(f"[{self.cfg.connector_name}] ══ {title} {bar}")


# ─── Helper functions ─────────────────────────────────────────────────────────


def get_orderbook_snapshot(cx: ConnectorWrapper):
    ob = cx.connector.get_order_book(cx.cfg.trading_pair)
    return list(ob.bid_entries()), list(ob.ask_entries())


def get_mid_price(cx: ConnectorWrapper) -> Decimal:
    bids, asks = get_orderbook_snapshot(cx)
    assert bids and asks, (
        f"[{cx.cfg.connector_name}] Order book is empty for {cx.cfg.trading_pair}."
    )
    return (Decimal(str(bids[0].price)) + Decimal(str(asks[0].price))) / Decimal("2")


def place_limit_buy(cx: ConnectorWrapper, price: Decimal) -> str:
    client_id = cx.connector.buy(
        cx.cfg.trading_pair, cx.cfg.limit_buy_amount, OrderType.LIMIT, price,
    )
    assert client_id, "connector.buy() returned an empty order id"
    return client_id


def place_limit_sell(cx: ConnectorWrapper, price: Decimal) -> str:
    client_id = cx.connector.sell(
        cx.cfg.trading_pair, cx.cfg.limit_sell_amount, OrderType.LIMIT, price,
    )
    assert client_id, "connector.sell() returned an empty order id"
    return client_id


def cancel_order(cx: ConnectorWrapper, client_id: str) -> None:
    try:
        cx.connector.cancel(cx.cfg.trading_pair, client_id)
    except Exception:
        pass


def _is_order_open(order) -> bool:
    """
    Return True ONLY when the exchange has actually acknowledged the order.

    We gate on exchange_order_id — not on is_open or the state enum — because:

      • Some connectors (e.g. older WazirX style) set is_open=True even for
        PENDING_CREATE orders, before the exchange has replied at all.
        Trusting is_open would make tests pass while the order is completely
        unacknowledged (and the account may have zero balance).

      • The exchange assigns an exchange_order_id only when it accepts the order.
        Until then exchange_order_id is None.  This is the definitive signal:

            PENDING_CREATE  → exchange_order_id = None  → return False  ✓
            Exchange acked  → exchange_order_id = "123" → return True   ✓
            Wrong creds     → order stays PENDING/is_done → return False ✓
    """
    exchange_id = getattr(order, "exchange_order_id", None)
    if not exchange_id:
        return False          # No exchange ID = exchange has not acknowledged yet
    return not order.is_done  # Has an ID and is still live → genuinely open


async def wait_for_order_open(
    cx: ConnectorWrapper,
    client_id: str,
    timeout: Optional[int] = None,
) -> Optional[object]:
    """
    Poll until the order is exchange-acknowledged (OPEN / PARTIALLY_FILLED).

    Returns InFlightOrder only when is_open is True.
    Returns None when:
      - The exchange rejected the order (is_done becomes True)
      - Timeout expired with the order still in PENDING_CREATE or absent

    WHY we do NOT fall back to returning PENDING_CREATE at timeout:
      A PENDING_CREATE order that never transitions to OPEN means the exchange
      never acknowledged it.  This happens when:
        • credentials are wrong (API returns 4xx, connector may silently swallow
          the error and leave the order stuck in PENDING_CREATE)
        • account has insufficient balance
        • the connector has a bug that ignores API errors
      Accepting PENDING_CREATE at timeout would make tests pass falsely.
      Instead: return None and let the caller fail with a clear message.
      If your exchange is slow, increase ORDER_PROPAGATION_WAIT in .env.
    """
    timeout = timeout or cx.order_wait
    start = time.monotonic()
    deadline = start + timeout

    # Yield once so the background _create_order coroutine can start.
    await asyncio.sleep(0)

    while time.monotonic() < deadline:
        # Fast-rejection check: the order may have been placed, immediately
        # rejected by the exchange (insufficient balance / auth error / trading
        # rule violation), and removed from in_flight_orders all within the
        # first polling interval.  The OrderFailure event is fired before the
        # order disappears, so _failure_collector catches it even when we miss
        # the FAILED state in in_flight_orders.
        if cx._failure_collector.has_failed(client_id):
            cx.log(
                f"Order {client_id} received OrderFailure event — exchange rejected it. "
                "Possible causes: insufficient balance, trading rule violation, or "
                "invalid credentials. Check your .env amounts and account balance.",
                "warning",
            )
            return None

        order = cx.connector.in_flight_orders.get(client_id)
        if order is not None:
            if order.is_done:
                cx.log(
                    f"Order {client_id} reached terminal state '{order.current_state}' "
                    "— exchange rejected it.",
                    "warning",
                )
                return None
            if _is_order_open(order):
                elapsed = time.monotonic() - start
                cx.log(
                    f"Order {client_id} exchange-acknowledged after {elapsed:.1f}s  "
                    f"state={order.current_state}  "
                    f"exchange_id={order.exchange_order_id}"
                )
                return order

        await asyncio.sleep(0.2)

    # Timeout — do a final failure check before reporting
    if cx._failure_collector.has_failed(client_id):
        cx.log(
            f"Order {client_id} received OrderFailure event (detected at timeout). "
            "Exchange rejected the order.",
            "warning",
        )
        return None

    order = cx.connector.in_flight_orders.get(client_id)
    if order is not None:
        cx.log(
            f"Order {client_id} timed out in state '{order.current_state}' after {timeout}s. "
            "Exchange never acknowledged. Check credentials, balance, and increase "
            "ORDER_PROPAGATION_WAIT in .env if the exchange is slow.",
            "warning",
        )
    else:
        cx.log(
            f"Order {client_id} not found in in_flight_orders after {timeout}s. "
            f"Active order ids: {list(cx.connector.in_flight_orders)}",
            "warning",
        )
    return None


async def wait_until_not_active(
    cx: ConnectorWrapper,
    client_id: str,
    timeout: Optional[int] = None,
) -> bool:
    deadline = time.monotonic() + (timeout or cx.cancel_wait)
    while time.monotonic() < deadline:
        order = cx.connector.in_flight_orders.get(client_id)
        if order is None or order.is_done:
            return True
        await asyncio.sleep(0.3)
    return False


async def ensure_order_cancelled(cx: ConnectorWrapper, client_id: Optional[str]) -> None:
    """Send a cancel and wait for confirmation. Used in finally-blocks."""
    if not client_id:
        return
    try:
        order = cx.connector.in_flight_orders.get(client_id)
        if order is not None and not order.is_done:
            cancel_order(cx, client_id)
            await wait_until_not_active(cx, client_id)
    except Exception:
        pass


async def _reverse_if_filled(cx: ConnectorWrapper, client_id: str) -> None:
    """
    If fill events were recorded for client_id, place a reverse limit order
    at 0.5% from mid-price to restore the account balance.
    Best-effort; orders placed at 20% offsets should never fill in practice.
    """
    fills = [
        f for f in cx.collected_fills
        if getattr(f, "order_id", None) == client_id
    ]
    if not fills:
        return
    total_amount = sum(f.amount for f in fills)
    if total_amount <= Decimal("0"):
        return
    cx.log(
        f"REVERSAL: order {client_id} was FILLED for {total_amount}. "
        "Placing reverse order to restore balance.",
        "warning",
    )
    try:
        mid = get_mid_price(cx)
        trade_type = fills[0].trade_type
        if trade_type == TradeType.BUY:
            rev_price = round(mid * Decimal("0.995"), 8)
            rev_id = cx.connector.sell(cx.cfg.trading_pair, total_amount, OrderType.LIMIT, rev_price)
        else:
            rev_price = round(mid * Decimal("1.005"), 8)
            rev_id = cx.connector.buy(cx.cfg.trading_pair, total_amount, OrderType.LIMIT, rev_price)
        cx.log(f"REVERSAL: placed {'SELL' if trade_type == TradeType.BUY else 'BUY'} "
               f"rev_id={rev_id}  rev_price={rev_price}  amount={total_amount}")
        await asyncio.sleep(8)
        await ensure_order_cancelled(cx, rev_id)
    except Exception as exc:
        cx.log(f"REVERSAL: failed — {exc}", "warning")


async def ensure_order_closed(cx: ConnectorWrapper, client_id: Optional[str]) -> None:
    """Full per-order cleanup: cancel if open, then reverse if filled."""
    await ensure_order_cancelled(cx, client_id)
    if client_id:
        await _reverse_if_filled(cx, client_id)


async def cancel_all_open_orders(cx: ConnectorWrapper) -> int:
    open_ids = [oid for oid, o in cx.connector.in_flight_orders.items() if not o.is_done]
    for oid in open_ids:
        cancel_order(cx, oid)
    return len(open_ids)


def assert_sufficient_balance(
    cx: ConnectorWrapper,
    trade_type,
    amount: Decimal,
    price: Decimal,
) -> None:
    """
    Skip the current test with a clear message if the account lacks funds.

    Called BEFORE place_limit_buy / place_limit_sell so we get an actionable
    skip reason rather than a cryptic "order never appeared in in_flight_orders"
    (which happens because the exchange rejects the order in < 200 ms and it
    disappears from in_flight_orders before our first poll).
    """
    base, quote = cx.cfg.trading_pair.split("-")
    if trade_type == TradeType.BUY:
        required = amount * price
        available = cx.connector.get_available_balance(quote)
        cx.log_check(f"{quote}_balance_for_buy",
                     actual=f"{available:.4f}", expected=f">= {required:.4f}")
        if available < required:
            max_safe = available / price if price > 0 else Decimal("0")
            pytest.skip(
                f"\n[{cx.cfg.connector_name}] Insufficient {quote} for buy order.\n\n"
                f"  Order needs : {required:.4f} {quote}  "
                f"({amount} {base} × {price:.2f} {quote}/{base})\n"
                f"  Account has : {available:.4f} {quote}\n\n"
                f"  FIX — in your .env set a smaller buy amount, e.g.:\n"
                f"    {cx.cfg.key}_LIMIT_BUY_AMOUNT={float(max_safe * Decimal('0.95')):.8f}\n"
                f"  (95% of your {quote} balance at this price)"
            )
    else:  # SELL
        available = cx.connector.get_available_balance(base)
        cx.log_check(f"{base}_balance_for_sell",
                     actual=f"{available:.8f}", expected=f">= {amount:.8f}")
        if available < amount:
            pytest.skip(
                f"\n[{cx.cfg.connector_name}] Insufficient {base} for sell order.\n\n"
                f"  Order needs : {amount:.8f} {base}\n"
                f"  Account has : {available:.8f} {base}\n\n"
                f"  FIX — in your .env set a smaller sell amount, e.g.:\n"
                f"    {cx.cfg.key}_LIMIT_SELL_AMOUNT={float(available * Decimal('0.95')):.8f}\n"
                f"  (95% of your {base} balance)"
            )


async def _poll_balance_increase(
    cx: ConnectorWrapper,
    token: str,
    baseline: Decimal,
    target_delta: Decimal,
    timeout: int = 30,
) -> Decimal:
    """
    Poll get_available_balance(token) until it exceeds baseline + target_delta.

    Returns the actual delta observed (may be less than target_delta on timeout).

    WHY balance polling instead of fill events:
      connector.buy() schedules _create_order via safe_ensure_future — it returns
      the order_id BEFORE _create_order has run, so in_flight_orders is still empty
      on the very first poll.  Additionally, WazirX fires MarketEvent.OrderFilled
      only from the WebSocket user stream (not from the REST create-order response),
      so collected_fills is empty even when the order is already done.  Balance
      updates arrive via WebSocket balanceUpdate events and are reliable.
    """
    deadline = time.monotonic() + timeout
    delta = Decimal("0")
    while time.monotonic() < deadline:
        await asyncio.sleep(1)
        avail = cx.connector.get_available_balance(token)
        delta = avail - baseline
        if delta >= target_delta:
            break
    return delta


async def _poll_balance_decrease(
    cx: ConnectorWrapper,
    token: str,
    baseline: Decimal,
    threshold_fraction: Decimal = Decimal("0.1"),
    timeout: int = 30,
) -> bool:
    """
    Poll until get_available_balance(token) drops below baseline * threshold_fraction.

    Returns True when the balance has fallen far enough (i.e. taker sell filled).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(1)
        avail = cx.connector.get_available_balance(token)
        if avail < baseline * threshold_fraction:
            return True
    return False


def _rejection_msg(cx: ConnectorWrapper, client_id: str, timeout: int) -> str:
    order = cx.connector.in_flight_orders.get(client_id)
    if order is not None:
        return (
            f"[{cx.cfg.connector_name}] Order {client_id} stuck in state "
            f"'{order.current_state}' — never reached OPEN after {timeout}s. "
            "Verify credentials, account balance, and trading pair. "
            "If the exchange is slow, increase ORDER_PROPAGATION_WAIT in .env."
        )
    return (
        f"[{cx.cfg.connector_name}] Order {client_id} was rejected or never acknowledged "
        f"within {timeout}s. Active orders: {list(cx.connector.in_flight_orders)}. "
        "Verify API credentials, account balance, and trading pair in .env."
    )


# ─── Fixtures ─────────────────────────────────────────────────────────────────

_FIXTURE_PARAMS = CONFIGURED_EXCHANGES if CONFIGURED_EXCHANGES else [None]


def _fixture_id(cfg: Optional[ExchangeConfig]) -> str:
    return cfg.connector_name if cfg else "no-exchanges-configured"


@pytest_asyncio.fixture(scope="module", params=_FIXTURE_PARAMS, ids=_fixture_id)
async def cx(request) -> ConnectorWrapper:
    """
    Module-scoped fixture — one ConnectorWrapper per enabled exchange.

    Lifecycle:
      1. Validates connector is registered in Hummingbot
      2. Validates credentials are present in .env
      3. Instantiates the connector and calls start_network()
      4. Waits for connector.ready (order book + user stream)
      5. Verifies authentication succeeded via balance data and status_dict
      6. Yields ConnectorWrapper to all tests
      7. Teardown: cancel open orders → cancel_all() → stop_network()
    """
    cfg: Optional[ExchangeConfig] = request.param
    if cfg is None:
        pytest.skip("No exchanges enabled. Copy .env.example → .env and set TEST_{NAME}=true")

    _log.info(f"\n{'=' * 60}")
    _log.info(f"FIXTURE SETUP: {cfg.connector_name}  pair={cfg.trading_pair}")
    _log.info(f"  order_wait={cfg.order_propagation_wait}s  cancel_wait={cfg.cancel_propagation_wait}s")
    _log.info(f"  credentials: {list(cfg.credentials.keys())}")

    all_settings = AllConnectorSettings.get_connector_settings()
    if cfg.connector_name not in all_settings:
        pytest.skip(
            f"Connector '{cfg.connector_name}' is not registered in Hummingbot. "
            f"Available: {sorted(all_settings.keys())[:20]} …"
        )

    if not cfg.credentials:
        pytest.skip(
            f"No credentials found for {cfg.key}. "
            f"Add {cfg.key}_CRED_<param>=<value> entries to .env."
        )

    conn_setting = all_settings[cfg.connector_name]
    init_params = conn_setting.conn_init_parameters(
        trading_pairs=[cfg.trading_pair],
        trading_required=True,
        api_keys=cfg.credentials,
    )
    connector_class = get_connector_class(cfg.connector_name)
    connector = connector_class(**init_params)

    await connector.start_network()
    _log.info(f"[{cfg.connector_name}] start_network() called")

    # Wait for connector.ready
    ready_deadline = time.monotonic() + cfg.ready_timeout
    while time.monotonic() < ready_deadline:
        if connector.ready:
            break
        await asyncio.sleep(1)

    if not connector.ready:
        status = getattr(connector, "status_dict", {})
        _log.error(f"[{cfg.connector_name}] Not ready after {cfg.ready_timeout}s. Status: {status}")
        await connector.stop_network()
        pytest.skip(
            f"[{cfg.connector_name}] Not ready after {cfg.ready_timeout}s. "
            f"Status: {status}"
        )

    elapsed_ready = cfg.ready_timeout - (ready_deadline - time.monotonic())
    _log.info(f"[{cfg.connector_name}] connector.ready=True after {elapsed_ready:.1f}s")

    # ── Authentication verification ────────────────────────────────────────────
    # connector.ready can become True from the public order-book subscription
    # alone, even when API credentials are wrong.  We additionally verify auth
    # by explicitly calling _update_balances() and checking for non-zero values.
    #
    # WHY explicit call instead of passive wait:
    #   Some connectors (WazirX included) pre-fill _account_available_balances
    #   with Decimal("0") for every asset in the trading pair RIGHT IN __init__,
    #   before any network call.  A passive poll would see that pre-filled dict
    #   immediately and report "still zero" even if the background task hasn't
    #   run yet.  Calling _update_balances() ourselves forces the REST request
    #   synchronously so the balance dict reflects the actual exchange response.
    #
    # WHY check for non-zero (not just any value):
    #   Some exchanges (WazirX included) respond with HTTP 200 + zero amounts
    #   when credentials are wrong — they never return HTTP 401/403.  After the
    #   connector marks itself "authenticated" we still can't trust all-zero
    #   balances; we need at least one positive value to confirm the key works.

    # Step 1 — status_dict (quick flag check, not the definitive auth gate)
    status = getattr(connector, "status_dict", {})
    _log.info(f"[{cfg.connector_name}] status_dict: {status}")

    auth_keywords = ("account", "balance", "user_stream", "auth", "trading")
    failing_auth_flags = [
        k for k, v in status.items()
        if not v and any(kw in k.lower() for kw in auth_keywords)
    ]
    if failing_auth_flags:
        _log.error(f"[{cfg.connector_name}] Auth status flags are False: {failing_auth_flags}")
        await connector.stop_network()
        pytest.skip(
            f"[{cfg.connector_name}] Authentication failed — status_dict flags False: "
            f"{failing_auth_flags}. Verify API credentials in .env."
        )

    # Step 2 — balance check (REST + WebSocket fallback)
    #
    # WHY two paths:
    #   Some connectors (WazirX) populate _account_available_balances from the
    #   WebSocket user-stream (balanceUpdate events) rather than from a REST call.
    #   For those connectors _update_balances() may fail silently, and the real
    #   balance data only arrives once the WS user-stream sends events.
    #
    #   Strategy:
    #     a) Call _update_balances() once immediately — covers REST-based connectors.
    #     b) If still zero, poll for up to 45 s — covers WS-based connectors that
    #        push balance snapshots after connection.  Re-call _update_balances()
    #        every 10 s as a REST retry.
    #   A genuine all-zero account is rare for an active trader; if we see all-zero
    #   after 45 s we treat it as a credential failure.

    _update_fn = getattr(connector, "_update_balances", None)
    non_zero: Dict[str, str] = {}

    # -- Attempt a — immediate REST fetch
    if _update_fn is not None:
        try:
            await _update_fn()
            _log.info(f"[{cfg.connector_name}] _update_balances() called (attempt 1)")
        except Exception as exc:
            _log.warning(f"[{cfg.connector_name}] _update_balances() raised: {exc}")

    raw = getattr(connector, "_account_available_balances", {})
    all_bal = {k: str(v) for k, v in raw.items()}
    non_zero = {k: str(v) for k, v in raw.items() if Decimal(str(v or "0")) > Decimal("0")}
    _log.info(
        f"[{cfg.connector_name}] Balance snapshot: "
        f"{len(all_bal)} assets total, {len(non_zero)} non-zero"
    )
    if non_zero:
        _log.info(f"[{cfg.connector_name}] Non-zero balances: {non_zero}")

    # -- Attempt b — wait for WS events + periodic REST retries
    if not non_zero:
        _log.info(
            f"[{cfg.connector_name}] Balances all-zero after REST fetch — "
            "waiting up to 45 s for WebSocket balance events..."
        )
        balance_deadline = time.monotonic() + 45
        _next_rest_retry = time.monotonic() + 10   # retry REST every 10 s

        while time.monotonic() < balance_deadline:
            await asyncio.sleep(2)

            raw = getattr(connector, "_account_available_balances", {})
            all_bal = {k: str(v) for k, v in raw.items()}
            non_zero = {k: str(v) for k, v in raw.items() if Decimal(str(v or "0")) > Decimal("0")}

            if non_zero:
                _log.info(
                    f"[{cfg.connector_name}] Non-zero balances received "
                    f"(via WS or delayed REST): {non_zero}"
                )
                break

            if time.monotonic() >= _next_rest_retry and _update_fn is not None:
                try:
                    await _update_fn()
                    _log.info(f"[{cfg.connector_name}] _update_balances() retry at "
                              f"{45 - (balance_deadline - time.monotonic()):.0f}s")
                except Exception as exc:
                    _log.warning(f"[{cfg.connector_name}] _update_balances() retry raised: {exc}")
                _next_rest_retry = time.monotonic() + 10

        if not non_zero:
            _log.info(
                f"[{cfg.connector_name}] Still all-zero after 45 s. "
                f"({len(all_bal)} assets, all zero)"
            )

    if non_zero:
        _log.info(f"[{cfg.connector_name}] Auth confirmed — non-zero balances: {non_zero}")
    else:
        raw = getattr(connector, "_account_available_balances", {})
        all_bal = {k: str(v) for k, v in raw.items()}
        _log.error(
            f"[{cfg.connector_name}] AUTHENTICATION FAILED — all balances zero after 45s. "
            f"Balance dict: {all_bal}"
        )
        await connector.stop_network()
        pytest.skip(
            f"\n[{cfg.connector_name}] Authentication check failed — all account balances "
            f"are zero after 45 s (REST + WebSocket combined wait).\n\n"
            f"  Balance dict: {all_bal}\n\n"
            f"  DIAGNOSIS: Run the standalone auth checker first:\n"
            f"    python test/e2e_tests/check_wazirx_auth.py\n\n"
            f"  It tests multiple auth methods and shows the raw WazirX response,\n"
            f"  including whether HMAC signing and WebSocket auth work.\n\n"
            f"  HOW TO FIX:\n"
            f"  1. Run check_wazirx_auth.py and check which step passes/fails.\n"
            f"  2. Verify {cfg.key}_CRED_* values in .env match your exchange API\n"
            f"     credentials exactly (case-sensitive, no extra spaces).\n"
            f"  3. Ensure the API key has 'Read' AND 'Trade' permissions.\n"
            f"  4. If the account genuinely has zero assets, fund it before testing."
        )

    wrapper = ConnectorWrapper(connector, cfg)
    _log.info(f"[{cfg.connector_name}] Fixture ready — yielding to tests\n")
    yield wrapper

    # ── Teardown ──────────────────────────────────────────────────────────────
    _log.info(f"[{cfg.connector_name}] TEARDOWN starting")
    try:
        cancelled = await cancel_all_open_orders(wrapper)
        if cancelled:
            _log.info(f"[{cfg.connector_name}] Teardown cancelled {cancelled} open order(s)")
            await asyncio.sleep(2)
    except Exception:
        pass
    try:
        await connector.cancel_all(timeout_seconds=10)
    except Exception:
        pass
    wrapper.remove_listeners()
    await connector.stop_network()
    _log.info(f"[{cfg.connector_name}] TEARDOWN complete\n{'=' * 60}\n")


# ─── Test suite ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio(loop_scope="module")
class TestConnectorE2E:
    """
    Generic E2E test suite running directly against Hummingbot connectors.

    Key design rules applied to every order-placing test:
      • wait_for_order_open() — only returns when the exchange acknowledges
        (OPEN state).  PENDING_CREATE at timeout is treated as rejection.
        This catches wrong-credential scenarios where the connector silently
        ignores API errors and leaves orders stuck in PENDING_CREATE.
      • ensure_order_closed() in every finally block — cancels the order,
        waits for confirmation, then reverses if it somehow got filled.
      • Log actual values before each assertion so you can verify what was
        compared without rerunning.  Log file: test/e2e_tests/logs/
    """

    # ── 1. Order book ──────────────────────────────────────────────────────────

    async def test_01_orderbook_matches_in_memory(self, cx: ConnectorWrapper):
        cx.log_section("TEST_01: orderbook")
        cx.log_value("trading_pair", cx.cfg.trading_pair)

        bids, asks = get_orderbook_snapshot(cx)

        cx.log_value("bid_count", len(bids))
        cx.log_value("ask_count", len(asks))

        assert len(bids) > 0, (
            f"[{cx.cfg.connector_name}] Bids empty for {cx.cfg.trading_pair}."
        )
        assert len(asks) > 0, f"[{cx.cfg.connector_name}] Asks empty"

        for i, entry in enumerate(bids[:3]):
            cx.log_value(f"bid[{i}]", f"price={entry.price}  amount={entry.amount}")
        for i, entry in enumerate(asks[:3]):
            cx.log_value(f"ask[{i}]", f"price={entry.price}  amount={entry.amount}")

        for entry in bids:
            assert entry.price > 0, f"Bid price non-positive: {entry.price}"
            assert entry.amount > 0, f"Bid amount non-positive: {entry.amount}"
        for entry in asks:
            assert entry.price > 0, f"Ask price non-positive: {entry.price}"
            assert entry.amount > 0, f"Ask amount non-positive: {entry.amount}"

        best_bid = bids[0].price
        best_ask = asks[0].price
        spread = best_ask - best_bid
        spread_pct = spread / best_bid * 100

        cx.log_check("best_bid < best_ask", actual=f"bid={best_bid}  ask={best_ask}  spread={spread:.4f} ({spread_pct:.4f}%)", expected="bid < ask")
        assert best_bid < best_ask, (
            f"[{cx.cfg.connector_name}] Crossed book: bid={best_bid} >= ask={best_ask}"
        )

        bid_prices = [b.price for b in bids]
        ask_prices = [a.price for a in asks]
        bids_sorted = bid_prices == sorted(bid_prices, reverse=True)
        asks_sorted = ask_prices == sorted(ask_prices)

        cx.log_check("bids_sorted_descending", actual=bids_sorted, expected=True)
        cx.log_check("asks_sorted_ascending", actual=asks_sorted, expected=True)
        assert bids_sorted, "Bids not sorted descending"
        assert asks_sorted, "Asks not sorted ascending"

        bids2, asks2 = get_orderbook_snapshot(cx)
        best_bid2 = bids2[0].price
        best_ask2 = asks2[0].price
        drift = abs(best_bid2 - best_bid) / best_bid * 100

        cx.log_value("snapshot2_best_bid", best_bid2)
        cx.log_value("snapshot2_best_ask", best_ask2)
        cx.log_check("snapshot_drift_pct < 1%", actual=f"{drift:.4f}%", expected="< 1%")

        assert best_bid2 < best_ask2, "Second snapshot: crossed book"
        assert drift < 1, (
            f"[{cx.cfg.connector_name}] Best bid moved {drift:.2f}% between snapshots"
        )

        cx.log_pass("TEST_01")

    # ── 2. Create order → fetch by id ─────────────────────────────────────────

    async def test_02_create_order_then_fetch_by_id(self, cx: ConnectorWrapper):
        cx.log_section("TEST_02: create_order")

        mid = get_mid_price(cx)
        buy_price = round(mid * Decimal(str(cx.cfg.buy_price_offset)), 8)

        cx.log_value("mid_price", mid)
        cx.log_value("buy_offset", cx.cfg.buy_price_offset)
        cx.log_value("buy_price", buy_price)
        cx.log_value("order_amount", cx.cfg.limit_buy_amount)

        assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

        client_id = place_limit_buy(cx, buy_price)
        cx.log_value("placed_order_client_id", client_id)

        try:
            order = await wait_for_order_open(cx, client_id)

            cx.log_check("order_acknowledged (not None)", actual=order is not None, expected=True)
            assert order is not None, _rejection_msg(cx, client_id, cx.order_wait)

            cx.log_value("order_state", str(order.current_state))
            cx.log_value("exchange_order_id", getattr(order, "exchange_order_id", "N/A"))

            cx.log_check("trade_type", actual=order.trade_type, expected=TradeType.BUY)
            assert order.trade_type == TradeType.BUY, f"Expected BUY, got {order.trade_type}"

            cx.log_check("order_type", actual=order.order_type, expected=OrderType.LIMIT)
            assert order.order_type == OrderType.LIMIT, f"Expected LIMIT, got {order.order_type}"

            cx.log_check("trading_pair", actual=order.trading_pair, expected=cx.cfg.trading_pair)
            assert order.trading_pair == cx.cfg.trading_pair

            price_diff_pct = abs(float(order.price) - float(buy_price)) / float(buy_price) * 100
            cx.log_check("price", actual=float(order.price), expected=float(buy_price),
                         note=f"diff={price_diff_pct:.4f}%  threshold<0.1%")
            assert price_diff_pct < 0.1, (
                f"Price mismatch: submitted {buy_price}, connector shows {order.price} "
                f"({price_diff_pct:.4f}% diff)"
            )

            cx.log_check("is_done", actual=order.is_done, expected=False,
                         note="False = order is live")
            assert not order.is_done, f"Order in terminal state: {order.current_state}"

            cx.log_pass("TEST_02")

        finally:
            cx.log_value("cleanup", f"cancelling {client_id}")
            await ensure_order_closed(cx, client_id)

    # ── 3. Active orders list contains the created order ──────────────────────

    async def test_03_active_orders_contain_created_order(self, cx: ConnectorWrapper):
        cx.log_section("TEST_03: active_orders")

        mid = get_mid_price(cx)
        buy_price = round(mid * Decimal(str(cx.cfg.buy_price_offset)), 8)

        cx.log_value("mid_price", mid)
        cx.log_value("buy_price", buy_price)

        assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

        client_id = place_limit_buy(cx, buy_price)
        cx.log_value("placed_order_client_id", client_id)

        try:
            order = await wait_for_order_open(cx, client_id)

            cx.log_check("order_acknowledged", actual=order is not None, expected=True)
            assert order is not None, _rejection_msg(cx, client_id, cx.order_wait)

            cx.log_value("order_state", str(order.current_state))
            cx.log_check("trading_pair", actual=order.trading_pair, expected=cx.cfg.trading_pair)
            cx.log_check("trade_type", actual=order.trade_type, expected=TradeType.BUY)
            assert order.trading_pair == cx.cfg.trading_pair
            assert order.trade_type == TradeType.BUY

            open_orders = {oid: o for oid, o in cx.connector.in_flight_orders.items() if not o.is_done}
            cx.log_value("open_order_ids", list(open_orders.keys()))
            cx.log_check("client_id in open_orders", actual=client_id in open_orders, expected=True)
            assert client_id in open_orders, (
                f"Order {client_id} in in_flight_orders but already done: "
                f"{cx.connector.in_flight_orders.get(client_id)}"
            )

            cx.log_pass("TEST_03")

        finally:
            await ensure_order_closed(cx, client_id)

    # ── 4. Cancel order → verify cancelled ────────────────────────────────────

    async def test_04_cancel_order_verify_cancelled(self, cx: ConnectorWrapper):
        cx.log_section("TEST_04: cancel_order")

        mid = get_mid_price(cx)
        buy_price = round(mid * Decimal(str(cx.cfg.buy_price_offset)), 8)

        cx.log_value("mid_price", mid)
        cx.log_value("buy_price", buy_price)

        assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

        client_id = place_limit_buy(cx, buy_price)
        cx.log_value("placed_order_client_id", client_id)

        try:
            order = await wait_for_order_open(cx, client_id)

            cx.log_check("order_acknowledged", actual=order is not None, expected=True)
            assert order is not None, _rejection_msg(cx, client_id, cx.order_wait)

            cx.log_value("pre_cancel_state", str(order.current_state))
            cancel_order(cx, client_id)
            cx.log_value("cancel_sent", True)

            gone = await wait_until_not_active(cx, client_id)
            post_state = cx.connector.in_flight_orders.get(client_id)

            cx.log_check("order_gone_or_done", actual=gone, expected=True,
                         note=f"post_state={post_state.current_state if post_state else 'removed'}")
            assert gone, (
                f"[{cx.cfg.connector_name}] Order {client_id} still active {cx.cancel_wait}s "
                f"after cancel. State: {post_state}"
            )

            cx.log_pass("TEST_04")

        finally:
            await ensure_order_closed(cx, client_id)

    # ── 5. Cancel all orders ───────────────────────────────────────────────────

    async def test_05_cancel_all_orders(self, cx: ConnectorWrapper):
        cx.log_section("TEST_05: cancel_all")

        mid = get_mid_price(cx)
        prices = [
            round(mid * Decimal(str(cx.cfg.buy_price_offset - 0.02 * i)), 8)
            for i in range(3)
        ]
        cx.log_value("mid_price", mid)
        cx.log_value("order_prices", [str(p) for p in prices])

        # Pre-check: need enough quote balance for 3 simultaneous buy orders
        total_needed = cx.cfg.limit_buy_amount * sum(prices) / len(prices) * 3
        _, quote = cx.cfg.trading_pair.split("-")
        available_quote = cx.connector.get_available_balance(quote)
        cx.log_check(f"{quote}_balance_for_3_buys",
                     actual=f"{available_quote:.4f}", expected=f">= ~{total_needed:.4f}")
        # Individual check (sufficient for each order at its price)
        for p in prices:
            assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, p)

        order_ids: List[str] = []
        for price in prices:
            order_ids.append(place_limit_buy(cx, price))
        cx.log_value("placed_order_ids", order_ids)

        try:
            for oid in order_ids:
                order = await wait_for_order_open(cx, oid)
                cx.log_check(f"order {oid[:20]}... acknowledged", actual=order is not None, expected=True)
                assert order is not None, _rejection_msg(cx, oid, cx.order_wait)

            cx.log_value("calling_cancel_all", True)
            await cx.connector.cancel_all(timeout_seconds=cx.cancel_wait)
            await asyncio.sleep(cx.cancel_wait)

            for oid in order_ids:
                o = cx.connector.in_flight_orders.get(oid)
                state = o.current_state if o else "removed"
                done = o is None or o.is_done
                cx.log_check(f"order {oid[:20]}... gone_or_done", actual=done, expected=True,
                             note=f"state={state}")
                assert done, (
                    f"[{cx.cfg.connector_name}] Order {oid} still active after cancel-all. "
                    f"State: {state}"
                )

            cx.log_pass("TEST_05")

        finally:
            for oid in order_ids:
                await ensure_order_closed(cx, oid)

    # ── 6. Edit order (cancel + re-create at different price) ─────────────────

    async def test_06_edit_order_price_changed(self, cx: ConnectorWrapper):
        cx.log_section("TEST_06: edit_order")

        mid = get_mid_price(cx)
        price_a = round(mid * Decimal(str(cx.cfg.buy_price_offset)), 8)
        price_b = round(mid * Decimal(str(cx.cfg.edit_price_multiplier)), 8)

        cx.log_value("mid_price", mid)
        cx.log_value("price_a (original)", price_a)
        cx.log_value("price_b (edited)", price_b)

        diff_ab_pct = abs(float(price_a - price_b)) / float(price_a) * 100
        cx.log_check("price_a vs price_b differ by > 0.1%", actual=f"{diff_ab_pct:.4f}%", expected="> 0.1%")
        assert diff_ab_pct > 0.1, (
            "buy_price_offset and edit_price_multiplier produce prices within 0.1% — "
            "adjust them in .env."
        )

        assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, price_a)
        assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, price_b)

        original_id = place_limit_buy(cx, price_a)
        cx.log_value("original_order_id", original_id)
        edited_id: Optional[str] = None

        try:
            original = await wait_for_order_open(cx, original_id)
            cx.log_check("original order acknowledged", actual=original is not None, expected=True)
            assert original is not None, _rejection_msg(cx, original_id, cx.order_wait)

            actual_a = float(original.price)
            diff_a_pct = abs(actual_a - float(price_a)) / float(price_a) * 100
            cx.log_check("original_price", actual=actual_a, expected=float(price_a),
                         note=f"diff={diff_a_pct:.4f}%")
            assert diff_a_pct < 0.1, f"Original price mismatch: expected ~{price_a}, got {actual_a}"

            cancel_order(cx, original_id)
            await wait_until_not_active(cx, original_id)
            cx.log_value("original_order_cancelled", True)

            edited_id = place_limit_buy(cx, price_b)
            cx.log_value("edited_order_id", edited_id)
            cx.log_check("ids_differ", actual=edited_id != original_id, expected=True)
            assert edited_id != original_id, "Edited order must have a new order id"

            edited = await wait_for_order_open(cx, edited_id)
            cx.log_check("edited order acknowledged", actual=edited is not None, expected=True)
            assert edited is not None, _rejection_msg(cx, edited_id, cx.order_wait)

            actual_b = float(edited.price)
            diff_b_pct = abs(actual_b - float(price_b)) / float(price_b) * 100
            cx.log_check("edited_price", actual=actual_b, expected=float(price_b),
                         note=f"diff={diff_b_pct:.4f}%")
            assert diff_b_pct < 0.1, f"Edited price wrong: expected ~{price_b}, got {actual_b}"

            still_same = abs(actual_b - float(price_a)) / float(price_a) * 100
            cx.log_check("price_actually_changed", actual=f"{still_same:.4f}% diff from original",
                         expected="> 0.1%")
            assert still_same > 0.1, "Edited order still shows the original price — edit had no effect"

            cx.log_pass("TEST_06")

        finally:
            await ensure_order_closed(cx, original_id)
            await ensure_order_closed(cx, edited_id)

    # ── 7. Balance → limit sell → balance check ───────────────────────────────
    #
    # Three phases:
    #   Phase 1 — Setup buy  : if base-token balance < sell amount, buy at best_ask
    #                          (taker, fills immediately). Fill confirmed by polling
    #                          the balance, not by fill events — connector.buy()
    #                          schedules _create_order async so in_flight_orders is
    #                          empty on the first poll, and WazirX fires OrderFilled
    #                          only via WebSocket, not the REST response.
    #   Phase 2 — Sell test  : place sell far above market, assert balance locked,
    #                          cancel, assert balance recovered.
    #   Phase 3 — Cleanup sell: sell back what Phase 1 bought so the account is
    #                           left as found. Always runs (inside finally).

    async def test_07_balance_then_limit_sell_check_balance(self, cx: ConnectorWrapper):
        cx.log_section("TEST_07: balance_sell")

        base_token, quote_token = cx.cfg.trading_pair.split("-")
        cx.log_value("base_token", base_token)
        cx.log_value("quote_token", quote_token)

        # ── Phase 1: acquire base token if the account doesn't have enough ────
        setup_buy_id: Optional[str] = None
        avail_base = cx.connector.get_available_balance(base_token)
        cx.log_value("avail_base_initial", str(avail_base))

        if avail_base < cx.cfg.limit_sell_amount:
            cx.log(
                f"Insufficient {base_token} ({avail_base}) — "
                f"need {cx.cfg.limit_sell_amount}. "
                f"Placing near-market BUY to acquire it using {quote_token}."
            )
            _, asks = get_orderbook_snapshot(cx)
            assert asks, f"Ask side empty for {cx.cfg.trading_pair} — cannot place setup buy"

            # Buy at best_ask + 0.1 % → taker order that fills immediately.
            setup_buy_price = round(Decimal(str(asks[0].price)) * Decimal("1.001"), 8)
            cx.log_value("setup_buy_price", setup_buy_price)
            cx.log_value("setup_buy_amount", cx.cfg.limit_sell_amount)

            assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_sell_amount, setup_buy_price)

            baseline_base = cx.connector.get_available_balance(base_token)
            setup_buy_id = cx.connector.buy(
                cx.cfg.trading_pair,
                cx.cfg.limit_sell_amount,
                OrderType.LIMIT,
                setup_buy_price,
            )
            cx.log_value("setup_buy_id", setup_buy_id)

            # Poll balance: WazirX sends balanceUpdate via WebSocket within ~1 s.
            target = cx.cfg.limit_sell_amount * Decimal("0.99")
            acquired = await _poll_balance_increase(cx, base_token, baseline_base, target, timeout=30)
            cx.log_check(
                "setup_buy_balance_delta",
                actual=f"+{acquired} {base_token}",
                expected=f">= {target} {base_token}",
            )
            if acquired < target:
                await ensure_order_cancelled(cx, setup_buy_id)
                pytest.skip(
                    f"[{cx.cfg.connector_name}] Setup BUY did not appear in {base_token} "
                    f"balance within 30 s (acquired {acquired}, need {target}). "
                    "Try again or fund the account with BTC directly."
                )
            cx.log(f"Setup BUY confirmed — acquired {acquired} {base_token}.")

        # ── Phase 2: sell test ─────────────────────────────────────────────────
        sell_id: Optional[str] = None

        mid = get_mid_price(cx)
        sell_price = round(mid * Decimal(str(cx.cfg.sell_price_offset)), 8)
        avail_before = cx.connector.get_available_balance(base_token)
        cx.log_value("avail_before_sell_test", str(avail_before))
        cx.log_value("mid_price", mid)
        cx.log_value("sell_offset", cx.cfg.sell_price_offset)
        cx.log_value("sell_price", sell_price)

        try:
            assert_sufficient_balance(cx, TradeType.SELL, cx.cfg.limit_sell_amount, sell_price)

            sell_id = place_limit_sell(cx, sell_price)
            cx.log_value("placed_sell_id", sell_id)

            sell_order = await wait_for_order_open(cx, sell_id)
            cx.log_check("sell order acknowledged", actual=sell_order is not None, expected=True)
            assert sell_order is not None, _rejection_msg(cx, sell_id, cx.order_wait)

            cx.log_value("sell_order_state", str(sell_order.current_state))

            await asyncio.sleep(3)

            avail_after = cx.connector.get_available_balance(base_token)
            cx.log_check(
                f"{base_token} avail_after <= avail_before",
                actual=str(avail_after), expected=f"<= {avail_before}",
                note=f"delta={avail_before - avail_after}",
            )
            assert avail_after <= avail_before, (
                f"[{cx.cfg.connector_name}] {base_token} balance INCREASED after sell: "
                f"{avail_before} → {avail_after}"
            )

            cancel_order(cx, sell_id)
            await wait_until_not_active(cx, sell_id)
            cx.log_value("sell_cancelled", True)
            await asyncio.sleep(4)

            avail_recovered = cx.connector.get_available_balance(base_token)
            deviation_pct = (
                abs(avail_recovered - avail_before)
                / max(avail_before, Decimal("1e-8"))
                * 100
            )
            cx.log_check(
                "balance_recovered",
                actual=str(avail_recovered), expected=f"~{avail_before}",
                note=f"deviation={float(deviation_pct):.4f}%  threshold=1%",
            )
            assert deviation_pct < Decimal("1"), (
                f"[{cx.cfg.connector_name}] {base_token} balance did not recover after cancel: "
                f"before={avail_before}  after_sell={avail_after}  "
                f"recovered={avail_recovered}  deviation={float(deviation_pct):.4f}%"
            )

            cx.log_pass("TEST_07")

        finally:
            await ensure_order_closed(cx, sell_id)

            # ── Phase 3: sell off the base token we bought in Phase 1 ─────────
            if setup_buy_id is not None:
                cx.log("Cleanup: converting acquired BTC back to INR via near-market sell.")
                avail_cleanup = cx.connector.get_available_balance(base_token)
                cx.log_value("avail_for_cleanup_sell", str(avail_cleanup))

                if avail_cleanup > Decimal("0"):
                    bids, _ = get_orderbook_snapshot(cx)
                    if not bids:
                        cx.log(
                            f"Bid side empty — cannot place cleanup sell. "
                            f"~{avail_cleanup} {base_token} remains in account.",
                            "warning",
                        )
                    else:
                        # Sell at best_bid - 0.1 % → taker, fills immediately.
                        cleanup_sell_price = round(
                            Decimal(str(bids[0].price)) * Decimal("0.999"), 8
                        )
                        cx.log_value("cleanup_sell_price", cleanup_sell_price)
                        cleanup_sell_id = cx.connector.sell(
                            cx.cfg.trading_pair,
                            avail_cleanup,
                            OrderType.LIMIT,
                            cleanup_sell_price,
                        )
                        cx.log_value("cleanup_sell_id", cleanup_sell_id)
                        sold = await _poll_balance_decrease(
                            cx, base_token, avail_cleanup, timeout=30
                        )
                        cx.log_check("cleanup_sell_filled", actual=sold, expected=True)
                        if sold:
                            cx.log(
                                f"Cleanup SELL confirmed — "
                                f"{base_token} converted back to {quote_token}."
                            )
                        else:
                            await ensure_order_cancelled(cx, cleanup_sell_id)
                            cx.log(
                                f"Cleanup SELL did not fill in 30 s — order cancelled. "
                                f"~{avail_cleanup} {base_token} remains in account.",
                                "warning",
                            )

    # ── 8. Trade fills — validate OrderFilledEvent structure ──────────────────

    async def test_08_fetch_trades_match_in_memory(self, cx: ConnectorWrapper):
        cx.log_section("TEST_08: trade_fills")

        fills = cx.collected_fills
        cx.log_value("fill_count", len(fills))

        if not fills:
            cx.log("No fills recorded — skipping (expected for far-from-market orders)")
            pytest.skip(
                f"[{cx.cfg.connector_name}] No order fills during this session. "
                "Reduce BUY/SELL_PRICE_OFFSET in .env to allow fills, or place a "
                "filled order externally before running the suite."
            )

        for i, fill in enumerate(fills):
            cx.log_value(f"fill[{i}]",
                         f"order_id={getattr(fill, 'order_id', 'N/A')}  "
                         f"pair={getattr(fill, 'trading_pair', 'N/A')}  "
                         f"type={getattr(fill, 'trade_type', 'N/A')}  "
                         f"price={getattr(fill, 'price', 'N/A')}  "
                         f"amount={getattr(fill, 'amount', 'N/A')}  "
                         f"ts={getattr(fill, 'timestamp', 'N/A')}")

            assert hasattr(fill, "order_id"), f"Fill[{i}] missing order_id"
            assert hasattr(fill, "trading_pair"), f"Fill[{i}] missing trading_pair"
            assert hasattr(fill, "trade_type"), f"Fill[{i}] missing trade_type"
            assert hasattr(fill, "amount"), f"Fill[{i}] missing amount"
            assert hasattr(fill, "price"), f"Fill[{i}] missing price"
            assert hasattr(fill, "timestamp"), f"Fill[{i}] missing timestamp"

            cx.log_check(f"fill[{i}] amount > 0", actual=fill.amount, expected="> 0")
            cx.log_check(f"fill[{i}] price > 0", actual=fill.price, expected="> 0")
            cx.log_check(f"fill[{i}] trading_pair", actual=fill.trading_pair, expected=cx.cfg.trading_pair)
            cx.log_check(f"fill[{i}] timestamp > 0", actual=fill.timestamp, expected="> 0")
            cx.log_check(f"fill[{i}] trade_type valid",
                         actual=fill.trade_type,
                         expected="BUY or SELL")

            assert fill.amount > Decimal("0"), f"Fill[{i}] non-positive amount"
            assert fill.price > Decimal("0"), f"Fill[{i}] non-positive price"
            assert fill.trade_type in (TradeType.BUY, TradeType.SELL)
            assert fill.trading_pair == cx.cfg.trading_pair
            assert fill.timestamp > 0, f"Fill[{i}] non-positive timestamp"

        cx.log_pass("TEST_08")
