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
from hummingbot.core.data_type.in_flight_order import OrderState
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
    """Collects MarketEvent.OrderFailure events to detect fast-rejected orders."""

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
    """True only when the exchange has acknowledged the order (exchange_order_id set and not done)."""
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
    Poll until the exchange acknowledges the order (exchange_order_id set).
    Returns the InFlightOrder on success, or None if rejected / timed out.
    PENDING_CREATE at timeout is treated as rejection — never returns an unacknowledged order.
    """
    timeout = timeout or cx.order_wait
    start = time.monotonic()
    deadline = start + timeout

    # Yield once so the background _create_order coroutine can start.
    await asyncio.sleep(0)

    while time.monotonic() < deadline:
        # Fast-rejection: exchange may reject and remove the order before our first poll.
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


async def _drive_order_status(cx: ConnectorWrapper) -> None:
    """Force the connector to refresh tracked-order state from the exchange.

    Like balances, order-status updates are normally driven by the connector's
    Clock-ticked polling loop, which this fixture never runs (no Clock attached).
    Critically, some exchanges (e.g. CoinSwitch) cancel orders ASYNCHRONOUSLY:
    the DELETE only acknowledges receipt (the order still reads 'OPEN' in the
    response) and the order transitions to CANCELLED a moment later, observable
    only via the order-status poll.  Calling _update_order_status() ourselves
    replicates the tick a Clock would have fired so the cancel is actually seen.
    """
    update_fn = getattr(cx.connector, "_update_order_status", None)
    if update_fn is None:
        return
    try:
        await update_fn()
    except Exception:
        pass


async def wait_until_not_active(
    cx: ConnectorWrapper,
    client_id: str,
    timeout: Optional[int] = None,
) -> bool:
    deadline = time.monotonic() + (timeout or cx.cancel_wait)
    next_refresh = time.monotonic()
    while time.monotonic() < deadline:
        order = cx.connector.in_flight_orders.get(client_id)
        if order is None or order.is_done:
            return True
        if time.monotonic() >= next_refresh:
            await _drive_order_status(cx)
            next_refresh = time.monotonic() + 1.5
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


async def assert_sufficient_balance(
    cx: ConnectorWrapper,
    trade_type,
    amount: Decimal,
    price: Decimal,
) -> None:
    """Skip the test with a clear message if the account lacks funds for the order.

    Refreshes balances via REST first: with no Clock attached the connector
    never re-polls on its own, and a stale (or connector-corrupted) in-memory
    cache would make every balance check read the wrong number — e.g. a balance
    that never recovers after a previous test placed and cancelled an order.
    """
    update_fn = getattr(cx.connector, "_update_balances", None)
    if update_fn is not None:
        try:
            await update_fn()
        except Exception:
            pass
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
    Returns the actual delta (may be less than target_delta on timeout).
    Calls _update_balances() every 5 s to keep the cache current via REST.
    """
    update_fn = getattr(cx.connector, "_update_balances", None)
    deadline = time.monotonic() + timeout
    next_refresh = time.monotonic()
    delta = Decimal("0")
    while time.monotonic() < deadline:
        await asyncio.sleep(1)
        if update_fn is not None and time.monotonic() >= next_refresh:
            try:
                await update_fn()
            except Exception:
                pass
            next_refresh = time.monotonic() + 5
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
    """Poll until get_available_balance(token) drops below baseline * threshold_fraction.
    Returns True when balance has fallen far enough. Refreshes cache via REST every 5 s."""
    update_fn = getattr(cx.connector, "_update_balances", None)
    deadline = time.monotonic() + timeout
    next_refresh = time.monotonic()
    while time.monotonic() < deadline:
        await asyncio.sleep(1)
        if update_fn is not None and time.monotonic() >= next_refresh:
            try:
                await update_fn()
            except Exception:
                pass
            next_refresh = time.monotonic() + 5
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


# ─── Cache / in-memory copy verification ──────────────────────────────────────
# The connector keeps IN-MEMORY copies of exchange data — the order book, account
# balances, active orders and personal trades — continuously updated from the
# websocket feeds (with REST as a fallback).  Every test above reads from those
# caches (e.g. balance gates order placement, the order book gives mid price).
#
# These helpers prove the cache is TRUSTWORTHY: they fetch the same datum FRESH
# from the exchange's REST API and assert the cached copy agrees.  A genuine
# mismatch (stale / wrongly-updated cache) fails the test — that is the point.
# If a connector doesn't expose the REST probe, the check logs a warning and
# returns without failing, so existing behaviour is unaffected.
#
# NOTE on persistence: Hummingbot's on-disk SQLite store (Order / TradeFill rows)
# is written by MarketsRecorder, an APPLICATION-layer component wired by the
# strategy/clock runtime — it is NOT instantiated in this direct-connector
# harness, so there is no local DB to reconcile here. These checks therefore
# target the in-memory caches, which are what this harness actually populates.


def _pct_diff(a: Decimal, b: Decimal) -> Decimal:
    """Percentage difference relative to the larger magnitude (avoids div-by-zero)."""
    base = max(abs(a), abs(b), Decimal("1e-12"))
    return abs(a - b) / base * 100


async def verify_orderbook_cache(cx: ConnectorWrapper, tol_pct: Decimal = Decimal("3")) -> None:
    """
    CACHED order book (websocket-fed, via get_order_book) vs FRESH REST snapshot.

    Asserts the cached top-of-book is within tol_pct of the REST truth — i.e. the
    websocket diff/snapshot stream is keeping the in-memory book current.
    """
    pair = cx.cfg.trading_pair
    tracker = getattr(cx.connector, "order_book_tracker", None)
    ds = getattr(tracker, "data_source", None) if tracker is not None else None
    if ds is None or not hasattr(ds, "get_new_order_book"):
        cx.log("orderbook cache check skipped — no REST snapshot source on this connector", "warning")
        return

    cached_bids, cached_asks = get_orderbook_snapshot(cx)
    if not cached_bids or not cached_asks:
        cx.log("orderbook cache check skipped — cached book empty", "warning")
        return
    cached_bid = Decimal(str(cached_bids[0].price))
    cached_ask = Decimal(str(cached_asks[0].price))

    try:
        rest_ob = await ds.get_new_order_book(pair)
        rest_bids = list(rest_ob.bid_entries())
        rest_asks = list(rest_ob.ask_entries())
    except Exception as exc:
        cx.log(f"orderbook cache check skipped — REST snapshot failed: {exc}", "warning")
        return
    if not rest_bids or not rest_asks:
        cx.log("orderbook cache check skipped — REST snapshot empty", "warning")
        return
    rest_bid = Decimal(str(rest_bids[0].price))
    rest_ask = Decimal(str(rest_asks[0].price))

    bid_diff = _pct_diff(cached_bid, rest_bid)
    ask_diff = _pct_diff(cached_ask, rest_ask)
    cx.log_check("orderbook_cache_best_bid_vs_REST",
                 actual=f"cache={cached_bid} REST={rest_bid}",
                 expected=f"diff <= {tol_pct}%", note=f"diff={bid_diff:.4f}%")
    cx.log_check("orderbook_cache_best_ask_vs_REST",
                 actual=f"cache={cached_ask} REST={rest_ask}",
                 expected=f"diff <= {tol_pct}%", note=f"diff={ask_diff:.4f}%")
    assert bid_diff <= tol_pct, (
        f"[{cx.cfg.connector_name}] Cached best bid {cached_bid} differs from REST {rest_bid} "
        f"by {bid_diff:.4f}% (> {tol_pct}%) — websocket order-book cache may be stale."
    )
    assert ask_diff <= tol_pct, (
        f"[{cx.cfg.connector_name}] Cached best ask {cached_ask} differs from REST {rest_ask} "
        f"by {ask_diff:.4f}% (> {tol_pct}%) — websocket order-book cache may be stale."
    )


async def verify_balance_cache(cx: ConnectorWrapper, token: str, tol_pct: Decimal = Decimal("1")) -> None:
    """
    CACHED available balance (get_available_balance) vs FRESH REST balance.

    Reads the current cached value, forces a REST refresh via _update_balances(),
    then re-reads.  If they agree within tol_pct the cache was accurate at the
    moment of the check.  Call at a QUIET point (no order placed/cancelled in the
    prior second) so a legitimate in-flight balance change isn't misread as drift.
    """
    update_fn = getattr(cx.connector, "_update_balances", None)
    if update_fn is None:
        cx.log("balance cache check skipped — connector has no _update_balances()", "warning")
        return
    cached = cx.connector.get_available_balance(token)
    try:
        await update_fn()
    except Exception as exc:
        cx.log(f"balance cache check skipped — REST refresh failed: {exc}", "warning")
        return
    rest = cx.connector.get_available_balance(token)
    diff = _pct_diff(cached, rest)
    cx.log_check(f"{token}_balance_cache_vs_REST",
                 actual=f"cache={cached} REST={rest}",
                 expected=f"diff <= {tol_pct}%", note=f"diff={diff:.4f}%")
    assert diff <= tol_pct, (
        f"[{cx.cfg.connector_name}] Cached {token} available balance {cached} differs from "
        f"REST {rest} by {diff:.4f}% (> {tol_pct}%) — balance cache out of sync."
    )


async def verify_active_order_cache(cx: ConnectorWrapper, client_id: str) -> None:
    """
    CACHED active order (the InFlightOrder kept current by the websocket user
    stream) vs FRESH REST order status.  Confirms the exchange_order_id agrees and
    that REST reports the order still live (not a terminal state) — matching the
    cache's view that it is an open order.
    """
    cached = cx.connector.in_flight_orders.get(client_id)
    if cached is None:
        cx.log(f"active-order cache check skipped — {client_id} not in in_flight_orders", "warning")
        return
    status_fn = getattr(cx.connector, "_request_order_status", None)
    if status_fn is None:
        cx.log("active-order cache check skipped — no _request_order_status()", "warning")
        return
    try:
        rest = await status_fn(cached)
    except Exception as exc:
        cx.log(f"active-order cache check skipped — REST status failed: {exc}", "warning")
        return

    cx.log_check("active_order_cache_exchange_id_vs_REST",
                 actual=f"cache={cached.exchange_order_id} REST={rest.exchange_order_id}",
                 expected="equal")
    assert str(cached.exchange_order_id) == str(rest.exchange_order_id), (
        f"[{cx.cfg.connector_name}] Cached exchange_order_id {cached.exchange_order_id} != "
        f"REST {rest.exchange_order_id} for {client_id} — active-order cache mismatch."
    )
    terminal = (OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED)
    cx.log_check("active_order_cache_state_vs_REST",
                 actual=f"cache={cached.current_state.name} REST={rest.new_state.name}",
                 expected="REST live (not terminal)")
    assert rest.new_state not in terminal, (
        f"[{cx.cfg.connector_name}] REST reports terminal state {rest.new_state.name} for an "
        f"order the cache lists as active ({client_id}) — caches disagree."
    )


async def verify_personal_trades_cache(cx: ConnectorWrapper, order) -> None:
    """
    IN-MEMORY fills (captured from OrderFilled events) vs FRESH REST trade history
    for one order.  Asserts REST reports at least as many fills as we cached and
    that the cached filled amount doesn't exceed what REST confirms.
    """
    trade_fn = getattr(cx.connector, "_all_trade_updates_for_order", None)
    if trade_fn is None:
        cx.log("personal-trades cache check skipped — no _all_trade_updates_for_order()", "warning")
        return
    cached_amount = sum(
        (f.amount for f in cx.collected_fills
         if getattr(f, "order_id", None) == order.client_order_id),
        Decimal("0"),
    )
    try:
        rest_trades = await trade_fn(order)
    except Exception as exc:
        cx.log(f"personal-trades cache check skipped — REST fetch failed: {exc}", "warning")
        return
    rest_amount = sum((t.fill_base_amount for t in rest_trades), Decimal("0"))
    cx.log_check("personal_trades_cache_vs_REST",
                 actual=f"cache_filled={cached_amount} REST_filled={rest_amount}",
                 expected="cache <= REST", note=f"rest_trade_count={len(rest_trades)}")
    assert cached_amount <= rest_amount + Decimal("1e-8"), (
        f"[{cx.cfg.connector_name}] In-memory fills total {cached_amount} exceed REST-confirmed "
        f"{rest_amount} for {order.client_order_id} — fill cache overstates reality."
    )


async def verify_last_trade_price_cache(cx: ConnectorWrapper, tol_pct: Decimal = Decimal("5")) -> bool:
    """
    CACHED last-trade price (order_book.last_trade_price, fed by the websocket
    public-trade stream) vs FRESH REST last price.  Returns True if it ran an
    assertion, False if it skipped (no cached trade yet / unsupported).
    """
    pair = cx.cfg.trading_pair
    try:
        cached_last = Decimal(str(cx.connector.get_order_book(pair).last_trade_price))
    except Exception:
        cached_last = Decimal("0")
    if not cached_last.is_finite() or cached_last <= 0:
        cx.log("last-trade-price cache check skipped — websocket trade stream has not "
               "delivered a trade yet (cache empty)", "warning")
        return False

    tracker = getattr(cx.connector, "order_book_tracker", None)
    ds = getattr(tracker, "data_source", None) if tracker is not None else None
    get_last = getattr(ds, "get_last_traded_prices", None) if ds is not None else None
    if get_last is None:
        cx.log("last-trade-price cache check skipped — no get_last_traded_prices()", "warning")
        return False
    try:
        rest_map = await get_last(trading_pairs=[pair])
        rest_last = Decimal(str(rest_map.get(pair, 0)))
    except Exception as exc:
        cx.log(f"last-trade-price cache check skipped — REST fetch failed: {exc}", "warning")
        return False
    if rest_last <= 0:
        cx.log("last-trade-price cache check skipped — REST returned no last price", "warning")
        return False

    diff = _pct_diff(cached_last, rest_last)
    cx.log_check("last_trade_price_cache_vs_REST",
                 actual=f"cache={cached_last} REST={rest_last}",
                 expected=f"diff <= {tol_pct}%", note=f"diff={diff:.4f}%")
    assert diff <= tol_pct, (
        f"[{cx.cfg.connector_name}] Cached last-trade price {cached_last} differs from REST "
        f"{rest_last} by {diff:.4f}% (> {tol_pct}%) — last-trade cache may be stale."
    )
    return True


async def _poll_ws_balance_change(
    cx: ConnectorWrapper,
    token: str,
    baseline: Decimal,
    min_delta: Decimal,
    timeout: int = 30,
) -> tuple:
    """
    Poll get_available_balance(token) for a change of at least min_delta, WITHOUT
    ever calling _update_balances().  With REST refresh suppressed, the ONLY thing
    that can move the cached balance is a websocket balanceUpdate event — so a
    detected change isolates the websocket path.

    Returns (changed: bool, observed: Decimal, elapsed: float).
    """
    start = time.monotonic()
    deadline = start + timeout
    observed = baseline
    while time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        observed = cx.connector.get_available_balance(token)
        if abs(observed - baseline) >= min_delta:
            return True, observed, time.monotonic() - start
    return False, observed, time.monotonic() - start


async def _capture_fills(cx: ConnectorWrapper, attempts: int = 4, delay: float = 2.0) -> int:
    """
    Drive the REST trade-history poll so MarketEvent.OrderFilled events fire for
    recently-filled orders, populating cx.collected_fills for test_08.

    WHY this is needed: collected_fills is fed only by OrderFilled events, which
    the connector emits when it processes a TRADE update. Those trade updates
    arrive either via the websocket trade channel (silent on these venues) or via
    the REST trade-history poll inside _update_order_status() — which the Clock
    drives in production but never fires in this harness. test_07 only drives the
    REST *balance* poll, so a real fill moves the balance but never emits an
    OrderFilled, and test_08 then has nothing to validate. Calling
    _update_order_status() here fetches the trade for the just-filled order and
    emits the event. Best-effort: returns the number of fills captured.
    """
    fn = getattr(cx.connector, "_update_order_status", None)
    if fn is None:
        return len(cx.collected_fills)
    for _ in range(attempts):
        try:
            await fn()
        except Exception:
            pass
        if cx.collected_fills:
            break
        await asyncio.sleep(delay)
    return len(cx.collected_fills)


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
    #
    # WHY we drive _update_balances() ourselves here:
    #   The connector's status-polling loop only calls _update_balances() when
    #   its _poll_notifier fires, and that notifier is set exclusively by tick(),
    #   which a Hummingbot Clock calls.  This fixture starts the connector with
    #   start_network() but never attaches it to a Clock, so the polling loop
    #   blocks forever and the account_balance status flag never flips True.
    #   Connectors whose user-stream pushes a balance snapshot on connect (e.g.
    #   WazirX) become ready anyway; connectors that only fetch balances via REST
    #   (e.g. CoinSwitch) would otherwise time out.  Calling _update_balances()
    #   on each wait iteration replicates what a Clock tick would have triggered.
    _ready_update_fn = getattr(connector, "_update_balances", None)
    ready_deadline = time.monotonic() + cfg.ready_timeout
    while time.monotonic() < ready_deadline:
        if connector.ready:
            break
        if _ready_update_fn is not None:
            try:
                await _ready_update_fn()
            except Exception:
                pass
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

    # ── Authentication verification ──────────────────────────────────────────────
    # connector.ready can go True from public WS alone even with wrong creds.
    # Step 1: check status_dict flags. Step 2: call _update_balances() and confirm
    # at least one non-zero balance (wrong keys return HTTP 200 with all-zeros).

    # Step 1 — status_dict flag check
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

    # Step 2 — balance check: immediate REST call, then wait up to 45 s for WS events.
    # All-zero after 45 s is treated as a credential failure.
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
    """Generic E2E test suite for Hummingbot connectors. Log file: test/e2e_tests/logs/"""

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

        # Cache verification: the cached book above is websocket-fed; confirm it
        # matches a fresh REST snapshot of the same book.
        await verify_orderbook_cache(cx)

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

        # Cache verification: the quote balance this test relies on to size/gate
        # the order is a websocket-fed cache — confirm it matches REST before use.
        quote_token = cx.cfg.trading_pair.split("-")[1]
        await verify_balance_cache(cx, quote_token)

        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

        client_id = place_limit_buy(cx, buy_price)
        cx.log_value("placed_order_client_id", client_id)

        try:
            order = await wait_for_order_open(cx, client_id)

            cx.log_check("order_acknowledged (not None)", actual=order is not None, expected=True)
            assert order is not None, _rejection_msg(cx, client_id, cx.order_wait)

            # Cache verification: the just-tracked in-memory order vs REST status.
            await verify_active_order_cache(cx, client_id)

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

        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

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

            # Cache verification: the cached active order vs REST order status.
            await verify_active_order_cache(cx, client_id)

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

        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, buy_price)

        client_id = place_limit_buy(cx, buy_price)
        cx.log_value("placed_order_client_id", client_id)

        try:
            order = await wait_for_order_open(cx, client_id)

            cx.log_check("order_acknowledged", actual=order is not None, expected=True)
            assert order is not None, _rejection_msg(cx, client_id, cx.order_wait)

            cx.log_value("pre_cancel_state", str(order.current_state))
            cancel_order(cx, client_id)
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
            round(mid * Decimal(str(cx.cfg.buy_price_offset + 0.002 * i)), 8)
            for i in range(3)
        ]
        cx.log_value("mid_price", mid)
        cx.log_value("order_prices", [str(p) for p in prices])

        for p in prices:
            await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, p)

        order_ids: List[str] = []
        for price in prices:
            order_ids.append(place_limit_buy(cx, price))
        cx.log_value("placed_order_ids", order_ids)

        try:
            for oid in order_ids:
                order = await wait_for_order_open(cx, oid)
                cx.log_check(f"order {oid[:20]}... acknowledged", actual=order is not None, expected=True)
                assert order is not None, _rejection_msg(cx, oid, cx.order_wait)

            await cx.connector.cancel_all(timeout_seconds=cx.cancel_wait)

            # Drive order-status polling until every order reaches a terminal
            # state. cancel_all() sends the cancel requests but, on exchanges
            # with asynchronous cancels (e.g. CoinSwitch), the orders only flip
            # to CANCELLED on a later status poll — which no Clock fires here.
            cancel_deadline = time.monotonic() + cx.cancel_wait
            while time.monotonic() < cancel_deadline:
                await _drive_order_status(cx)
                if all(
                    (cx.connector.in_flight_orders.get(oid) is None
                     or cx.connector.in_flight_orders.get(oid).is_done)
                    for oid in order_ids
                ):
                    break
                await asyncio.sleep(1.5)

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
        # Edited price: move the SAME distance the configured multiplier implies,
        # but UPWARD from buy_price_offset, so the edited order's notional stays
        # >= the original's (which test_02 proves clears the min-notional rule).
        # edit_price_multiplier sits below buy_price_offset, so applying it
        # directly shrank the edited order's notional below the exchange minimum
        # and the connector silently refused to create it (same failure mode the
        # old test_05 had). Stepping upward keeps it ~18% below market (won't fill).
        edit_distance = abs(Decimal(str(cx.cfg.buy_price_offset)) - Decimal(str(cx.cfg.edit_price_multiplier)))
        price_b = round(mid * (Decimal(str(cx.cfg.buy_price_offset)) + edit_distance), 8)

        cx.log_value("mid_price", mid)
        cx.log_value("price_a (original)", price_a)
        cx.log_value("price_b (edited)", price_b)

        diff_ab_pct = abs(float(price_a - price_b)) / float(price_a) * 100
        cx.log_check("price_a vs price_b differ by > 0.1%", actual=f"{diff_ab_pct:.4f}%", expected="> 0.1%")
        assert diff_ab_pct > 0.1, (
            "buy_price_offset and edit_price_multiplier produce prices within 0.1% — "
            "adjust them in .env."
        )

        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, price_a)
        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, price_b)

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
    # Phase 1: buy base token if needed. Phase 2: place/cancel sell, check balance.
    # Phase 3 (finally): sell back the base token to leave the account as found.

    async def test_07_balance_then_limit_sell_check_balance(self, cx: ConnectorWrapper):
        cx.log_section("TEST_07: balance_sell")

        base_token, quote_token = cx.cfg.trading_pair.split("-")

        # Cache verification: this test is all about balance accounting, so first
        # confirm both the base and quote balance caches match REST before we
        # start trading against them.
        await verify_balance_cache(cx, quote_token)
        await verify_balance_cache(cx, base_token)

        # ── Phase 1: acquire base token if the account doesn't have enough ────
        setup_buy_id: Optional[str] = None
        # True when Phase 3 should sell back the base token (whether we bought
        # it here or re-used a pre-existing balance for the sell test).
        should_cleanup_sell = False
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

            # Buy at best_ask + 1 % — generous taker premium so the order still
            # crosses the spread even if the market moves a fraction between the
            # order-book snapshot and the REST call landing on WazirX.
            # (The order fills at the actual ask price, not at our limit price.)
            setup_buy_price = round(Decimal(str(asks[0].price)) * Decimal("1.01"), 8)
            cx.log_value("setup_buy_price", setup_buy_price)
            cx.log_value("setup_buy_amount", cx.cfg.limit_sell_amount)

            await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_sell_amount, setup_buy_price)

            baseline_base = cx.connector.get_available_balance(base_token)
            setup_buy_id = cx.connector.buy(
                cx.cfg.trading_pair,
                cx.cfg.limit_sell_amount,
                OrderType.LIMIT,
                setup_buy_price,
            )
            cx.log_value("setup_buy_id", setup_buy_id)

            # Yield so _create_order runs.  If the exchange rejects the order
            # immediately (min-size violation, bad price precision, etc.) it fires
            # OrderFailure within the first second — catch that early to avoid
            # waiting the full poll timeout.
            await asyncio.sleep(0)
            await asyncio.sleep(1)
            if cx._failure_collector.has_failed(setup_buy_id):
                pytest.skip(
                    f"[{cx.cfg.connector_name}] Setup BUY was rejected by the exchange. "
                    f"Check that {cx.cfg.limit_sell_amount} {base_token} meets the minimum "
                    "order size and that the price is valid."
                )

            # Poll balance: WazirX sends balanceUpdate via WebSocket within ~1 s of fill.
            target = cx.cfg.limit_sell_amount * Decimal("0.99")
            acquired = await _poll_balance_increase(cx, base_token, baseline_base, target, timeout=30)
            cx.log_check(
                "setup_buy_balance_delta",
                actual=f"+{acquired} {base_token}",
                expected=f">= {target} {base_token}",
            )
            if acquired < target:
                order_state = cx.connector.in_flight_orders.get(setup_buy_id)
                state_str = str(order_state.current_state) if order_state else "removed"
                await ensure_order_cancelled(cx, setup_buy_id)
                if order_state is not None and not order_state.is_done:
                    reason = (
                        f"Order was still OPEN after 30 s (state={state_str}) — "
                        "it became a maker order because the market moved above the buy price. "
                        "The 1 % premium should prevent this; if it happens again the market "
                        "is unusually fast-moving right now."
                    )
                else:
                    reason = (
                        f"acquired {acquired}, need {target}. "
                        "Try again or fund the account with BTC directly."
                    )
                pytest.skip(
                    f"[{cx.cfg.connector_name}] Setup BUY did not appear in {base_token} "
                    f"balance within 30 s. {reason}"
                )
            cx.log(f"Setup BUY confirmed — acquired {acquired} {base_token}.")
            should_cleanup_sell = True

            # The setup buy is a taker order that just FILLED. Drive the REST
            # trade-history poll now (while the order is still freshly tracked)
            # so an OrderFilled event fires and test_08 has a real fill to
            # validate. The websocket trade channel is silent on these venues, so
            # without this collected_fills stays empty and test_08 always skips.
            captured = await _capture_fills(cx)
            cx.log_value("fills_captured_after_setup_buy", captured)
        else:
            cx.log(
                f"Sufficient {base_token} already available ({avail_base}) — "
                f"skipping setup buy. Will sell {cx.cfg.limit_sell_amount} {base_token} "
                f"back to {quote_token} in Phase 3 cleanup."
            )
            should_cleanup_sell = True

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
            await assert_sufficient_balance(cx, TradeType.SELL, cx.cfg.limit_sell_amount, sell_price)

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

            # ── Phase 3: sell off the base token used for this test ──────────
            # Runs when Phase 1 acquired BTC OR when pre-existing BTC was used
            # for Phase 2 — in both cases we sell it back to leave the account
            # in the same INR-only state it started in.
            if should_cleanup_sell:
                avail_cleanup = cx.connector.get_available_balance(base_token)
                cx.log_value("avail_for_cleanup_sell", str(avail_cleanup))

                # Cap at the test amount so we never sell more than what the
                # test consumed (guards against the user having extra holdings).
                amount_to_sell = min(avail_cleanup, cx.cfg.limit_sell_amount)

                if amount_to_sell > Decimal("0"):
                    cx.log(
                        f"Cleanup: selling {amount_to_sell} {base_token} back "
                        f"to {quote_token} via near-market sell."
                    )
                    bids, _ = get_orderbook_snapshot(cx)
                    if not bids:
                        cx.log(
                            f"Bid side empty — cannot place cleanup sell. "
                            f"~{amount_to_sell} {base_token} remains in account.",
                            "warning",
                        )
                    else:
                        # Sell at best_bid - 1 % → generous taker premium, fills
                        # immediately even if the market dips slightly.
                        cleanup_sell_price = round(
                            Decimal(str(bids[0].price)) * Decimal("0.99"), 8
                        )
                        cx.log_value("cleanup_sell_price", cleanup_sell_price)
                        cleanup_sell_id = cx.connector.sell(
                            cx.cfg.trading_pair,
                            amount_to_sell,
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
                                f"~{amount_to_sell} {base_token} remains in account.",
                                "warning",
                            )

    # ── 8. Trade fills — validate OrderFilledEvent structure ──────────────────

    async def test_08_fetch_trades_match_in_memory(self, cx: ConnectorWrapper):
        cx.log_section("TEST_08: trade_fills")

        fills = cx.collected_fills
        cx.log_value("fill_count", len(fills))

        if not fills:
            cx.log("No OrderFilled events captured — skipping", "warning")
            pytest.skip(
                f"[{cx.cfg.connector_name}] No OrderFilled events were captured this session, "
                "even though test_07 fills a taker order and drives the REST trade-history poll "
                "(_capture_fills). That means this connector emits fills via neither the websocket "
                "trade channel nor _all_trade_updates_for_order — a connector-level gap worth a "
                "separate ticket, not a test-config issue. If test_07 itself was skipped (no fill "
                "occurred), fund the account or adjust amounts in .env."
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

        # Cache verification: reconcile the in-memory fills against the exchange's
        # REST trade history, per order. Best-effort — the order must still be
        # resolvable in the tracker for the REST trade query.
        tracker = getattr(cx.connector, "_order_tracker", None)
        resolvable = getattr(tracker, "all_fillable_orders", {}) if tracker is not None else {}
        seen_orders = set()
        for fill in fills:
            oid = getattr(fill, "order_id", None)
            if oid in seen_orders:
                continue
            seen_orders.add(oid)
            order = resolvable.get(oid) or cx.connector.in_flight_orders.get(oid)
            if order is not None:
                await verify_personal_trades_cache(cx, order)

        cx.log_pass("TEST_08")

    # ── 9. Last-trade-price cache — public trade stream not used elsewhere ────

    async def test_09_last_trade_price_cache_matches_rest(self, cx: ConnectorWrapper):
        cx.log_section("TEST_09: last_trade_cache")

        # The cached last-trade price is fed by the websocket public-trade stream
        # and is not consulted by any other test (mid price comes from the order
        # book), so it gets its own cache-vs-REST verification here.
        ran = await verify_last_trade_price_cache(cx)
        if not ran:
            pytest.skip(
                f"[{cx.cfg.connector_name}] Last-trade-price cache could not be verified "
                "(no websocket trade observed yet, or connector exposes no REST last-price)."
            )
        cx.log_pass("TEST_09")

    # ── 10. ISOLATED websocket balance update ─────────────────────────────────
    # Every other test lets REST polling refresh the balance cache (this harness
    # has no Clock, so assert_sufficient_balance / the balance pollers call
    # _update_balances() themselves). That means a balance "cache vs REST" check
    # is really REST-vs-REST and CANNOT catch a broken websocket balanceUpdate
    # handler — the REST refresh masks it.
    #
    # This test removes that mask: it places a TAKER order that fills (changing
    # the real account balance), then watches the cached QUOTE balance inside a
    # window where NO REST refresh is issued. The connector's websocket user
    # stream is the only thing that can update the cache there, so a detected
    # change proves the websocket balance path works — and no change proves it is
    # broken (the cache only stays correct because REST elsewhere papers over it).
    # The quote token is used because it is non-zero (newly-bought base tokens can
    # have unreliable WS updates on some venues).

    async def test_10_websocket_balance_update_isolated(self, cx: ConnectorWrapper):
        cx.log_section("TEST_10: ws_balance")
        base, quote = cx.cfg.trading_pair.split("-")

        _, asks = get_orderbook_snapshot(cx)
        assert asks, f"Ask side empty for {cx.cfg.trading_pair} — cannot place taker buy"
        taker_price = round(Decimal(str(asks[0].price)) * Decimal("1.01"), 8)
        est_cost = cx.cfg.limit_buy_amount * Decimal(str(asks[0].price))

        # REST-accurate baseline for the (non-zero) quote balance.
        await assert_sufficient_balance(cx, TradeType.BUY, cx.cfg.limit_buy_amount, taker_price)
        baseline_quote = cx.connector.get_available_balance(quote)
        cx.log_value("ws_baseline_quote", f"{baseline_quote} {quote}")
        cx.log_value("ws_taker_price", taker_price)
        cx.log_value("ws_est_cost", f"{est_cost} {quote}")

        buy_id: Optional[str] = None
        try:
            buy_id = cx.connector.buy(
                cx.cfg.trading_pair, cx.cfg.limit_buy_amount, OrderType.LIMIT, taker_price,
            )
            cx.log_value("ws_taker_buy_id", buy_id)

            await asyncio.sleep(0)
            await asyncio.sleep(1)
            if cx._failure_collector.has_failed(buy_id):
                pytest.skip(
                    f"[{cx.cfg.connector_name}] WS-probe taker BUY was rejected — cannot test "
                    "the websocket balance path. Check minimum order size in .env."
                )

            # ── Isolated websocket window — NO _update_balances() in here ──────
            # Quote balance must DROP by ~est_cost when the fill/lock lands, and
            # the only mechanism that can update the cache here is the websocket.
            min_drop = est_cost * Decimal("0.5")
            delivered, observed, elapsed = await _poll_ws_balance_change(
                cx, quote, baseline_quote, min_drop, timeout=30,
            )
            cx.log_check(
                "ws_balance_update_delivered",
                actual=f"{baseline_quote} -> {observed}  (Δ={observed - baseline_quote})",
                expected=f"drop >= {min_drop} {quote} via websocket (REST suppressed)",
                note=f"elapsed={elapsed:.1f}s",
            )

            # Cross-check: now allow REST, and confirm the WS-observed number
            # agrees with REST truth (i.e. the cache wasn't just partially right).
            update_fn = getattr(cx.connector, "_update_balances", None)
            if update_fn is not None:
                try:
                    await update_fn()
                except Exception:
                    pass
            rest_quote = cx.connector.get_available_balance(quote)
            cx.log_check(
                "ws_observed_balance_vs_REST",
                actual=f"ws_observed={observed} REST={rest_quote}",
                expected="diff <= 1%",
                note=f"diff={_pct_diff(observed, rest_quote):.4f}%",
            )

            # Informational: did the fill also arrive as an OrderFilled event
            # (the websocket user-stream trade channel)?  test_08 shows this is
            # often empty for these venues — logged here for the same diagnosis.
            ws_fills = [f for f in cx.collected_fills if getattr(f, "order_id", None) == buy_id]
            cx.log_value("ws_orderfilled_events_for_probe", len(ws_fills))

            assert delivered, (
                f"[{cx.cfg.connector_name}] The websocket user stream did NOT update the cached "
                f"{quote} balance within 30s after a filled order, with REST polling suppressed "
                f"(stayed at {baseline_quote}). The in-memory balance cache is NOT being kept "
                f"current by the websocket balanceUpdate path — it only looks correct elsewhere "
                f"because REST refreshes mask the gap. This is a real cached-data problem."
            )
            cx.log_pass("TEST_10")

        finally:
            # Cancel the probe order if it rested instead of filling.
            await ensure_order_closed(cx, buy_id)
            # Force a REST balance refresh BEFORE reading the base balance. The
            # websocket cache is the very thing under test here and may be stale
            # (CoinSwitch/WazirX) — reading it directly would show ~0 base token,
            # skip the sell-back, and leave the bought coin stranded. A REST
            # refresh reflects the real holding so cleanup always runs.
            cleanup_update_fn = getattr(cx.connector, "_update_balances", None)
            if cleanup_update_fn is not None:
                try:
                    await cleanup_update_fn()
                except Exception:
                    pass
            # Sell back any base token acquired so the account is left as found.
            avail_base = cx.connector.get_available_balance(base)
            amount_back = min(avail_base, cx.cfg.limit_buy_amount)
            if amount_back > Decimal("0"):
                bids, _ = get_orderbook_snapshot(cx)
                if bids:
                    sell_price = round(Decimal(str(bids[0].price)) * Decimal("0.99"), 8)
                    back_id = cx.connector.sell(
                        cx.cfg.trading_pair, amount_back, OrderType.LIMIT, sell_price,
                    )
                    sold = await _poll_balance_decrease(cx, base, avail_base, timeout=30)
                    if sold:
                        cx.log(f"WS-probe cleanup: {base} sold back to {quote}.")
                    else:
                        await ensure_order_cancelled(cx, back_id)
                        cx.log(
                            f"WS-probe cleanup SELL did not fill in 30s — cancelled. "
                            f"~{amount_back} {base} remains.", "warning",
                        )
