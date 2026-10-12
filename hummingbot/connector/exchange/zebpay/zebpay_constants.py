from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "ZEB"
MAX_ORDER_ID_LEN = 32

# Spot API base URL (REST only — Zebpay spot exposes no WebSocket feed).
REST_URL = "https://sapi.zebpay.com"

# ── Public endpoints (no auth required) ───────────────────────────────────────
EXCHANGE_INFO_PATH_URL = "/api/v2/ex/exchangeInfo"
CURRENCIES_PATH_URL = "/api/v2/ex/currencies"
ALL_TICKERS_PATH_URL = "/api/v2/market/allTickers"
TICKER_PATH_URL = "/api/v2/market/ticker"
ORDERBOOK_PATH_URL = "/api/v2/market/orderbook"
ORDERBOOK_TICKER_PATH_URL = "/api/v2/market/orderbook/ticker"
TRADES_PATH_URL = "/api/v2/market/trades"

# ── Private endpoints (auth required) ─────────────────────────────────────────
BALANCE_PATH_URL = "/api/v2/account/balance"
CREATE_ORDER_PATH_URL = "/api/v2/ex/orders"
ORDER_PATH_URL = "/api/v2/ex/order"            # GET (status) / DELETE (cancel) — orderId as query
ORDERS_PATH_URL = "/api/v2/ex/orders"          # GET (list) / DELETE (cancel all by symbol)
CANCEL_ALL_PATH_URL = "/api/v2/ex/orders/cancelAll"
ORDER_FILLS_PATH_URL = "/api/v2/ex/order/fills"

# Used as a stable network/health check (public, cheap).
PING_PATH_URL = ALL_TICKERS_PATH_URL

# Observed live: INR pairs require >= 99 INR
MIN_NOTIONAL_BY_QUOTE = {"INR": "99"}
DEFAULT_MIN_NOTIONAL = "1"

# Last-resort tick/lot increments used only when exchangeInfo publishes neither an
# explicit size nor a precision for a pair. A warning is logged whenever these apply.
DEFAULT_PRICE_INCREMENT = Decimal("0.01")
DEFAULT_BASE_INCREMENT = Decimal("0.0001")

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

# Order-list status filter values
ORDER_STATUS_ACTIVE = "ACTIVE"

# Map every status string Zebpay can return to hummingbot's OrderState.
# Documented spot statuses: OPEN, FILLED, CANCELLED, COMPLETED.
# Extra spellings mapped defensively.
ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "ACTIVE": OrderState.OPEN,
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "PARTIALLYFILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "COMPLETED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
    "FAILED": OrderState.FAILED,
}

# Error-message substrings that classify a status/cancel failure as "order not found".
ORDER_NOT_FOUND_MESSAGES = ("not found", "does not exist", "404")
# Additional substrings meaning a cancel hit an order that is already in a terminal
# state. Zebpay returns these as HTTP 200 business errors; they must also be treated
# as not-found so the cancel flow settles the order instead of leaving it in-flight.
CANCEL_TERMINAL_MESSAGES = (
    "already cancelled", "already canceled", "already filled", "already completed",
    "not in active state", "not active", "cannot be cancelled", "cannot be canceled",
)

# ── Rate limits (docs: public 1200/min, private 600/min, per key) ─────────────
PUBLIC_LIMIT_ID = "PUBLIC"
PRIVATE_LIMIT_ID = "PRIVATE"

ONE_MINUTE = 60
MAX_PUBLIC = 1200
MAX_PRIVATE = 600

# ── Realtime REST poll intervals (seconds), per data type ─────────────────────
# Zebpay exposes no WebSocket, so account + market data is kept "realtime" by tight
# polling. Each data type has its OWN cadence (the user-stream source runs them in
# concurrent loops), so latency can be traded against the private rate limit
# (600/min) independently and a slow data type never blocks a fast one. Lower =
# fresher data but more requests; raise these if you track many pairs/orders.
BALANCE_POLL_INTERVAL = 3.0          # GET /account/balance
ACTIVE_ORDERS_POLL_INTERVAL = 2.0    # GET /ex/orders?status=ACTIVE (+ settled detection)
ACCOUNT_TRADES_POLL_INTERVAL = 2.0   # GET /ex/order/fills per in-flight order
ORDER_BOOK_POLL_INTERVAL = 2.0       # GET /market/orderbook snapshot (was 30s)
PUBLIC_TRADES_POLL_INTERVAL = 3.0    # GET /market/trades

# Ceiling for the exponential backoff each poll loop applies after consecutive
# failures. The loops are independent, so without a backoff a sustained outage has
# all of them retrying at their full 2-3s cadence at once.
MAX_POLL_BACKOFF_INTERVAL = 60.0

_PUBLIC_PATHS = [
    EXCHANGE_INFO_PATH_URL, CURRENCIES_PATH_URL, ALL_TICKERS_PATH_URL, TICKER_PATH_URL,
    ORDERBOOK_PATH_URL, ORDERBOOK_TICKER_PATH_URL, TRADES_PATH_URL,
]
_PRIVATE_PATHS = [
    BALANCE_PATH_URL, CREATE_ORDER_PATH_URL, ORDER_PATH_URL, ORDERS_PATH_URL,
    CANCEL_ALL_PATH_URL, ORDER_FILLS_PATH_URL,
]

RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_LIMIT_ID, limit=MAX_PUBLIC, time_interval=ONE_MINUTE),
    RateLimit(limit_id=PRIVATE_LIMIT_ID, limit=MAX_PRIVATE, time_interval=ONE_MINUTE),
]
# Distinct rate-limit ids: ORDER_PATH_URL and ORDERS_PATH_URL share a prefix, so
# a set() de-duplicates them into one private bucket linkage each.
for _p in set(_PUBLIC_PATHS):
    RATE_LIMITS.append(
        RateLimit(limit_id=_p, limit=MAX_PUBLIC, time_interval=ONE_MINUTE,
                  linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID, 1)])
    )
for _p in set(_PRIVATE_PATHS):
    RATE_LIMITS.append(
        RateLimit(limit_id=_p, limit=MAX_PRIVATE, time_interval=ONE_MINUTE,
                  linked_limits=[LinkedLimitWeightPair(PRIVATE_LIMIT_ID, 1)])
    )
