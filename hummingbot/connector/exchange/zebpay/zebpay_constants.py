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

# ── Rate limits (docs: public 1200/min, private 600/min, per key) ─────────────
PUBLIC_LIMIT_ID = "PUBLIC"
PRIVATE_LIMIT_ID = "PRIVATE"

ONE_MINUTE = 60
MAX_PUBLIC = 1200
MAX_PRIVATE = 600

USER_STREAM_POLL_INTERVAL = 5.0  # seconds between REST polls in the user-stream source

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
