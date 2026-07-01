from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

# CSX rejects client order IDs containing dashes ("Invalid ClientOrderId"),
# so the prefix must be alphanumeric only — NOT the usual "x-XXX" broker format.
HBOT_ORDER_ID_PREFIX = "xCSX"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://exchange.coinswitch.co"

# ── Public endpoints (no auth required) ───────────────────────────────────────
HEALTH_PATH_URL = "/api/v1/public/health/"
INSTRUMENTS_PATH_URL = "/api/v1/public/instrument"
TICKER_V2_PATH_URL = "/api/v2/public/ticker/"
DEPTH_V2_PATH_URL = "/api/v1/public/depth/"   # v2 returns 500; v1 confirmed working
TRADES_PATH_URL = "/api/v1/public/trades/"

# ── Private endpoints (auth required) ─────────────────────────────────────────
PROFILE_PATH_URL = "/api/v1/me/"
CREATE_ORDER_PATH_URL = "/api/v2/orders/"
# Base path used as rate-limit ID for per-order GET/DELETE (id appended at runtime)
ORDER_BY_ID_PATH_URL = "/api/v1/orders"
ME_ORDERS_PATH_URL = "/api/v1/me/orders/"
BALANCE_V2_PATH_URL = "/api/v2/me/balance/"

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

QUANTITY_TYPE_BASE = "BASE"

# CSX uses "FULFILLED" terminology (confirmed live: "PARTIALLY_FULFILLED").
# Mapped defensively to also accept the "FILLED"/"CANCELED" spellings so the
# connector is robust to either form the API may return.
ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "PENDING": OrderState.OPEN,
    "PARTIALLY_FULFILLED": OrderState.PARTIALLY_FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FULFILLED": OrderState.FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
    "FAILED": OrderState.FAILED,
}

# ── Rate limits ────────────────────────────────────────────────────────────────
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
RAW_REQUESTS = "RAW_REQUESTS"

ONE_MINUTE = 60
ONE_SECOND = 1

MAX_REQUEST = 2000

USER_STREAM_POLL_INTERVAL = 5.0  # retained for back-compat / fallback

# ── Realtime REST poll intervals (seconds), per data type ─────────────────────
# CSX exposes no WebSocket, so account + market data is kept "realtime" by tight
# polling. Each data type has its OWN cadence (the user-stream source runs them in
# concurrent loops), so latency can be traded against the weighted rate limit
# (2000/min) independently and a slow data type never blocks a fast one. Lower =
# fresher data but more requests; raise these if you track many pairs/orders.
BALANCE_POLL_INTERVAL = 3.0          # GET /api/v2/me/balance/
ACTIVE_ORDERS_POLL_INTERVAL = 2.0    # GET /api/v1/me/orders/?onlyOpen=true (+ settled detection)
ACCOUNT_TRADES_POLL_INTERVAL = 2.0   # GET /api/v1/orders/{id} per in-flight order (cumulative fills)
ORDER_BOOK_POLL_INTERVAL = 2.0       # GET /api/v1/public/depth/ snapshot (was 30s)
PUBLIC_TRADES_POLL_INTERVAL = 3.0    # GET /api/v1/public/trades/

RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=100, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=RAW_REQUESTS, limit=2000, time_interval=ONE_MINUTE),
    # Public
    RateLimit(limit_id=HEALTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=INSTRUMENTS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKER_V2_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=DEPTH_V2_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    # Private
    RateLimit(limit_id=PROFILE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=100, time_interval=10 * ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(ORDERS, 1)]),
    RateLimit(limit_id=ORDER_BY_ID_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ME_ORDERS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=BALANCE_V2_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]
