from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "x-CSX"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://exchange.coinswitch.co"

# ── Public endpoints (no auth required) ───────────────────────────────────────
HEALTH_PATH_URL = "/api/v1/public/health/"
INSTRUMENTS_PATH_URL = "/api/v1/public/instrument"
TICKER_V2_PATH_URL = "/api/v2/public/ticker/"
DEPTH_V2_PATH_URL = "/api/v1/public/depth/"   # v2 returns 500; v1 confirmed working
TRADES_PATH_URL = "/api/v1/public/trades/"

# ── Private endpoints (auth required) ─────────────────────────────────────────
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

ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}

# ── Rate limits ────────────────────────────────────────────────────────────────
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
RAW_REQUESTS = "RAW_REQUESTS"

ONE_MINUTE = 60
ONE_SECOND = 1

MAX_REQUEST = 2000

USER_STREAM_POLL_INTERVAL = 5.0  # seconds between REST polls in user-stream source

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
