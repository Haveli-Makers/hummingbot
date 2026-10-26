from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = ""

HBOT_ORDER_ID_PREFIX = "haveli-"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://api.wazirx.com/sapi"
WSS_URL = "wss://stream.wazirx.com/stream"

TICKERS_PATH_URL = "/v1/tickers/24hr"
TICKER_24HR_PATH_URL = "/v1/ticker/24hr"
DEPTH_PATH_URL = "/v1/depth"
TRADE_HISTORY_PATH_URL = "/v1/trades"
EXCHANGE_INFO_PATH_URL = "/v1/exchangeInfo"
PING_PATH_URL = "/v1/ping"
SERVER_TIME_PATH_URL = "/v1/time"

USER_BALANCES_PATH_URL = "/v1/funds"
CREATE_ORDER_PATH_URL = "/v1/order"
ORDER_STATUS_PATH_URL = "/v1/order"
CANCEL_ORDER_PATH_URL = "/v1/order"
OPEN_ORDERS_PATH_URL = "/v1/openOrders"
MY_TRADES_PATH_URL = "/v1/myTrades"
CREATE_AUTH_TOKEN_PATH_URL = "/v1/create_auth_token"

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

ONE_SECOND = 1

ORDER_STATE = {
    "idle": OrderState.PENDING_CREATE,
    "wait": OrderState.OPEN,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED,
}

# ---- API key rotation ----------------------------------------------------------
# WazirX applies its rate limits per API key ("The limits on the API are based on
# the API keys"), and an account can have 5 keys. The connector accepts up to 5
# comma-separated keys and secrets and spreads signed requests across them; see
# WazirxApiKeyPool.
MAX_API_KEYS = 5

# After an HTTP 429 the key rests on that endpoint for the Retry-After seconds, or
# this long when the header is missing.
DEFAULT_RATE_LIMIT_COOLDOWN = 2.0

# HTTP 418 means WazirX banned this IP for sending requests after 429s. Signed
# requests stop for the Retry-After seconds, or this long when the header is
# missing: requests during a ban make the next one longer (2 minutes up to 3 days).
DEFAULT_IP_BAN_COOLDOWN = 120.0

# The start-up check sends one balance request per key; past this many seconds the
# connector starts with every key.
API_KEY_CHECK_TIMEOUT = 10.0

# Signed timestamps are back-dated by this much. A request is accepted when
# serverTime - timestamp <= recvWindow (60000 ms) but rejected when it is more
# than 1s early (error 2098). Being a few seconds late costs nothing.
TIMESTAMP_SAFETY_MARGIN_MS = 2000

RATE_LIMITED_HTTP_STATUS = 429
IP_BANNED_HTTP_STATUS = 418

# /v1/order has a different limit per method (10/s create, 2/s query, 10/s cancel),
# so each method gets its own limit id.
CREATE_ORDER_LIMIT_ID = "SAPI_CREATE_ORDER"
ORDER_STATUS_LIMIT_ID = "SAPI_QUERY_ORDER"
CANCEL_ORDER_LIMIT_ID = "SAPI_CANCEL_ORDER"
ORDER_LIMIT_IDS = {
    "POST": CREATE_ORDER_LIMIT_ID,
    "GET": ORDER_STATUS_LIMIT_ID,
    "DELETE": CANCEL_ORDER_LIMIT_ID,
}

# Documented limits. Each window is stretched to 1.5s: the limits are applied when
# a call is SENT, while WazirX counts it when it ARRIVES, so calls spaced just over
# 1s apart still get 429 (seen on /sapi/v1/depth and on the futures API). Signed
# endpoints are counted per API key by WazirxApiKeyPool; public endpoints go
# through the connector's throttler.
RATE_LIMIT_WINDOW = 1.5

RATE_LIMITS = [
    RateLimit(limit_id=PING_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=TICKERS_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=TICKER_24HR_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=2, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CREATE_ORDER_LIMIT_ID, limit=10, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=ORDER_STATUS_LIMIT_ID, limit=2, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=10, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=2, time_interval=RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CREATE_AUTH_TOKEN_PATH_URL, limit=1, time_interval=RATE_LIMIT_WINDOW),
]
