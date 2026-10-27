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

# WazirX rate-limits each API key separately; an account can have up to 5 keys.
MAX_API_KEYS = 5
DEFAULT_RATE_LIMIT_COOLDOWN = 2.0   # seconds a key rests after a 429 without Retry-After
DEFAULT_IP_BAN_COOLDOWN = 120.0     # seconds signed requests pause after a 418 IP ban
API_KEY_CHECK_TIMEOUT = 10.0
TIMESTAMP_SAFETY_MARGIN_MS = 2000   # WazirX rejects timestamps more than 1 s early (error 2098)

RATE_LIMITED_HTTP_STATUS = 429
IP_BANNED_HTTP_STATUS = 418

# /v1/order has separate limits for create, query and cancel.
CREATE_ORDER_LIMIT_ID = "SAPI_CREATE_ORDER"
ORDER_STATUS_LIMIT_ID = "SAPI_QUERY_ORDER"
CANCEL_ORDER_LIMIT_ID = "SAPI_CANCEL_ORDER"
ORDER_LIMIT_IDS = {"POST": CREATE_ORDER_LIMIT_ID, "GET": ORDER_STATUS_LIMIT_ID, "DELETE": CANCEL_ORDER_LIMIT_ID}

# Signed limits are counted per key by WazirxApiKeyPool. WazirX counts requests on
# arrival, so their windows are stretched to 1.5 s.
SIGNED_RATE_LIMIT_WINDOW = 1.5

RATE_LIMITS = [
    RateLimit(limit_id=PING_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_24HR_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=2, time_interval=ONE_SECOND),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=1, time_interval=SIGNED_RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CREATE_ORDER_LIMIT_ID, limit=10, time_interval=SIGNED_RATE_LIMIT_WINDOW),
    RateLimit(limit_id=ORDER_STATUS_LIMIT_ID, limit=2, time_interval=SIGNED_RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=10, time_interval=SIGNED_RATE_LIMIT_WINDOW),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=2, time_interval=SIGNED_RATE_LIMIT_WINDOW),
    RateLimit(limit_id=CREATE_AUTH_TOKEN_PATH_URL, limit=1, time_interval=ONE_SECOND),
]
