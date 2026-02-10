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

RATE_LIMITS = [
    RateLimit(limit_id=PING_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_24HR_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=2, time_interval=ONE_SECOND),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=2, time_interval=ONE_SECOND),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=2, time_interval=ONE_SECOND),
    RateLimit(limit_id=CREATE_AUTH_TOKEN_PATH_URL, limit=1, time_interval=ONE_SECOND),
]
