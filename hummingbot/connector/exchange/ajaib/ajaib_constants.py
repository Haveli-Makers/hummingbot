from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "api.kripto.ajaib.co.id"

BASE_URL = "https://{0}"

HBOT_ORDER_ID_PREFIX = "haveli-"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://api.kripto.ajaib.co.id"
WSS_URL = "wss://stream.kripto.ajaib.co.id"

# Market Info endpoints
SERVER_TIME_PATH_URL = "/v1/time"
EXCHANGE_INFO_PATH_URL = "/v1/exchange-info"
KLINES_PATH_URL = "/v1/klines"
TICKER_BOOK_PATH_URL = "/v1/ticker/bookTicker"
DEPTH_PATH_URL = "/v1/depth"

# Spot Trading endpoints
CREATE_ORDER_PATH_URL = "/v1/order"
ORDER_STATUS_PATH_URL = "/v1/order"
CANCEL_ORDER_PATH_URL = "/v1/order"
OPEN_ORDERS_PATH_URL = "/v1/order/open"
CANCEL_ALL_ORDERS_PATH_URL = "/v1/order/all"
TRADES_PATH_URL = "/v1/trades"
ALL_ORDERS_PATH_URL = "/v1/order/all"

# Wallet endpoints
PORTFOLIO_PATH_URL = "/v1/portfolio"

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_LIMIT_MAKER = "LIMIT_MAKER"
ORDER_TYPE_MARKET = "MARKET"

ONE_MINUTE = 60
ONE_SECOND = 1

ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "OPEN": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELED": OrderState.CANCELED,
    "CANCELLED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.CANCELED,
}

RATE_LIMITS = [
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=KLINES_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TICKER_BOOK_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ALL_ORDERS_PATH_URL, limit=30, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADES_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ALL_ORDERS_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=PORTFOLIO_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
]

ORDER_NOT_EXIST_ERROR_CODE = 404
ORDER_NOT_EXIST_MESSAGE = "Order not found"
UNKNOWN_ORDER_ERROR_CODE = 400
UNKNOWN_ORDER_MESSAGE = "Unknown order"
