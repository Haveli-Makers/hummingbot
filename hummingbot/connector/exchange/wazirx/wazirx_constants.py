from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = ""

HBOT_ORDER_ID_PREFIX = "hbot-"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://api.wazirx.com"
WSS_URL = "wss://stream.wazirx.com"

TICKERS_PATH_URL = "/sapi/v1/tickers/24hr"
TICKER_24HR_PATH_URL = "/sapi/v1/ticker/24hr"
DEPTH_PATH_URL = "/sapi/v1/depth"
TRADE_HISTORY_PATH_URL = "/sapi/v1/trades"
EXCHANGE_INFO_PATH_URL = "/sapi/v1/exchangeInfo"
PING_PATH_URL = "/sapi/v1/ping"
SERVER_TIME_PATH_URL = "/sapi/v1/time"

USER_BALANCES_PATH_URL = "/sapi/v1/funds"
CREATE_ORDER_PATH_URL = "/sapi/v1/order"
ORDER_STATUS_PATH_URL = "/sapi/v1/order"
CANCEL_ORDER_PATH_URL = "/sapi/v1/order"
OPEN_ORDERS_PATH_URL = "/sapi/v1/openOrders"
MY_TRADES_PATH_URL = "/sapi/v1/myTrades"

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"

ONE_MINUTE = 60
ONE_SECOND = 1

MAX_REQUEST = 1200

ORDER_STATE = {
    "idle": OrderState.PENDING_CREATE,
    "wait": OrderState.OPEN,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED,
    "NEW": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}

RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=100, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=TICKERS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5)]),
    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10)]),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1), LinkedLimitWeightPair(ORDERS, 1)]),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=PING_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)]),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)]),
]
