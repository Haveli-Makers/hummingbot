from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "x-CS"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://coinswitch.co"
WSS_URL = "wss://ws.coinswitch.co"

PUBLIC_API_VERSION = "v2"
PRIVATE_API_VERSION = "v2"

PING_PATH_URL = "/trade/api/v2/ping"
SERVER_TIME_PATH_URL = "/trade/api/v2/time"
EXCHANGE_INFO_PATH_URL = "/trade/api/v2/exchangePrecision"
ACTIVE_COINS_PATH_URL = "/trade/api/v2/coins"
TRADE_INFO_PATH_URL = "/trade/api/v2/tradeInfo"
TICKER_PATH_URL = "/trade/api/v2/24hr/ticker"
TICKER_ALL_PATH_URL = "/trade/api/v2/24hr/all-pairs/ticker"
DEPTH_PATH_URL = "/trade/api/v2/depth"
TRADES_PATH_URL = "/trade/api/v2/trades"
CANDLES_PATH_URL = "/trade/api/v2/candles"

CREATE_ORDER_PATH_URL = "/trade/api/v2/order"
CANCEL_ORDER_PATH_URL = "/trade/api/v2/order"
GET_ORDER_PATH_URL = "/trade/api/v2/order"
OPEN_ORDERS_PATH_URL = "/trade/api/v2/orders"
CLOSED_ORDERS_PATH_URL = "/trade/api/v2/orders"
GET_PORTFOLIO_PATH_URL = "/trade/api/v2/user/portfolio"
VALIDATE_KEYS_PATH_URL = "/trade/api/v2/validate/keys"
TRADING_FEE_PATH_URL = "/trade/api/v2/tradingFee"

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit"

ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "PARTIALLY_EXECUTED": OrderState.PARTIALLY_FILLED,
    "EXECUTED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "EXPIRED": OrderState.FAILED,
    "DISCARDED": OrderState.FAILED,
    "CANCELLATION_RAISED": OrderState.PENDING_CANCEL,
    "EXPIRATION_RAISED": OrderState.OPEN,
}

REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"

ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 10000

ORDER_BOOK_EVENT_TYPE = "FETCH_ORDER_BOOK_CS_PRO"
TRADE_EVENT_TYPE = "FETCH_TRADES_CS_PRO"
CANDLESTICK_EVENT_TYPE = "FETCH_CANDLESTICK_CS_PRO"
ORDER_UPDATE_EVENT_TYPE = "FETCH_ORDER_UPDATES"
BALANCE_UPDATE_EVENT_TYPE = "FETCH_BALANCE_UPDATES"

SUPPORTED_EXCHANGES = ["coinswitchx", "wazirx", "c2c1", "c2c2"]
DEFAULT_EXCHANGE = "coinswitchx"

RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=10000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=100, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=ORDERS_24HR, limit=200000, time_interval=ONE_DAY),
    RateLimit(limit_id=RAW_REQUESTS, limit=61000, time_interval=5 * ONE_MINUTE),
    RateLimit(limit_id=PING_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ACTIVE_COINS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADE_INFO_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKER_ALL_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=CANDLES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=100, time_interval=10 * ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(ORDERS, 1)]),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=100, time_interval=10 * ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(ORDERS, 1)]),
    RateLimit(limit_id=GET_ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 4),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=10000, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=CLOSED_ORDERS_PATH_URL, limit=10000, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=GET_PORTFOLIO_PATH_URL, limit=5000, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=VALIDATE_KEYS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADING_FEE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE),
]
