from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "api.coindcx.com"

BASE_URL = "https://{0}"
PUBLIC_DOMAIN = "public.coindcx.com"

HBOT_ORDER_ID_PREFIX = "haveli-"
MAX_ORDER_ID_LEN = 36

REST_URL = "https://api.coindcx.com"
PUBLIC_REST_URL = "https://public.coindcx.com"
WSS_URL = "wss://stream.coindcx.com"

MARKETS_PATH_URL = "/exchange/v1/markets"
MARKETS_DETAILS_PATH_URL = "/exchange/v1/markets_details"
ORDER_BOOK_PATH_URL = "/market_data/orderbook"
TRADE_HISTORY_PATH_URL = "/market_data/trade_history"
CANDLES_PATH_URL = "/market_data/candles"
TICKER_PATH_URL = "/exchange/ticker"

USER_BALANCES_PATH_URL = "/exchange/v1/users/balances"
CREATE_ORDER_PATH_URL = "/exchange/v1/orders/create"
ORDER_STATUS_PATH_URL = "/exchange/v1/orders/status"
CANCEL_ORDER_PATH_URL = "/exchange/v1/orders/cancel"
CANCEL_ALL_ORDERS_PATH_URL = "/exchange/v1/orders/cancel_all"
ACTIVE_ORDERS_PATH_URL = "/exchange/v1/orders/active_orders"
TRADE_HISTORY_ACCOUNT_PATH_URL = "/exchange/v1/orders/trade_history"
ORDER_EDIT_PATH_URL = "/exchange/v1/orders/edit"

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit_order"
ORDER_TYPE_MARKET = "market_order"

ONE_MINUTE = 60
ONE_SECOND = 1

ORDER_STATE = {
    "init": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "cancelled": OrderState.CANCELED,
    "partially_cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

DIFF_EVENT_TYPE = "depth-update"
DEPTH_SNAPSHOT_EVENT_TYPE = "depth-snapshot"
TRADE_EVENT_TYPE = "new-trade"
ORDER_UPDATE_EVENT_TYPE = "order-update"
BALANCE_UPDATE_EVENT_TYPE = "balance-update"
TRADE_UPDATE_EVENT_TYPE = "trade-update"

ECODE_BINANCE = "B"
ECODE_COINDCX = "I"

RATE_LIMITS = [
    RateLimit(limit_id=MARKETS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=MARKETS_DETAILS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=2000, time_interval=ONE_MINUTE),

    RateLimit(limit_id=USER_BALANCES_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ALL_ORDERS_PATH_URL, limit=30, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ACTIVE_ORDERS_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADE_HISTORY_ACCOUNT_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_EDIT_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
]

ORDER_NOT_EXIST_ERROR_CODE = 404
ORDER_NOT_EXIST_MESSAGE = "Order not found"
UNKNOWN_ORDER_ERROR_CODE = 400
UNKNOWN_ORDER_MESSAGE = "Unknown order"
