from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "ajaib"

# Ajaib only exposes a single production host for the Open API. ``domain`` is kept
# as a label threaded through the base class / data sources for compatibility, but
# the actual REST/WSS hosts are fixed below.
DEFAULT_DOMAIN = "ajaib"

REST_URL = "https://api.kripto.ajaib.co.id"
WSS_URL = "wss://stream.kripto.ajaib.co.id"

# Public market-data streams are subscribed to on /ws via a SUBSCRIBE message; the
# private user-data stream connects to /ws/<listenKey>.
WS_PUBLIC_PATH = "/ws"
WS_USER_PATH = "/ws"

HBOT_ORDER_ID_PREFIX = "haveli-"
# Ajaib expects ``newClientOrderId`` in UUIDv4 form; keep room for the prefix.
MAX_ORDER_ID_LEN = 36

# Generous recvWindow (ms) to tolerate the extra latency / clock skew introduced
# by routing through an Indonesian proxy. Ajaib defaults to 5000 when omitted.
RECV_WINDOW = 20000

# ---- Market info / public endpoints -----------------------------------------
SERVER_TIME_PATH_URL = "/v1/time"
EXCHANGE_INFO_PATH_URL = "/v1/exchange-info"
KLINES_PATH_URL = "/v1/klines"

# ---- Spot trading endpoints --------------------------------------------------
CREATE_ORDER_PATH_URL = "/v1/order"
ORDER_STATUS_PATH_URL = "/v1/order"
CANCEL_ORDER_PATH_URL = "/v1/order"
OPEN_ORDERS_PATH_URL = "/v1/order/open"
CANCEL_ALL_ORDERS_PATH_URL = "/v1/order/open"
ALL_ORDERS_PATH_URL = "/v1/order/all"
TRADES_PATH_URL = "/v1/trades"

# ---- Wallet ------------------------------------------------------------------
PORTFOLIO_PATH_URL = "/v1/portfolio"

# ---- User data stream (listenKey) -------------------------------------------
LISTEN_KEY_PATH_URL = "/auth/v1/listen-key"

WS_HEARTBEAT_TIME_INTERVAL = 30

# WebSocket throttler ids (the SUBSCRIBE message / connection are weighted too).
WS_CONNECTIONS_LIMIT_ID = "WSConnections"
WS_SUBSCRIPTIONS_LIMIT_ID = "WSSubscriptions"

# WebSocket event types (the ``e`` field on each raw payload).
WS_DEPTH_EVENT_TYPE = "depth"
WS_TRADE_EVENT_TYPE = "trade"
WS_EXECUTION_REPORT_EVENT_TYPE = "executionReport"

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_LIMIT_MAKER = "LIMIT_MAKER"
ORDER_TYPE_MARKET = "MARKET"

TIME_IN_FORCE_GTC = "GTC"

ONE_MINUTE = 60
ONE_SECOND = 1

# Mapping of Ajaib order statuses to Hummingbot order states (see docs > Definitions).
ORDER_STATE = {
    "PENDING_NEW": OrderState.PENDING_CREATE,
    "NEW": OrderState.OPEN,
    "OPEN": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "PARTIALLY_CANCELLED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "CANCELLED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.CANCELED,
    "EXPIRED_IN_MATCH": OrderState.CANCELED,
    "PARTIALLY_EXPIRED_IN_MATCH": OrderState.CANCELED,
}

RATE_LIMITS = [
    RateLimit(limit_id=WS_CONNECTIONS_LIMIT_ID, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=WS_SUBSCRIPTIONS_LIMIT_ID, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=KLINES_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ALL_ORDERS_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADES_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=PORTFOLIO_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=LISTEN_KEY_PATH_URL, limit=1200, time_interval=ONE_MINUTE),
]

ORDER_NOT_EXIST_ERROR_CODE = 404
ORDER_NOT_EXIST_MESSAGE = "Order not found"
UNKNOWN_ORDER_ERROR_CODE = 400
UNKNOWN_ORDER_MESSAGE = "Unknown order"
