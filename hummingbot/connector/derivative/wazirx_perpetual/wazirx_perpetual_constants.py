from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "wazirx_perpetual"
DEFAULT_DOMAIN = "wazirx_perpetual"

# Futures REST lives under /fapi on the same host as spot (/sapi). The futures
# wallet balance and the websocket auth token are SAPI endpoints, so both
# prefixes are needed.
REST_URL = "https://api.wazirx.com"
WSS_URL = "wss://fstreamx.wazirx.com/stream"

# requestId must be alphanumeric and at most 64 characters (error 3017), so the
# prefix carries no hyphen. Keeping it alphanumeric lets the requestId BE the
# client order id, which the orderUpdate stream echoes back in ``c``.
HBOT_ORDER_ID_PREFIX = "haveli"
MAX_ORDER_ID_LEN = 36

# Every symbol margins from the INR futures wallet ("marginAsset": "INR" on all
# 484 contracts, including the USDT-quoted ones).
DEFAULT_MARGIN_ASSET = "INR"

# ---- Public / market data --------------------------------------------------
PING_PATH_URL = "/fapi/v1/ping"
SERVER_TIME_PATH_URL = "/fapi/v1/time"
EXCHANGE_INFO_PATH_URL = "/fapi/v1/exchangeInfo"
MARK_PRICE_PATH_URL = "/fapi/v1/premiumIndex"
DEPTH_PATH_URL = "/fapi/v1/depth"
TICKER_24HR_PATH_URL = "/fapi/v1/ticker/24hr"
KLINES_PATH_URL = "/fapi/v1/klines"

# Only 20 levels are supported; any other ``limit`` is rejected.
ORDER_BOOK_DEPTH = 20

# ---- Private -----------------------------------------------------------------
POSITION_RISK_PATH_URL = "/fapi/v1/positionRisk"
INCOME_PATH_URL = "/fapi/v1/income"
USER_TRADES_PATH_URL = "/fapi/v1/userTrades"
ORDER_PATH_URL = "/fapi/v1/order"
OPEN_ORDERS_PATH_URL = "/fapi/v1/openOrders"
ALL_ORDERS_PATH_URL = "/fapi/v1/allOrders"
CANCEL_ALL_OPEN_ORDERS_PATH_URL = "/fapi/v1/allOpenOrders"
POSITION_MARGIN_PATH_URL = "/fapi/v1/positionMargin"

# The futures wallet is read through the SAPI v2 funds endpoint, filtered to the
# futures wallet; the response nests it under the "futures" key.
FUNDS_PATH_URL = "/sapi/v2/funds"
FUTURES_WALLET = "futures"

# One auth_key per member, shared by spot and futures streams.
CREATE_AUTH_TOKEN_PATH_URL = "/sapi/v1/create_auth_token"

# Throttler ids for the two methods of /fapi/v1/order that have their own budget.
CREATE_ORDER_LIMIT_ID = "FAPI_CREATE_ORDER"
CANCEL_ORDER_LIMIT_ID = "FAPI_CANCEL_ORDER"
QUERY_ORDER_LIMIT_ID = "FAPI_QUERY_ORDER"

# ---- WebSocket -------------------------------------------------------------
WS_HEARTBEAT_TIME_INTERVAL = 30
# Application-level {"event": "ping"}. The server answers with a "pong" carrying
# timeout_duration 1800, i.e. the connection's remaining validity.
WS_PING_INTERVAL = 60
# The auth_key lives 30 minutes and requesting it again extends it, so it is
# refreshed well before it lapses.
AUTH_KEY_TIMEOUT = 1800
AUTH_KEY_REFRESH_INTERVAL = 15 * 60

SUBSCRIBE_EVENT = "subscribe"
SUBSCRIBED_EVENT = "subscribed"
PING_EVENT = "ping"
PONG_EVENT = "pong"
ERROR_EVENT = "error"
CONNECTED_EVENT = "connected"

# Stream names. Symbols are lowercase on the socket ("btcinr@depth").
DEPTH_STREAM = "{symbol}@depth"
TRADE_STREAM = "{symbol}@aggTrade"
MARK_PRICE_STREAM = "!markPrice@arr"
DEPTH_STREAM_SUFFIX = "@depth"
TRADE_STREAM_SUFFIX = "@aggTrade"

ORDER_UPDATE_STREAM = "orderUpdate"
OWN_TRADE_STREAM = "ownTrade"
BALANCE_UPDATE_STREAM = "outboundAccountPosition"
POSITION_UPDATE_STREAM = "positionUpdate"
PRIVATE_STREAMS = [ORDER_UPDATE_STREAM, OWN_TRADE_STREAM, BALANCE_UPDATE_STREAM, POSITION_UPDATE_STREAM]

# ---- Order enums -------------------------------------------------------------
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

POSITION_LONG = "LONG"
POSITION_SHORT = "SHORT"

INCOME_TYPE_FUNDING_FEE = "FUNDING_FEE"

# REST statuses are UPPERCASE and stream statuses lowercase; keys are lowercased
# before lookup. INIT means "accepted, being handed to the matching engine",
# which Hummingbot treats as OPEN (Binance maps NEW the same way). WAIT covers
# partial fills too; the connector upgrades it to PARTIALLY_FILLED when
# executedQty > 0. IDLE is a TP/SL leg waiting for its trigger.
ORDER_STATE = {
    "init": OrderState.OPEN,
    "wait": OrderState.OPEN,
    "idle": OrderState.OPEN,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED,
    "expire": OrderState.CANCELED,
    "reject": OrderState.FAILED,
}

# Funding is settled every 8 hours on most contracts; the exact next time comes
# from premiumIndex / the mark-price stream.
DEFAULT_FUNDING_INTERVAL_HOURS = 8
FUNDING_FEE_POLL_INTERVAL = 120

# Account-wide list endpoints page backwards with fromId.
PAGE_SIZE = 500
MAX_PAGES = 10

# ownTrade frames carry only the exchange order id. An order is matchable by it
# once orderUpdate or the REST create response has recorded the id, so a fill
# that beats both is held briefly and replayed instead of being dropped.
PENDING_TRADE_EVENT_TTL = 15.0
PENDING_TRADE_EVENT_RETRY_INTERVAL = 0.2
MAX_PENDING_TRADE_EVENTS = 256

# ---- Error codes ---------------------------------------------------------------
ORDER_NOT_EXIST_ERROR_CODE = 2004       # "Order doesn't exist."
UNKNOWN_REQUEST_ID_ERROR_CODE = 3020    # "Unknown requestId."
REQUEST_ID_USED_ERROR_CODE = 3018       # "requestId already used."
OUT_OF_RECV_WINDOW_ERROR_CODE = 2098    # "Request out of receiving window."
TONCE_USED_ERROR_CODE = 2006            # "The tonce has already been used."
FUTURES_NOT_ACTIVATED_ERROR_CODE = 2191
NO_OPEN_POSITIONS_ERROR_CODE = 3216

ONE_SECOND = 1

# Futures rate limits: 10/s for create and cancel, 2/s for depth, 1/s for every
# other endpoint. Depth is declared at 1/s: the spot depth endpoint answered
# HTTP 429 to 21% of calls at its documented 2/s, and the book is streamed over
# the websocket anyway, so REST depth is only a start-up / fallback path.
RATE_LIMITS = [
    RateLimit(limit_id=PING_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=MARK_PRICE_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_24HR_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=KLINES_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=POSITION_RISK_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=INCOME_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=USER_TRADES_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=CREATE_ORDER_LIMIT_ID, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=QUERY_ORDER_LIMIT_ID, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=ALL_ORDERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=CANCEL_ALL_OPEN_ORDERS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=POSITION_MARGIN_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=FUNDS_PATH_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=CREATE_AUTH_TOKEN_PATH_URL, limit=1, time_interval=ONE_SECOND),
]
