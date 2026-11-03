from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "delta_options"
DEFAULT_DOMAIN = "delta_options"
TESTNET_DOMAIN = "delta_options_testnet"

HBOT_ORDER_ID_PREFIX = "DLTO"
MAX_ORDER_ID_LEN = 32
USER_AGENT = "hummingbot"

# Testnet keys come from a demo.delta.exchange account.
REST_URLS = {
    DEFAULT_DOMAIN: "https://api.india.delta.exchange",
    TESTNET_DOMAIN: "https://cdn-ind.testnet.deltaex.org",
}
# Market data is on the public endpoint; Delta switches off the old market channels on the private one from 2026-10-31.
WSS_PUBLIC_URLS = {
    DEFAULT_DOMAIN: "wss://public-socket.india.delta.exchange",
    TESTNET_DOMAIN: "wss://socket-ind-pub.testnet.deltaex.org",
}
WSS_PRIVATE_URLS = {
    DEFAULT_DOMAIN: "wss://socket.india.delta.exchange",
    TESTNET_DOMAIN: "wss://socket-ind.testnet.deltaex.org",
}

PRODUCTS_PATH_URL = "/v2/products"
TICKERS_PATH_URL = "/v2/tickers/{symbols}"
ORDER_BOOK_PATH_URL = "/v2/l2orderbook/{symbol}"
POSITIONS_PATH_URL = "/v2/positions/margined"
WALLET_PATH_URL = "/v2/wallet/balances"
ORDERS_PATH_URL = "/v2/orders"
ORDER_BY_ID_PATH_URL = "/v2/orders/{order_id}"
FILLS_PATH_URL = "/v2/fills"
HEARTBEAT_CREATE_PATH_URL = "/v2/heartbeat/create"
HEARTBEAT_PATH_URL = "/v2/heartbeat"

CALL_OPTIONS = "call_options"
PUT_OPTIONS = "put_options"
OPTION_CONTRACT_TYPES = f"{CALL_OPTIONS},{PUT_OPTIONS}"
LIVE_STATE = "live"
PRODUCTS_PAGE_SIZE = 100
MAX_PRODUCT_PAGES = 50
MAX_SYMBOLS_PER_REQUEST = 10
FILLS_PAGE_SIZE = 50
MAX_FILL_PAGES = 10
SETTLEMENT_HOUR_UTC = 12

SIDE_BUY = "buy"
SIDE_SELL = "sell"
ORDER_TYPE_LIMIT = "limit_order"
TIME_IN_FORCE_GTC = "gtc"
ORDER_STATE = {
    "open": OrderState.OPEN,
    "pending": OrderState.OPEN,
    "closed": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

WS_ORDER_BOOK_CHANNEL = "ob_l2"
WS_TRADES_CHANNEL = "trades"
WS_TICKER_CHANNEL = "ticker"
WS_MAX_SYMBOLS = 100
WS_AUTH_TYPE = "key-auth"
WS_AUTH_PATH = "/live"
WS_ORDERS_CHANNEL = "orders"
WS_USER_TRADES_CHANNEL = "v2/user_trades"
WS_POSITIONS_CHANNEL = "positions"
WS_MARGINS_CHANNEL = "margins"
WS_PING_INTERVAL = 25.0
WS_AUTH_TIMEOUT = 10.0

# REST can lag the private stream by a moment; a fill signalled on the stream is read again after these delays.
FILL_FETCH_DELAYS = (0.0, 0.5, 1.0, 2.0, 4.0)

# Dead-man switch: Delta cancels the bot's option orders if no heartbeat arrives within the TTL.
HEARTBEAT_TTL_MS = 30000
HEARTBEAT_INTERVAL = 10.0
HEARTBEAT_STOP_TIMEOUT = 5.0

# Options fees: a rate on the underlying's value, capped at a share of the premium, plus GST.
# Each product carries its own cap; this conservative fallback is used only if it is missing.
DEFAULT_PREMIUM_CAP_RATE = Decimal("0.1")
GST_RATE = Decimal("0.18")

# Delta's quota: 20,000 weight per fixed 5-minute window. Endpoints not listed weigh 1.
REST_QUOTA_ID = "rest_quota"
REST_QUOTA = 20000
REST_QUOTA_INTERVAL = 300

RATE_LIMITS = [
    RateLimit(limit_id=REST_QUOTA_ID, limit=REST_QUOTA, time_interval=REST_QUOTA_INTERVAL),
] + [
    RateLimit(limit_id=path, limit=REST_QUOTA, time_interval=REST_QUOTA_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(REST_QUOTA_ID, weight)])
    for path, weight in (
        (PRODUCTS_PATH_URL, 3),
        (TICKERS_PATH_URL, 3),
        (ORDER_BOOK_PATH_URL, 3),
        (POSITIONS_PATH_URL, 3),
        (WALLET_PATH_URL, 3),
        (ORDERS_PATH_URL, 5),
        (ORDER_BY_ID_PATH_URL, 1),
        (FILLS_PATH_URL, 10),
        (HEARTBEAT_CREATE_PATH_URL, 1),
        (HEARTBEAT_PATH_URL, 1),
    )
]
