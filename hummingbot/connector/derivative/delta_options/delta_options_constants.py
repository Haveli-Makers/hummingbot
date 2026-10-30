from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

EXCHANGE_NAME = "delta_options"
DEFAULT_DOMAIN = "delta_options"

HBOT_ORDER_ID_PREFIX = "DLTO"
MAX_ORDER_ID_LEN = 32
USER_AGENT = "hummingbot"

REST_URL = "https://api.india.delta.exchange"
# Market data is on the public endpoint; Delta switches off the old market channels on the private one from 2026-10-31.
WSS_PUBLIC_URL = "wss://public-socket.india.delta.exchange"
WSS_PRIVATE_URL = "wss://socket.india.delta.exchange"

PRODUCTS_PATH_URL = "/v2/products"
TICKERS_PATH_URL = "/v2/tickers/{symbols}"
ORDER_BOOK_PATH_URL = "/v2/l2orderbook/{symbol}"
POSITIONS_PATH_URL = "/v2/positions/margined"
WALLET_PATH_URL = "/v2/wallet/balances"

CALL_OPTIONS = "call_options"
PUT_OPTIONS = "put_options"
OPTION_CONTRACT_TYPES = f"{CALL_OPTIONS},{PUT_OPTIONS}"
LIVE_STATE = "live"
PRODUCTS_PAGE_SIZE = 100
MAX_PRODUCT_PAGES = 50
MAX_SYMBOLS_PER_REQUEST = 10
SETTLEMENT_HOUR_UTC = 12

WS_ORDER_BOOK_CHANNEL = "ob_l2"
WS_TRADES_CHANNEL = "trades"
WS_TICKER_CHANNEL = "ticker"
WS_MAX_SYMBOLS = 100
WS_AUTH_TYPE = "key-auth"
WS_AUTH_PATH = "/live"
WS_POSITIONS_CHANNEL = "positions"
WS_MARGINS_CHANNEL = "margins"
WS_PING_INTERVAL = 25.0
WS_AUTH_TIMEOUT = 10.0

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
    )
]
