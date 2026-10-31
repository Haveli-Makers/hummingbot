from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "delta_perpetual"
DEFAULT_DOMAIN = "delta_perpetual"
TESTNET_DOMAIN = "delta_perpetual_testnet"

# Hummingbot puts the client order id in Delta's `client_order_id` (max 32 chars).
HBOT_ORDER_ID_PREFIX = "DLTA"
MAX_ORDER_ID_LEN = 32
BROKER_ID = "hummingbot"

# ── Base URLs (testnet keys come from a demo.delta.exchange account) ──────────
REST_URLS = {
    DEFAULT_DOMAIN: "https://api.india.delta.exchange",
    TESTNET_DOMAIN: "https://cdn-ind.testnet.deltaex.org",
}
# Market data moved to the public endpoint; the old channels on the private one stop on 2026-10-31.
WSS_PUBLIC_URLS = {
    DEFAULT_DOMAIN: "wss://public-socket.india.delta.exchange",
    TESTNET_DOMAIN: "wss://socket-ind-pub.testnet.deltaex.org",
}
WSS_PRIVATE_URLS = {
    DEFAULT_DOMAIN: "wss://socket.india.delta.exchange",
    TESTNET_DOMAIN: "wss://socket-ind.testnet.deltaex.org",
}

# ── Public REST endpoints ──────────────────────────────────────────────────────
PRODUCTS_PATH_URL = "/v2/products"
TICKERS_PATH_URL = "/v2/tickers"
TICKER_PATH_URL = "/v2/tickers/{symbol}"
ORDER_BOOK_PATH_URL = "/v2/l2orderbook/{symbol}"

# ── Private REST endpoints ─────────────────────────────────────────────────────
ORDERS_PATH_URL = "/v2/orders"                     # POST place, DELETE cancel
ORDER_BY_ID_PATH_URL = "/v2/orders/{order_id}"     # GET single order
POSITIONS_MARGINED_PATH_URL = "/v2/positions/margined"
WALLET_PATH_URL = "/v2/wallet/balances"
WALLET_TRANSACTIONS_PATH_URL = "/v2/wallet/transactions"
FILLS_PATH_URL = "/v2/fills"
SET_LEVERAGE_PATH_URL = "/v2/products/{product_id}/orders/leverage"

PING_PATH_URL = PRODUCTS_PATH_URL

PERPETUAL_CONTRACT_TYPE = "perpetual_futures"
LIVE_STATE = "live"
PRODUCTS_PAGE_SIZE = 100
MAX_PRODUCT_PAGES = 20
FILLS_PAGE_SIZE = 50
MAX_FILL_PAGES = 10
FUNDING_TRANSACTION_TYPE = "funding"
FUNDING_LOOKBACK_SECONDS = 9 * 60 * 60

# ── WebSocket channels ─────────────────────────────────────────────────────────
WS_AUTH_TYPE = "key-auth"
WS_AUTH_PREHASH_PATH = "/live"          # WS auth signs "GET" + timestamp + "/live"
WS_AUTH_TIMEOUT = 10.0
WS_MAX_SYMBOLS = 100                    # ob_l2 accepts at most 100 symbols per connection
# Public
WS_ORDERBOOK_CHANNEL = "ob_l2"
WS_TRADES_CHANNEL = "trades"
WS_TICKER_CHANNEL = "ticker"
WS_FUNDING_CHANNEL = "funding_rate"
# Private
WS_ORDERS_CHANNEL = "orders"
WS_POSITIONS_CHANNEL = "positions"
WS_USER_TRADES_CHANNEL = "v2/user_trades"
WS_WALLET_CHANNEL = "margins"

PING_TIMEOUT = 25.0

# ── Order sides / types ────────────────────────────────────────────────────────
SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit_order"
ORDER_TYPE_MARKET = "market_order"

TIME_IN_FORCE_GTC = "gtc"

# Delta order states: open, pending, closed, cancelled.
ORDER_STATE = {
    "open": OrderState.OPEN,
    "pending": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "closed": OrderState.FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

# ── Rate limits ────────────────────────────────────────────────────────────────
# 20,000 weight per 5-minute window; endpoints not listed weigh 1.
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
        (TICKER_PATH_URL, 3),
        (ORDER_BOOK_PATH_URL, 3),
        (ORDERS_PATH_URL, 5),
        (ORDER_BY_ID_PATH_URL, 1),
        (POSITIONS_MARGINED_PATH_URL, 3),
        (WALLET_PATH_URL, 3),
        (WALLET_TRANSACTIONS_PATH_URL, 10),
        (FILLS_PATH_URL, 10),
        (SET_LEVERAGE_PATH_URL, 5),
    )
]
