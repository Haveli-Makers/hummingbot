from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "delta_perpetual"
DEFAULT_DOMAIN = "delta_perpetual"

# Hummingbot puts the client order id in Delta's `client_order_id` (max 32 chars).
HBOT_ORDER_ID_PREFIX = "DLTA"
MAX_ORDER_ID_LEN = 32
BROKER_ID = "hummingbot"

# ── Base URLs ──────────────────────────────────────────────────────────────────
REST_URL = "https://api.india.delta.exchange"
WSS_URL = "wss://socket.india.delta.exchange"

# Delta (India) perpetual futures are USD-quoted/settled linear contracts.
# Collateral is the settling asset of each product (resolved from /v2/products).
CURRENCY = "USD"

# ── Public REST endpoints ──────────────────────────────────────────────────────
PRODUCTS_PATH_URL = "/v2/products"
TICKERS_PATH_URL = "/v2/tickers"
TICKER_PATH_URL = "/v2/tickers/{symbol}"
ORDER_BOOK_PATH_URL = "/v2/l2orderbook/{symbol}"

# ── Private REST endpoints ─────────────────────────────────────────────────────
ORDERS_PATH_URL = "/v2/orders"                     # POST place, DELETE cancel, PUT edit, GET list
ORDER_BY_ID_PATH_URL = "/v2/orders/{order_id}"     # GET single order
# NOTE: GET /v2/positions returns a SINGLE position and REQUIRES a product_id /
# underlying_asset_symbol filter (else HTTP 400 bad_schema).

# To poll ALL open positions in one call, use /v2/positions/margined.
POSITIONS_PATH_URL = "/v2/positions"
POSITIONS_MARGINED_PATH_URL = "/v2/positions/margined"
WALLET_PATH_URL = "/v2/wallet/balances"
SET_LEVERAGE_PATH_URL = "/v2/products/{product_id}/orders/leverage"

# Used as a public, cheap network check.
PING_PATH_URL = PRODUCTS_PATH_URL

# ── WebSocket channels ─────────────────────────────────────────────────────────
WS_AUTH_PREHASH_PATH = "/live"          # WS auth signs "GET" + timestamp + "/live"
# Public
WS_ORDERBOOK_CHANNEL = "l2_orderbook"
WS_TRADES_CHANNEL = "all_trades"
WS_MARK_PRICE_CHANNEL = "mark_price"
WS_FUNDING_CHANNEL = "funding_rate"
# Private
WS_ORDERS_CHANNEL = "orders"
WS_POSITIONS_CHANNEL = "positions"
WS_USER_TRADES_CHANNEL = "v2/user_trades"
WS_WALLET_CHANNEL = "margins"

PING_TIMEOUT = 30.0
WS_HEARTBEAT_TIME_INTERVAL = 25.0

# ── Order sides / types ────────────────────────────────────────────────────────
SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit_order"
ORDER_TYPE_MARKET = "market_order"

TIME_IN_FORCE_GTC = "gtc"

# Delta perpetual order states: open, pending, closed, cancelled.
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

PERPETUAL_CONTRACT_TYPE = "perpetual_futures"

# ── Rate limits ────────────────────────────────────────────────────────────────
# Delta uses a per-endpoint quota; use a generous global bucket plus per-path ids.
GLOBAL_LIMIT_ID = "GLOBAL"
ONE_MINUTE = 60
MAX_REQUESTS = 6000

_ALL_PATHS = [
    PRODUCTS_PATH_URL, TICKERS_PATH_URL, TICKER_PATH_URL, ORDER_BOOK_PATH_URL,
    ORDERS_PATH_URL, ORDER_BY_ID_PATH_URL, POSITIONS_PATH_URL, POSITIONS_MARGINED_PATH_URL,
    WALLET_PATH_URL, SET_LEVERAGE_PATH_URL,
]

RATE_LIMITS = [RateLimit(limit_id=GLOBAL_LIMIT_ID, limit=MAX_REQUESTS, time_interval=ONE_MINUTE)]
for _p in set(_ALL_PATHS):
    RATE_LIMITS.append(
        RateLimit(limit_id=_p, limit=MAX_REQUESTS, time_interval=ONE_MINUTE,
                  linked_limits=[LinkedLimitWeightPair(GLOBAL_LIMIT_ID, 1)])
    )
