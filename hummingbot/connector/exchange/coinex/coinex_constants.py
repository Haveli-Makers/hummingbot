from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "coinex"
DEFAULT_DOMAIN = "coinex"

# CoinEx accepts a custom client order id (alphanumeric / hyphen / underscore, <= 32 bytes).
HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_ORDER_ID_LEN = 32

# ── Base URLs ──────────────────────────────────────────────────────────────────
REST_URL = "https://api.coinex.com/v2"
WSS_URL = "wss://socket.coinex.com/v2/spot"

# CoinEx spot markets are quoted in their quote currency (e.g. BTCUSDT -> BTC-USDT).
MARKET_TYPE_SPOT = "SPOT"

# ── Public REST endpoints ──────────────────────────────────────────────────────
MARKETS_PATH_URL = "/spot/market"          # GET list markets / trading rules
TICKER_PATH_URL = "/spot/ticker"           # GET 24h ticker (last price + volume)
DEPTH_PATH_URL = "/spot/depth"             # GET order book snapshot
SERVER_TIME_PATH_URL = "/time"             # GET server time (ms)

# A cheap public endpoint used for the network check.
PING_PATH_URL = MARKETS_PATH_URL

# ── Private REST endpoints ─────────────────────────────────────────────────────
ORDER_PATH_URL = "/spot/order"                       # POST place order
CANCEL_ORDER_PATH_URL = "/spot/cancel-order"         # POST cancel by order_id
ORDER_STATUS_PATH_URL = "/spot/order-status"         # GET single order status
ORDER_DEALS_PATH_URL = "/spot/order-deals"           # GET fills (deals) of an order
PENDING_ORDERS_PATH_URL = "/spot/pending-order"      # GET open orders
BALANCE_PATH_URL = "/assets/spot/balance"            # GET spot balances

# ── WebSocket methods / channels ────────────────────────────────────────────────
WS_SIGN_METHOD = "server.sign"             # auth: sign str(timestamp) with secret
# Public
WS_DEPTH_SUBSCRIBE = "depth.subscribe"
WS_DEPTH_UPDATE = "depth.update"
WS_DEALS_SUBSCRIBE = "deals.subscribe"
WS_DEALS_UPDATE = "deals.update"
# Private
WS_ORDER_SUBSCRIBE = "order.subscribe"
WS_ORDER_UPDATE = "order.update"
WS_BALANCE_SUBSCRIBE = "balance.subscribe"
WS_BALANCE_UPDATE = "balance.update"

# Default order book depth subscription tuple: [market, limit, interval, full].
WS_DEPTH_LIMIT = 50
WS_DEPTH_INTERVAL = "0"

PING_TIMEOUT = 20.0
WS_HEARTBEAT_TIME_INTERVAL = 25.0

# ── Order sides / types ────────────────────────────────────────────────────────
SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit"
ORDER_TYPE_MARKET = "market"
ORDER_TYPE_MAKER_ONLY = "maker_only"       # post-only

# CoinEx spot order statuses (GET /spot/order-status `status` field).
ORDER_STATE = {
    "open": OrderState.OPEN,
    "part_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "part_canceled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
}

# WS order.update `event` values.
WS_ORDER_EVENT_STATE = {
    "put": OrderState.OPEN,
    "update": OrderState.PARTIALLY_FILLED,
    "modify": OrderState.OPEN,
    "finish": OrderState.FILLED,   # refined to CANCELED when unfilled remains on cancel
}

# ── Rate limits ──────────────────────────────────────────────────────────────────
# CoinEx applies per-endpoint quotas; use a generous global bucket plus per-path ids.
GLOBAL_LIMIT_ID = "GLOBAL"
ONE_MINUTE = 60
MAX_REQUESTS = 6000

_ALL_PATHS = [
    MARKETS_PATH_URL, TICKER_PATH_URL, DEPTH_PATH_URL, SERVER_TIME_PATH_URL,
    ORDER_PATH_URL, CANCEL_ORDER_PATH_URL, ORDER_STATUS_PATH_URL, ORDER_DEALS_PATH_URL,
    PENDING_ORDERS_PATH_URL, BALANCE_PATH_URL,
]

RATE_LIMITS = [RateLimit(limit_id=GLOBAL_LIMIT_ID, limit=MAX_REQUESTS, time_interval=ONE_MINUTE)]
for _p in set(_ALL_PATHS):
    RATE_LIMITS.append(
        RateLimit(limit_id=_p, limit=MAX_REQUESTS, time_interval=ONE_MINUTE,
                  linked_limits=[LinkedLimitWeightPair(GLOBAL_LIMIT_ID, 1)])
    )
