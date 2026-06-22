from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "valr"
DEFAULT_DOMAIN = "valr"

# VALR accepts a customerOrderId (alphanumeric, <= 50 chars) for client order ids.
HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_ORDER_ID_LEN = 50

# ── Base URLs ──────────────────────────────────────────────────────────────────
REST_URL = "https://api.valr.com"
WSS_TRADE_URL = "wss://api.valr.com/ws/trade"        # public market data
WSS_ACCOUNT_URL = "wss://api.valr.com/ws/account"    # private account stream (auth headers)

# Only SPOT pairs are traded by this connector.
PAIR_TYPE_SPOT = "SPOT"

# ── Public REST endpoints ──────────────────────────────────────────────────────
PAIRS_PATH_URL = "/v1/public/pairs"                          # GET trading pairs / rules
MARKET_SUMMARY_PATH_URL = "/v1/public/marketsummary"        # GET 24h summary (bid/ask/last/volume)
ORDER_BOOK_PATH_URL = "/v1/public/{pair}/orderbook"        # GET aggregated order book
SERVER_TIME_PATH_URL = "/v1/public/time"                   # GET server time

# A cheap public endpoint used for the network check.
PING_PATH_URL = SERVER_TIME_PATH_URL

# ── Private REST endpoints ─────────────────────────────────────────────────────
BALANCES_PATH_URL = "/v1/account/balances"                            # GET balances
PLACE_LIMIT_ORDER_PATH_URL = "/v1/orders/limit"                       # POST limit order
PLACE_MARKET_ORDER_PATH_URL = "/v1/orders/market"                     # POST market order
CANCEL_ORDER_PATH_URL = "/v1/orders/order"                           # DELETE cancel order
ORDER_STATUS_PATH_URL = "/v1/orders/{pair}/orderid/{order_id}"      # GET single order status
ORDER_HISTORY_SUMMARY_PATH_URL = "/v1/orders/history/summary/orderid/{order_id}"  # GET final state
OPEN_ORDERS_PATH_URL = "/v1/orders/open"                            # GET open orders

# ── Auth headers ────────────────────────────────────────────────────────────────
HEADER_API_KEY = "X-VALR-API-KEY"
HEADER_SIGNATURE = "X-VALR-SIGNATURE"
HEADER_TIMESTAMP = "X-VALR-TIMESTAMP"

# ── WebSocket events ─────────────────────────────────────────────────────────────
# Public (/ws/trade)
WS_AGGREGATED_ORDERBOOK_UPDATE = "AGGREGATED_ORDERBOOK_UPDATE"
WS_NEW_TRADE = "NEW_TRADE"
# Private (/ws/account)
WS_ORDER_STATUS_UPDATE = "ORDER_STATUS_UPDATE"
WS_BALANCE_UPDATE = "BALANCE_UPDATE"
WS_NEW_ACCOUNT_TRADE = "NEW_ACCOUNT_TRADE"
WS_OPEN_ORDERS_UPDATE = "OPEN_ORDERS_UPDATE"
# Control
WS_SUBSCRIBE = "SUBSCRIBE"
WS_PING = "PING"
WS_PONG = "PONG"
WS_AUTHENTICATED = "AUTHENTICATED"
WS_UNAUTHORIZED = "UNAUTHORIZED"

PING_TIMEOUT = 20.0

# ── Order sides / types ────────────────────────────────────────────────────────
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"

# VALR order status types (`orderStatusType`).
ORDER_STATE = {
    "Placed": OrderState.OPEN,
    "Active": OrderState.OPEN,
    "Partially Filled": OrderState.PARTIALLY_FILLED,
    "Filled": OrderState.FILLED,
    "Cancelled": OrderState.CANCELED,
    "Partially Cancelled": OrderState.CANCELED,
    "Failed": OrderState.FAILED,
}

# ── Rate limits ──────────────────────────────────────────────────────────────────
# VALR applies per-endpoint quotas; use a generous global bucket plus per-path ids.
GLOBAL_LIMIT_ID = "GLOBAL"
ONE_MINUTE = 60
MAX_REQUESTS = 6000

_ALL_PATHS = [
    PAIRS_PATH_URL, MARKET_SUMMARY_PATH_URL, ORDER_BOOK_PATH_URL, SERVER_TIME_PATH_URL,
    BALANCES_PATH_URL, PLACE_LIMIT_ORDER_PATH_URL, PLACE_MARKET_ORDER_PATH_URL,
    CANCEL_ORDER_PATH_URL, ORDER_STATUS_PATH_URL, ORDER_HISTORY_SUMMARY_PATH_URL, OPEN_ORDERS_PATH_URL,
]

RATE_LIMITS = [RateLimit(limit_id=GLOBAL_LIMIT_ID, limit=MAX_REQUESTS, time_interval=ONE_MINUTE)]
for _p in set(_ALL_PATHS):
    RATE_LIMITS.append(
        RateLimit(limit_id=_p, limit=MAX_REQUESTS, time_interval=ONE_MINUTE,
                  linked_limits=[LinkedLimitWeightPair(GLOBAL_LIMIT_ID, 1)])
    )
