import uuid

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "coinswitch_perpetual"
DEFAULT_DOMAIN = "coinswitch_perpetual"

# CoinSwitch PRO Futures v2 (https://api-trading.coinswitch.co/futures/). REST
# shares the host and the Ed25519 signing scheme with the spot API; the public
# market-data sockets are Socket.IO v4 on ws.coinswitch.co.
REST_URL = "https://coinswitch.co"
WSS_URL = "https://ws.coinswitch.co"

# Every futures request names the venue. EXCHANGE_2 is the only futures exchange.
EXCHANGE = "EXCHANGE_2"
WS_EXCHANGE = "exchange_2"
WS_NAMESPACE = f"/{WS_EXCHANGE}"
WS_SOCKETIO_PATH = f"/pro/realtime-rates-socket/futures/{WS_EXCHANGE}"

HBOT_ORDER_ID_PREFIX = "x-CSP"
MAX_ORDER_ID_LEN = 36

# The API's client_order_id must be a UUID, which Hummingbot ids are not. Each
# order is sent with uuid5(namespace, client order id), so the exchange id can be
# recomputed from the Hummingbot id at any time (lookups after a lost create
# response need no stored mapping).
CLIENT_ORDER_ID_NAMESPACE = uuid.UUID("6f1c3a52-9a7e-4f0c-8a55-0c5b2f3e7d11")

# All contracts are USDT-quoted and USDT-margined.
COLLATERAL_TOKEN = "USDT"

# ---- Public -------------------------------------------------------------------
# /time lives on the spot surface and is the one endpoint documented as unsigned.
SERVER_TIME_PATH_URL = "/trade/api/v2/time"
INSTRUMENT_INFO_PATH_URL = "/trade/api/v2/futures/instrument_info"
ORDER_BOOK_PATH_URL = "/trade/api/v2/futures/order_book"
TICKER_PATH_URL = "/trade/api/v2/futures/ticker"
ALL_PAIRS_TICKER_PATH_URL = "/trade/api/v2/futures/all-pairs/ticker"
KLINES_PATH_URL = "/trade/api/v2/futures/klines"
TRADES_PATH_URL = "/trade/api/v2/futures/trades"

# ---- Private ------------------------------------------------------------------
# POST places, DELETE cancels and GET reads an order; each method has its own
# rate limit, so each gets its own throttler id below.
ORDER_PATH_URL = "/trade/api/v2/futures/order"
OPEN_ORDERS_PATH_URL = "/trade/api/v2/futures/orders/open"
CLOSED_ORDERS_PATH_URL = "/trade/api/v2/futures/orders/closed"
POSITIONS_PATH_URL = "/trade/api/v2/futures/positions"
LEVERAGE_PATH_URL = "/trade/api/v2/futures/leverage"
ADD_MARGIN_PATH_URL = "/trade/api/v2/futures/add_margin"
WALLET_BALANCE_PATH_URL = "/trade/api/v2/futures/wallet_balance"
TRANSACTIONS_PATH_URL = "/trade/api/v2/futures/transactions"

CREATE_ORDER_LIMIT_ID = "FUTURES_CREATE_ORDER"
CANCEL_ORDER_LIMIT_ID = "FUTURES_CANCEL_ORDER"
ORDER_STATUS_LIMIT_ID = "FUTURES_ORDER_STATUS"
GET_LEVERAGE_LIMIT_ID = "FUTURES_GET_LEVERAGE"
SET_LEVERAGE_LIMIT_ID = "FUTURES_SET_LEVERAGE"

# ---- WebSocket (Socket.IO, public only) -----------------------------------------
# The futures sockets carry market data only: there is no order, fill, position
# or balance stream, so the private side is polled over REST.
ORDER_BOOK_EVENT = "FETCH_ORDER_BOOK_CS_PRO"
TRADES_EVENT = "FETCH_TRADES_CS_PRO"
TICKER_EVENT = "FETCH_TICKER_INFO_CS_PRO"
KLINES_EVENT = "FETCH_CANDLESTICK_CS_PRO"
SUBSCRIBE_EVENT = "subscribe"

# Socket.IO runs its own ping/pong. This watchdog only catches a socket that stays
# connected but stops delivering frames: with no frame at all for this long the
# connection is dropped and rebuilt.
WS_SILENCE_TIMEOUT = 120.0
WS_CONNECT_TIMEOUT = 15.0
WS_RECONNECT_DELAY = 5.0

# How long the REST-snapshot and REST-funding fallbacks wait for the first socket
# frame of a pair (used when the signed REST endpoints are unavailable, e.g. a
# connector without keys).
WS_FIRST_FRAME_TIMEOUT = 20.0

# ---- Order enums ----------------------------------------------------------------
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"

POSITION_LONG = "LONG"
POSITION_SHORT = "SHORT"

INSTRUMENT_STATUS_TRADING = "TRADING"
INSTRUMENT_TYPE_PERPETUAL = "PERPETUAL_FUTURES"

TRANSACTION_TYPE_FUNDING_FEE = "FUNDING_FEE"
# The transactions endpoint documents lowercase, spaced filter values ("funding
# fee") while rows carry FUNDING_FEE; rows are filtered locally as well.
TRANSACTION_FILTER_FUNDING_FEE = "funding fee"

# Order status values. PARTIALLY_EXECUTED is TERMINAL on futures (the unfilled
# remainder is no longer working), unlike spot; it is resolved in the connector,
# which maps it to CANCELED unless the order is still listed as open.
# CANCELLATION_RAISED is the first half of the two-phase cancel: fills can still
# land until it resolves to CANCELLED.
STATUS_RAISED = "RAISED"
STATUS_PARTIALLY_EXECUTED = "PARTIALLY_EXECUTED"
ORDER_STATE = {
    "RAISED": OrderState.OPEN,
    "EXECUTED": OrderState.FILLED,
    "PARTIALLY_EXECUTED": OrderState.CANCELED,
    "CANCELLED": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "CANCELLATION_RAISED": OrderState.PENDING_CANCEL,
    # Not documented for futures; spot uses them, so they are mapped defensively.
    "EXPIRATION_RAISED": OrderState.OPEN,
    "EXPIRED": OrderState.CANCELED,
    "DISCARDED": OrderState.FAILED,
    "REJECTED": OrderState.FAILED,
    "FAILED": OrderState.FAILED,
}

# Funding settles every 8 hours; the exact next time comes from the ticker.
DEFAULT_FUNDING_INTERVAL_HOURS = 8
FUNDING_FEE_POLL_INTERVAL = 120
FUNDING_TRANSACTIONS_LIMIT = 20

# ---- Private REST polling (there is no private socket) ---------------------------
# Budgets come from the per-endpoint limits below (20 calls / 60s for wallet,
# positions, open orders, closed orders and order status). The poll loops use
# about 12-15 of each 20 and leave the rest for the base class's 2-minute safety
# poll, order lookups and the user's own calls.
BALANCE_POLL_INTERVAL = 5.0
OPEN_ORDERS_POLL_INTERVAL = 4.0
# Positions need a symbol per call, so the loop interval grows with the pair count
# to keep the calls per minute at this budget.
POSITIONS_CALLS_PER_MINUTE = 12
MIN_POSITIONS_POLL_INTERVAL = 5.0
# Back-off ceiling for a poll loop that keeps failing.
MAX_POLL_BACKOFF_INTERVAL = 60.0
# A just-placed order can be missing from the open list for a moment; only orders
# older than this are looked up one by one when the batch lists miss them.
ORDER_LIST_GRACE_PERIOD = 3.0
# At most this many single-order status lookups per order poll (20/60s budget).
MAX_STATUS_LOOKUPS_PER_POLL = 3
# Open and closed order lists return at most 50 rows (the documented maximum).
ORDER_LIST_LIMIT = 50

# ---- Rate limits -------------------------------------------------------------------
# Per API key, per endpoint, as published on "Errors & Rate Limits". The
# throttler adds its own ~5% margin to each window.
ONE_MINUTE = 60

RATE_LIMITS = [
    # Undocumented for /time; kept modest because it is polled with the status loop.
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=30, time_interval=ONE_MINUTE),
    RateLimit(limit_id=INSTRUMENT_INFO_PATH_URL, limit=100, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=100, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TICKER_PATH_URL, limit=100, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ALL_PAIRS_TICKER_PATH_URL, limit=100, time_interval=ONE_MINUTE),
    RateLimit(limit_id=KLINES_PATH_URL, limit=30, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADES_PATH_URL, limit=100, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CREATE_ORDER_LIMIT_ID, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ORDER_LIMIT_ID, limit=10, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_STATUS_LIMIT_ID, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=OPEN_ORDERS_PATH_URL, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CLOSED_ORDERS_PATH_URL, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=POSITIONS_PATH_URL, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SET_LEVERAGE_LIMIT_ID, limit=10, time_interval=ONE_MINUTE),
    RateLimit(limit_id=GET_LEVERAGE_LIMIT_ID, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ADD_MARGIN_PATH_URL, limit=10, time_interval=ONE_MINUTE),
    RateLimit(limit_id=WALLET_BALANCE_PATH_URL, limit=20, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRANSACTIONS_PATH_URL, limit=20, time_interval=ONE_MINUTE),
]
