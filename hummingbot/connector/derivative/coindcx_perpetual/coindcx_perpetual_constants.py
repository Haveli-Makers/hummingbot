from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "coindcx_perpetual"
DEFAULT_DOMAIN = "coindcx_perpetual"

# CoinDCX serves private/instrument data from api.coindcx.com and public market data
# (order book, current prices) from public.coindcx.com.
REST_URL = "https://api.coindcx.com"
PUBLIC_REST_URL = "https://public.coindcx.com"
WSS_URL = "wss://stream.coindcx.com"

# Every futures instrument is prefixed with the "B" exchange code and quoted in USDT
# (verified: all 485 active instruments are B-<BASE>_USDT).
ECODE = "B"

# Contracts are USDT-quoted, but margin may be posted in USDT or INR — chosen per
# connector instance with ``coindcx_perpetual_margin_currency``.
DEFAULT_MARGIN_CURRENCY = "USDT"
SUPPORTED_MARGIN_CURRENCIES = ("USDT", "INR")

# Every contract is quoted in USDT, and Hummingbot sizes collateral in this
# currency. Balances are therefore reported in it, converting the margin wallet
# when it differs (see CONVERSION_MARKET).
QUOTE_CURRENCY = "USDT"

# Fees and every margin figure the API reports are denominated in USDT even for
# INR-margined futures ("fee_amount and ideal_margin values are in USDT for INR
# Futures"), so fee tokens are always USDT whatever the margin currency is.
FEE_CURRENCY = "USDT"

HBOT_ORDER_ID_PREFIX = "haveli-"
MAX_ORDER_ID_LEN = 36

# ---- Public / market data --------------------------------------------------
ACTIVE_INSTRUMENTS_PATH_URL = "/exchange/v1/derivatives/futures/data/active_instruments"
INSTRUMENT_PATH_URL = "/exchange/v1/derivatives/futures/data/instrument"

# Order book lives on the public host and REQUIRES the "-futures" suffix; without it
# the very same path silently returns the SPOT book.
ORDER_BOOK_PATH_URL = "/market_data/v3/orderbook"
ORDER_BOOK_SUFFIX = "-futures"
ORDER_BOOK_DEPTH = 20

# Funding rate + mark price + last price + 24h volume for every pair in one call.
CURRENT_PRICES_PATH_URL = "/market_data/v3/current_prices/futures/rt"

# ---- Private ---------------------------------------------------------------
LIST_ORDERS_PATH_URL = "/exchange/v1/derivatives/futures/orders"
CREATE_ORDER_PATH_URL = "/exchange/v1/derivatives/futures/orders/create"
CANCEL_ORDER_PATH_URL = "/exchange/v1/derivatives/futures/orders/cancel"
POSITIONS_PATH_URL = "/exchange/v1/derivatives/futures/positions"
UPDATE_LEVERAGE_PATH_URL = "/exchange/v1/derivatives/futures/positions/update_leverage"
TRADES_PATH_URL = "/exchange/v1/derivatives/futures/trades"
WALLETS_PATH_URL = "/exchange/v1/derivatives/futures/wallets"
# Ledger of position transactions; ``stage=funding`` yields funding payments.
TRANSACTIONS_PATH_URL = "/exchange/v1/derivatives/futures/positions/transactions"

# Public spot ticker, used only to price the margin wallet in the quote currency
# when margining in something other than USDT (e.g. INR).
SPOT_TICKER_PATH_URL = "/exchange/ticker"
# Spot market that prices one unit of the quote currency in the margin currency.
CONVERSION_MARKET = "{quote}{margin}"
CONVERSION_RATE_TTL = 60.0

# ``stage`` values accepted by the transactions endpoint.
TRANSACTION_STAGE_FUNDING = "funding"

# ---- WebSocket (Socket.IO) --------------------------------------------------
PRIVATE_CHANNEL = "coindcx"

WS_HEARTBEAT_TIME_INTERVAL = 25

# Public events
DEPTH_SNAPSHOT_EVENT_TYPE = "depth-snapshot"
TRADE_EVENT_TYPE = "new-trade"
CURRENT_PRICES_EVENT_TYPE = "currentPrices@futures#update"

# Private events (df- prefix = derivatives futures)
ORDER_UPDATE_EVENT_TYPE = "df-order-update"
POSITION_UPDATE_EVENT_TYPE = "df-position-update"
BALANCE_UPDATE_EVENT_TYPE = "balance-update"

# Channel name templates. ``{pair}`` is the CoinDCX pair, e.g. B-BTC_USDT.
ORDER_BOOK_CHANNEL = "{pair}@orderbook@{depth}-futures"
TRADES_CHANNEL = "{pair}@trades-futures"
CURRENT_PRICES_CHANNEL = "currentPrices@futures@rt"


# ---- Order enums ------------------------------------------------------------
SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit_order"
ORDER_TYPE_MARKET = "market_order"
# Marks an order as closing an existing position rather than opening a new one, so the
# venue releases margin instead of demanding it.
REDUCE_ONLY_FIELD = "reduce_only"

TIME_IN_FORCE_GTC = "good_till_cancel"

NO_NOTIFICATION = "no_notification"


# Funding is settled every ``funding_frequency`` hours (8 for most instruments).
DEFAULT_FUNDING_FREQUENCY_HOURS = 8
FUNDING_FEE_POLL_INTERVAL = 120

ONE_MINUTE = 60

ORDER_STATE = {
    "initial": OrderState.PENDING_CREATE,
    "init": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "untriggered": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "partially_cancelled": OrderState.CANCELED,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

# Statuses queried when listing orders. Must be a COMMA-SEPARATED STRING: sending
# the same values as a JSON array is rejected with HTTP 422 "Invalid Request".
ALL_ORDER_STATUSES = "open,filled,partially_filled,partially_cancelled,cancelled,rejected,untriggered"
# Statuses that still have quantity resting in the book, for get_open_orders.
OPEN_ORDER_STATUSES = "open,partially_filled"

# The list endpoints (orders, positions, transactions) are account-wide and paged;
# none of them accepts a pair filter, so callers page through until the record is
# found or the pages run out.
PAGE_SIZE = 100
MAX_PAGES = 20

# Instrument details are fetched one request per pair; cap the fan-out so a
# connector built without trading pairs (as TradingPairFetcher does) cannot fire
# hundreds of concurrent requests.
INSTRUMENT_FETCH_CONCURRENCY = 10

# Substrings that mark an error as "this order is already gone". HTTP 422 alone is
# a generic validation status on CoinDCX (malformed params, wrong margin currency,
# permissions), so it must be paired with one of these before an order is treated
# as already cancelled.
ORDER_GONE_MESSAGE_HINTS = (
    "order not found",
    "not found",
    "already cancel",
    "already filled",
    "already executed",
    "cannot be cancel",
    "invalid order",
)

# CoinDCX orders carry no client-order-id, so a websocket order event can only be
# matched once _place_order has recorded the exchange id. A fast fill can beat the
# REST create response, so unmatched events are held briefly and replayed instead
# of being dropped and left to the much slower REST poll.
PENDING_ORDER_EVENT_TTL = 15.0
PENDING_ORDER_EVENT_RETRY_INTERVAL = 0.2
MAX_PENDING_ORDER_EVENTS = 256
# How many recent order-frame signatures to remember for duplicate suppression. Only needs
# to outlive the window in which the venue repeats a frame, which is seconds.
MAX_APPLIED_ORDER_EVENTS = 512

RATE_LIMITS = [
    RateLimit(limit_id=ACTIVE_INSTRUMENTS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=INSTRUMENT_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_BOOK_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CURRENT_PRICES_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=LIST_ORDERS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=POSITIONS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=UPDATE_LEVERAGE_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRADES_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=WALLETS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=TRANSACTIONS_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SPOT_TICKER_PATH_URL, limit=2000, time_interval=ONE_MINUTE),
]

ORDER_NOT_EXIST_ERROR_CODE = 404
ORDER_NOT_EXIST_MESSAGE = "Order not found"
# CoinDCX answers 422 when cancelling an order that is already filled/cancelled.
INVALID_REQUEST_ERROR_CODE = 422
