from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "coindcx_perpetual"
DEFAULT_DOMAIN = "coindcx_perpetual"

# The FAPI (Futures API Market Maker Integration Guide v1.2) is served from REST and
# WebSocket hosts that CoinDCX assigns per account at onboarding; the public hosts
# (api.coindcx.com, public.coindcx.com) answer 404 under /fapi. The hosts are kept out
# of the repo and read from these environment variables whenever a URL is built.
REST_URL_ENV_VAR = "COINDCX_FAPI_REST_URL"
WSS_URL_ENV_VAR = "COINDCX_FAPI_WS_URL"

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

# ---- Public market data (FAPI) -----------------------------------------------
# Public REST takes its parameters as a query string and needs no credentials.
PING_PATH_URL = "/fapi/v1/ping"
SERVER_TIME_PATH_URL = "/fapi/v1/time"
EXCHANGE_INFO_PATH_URL = "/fapi/v1/exchangeInfo"
DEPTH_PATH_URL = "/fapi/v1/depth"
BOOK_TICKER_PATH_URL = "/fapi/v1/ticker/bookTicker"
TICKER_24HR_PATH_URL = "/fapi/v1/ticker/24hr"
# The latest-price endpoint is the only public one under /fapi/v2.
TICKER_PRICE_PATH_URL = "/fapi/v2/ticker/price"
# Mark price, index price, funding rate and next funding time in one call.
PREMIUM_INDEX_PATH_URL = "/fapi/v1/premiumIndex"

# Only these instruments are tradable perpetuals.
SYMBOL_STATUS_TRADING = "TRADING"
CONTRACT_TYPE_PERPETUAL = "PERPETUAL"

# Snapshot depth for the local order book. 500 is the documented default; the
# allowed values and their weights are not documented.
ORDER_BOOK_SNAPSHOT_LIMIT = 500

# ---- Market data WebSocket (FAPI) ---------------------------------------------
# One multiplexed connection; streams are added with SUBSCRIBE messages. The guide
# names the path but not the message format, so the Binance one is assumed.
PUBLIC_WS_PATH = "/ws"
SUBSCRIBE_METHOD = "SUBSCRIBE"
PUBLIC_WS_HEARTBEAT_INTERVAL = 30

# Stream suffixes, appended to the lowercase exchange symbol (btcusdt@depth).
DIFF_DEPTH_STREAM = "@depth"
AGG_TRADE_STREAM = "@aggTrade"
MARK_PRICE_STREAM = "@markPrice"

# Values of the ``e`` field that route a frame to its queue.
DIFF_EVENT_TYPE = "depthUpdate"
TRADE_EVENT_TYPE = "aggTrade"
MARK_PRICE_EVENT_TYPE = "markPriceUpdate"

# Local order book protocol (guide §9): diffs are chained by ``pu`` (the previous
# event's ``u``); a break means updates were missed and the book is rebuilt from a
# fresh snapshot. Re-snapshots for one pair are spaced at least this far apart so a
# stream that never chains cleanly cannot hammer the depth endpoint.
MIN_RESYNC_INTERVAL = 5.0
# Diffs held per pair while waiting for the snapshot they must line up with.
MAX_BUFFERED_DIFFS = 1000

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
# when margining in something other than USDT (e.g. INR). It is part of CoinDCX's
# public spot API, not the FAPI, so it keeps its own host.
SPOT_REST_URL = "https://api.coindcx.com"
SPOT_TICKER_PATH_URL = "/exchange/ticker"
# Spot market that prices one unit of the quote currency in the margin currency.
CONVERSION_MARKET = "{quote}{margin}"
CONVERSION_RATE_TTL = 60.0

# ``stage`` values accepted by the transactions endpoint.
TRANSACTION_STAGE_FUNDING = "funding"

# ---- WebSocket (Socket.IO) --------------------------------------------------
PRIVATE_CHANNEL = "coindcx"

WS_HEARTBEAT_TIME_INTERVAL = 25

# Private events (df- prefix = derivatives futures)
ORDER_UPDATE_EVENT_TYPE = "df-order-update"
POSITION_UPDATE_EVENT_TYPE = "df-position-update"
BALANCE_UPDATE_EVENT_TYPE = "balance-update"


# ---- Order enums ------------------------------------------------------------
SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_TYPE_LIMIT = "limit_order"
ORDER_TYPE_MARKET = "market_order"

TIME_IN_FORCE_GTC = "good_till_cancel"

NO_NOTIFICATION = "no_notification"


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

# The list endpoints (orders, positions, transactions) are account-wide and paged;
# none of them accepts a pair filter, so callers page through until the record is
# found or the pages run out.
PAGE_SIZE = 100
MAX_PAGES = 20

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

# The FAPI tiers are unpublished (exchangeInfo ``rateLimits`` is empty until they
# are), so public calls share one weight pool sized at half of Binance's 2400/min.
# Weights are the guide's where it states them (bookTicker and 24hr are charged at
# their all-symbols cost, which is how the oracles call them) and Binance's
# otherwise (exchangeInfo, depth at limit=500).
REQUEST_WEIGHT = "REQUEST_WEIGHT"
REQUEST_WEIGHT_LIMIT = 1200


def _weighted(path_url: str, weight: int) -> RateLimit:
    return RateLimit(limit_id=path_url, limit=REQUEST_WEIGHT_LIMIT, time_interval=ONE_MINUTE,
                     linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=weight)])


RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=REQUEST_WEIGHT_LIMIT, time_interval=ONE_MINUTE),
    _weighted(PING_PATH_URL, 1),
    _weighted(SERVER_TIME_PATH_URL, 1),
    _weighted(EXCHANGE_INFO_PATH_URL, 1),
    _weighted(DEPTH_PATH_URL, 10),
    _weighted(BOOK_TICKER_PATH_URL, 5),
    _weighted(TICKER_24HR_PATH_URL, 40),
    _weighted(TICKER_PRICE_PATH_URL, 1),
    _weighted(PREMIUM_INDEX_PATH_URL, 1),
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
