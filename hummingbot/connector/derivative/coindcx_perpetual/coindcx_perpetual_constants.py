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

# Positions are margined per currency. Orders carry no margin field, so this only
# scopes the position-exit endpoints (``marginCurrency`` there defaults to USDT).
DEFAULT_MARGIN_CURRENCY = "USDT"
SUPPORTED_MARGIN_CURRENCIES = ("USDT", "INR")

# Estimated-fee token; actual fills report their own ``commissionAsset``.
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

# ---- Private REST (FAPI, SIGNED) ---------------------------------------------
# Every parameter goes in a signed JSON body, GET and DELETE included.
ORDER_PATH_URL = "/fapi/v1/order"  # POST create, PUT modify, DELETE cancel, GET query
OPEN_ORDERS_PATH_URL = "/fapi/v1/openOrders"
USER_TRADES_PATH_URL = "/fapi/v1/userTrades"
LEVERAGE_PATH_URL = "/fapi/v1/leverage"
EXIT_POSITION_PATH_URL = "/fapi/v1/exitPosition"
CLOSE_ALL_POSITIONS_PATH_URL = "/fapi/v1/closeAllPositions"
INCOME_PATH_URL = "/fapi/v1/income"
POSITION_RISK_PATH_URL = "/fapi/v2/positionRisk"
BALANCE_PATH_URL = "/fapi/v2/balance"
# The guide names a listenKey for the private stream but documents no endpoint to
# obtain one. Binance's (POST to create, PUT to keep alive) is assumed until CoinDCX
# says otherwise.
LISTEN_KEY_PATH_URL = "/fapi/v1/listenKey"
LISTEN_KEY_KEEP_ALIVE_INTERVAL = 30 * 60

# Throttler ids for the four methods that share /fapi/v1/order.
CREATE_ORDER_LIMIT_ID = "CreateOrder"
CANCEL_ORDER_LIMIT_ID = "CancelOrder"
MODIFY_ORDER_LIMIT_ID = "ModifyOrder"
QUERY_ORDER_LIMIT_ID = "QueryOrder"

# Page size for the paged private lists (openOrders, userTrades, income).
PAGE_LIMIT = 500
MAX_PAGES = 20

# ---- Private WebSocket (FAPI) ---------------------------------------------------
PRIVATE_WS_PATH = "/private/ws"
ORDER_TRADE_UPDATE_EVENT = "ORDER_TRADE_UPDATE"
ACCOUNT_UPDATE_EVENT = "ACCOUNT_UPDATE"
POSITION_UPDATE_EVENT = "POSITION_UPDATE"
# Binance's name for the frame announcing an expired listenKey.
LISTEN_KEY_EXPIRED_EVENT = "listenKeyExpired"
PRIVATE_EVENTS = (ORDER_TRADE_UPDATE_EVENT, ACCOUNT_UPDATE_EVENT, POSITION_UPDATE_EVENT)
PRIVATE_WS_HEARTBEAT_INTERVAL = 30

# ---- Orders ---------------------------------------------------------------------
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"
ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"
TIME_IN_FORCE_GTC = "GTC"
# Post-only: rejected rather than executed if it would match on arrival.
TIME_IN_FORCE_GTX = "GTX"

# Order statuses (guide §12) -> Hummingbot states. Creation, modification and
# cancellation are all asynchronous: the REST call only starts them.
ORDER_STATE = {
    "CREATE_IN_PROGRESS": OrderState.PENDING_CREATE,
    "PENDING_TRIGGER": OrderState.OPEN,
    "TRIGGERED": OrderState.OPEN,
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "EDIT_IN_PROGRESS": OrderState.PENDING_EDIT,
    "CANCEL_IN_PROGRESS": OrderState.PENDING_CANCEL,
    "CANCELED": OrderState.CANCELED,
    "EXPIRED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}
CANCEL_ACCEPTED_STATUSES = ("CANCEL_IN_PROGRESS", "CANCELED")

INCOME_TYPE_FUNDING_FEE = "FUNDING_FEE"
FUNDING_FEE_POLL_INTERVAL = 120

# ---- Error codes (guide §11) --------------------------------------------------------
UNKNOWN_ERROR_CODE = -1000
RATE_LIMIT_ERROR_CODE = -1003
TIMESTAMP_ERROR_CODE = -1021
CANCEL_REJECTED_ERROR_CODE = -2011  # "order already filled or unknown"
ORDER_NOT_FOUND_ERROR_CODE = -2013

ONE_MINUTE = 60
TEN_SECONDS = 10

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


# Order actions also count against an order-rate pool, sized like Binance's.
ORDERS_10S = "ORDERS_10S"
ORDERS_1M = "ORDERS_1M"
ORDERS_10S_LIMIT = 300
ORDERS_1M_LIMIT = 1200


def _order_action(limit_id: str) -> RateLimit:
    return RateLimit(limit_id=limit_id, limit=ORDERS_10S_LIMIT, time_interval=TEN_SECONDS,
                     linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1),
                                    LinkedLimitWeightPair(ORDERS_10S, weight=1),
                                    LinkedLimitWeightPair(ORDERS_1M, weight=1)])


RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=REQUEST_WEIGHT_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_10S, limit=ORDERS_10S_LIMIT, time_interval=TEN_SECONDS),
    RateLimit(limit_id=ORDERS_1M, limit=ORDERS_1M_LIMIT, time_interval=ONE_MINUTE),
    _weighted(PING_PATH_URL, 1),
    _weighted(SERVER_TIME_PATH_URL, 1),
    _weighted(EXCHANGE_INFO_PATH_URL, 1),
    _weighted(DEPTH_PATH_URL, 10),
    _weighted(BOOK_TICKER_PATH_URL, 5),
    _weighted(TICKER_24HR_PATH_URL, 40),
    _weighted(TICKER_PRICE_PATH_URL, 1),
    _weighted(PREMIUM_INDEX_PATH_URL, 1),
    _order_action(CREATE_ORDER_LIMIT_ID),
    _order_action(CANCEL_ORDER_LIMIT_ID),
    _order_action(MODIFY_ORDER_LIMIT_ID),
    _weighted(QUERY_ORDER_LIMIT_ID, 1),
    _weighted(OPEN_ORDERS_PATH_URL, 40),
    _weighted(USER_TRADES_PATH_URL, 5),
    _weighted(LEVERAGE_PATH_URL, 1),
    _weighted(EXIT_POSITION_PATH_URL, 1),
    _weighted(CLOSE_ALL_POSITIONS_PATH_URL, 1),
    _weighted(INCOME_PATH_URL, 30),
    _weighted(POSITION_RISK_PATH_URL, 5),
    _weighted(BALANCE_PATH_URL, 5),
    _weighted(LISTEN_KEY_PATH_URL, 1),
]
