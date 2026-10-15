from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

# The REST and WebSocket hosts are the connector's (assigned per account at
# onboarding, read from the environment); see coindcx_perpetual_web_utils.
HEALTH_CHECK_ENDPOINT = "/fapi/v1/ping"
CANDLES_ENDPOINT = "/fapi/v1/klines"
WS_PATH = "/ws"

# The guide's stream table lists "1s, 1m, 5m, …" without the full set; these are
# Binance's futures intervals, which the API otherwise mirrors.
INTERVALS = bidict({
    "1s": 1,
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
    "1w": 604800,
    "1M": 2592000
})
MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1500

# Limits are unpublished; same conservative pool as the connector, with klines
# charged Binance's weight for a 1500-candle request.
REQUEST_WEIGHT = "REQUEST_WEIGHT"
RATE_LIMITS = [
    RateLimit(REQUEST_WEIGHT, limit=1200, time_interval=60),
    RateLimit(CANDLES_ENDPOINT, limit=1200, time_interval=60, linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 10)]),
    RateLimit(HEALTH_CHECK_ENDPOINT, limit=1200, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)])]
