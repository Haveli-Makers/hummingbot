from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://api.wazirx.com/sapi"

HEALTH_CHECK_ENDPOINT = "/v1/ping"
CANDLES_ENDPOINT = "/v1/klines"

INTERVALS = bidict({
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1d",
    "1w": "1w",
})

NATIVE_INTERVALS = ["1w", "1d", "12h", "6h", "4h", "2h", "1h", "30m", "15m", "5m", "1m"]

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 2000

POLL_INTERVAL = 5.0

RATE_LIMITS = [
    RateLimit(limit_id="raw_requests", limit=1, time_interval=1),
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=1,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair("raw_requests", 1)],
    ),
    RateLimit(
        limit_id=HEALTH_CHECK_ENDPOINT,
        limit=1,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair("raw_requests", 1)],
    ),
]
