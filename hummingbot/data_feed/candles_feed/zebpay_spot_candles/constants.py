from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://sapi.zebpay.com"

HEALTH_CHECK_ENDPOINT = "/api/v2/system/status"
CANDLES_ENDPOINT = "/api/v2/market/klines"

INTERVALS = bidict({
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
})

NATIVE_INTERVALS = ["1d", "4h", "1h", "15m", "5m", "1m"]

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

POLL_INTERVAL = 5.0

RATE_LIMITS = [
    RateLimit(limit_id="PUBLIC", limit=1200, time_interval=60),
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=1200,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("PUBLIC", 1)],
    ),
    RateLimit(
        limit_id=HEALTH_CHECK_ENDPOINT,
        limit=1200,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("PUBLIC", 1)],
    ),
]
