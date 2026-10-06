from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://api.crypto.ajaib.co.id"

# Every Ajaib REST endpoint is signed -- klines included -- so the health check
# reuses the candles endpoint rather than an unauthenticated ping.
HEALTH_CHECK_ENDPOINT = "/v1/klines"
CANDLES_ENDPOINT = "/v1/klines"

# Same variables the Ajaib rate source reads (see ajaib_rate_source.py), so one
# set of entries in the api-server's .env serves both.
API_KEY_ENV_VAR = "AJAIB_API_KEY"
API_SECRET_ENV_VAR = "AJAIB_API_SECRET"
PROXY_URL_ENV_VAR = "AJAIB_PROXY_URL"

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
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
})

# Granularities /v1/klines actually serves (verified live). Any other value is
# NOT rejected -- the API silently answers with 1m candles -- so every other
# interval must be built by resampling one of these. "1w" is native too but its
# buckets open on Mondays, which does not line up with the epoch-aligned
# rounding CandlesBase uses, so it is not offered.
NATIVE_INTERVALS = ["1d", "4h", "1h", "15m", "1m"]

# limit above 1000 is clamped to 1000 by the server.
MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

POLL_INTERVAL = 5.0

# Retries for a request whose connection the proxy resets.
CONNECTION_RETRIES = 3

RATE_LIMITS = [
    RateLimit(limit_id="PUBLIC", limit=1200, time_interval=60),
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=1200,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("PUBLIC", 1)],
    ),
]
