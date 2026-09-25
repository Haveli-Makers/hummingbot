from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://api.coinex.com/v2"

HEALTH_CHECK_ENDPOINT = "/time"
CANDLES_ENDPOINT = "/futures/kline"

INTERVALS = bidict({
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "2h": "2hour",
    "4h": "4hour",
    "6h": "6hour",
    "12h": "12hour",
    "1d": "1day",
    "3d": "3day",
    "1w": "1week",
})

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

POLL_INTERVAL = 5.0

RATE_LIMITS = [
    RateLimit(limit_id="GLOBAL", limit=6000, time_interval=60),
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=6000,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("GLOBAL", 1)],
    ),
    RateLimit(
        limit_id=HEALTH_CHECK_ENDPOINT,
        limit=6000,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("GLOBAL", 1)],
    ),
]
