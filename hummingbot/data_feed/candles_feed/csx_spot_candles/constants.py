from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://coinswitch.co"

HEALTH_CHECK_ENDPOINT = "/trade/api/v2/time"
CANDLES_ENDPOINT = "/trade/api/v2/candles"

EXCHANGE_ID = "coinswitchx"

API_KEY_ENV_VAR = "COINSWITCH_PRO_API_KEY"
API_SECRET_ENV_VAR = "COINSWITCH_PRO_API_SECRET"

INTERVALS = bidict({
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "1440",
    "3d": "4320",
    "1w": "10080",
})

NATIVE_INTERVALS = ["1d", "12h", "6h", "4h", "2h", "1h", "30m", "15m", "5m", "1m"]

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

POLL_INTERVAL = 5.0

RATE_LIMITS = [
    RateLimit(limit_id="GLOBAL", limit=60, time_interval=60),
    RateLimit(
        limit_id=CANDLES_ENDPOINT,
        limit=60,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("GLOBAL", 1)],
    ),
    RateLimit(
        limit_id=HEALTH_CHECK_ENDPOINT,
        limit=60,
        time_interval=60,
        linked_limits=[LinkedLimitWeightPair("GLOBAL", 1)],
    ),
]
