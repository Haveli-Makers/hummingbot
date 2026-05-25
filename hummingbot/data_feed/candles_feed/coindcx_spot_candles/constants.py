from bidict import bidict

REST_URL = "https://api.coindcx.com"
PUBLIC_REST_URL = "https://public.coindcx.com"

HEALTH_CHECK_ENDPOINT = "/exchange/v1/markets"
CANDLES_ENDPOINT = "/market_data/candles"
MARKETS_DETAILS_ENDPOINT = "/exchange/v1/markets_details"

WSS_URL = None  

INTERVALS = bidict({
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1M": "1M",
})

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

POLL_INTERVAL = 5.0
