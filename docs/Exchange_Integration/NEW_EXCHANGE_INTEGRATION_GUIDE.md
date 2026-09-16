# How to Integrate a New Exchange into Hummingbot

This guide walks you through adding a brand-new spot exchange connector to
hummingbot from scratch. It is written for developers who are new to the
codebase. Every concept is explained, every file is covered, and nothing is
assumed.

---

## Table of Contents

1. [Prerequisites and mental model](#1-prerequisites-and-mental-model)
2. [What to read from the exchange's documentation](#2-what-to-read-from-the-exchanges-documentation)
3. [Connector file structure — what every file does](#3-connector-file-structure--what-every-file-does)
4. [Step-by-step: building each file](#4-step-by-step-building-each-file)
   - 4.1 [constants](#41-constants-file--exchange_nameconspy)
   - 4.2 [auth](#42-auth-file--exchange_nameauthpy)
   - 4.3 [web\_utils](#43-web_utils-file--exchange_nameweb_utilspy)
   - 4.4 [utils](#44-utils-file--exchange_nameutilspy)
   - 4.5 [api\_order\_book\_data\_source](#45-api_order_book_data_source-file)
   - 4.6 [api\_user\_stream\_data\_source](#46-api_user_stream_data_source-file)
   - 4.7 [exchange](#47-exchange-file--the-main-class)
5. [Registering the connector](#5-registering-the-connector)
6. [Adding rate oracle and volume oracle support](#6-adding-rate-oracle-and-volume-oracle-support)
7. [Writing tests](#7-writing-tests)
8. [Live verification](#8-live-verification)
9. [Checklist before raising a PR](#9-checklist-before-raising-a-pr)
10. [Reference connectors in this repo](#10-reference-connectors-in-this-repo)

---

## 1. Prerequisites and mental model

### What you need to know before starting

- Python 3.9+ and `asyncio` basics
- How REST APIs work (HTTP verbs, headers, JSON bodies)
- What HMAC / Ed25519 / JWT signatures are (whichever your exchange uses)
- What an order book is (bids, asks, snapshots, diffs)

### What a connector actually does

A hummingbot connector is a translation layer between hummingbot's generic
trading logic and one specific exchange's API. It does four things:

```
1. Authentication      Turn API credentials into signed HTTP headers
2. Market data         Stream or poll the order book and recent trades
3. Account data        Stream or poll open orders and balances
4. Order management    Place, cancel, and track orders
```

Everything else (strategies, risk management, position sizing) is handled by
hummingbot's core and is completely independent of your connector.

### The class hierarchy

```
ConnectorBase
    └── ExchangeBase
            └── ExchangePyBase          ← your class inherits this
                    └── YourExchange
```

`ExchangePyBase` provides the entire framework. Your connector only needs to
implement the abstract methods it declares — typically 15–20 methods.

---

## 2. What to read from the exchange's documentation

Open the exchange's API documentation and find answers to every question in
this checklist **before writing a single line of code**. Take notes.

### 2.1  Authentication

- [ ] What signing algorithm is used?
      Common options: HMAC-SHA256, Ed25519, RSA, API-key-only, JWT
- [ ] What goes into the signature message?
      (timestamp? method? path? body? query params? specific order?)
- [ ] What timestamp format? Epoch **seconds** or **milliseconds**?
- [ ] What are the header names?
      Common patterns: `X-API-KEY`, `X-SIGNATURE`, `X-TIMESTAMP`, `API-KEY`, etc.
- [ ] Does the signature cover the request body for POST requests?
- [ ] Does the signature cover query parameters for GET requests?

### 2.2  Trading pairs / instruments

- [ ] What format are pairs in? `BTC/INR`? `BTCINR`? `BTC_INR`? `btc-inr`?
- [ ] Which endpoint lists all available pairs?
- [ ] Does the instruments endpoint also return precision info
      (min quantity, step size, tick size, min notional)?
- [ ] Are there separate endpoints for "active pairs" vs "precision/rules"?

### 2.3  Public market data

- [ ] **Ticker:** which endpoint returns 24h price data for all pairs?
      What are the field names for last price, bid, ask, volume?
- [ ] **Order book:** which endpoint returns the current depth (bids/asks)?
      What key names are bids/asks stored under? (`bids`/`asks`? `buy`/`sell`?)
- [ ] **Trades:** which endpoint returns recent trades?
      What fields hold price, quantity, timestamp, side?
- [ ] **WebSocket:** is there a WebSocket feed for live order book / trades?
      If yes: what is the connection URL? What message format is used to
      subscribe? What does an update message look like?
      If no: you will poll the REST endpoints periodically (see CSX connector
      as a reference for REST-only connectors).

### 2.4  Account data (private, auth required)

- [ ] **Balances:** which endpoint returns the account balance?
      How is available balance distinguished from locked/reserved balance?
- [ ] **Open orders:** which endpoint returns current open orders?
      Can you filter by status (OPEN, PARTIALLY_FILLED)?
- [ ] **Order status:** which endpoint returns the status of one specific order?
      Is the order looked up by exchange order ID or client order ID?
- [ ] **Trade fills:** does the order status response include fill details
      (filled quantity, average price, individual trade list)?
- [ ] **WebSocket user stream:** is there a private WebSocket feed for
      order updates and balance changes?
      If yes: how is it authenticated? What events does it emit?

### 2.5  Order management (private, auth required)

- [ ] **Place order:** what is the endpoint and HTTP method (usually POST)?
      What fields are required in the body? (side, type, instrument,
      quantity, price, client order ID?)
- [ ] **Cancel order:** is the order ID in the URL path or in the body?
      What HTTP method? (DELETE or POST?)
- [ ] **Order states:** what status strings does the exchange use?
      Map each to hummingbot's `OrderState` enum:

| Exchange status | hummingbot `OrderState` |
|---|---|
| (your exchange) | `OrderState.OPEN` |
| (your exchange) | `OrderState.PARTIALLY_FILLED` |
| (your exchange) | `OrderState.FILLED` |
| (your exchange) | `OrderState.CANCELED` |
| (your exchange) | `OrderState.FAILED` |
| (your exchange) | `OrderState.PENDING_CANCEL` |

### 2.6  Rate limits

- [ ] How many requests per minute / second are allowed?
- [ ] Are there separate limits for order placement vs read requests?
- [ ] Does the exchange return a `Retry-After` header when rate-limited?
- [ ] Are there per-IP limits vs per-API-key limits?

---

## 3. Connector file structure — what every file does

Create this directory and these files:

```
hummingbot/connector/exchange/<exchange_name>/
├── __init__.py                              ← empty, required for discovery
├── <exchange_name>_constants.py             ← all URLs, constants, rate limits
├── <exchange_name>_auth.py                  ← request signing
├── <exchange_name>_web_utils.py             ← URL builders, factory helpers
├── <exchange_name>_utils.py                 ← config map, fees, helpers
├── <exchange_name>_api_order_book_data_source.py  ← market data
├── <exchange_name>_api_user_stream_data_source.py ← account events
└── <exchange_name>_exchange.py              ← the main class (biggest file)
```

Replace `<exchange_name>` with the short lowercase identifier you choose
(e.g. `csx`, `coindcx`, `wazirx`).

> **Naming convention:** the identifier must match the directory name, all
> file prefixes, and the `connector` field in your `ConfigMap`. Keep it
> lowercase with underscores. It will be shown in the hummingbot CLI.

---

## 4. Step-by-step: building each file

### 4.1  Constants file — `<exchange_name>_constants.py`

**Purpose:** a single place for every URL, magic string, rate limit, and
order-state mapping. No business logic lives here.

**What to put in it:**

```python
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

# ── Client order ID ────────────────────────────────────────────────────────────
# The prefix is prepended to every order ID hummingbot generates.
# Keep it short. Some exchanges have a max length for client order IDs.
HBOT_ORDER_ID_PREFIX = "x-HB"
MAX_ORDER_ID_LEN = 36

# ── Base URLs ──────────────────────────────────────────────────────────────────
REST_URL = "https://api.your-exchange.com"   # from exchange docs

# ── Public REST endpoints ──────────────────────────────────────────────────────
# (no authentication required)
HEALTH_PATH_URL    = "/v1/ping"
INSTRUMENTS_PATH_URL = "/v1/symbols"
TICKER_PATH_URL    = "/v1/ticker"
DEPTH_PATH_URL     = "/v1/depth"
TRADES_PATH_URL    = "/v1/trades"

# ── Private REST endpoints ─────────────────────────────────────────────────────
# (authentication required)
CREATE_ORDER_PATH_URL = "/v1/order"
ORDER_BY_ID_PATH_URL  = "/v1/order"   # used as rate-limit ID; append /{id} at runtime
CANCEL_ORDER_PATH_URL = "/v1/order"   # same base path as above
ME_ORDERS_PATH_URL    = "/v1/orders"
BALANCE_PATH_URL      = "/v1/account/balance"

# ── WebSocket URLs (if applicable) ────────────────────────────────────────────
# WSS_URL = "wss://stream.your-exchange.com"

# ── Order sides ───────────────────────────────────────────────────────────────
SIDE_BUY  = "buy"    # or "BUY", "b" — use whatever the exchange expects
SIDE_SELL = "sell"

# ── Order types ───────────────────────────────────────────────────────────────
ORDER_TYPE_LIMIT  = "limit"
ORDER_TYPE_MARKET = "market"

# ── Order state mapping ───────────────────────────────────────────────────────
# Map every status string the exchange returns to hummingbot's OrderState.
# These are the EXACT strings the exchange API returns — check the docs.
ORDER_STATE = {
    "open":              OrderState.OPEN,
    "partially_filled":  OrderState.PARTIALLY_FILLED,
    "filled":            OrderState.FILLED,
    "cancelled":         OrderState.CANCELED,
    "rejected":          OrderState.FAILED,
}

# ── WebSocket heartbeat ───────────────────────────────────────────────────────
WS_HEARTBEAT_TIME_INTERVAL = 30.0

# ── Rate limits ───────────────────────────────────────────────────────────────
# Define bucket names (strings used as IDs throughout the rate limiter).
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS         = "ORDERS"
RAW_REQUESTS   = "RAW_REQUESTS"

ONE_MINUTE = 60
ONE_SECOND = 1
MAX_REQUEST = 1200   # adjust to your exchange's actual limits

RATE_LIMITS = [
    # ── Buckets (capacity pools) ───────────────────────────────────────────
    RateLimit(limit_id=REQUEST_WEIGHT, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS,         limit=100,  time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=RAW_REQUESTS,   limit=6100, time_interval=5 * ONE_MINUTE),

    # ── Per-endpoint limits (linked to buckets) ────────────────────────────
    # Each request to this endpoint also consumes from the linked buckets.
    RateLimit(
        limit_id=HEALTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                       LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(
        limit_id=INSTRUMENTS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                       LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(
        limit_id=DEPTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                       LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(
        limit_id=CREATE_ORDER_PATH_URL, limit=100, time_interval=10 * ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ORDERS, 1)]),
    RateLimit(
        limit_id=ORDER_BY_ID_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                       LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(
        limit_id=BALANCE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                       LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    # ... add one entry for every endpoint you call
]
```

> **Key concept — `linked_limits`:**
> An endpoint can consume from multiple rate-limit buckets simultaneously.
> For example, a heavy request might count as weight=10 in the `REQUEST_WEIGHT`
> bucket AND as 1 in `RAW_REQUESTS`. This mirrors how most exchanges describe
> their limits in their documentation.

---

### 4.2  Auth file — `<exchange_name>_auth.py`

**Purpose:** sign every outgoing request so the exchange accepts it.

Your auth class must subclass `AuthBase` and implement two methods:

```python
async def rest_authenticate(self, request: RESTRequest) -> RESTRequest
async def ws_authenticate(self, request: WSRequest) -> WSRequest
```

`rest_authenticate` is called automatically before every HTTP request.
`ws_authenticate` is called before WebSocket connections.

**HMAC-SHA256 example** (most common):

```python
import hashlib
import hmac
import time
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class YourExchangeAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = str(int(time.time() * 1000))  # milliseconds — check your exchange

        # Build the string to sign — FOLLOW YOUR EXCHANGE'S EXACT SPEC
        # Common patterns:
        #   method + timestamp + path
        #   timestamp + method + path + body
        #   timestamp + path + query_params + body
        message = f"{timestamp}{request.method.value.upper()}{self._path(request.url)}"

        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = request.headers or {}
        headers["X-API-KEY"]   = self.api_key
        headers["X-SIGNATURE"] = signature
        headers["X-TIMESTAMP"] = timestamp
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # Implement only if your exchange requires authenticated WebSocket
        # connections. Otherwise, just return the request unchanged.
        return request

    @staticmethod
    def _path(url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).path
```

**Ed25519 example** (used by CSX, CoinSwitch):

```python
from cryptography.hazmat.primitives.asymmetric import ed25519

class YourExchangeAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider):
        secret_bytes = bytes.fromhex(secret_key)
        self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
        self.api_key = api_key
        self._time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # Build and sign the message
        timestamp = str(int(self._time_provider.time()))  # seconds
        message = f"{timestamp}{request.method.value.upper()}{path}"
        signature = self._private_key.sign(message.encode()).hex()

        headers = request.headers or {}
        headers["X-ACCESS-KEY"]       = self.api_key
        headers["X-SIGNATURE"]        = signature
        headers["X-ACCESS-TIMESTAMP"] = timestamp
        request.headers = headers
        return request
```

> **Test your auth in isolation first.** Use `curl` or a simple `aiohttp`
> script to confirm the signed headers are accepted before integrating into
> the connector.

---

### 4.3  Web utils file — `<exchange_name>_web_utils.py`

**Purpose:** URL builders and the `WebAssistantsFactory` builder function.
Keeps URL construction in one place so constants changes don't cascade.

```python
from typing import Callable, Optional
import hummingbot.connector.exchange.<name>.<name>_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return f"{CONSTANTS.REST_URL}{path_url}"


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
        proxy_url: Optional[str] = None,   # include if exchange may need proxy
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()

    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=time_provider,
            ),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(
        throttler: AsyncThrottler,
) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def _build_connections_factory(proxy_url: Optional[str]):
    if proxy_url:
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import ProxyConnectionsFactory
        return ProxyConnectionsFactory(proxy_url=proxy_url)
    from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
    return ConnectionsFactory()


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Fetch server time and return it as a float (seconds since epoch).
    Only implement if your exchange has a server-time endpoint. Used by
    TimeSynchronizer to correct clock drift between your machine and exchange.
    """
    throttler = throttler or create_throttler()
    factory = build_api_factory_without_time_synchronizer_pre_processor(throttler)
    rest = await factory.get_rest_assistant()
    response = await rest.execute_request(
        url=public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    return float(response["serverTime"]) / 1000.0   # adjust field name and scale
```

> **`TimeSynchronizerRESTPreProcessor`:** this pre-processor adjusts the
> timestamp in every signed request to account for drift between your local
> clock and the exchange's clock. Always include it unless your exchange does
> not use timestamps in authentication.

---

### 4.4  Utils file — `<exchange_name>_utils.py`

**Purpose:** configuration map (prompts for CLI connect), fee defaults, and
small helper functions.

**This file is critical for auto-discovery.** hummingbot scans
`hummingbot/connector/exchange/*/` at startup and reads four module-level
names from `*_utils.py`:

| Name | Type | Purpose |
|------|------|---------|
| `CENTRALIZED` | `bool` | `True` for CEX (centralized exchange) |
| `EXAMPLE_PAIR` | `str` | Shown in help text, e.g. `"BTC-USDT"` |
| `DEFAULT_FEES` | `TradeFeeSchema` | Used when live fees are unavailable |
| `KEYS` | `XyzConfigMap.model_construct()` | Defines what `connect xyz` prompts for |

```python
from decimal import Decimal
from pydantic import ConfigDict, Field, SecretStr
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"    # a real, liquid pair on your exchange

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),   # 0.1% — check exchange fee schedule
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True,
)


class YourExchangeConfigMap(BaseConnectorConfigMap):
    connector: str = "your_exchange"   # must match directory name exactly

    your_exchange_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Exchange API key",
            "is_secure": True,
            "is_connect_key": True,   # required for the connection to be valid
            "prompt_on_new": True,
        },
    )
    your_exchange_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Exchange API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    # Add more fields if needed (e.g. subaccount, proxy_url — see proxy guide)

    model_config = ConfigDict(title="your_exchange")


KEYS = YourExchangeConfigMap.model_construct()
```

---

### 4.5  API Order Book Data Source file

**Purpose:** fetch and stream the order book and recent trades. This class
feeds hummingbot's `OrderBookTracker`.

There are two architectures — choose based on what the exchange offers:

#### Option A — WebSocket (preferred, real-time)

```python
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource

class YourExchangeAPIOrderBookDataSource(OrderBookTrackerDataSource):

    async def listen_for_subscriptions(self):
        """Connect to WebSocket, subscribe, and forward messages."""
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(wss_url=CONSTANTS.WSS_URL)
                await self._subscribe(ws)

                async for ws_response in ws.iter_messages():
                    message = ws_response.data
                    if "bids" in message or "asks" in message:
                        self._message_queue[self._snapshot_messages_queue_key].put_nowait(message)
                    elif message.get("type") == "trade":
                        self._message_queue[self._trade_messages_queue_key].put_nowait(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Order book stream error. Retrying...")
                await asyncio.sleep(5.0)

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
        # Convert raw dict → OrderBookMessage(SNAPSHOT)
        ...

    async def _parse_trade_message(self, raw_message, message_queue):
        # Convert raw dict → OrderBookMessage(TRADE)
        ...

    async def _connected_websocket_assistant(self):
        # Return a connected WSAssistant for the base class snapshot polling
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(...)
        return ws
```

#### Option B — REST polling (when no WebSocket is available)

```python
class YourExchangeAPIOrderBookDataSource(OrderBookTrackerDataSource):

    SNAPSHOT_POLL_INTERVAL = 30.0   # seconds between polls

    async def listen_for_subscriptions(self):
        """Poll REST endpoints periodically instead of streaming."""
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    raw = await self._connector._api_get(
                        path_url=CONSTANTS.DEPTH_PATH_URL,
                        params={"symbol": self._symbol(trading_pair)},
                        is_auth_required=False,
                    )
                    raw["_trading_pair"] = trading_pair
                    self._message_queue[self._snapshot_messages_queue_key].put_nowait(raw)

                await asyncio.sleep(self.SNAPSHOT_POLL_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Order book poll error. Retrying...")
                await asyncio.sleep(5.0)

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("This connector uses REST polling.")
```

**In both cases you must also implement:**

```python
async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
    trading_pair = raw_message.get("_trading_pair") or ...
    timestamp = ...
    message_queue.put_nowait(OrderBookMessage(
        message_type=OrderBookMessageType.SNAPSHOT,
        content={
            "trading_pair": trading_pair,
            "update_id": int(timestamp * 1000),
            "bids": raw_message.get("bids", []),   # [[price, qty], ...]
            "asks": raw_message.get("asks", []),
        },
        timestamp=timestamp,
    ))

async def _parse_trade_message(self, raw_message, message_queue):
    message_queue.put_nowait(OrderBookMessage(
        message_type=OrderBookMessageType.TRADE,
        content={
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.SELL.value) if is_buyer_maker else float(TradeType.BUY.value),
            "trade_id": str(trade_id),
            "update_id": str(trade_id),
            "price": str(price),
            "amount": str(qty),
        },
        timestamp=timestamp,
    ))

async def _parse_order_book_diff_message(self, raw_message, message_queue):
    # For exchanges that send incremental diffs (not full snapshots).
    # Use OrderBookMessageType.DIFF instead of SNAPSHOT.
    # If your exchange only sends full snapshots, just call
    # _parse_order_book_snapshot_message from here.
    ...
```

> **Bids and asks format:** hummingbot expects `[[price_str, qty_str], ...]`
> or `[[price_float, qty_float], ...]`. Convert whatever the exchange gives
> you into one of these.

---

### 4.6  API User Stream Data Source file

**Purpose:** receive real-time updates about your account — order fills,
cancellations, balance changes. These events let hummingbot update its
internal state immediately instead of waiting for the next polling cycle.

#### Option A — WebSocket user stream

```python
class YourExchangeAPIUserStreamDataSource(UserStreamTrackerDataSource):

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(wss_url=CONSTANTS.WSS_USER_URL)
                # Authenticate the WebSocket connection
                await self._authenticate(ws)
                # Subscribe to account events
                await ws.send(WSRequest({"method": "SUBSCRIBE", "params": ["orders", "balance"]}))

                async for ws_response in ws.iter_messages():
                    event = ws_response.data
                    event_type = event.get("type") or event.get("e")
                    if event_type in ("orderUpdate", "order"):
                        event["event"] = "order_update"
                        await output.put(event)
                    elif event_type in ("balanceUpdate", "balance"):
                        event["event"] = "balance_update"
                        await output.put(event)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("User stream error. Retrying...")
                await asyncio.sleep(5.0)
```

#### Option B — REST polling (when no private WebSocket is available)

```python
class YourExchangeAPIUserStreamDataSource(UserStreamTrackerDataSource):

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        while True:
            try:
                # Poll balance
                balance = await self._connector._api_get(
                    path_url=CONSTANTS.BALANCE_PATH_URL,
                    is_auth_required=True,
                )
                await output.put({"event": "balance_update", "data": balance})

                # Poll open orders
                orders = await self._connector._api_get(
                    path_url=CONSTANTS.ME_ORDERS_PATH_URL,
                    params={"status": "open"},
                    is_auth_required=True,
                )
                if orders:
                    await output.put({"event": "order_update", "data": orders})

                await asyncio.sleep(5.0)   # poll every 5 seconds

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("User stream poll error. Retrying...")
                await asyncio.sleep(5.0)

    async def _subscribe_to_user_stream(self): pass
    async def _unsubscribe_from_user_stream(self): pass
```

---

### 4.7  Exchange file — the main class

**Purpose:** the core connector. Implements all order management, balance
updates, trading rules, and event processing.

This is the largest file (~400–900 lines). Here are all the methods you must
implement, with what each one does:

#### Constructor

```python
class YourExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0   # seconds between status polls
    web_utils = web_utils                      # module reference (not instance)

    def __init__(
        self,
        your_exchange_api_key: str,
        your_exchange_api_secret: str,
        your_exchange_proxy_url: str = "",
        balance_asset_limit=None,
        rate_limits_share_pct=Decimal("100"),
        trading_pairs=None,
        trading_required=True,
        domain=CONSTANTS.DEFAULT_DOMAIN,
    ):
        # Store connector-specific params BEFORE super().__init__()
        # because super().__init__() calls _create_web_assistants_factory()
        # which needs these values to be set already.
        self.api_key = your_exchange_api_key
        self.secret_key = your_exchange_api_secret
        self._proxy_url = your_exchange_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)
```

> **Important:** always set your own attributes (`self.api_key`, etc.)
> **before** calling `super().__init__()`. The parent `__init__` immediately
> calls `self.authenticator`, `_create_web_assistants_factory()`, and
> `_create_order_book_data_source()` — all of which need your attributes to
> already exist.

#### Required properties

```python
@property
def authenticator(self):
    # Return a new auth instance. Called once in super().__init__().
    return YourExchangeAuth(self.api_key, self.secret_key, self._time_synchronizer)

@property
def name(self) -> str:
    return "your_exchange"           # must match directory name

@property
def rate_limits_rules(self):
    return CONSTANTS.RATE_LIMITS

@property
def domain(self): return self._domain

@property
def client_order_id_max_length(self): return CONSTANTS.MAX_ORDER_ID_LEN
@property
def client_order_id_prefix(self): return CONSTANTS.HBOT_ORDER_ID_PREFIX

@property
def trading_rules_request_path(self): return CONSTANTS.INSTRUMENTS_PATH_URL
@property
def trading_pairs_request_path(self): return CONSTANTS.INSTRUMENTS_PATH_URL
@property
def check_network_request_path(self): return CONSTANTS.HEALTH_PATH_URL

@property
def trading_pairs(self): return self._trading_pairs
@property
def is_cancel_request_in_exchange_synchronous(self): return True
@property
def is_trading_required(self): return self._trading_required

def supported_order_types(self):
    return [OrderType.LIMIT, OrderType.LIMIT_MAKER]
```

#### Factory methods

```python
def _create_web_assistants_factory(self):
    return web_utils.build_api_factory(
        throttler=self._throttler,
        time_synchronizer=self._time_synchronizer,
        domain=self._domain,
        auth=self._auth,
        proxy_url=self._proxy_url or None,
    )

def _create_order_book_data_source(self):
    return YourExchangeAPIOrderBookDataSource(
        trading_pairs=self._trading_pairs,
        connector=self,
        api_factory=self._web_assistants_factory,
        domain=self._domain,
    )

def _create_user_stream_data_source(self):
    return YourExchangeAPIUserStreamDataSource(
        auth=self._auth,
        trading_pairs=self._trading_pairs,
        connector=self,
        api_factory=self._web_assistants_factory,
        domain=self._domain,
    )
```

#### Trading pair initialisation

```python
def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info):
    """
    Build the bidict that maps exchange symbol ↔ hummingbot pair.
    exchange_info is the raw API response from trading_pairs_request_path.

    Example:
      exchange symbol "BTC/INR" → hummingbot pair "BTC-INR"
      exchange symbol "BTCUSDT" → hummingbot pair "BTC-USDT"
    """
    from bidict import bidict
    mapping = bidict()

    instruments = exchange_info  # adjust if wrapped in {"data": [...]}
    if isinstance(exchange_info, dict):
        instruments = exchange_info.get("data", exchange_info.get("symbols", []))

    for item in instruments:
        try:
            symbol = item if isinstance(item, str) else item.get("symbol", "")
            # Normalise: split into base and quote
            if "/" in symbol:
                base, quote = symbol.split("/", 1)
            elif len(symbol) > 3:
                # For concatenated symbols like "BTCUSDT", you may need
                # a lookup table or the instruments endpoint to split them.
                base = item.get("baseAsset", "")
                quote = item.get("quoteAsset", "")
            else:
                continue

            hb_pair = combine_to_hb_trading_pair(base.upper(), quote.upper())
            mapping[symbol] = hb_pair
        except Exception:
            pass

    self._set_trading_pair_symbol_map(mapping)
```

#### Trading rules

```python
async def _format_trading_rules(self, exchange_info) -> List[TradingRule]:
    """
    Parse instrument/precision data into TradingRule objects.
    TradingRule tells hummingbot the minimum order size, price increment, etc.
    """
    rules = []
    instruments = ...  # parse from exchange_info

    for inst in instruments:
        try:
            trading_pair = f"{inst['baseAsset']}-{inst['quoteAsset']}"
            rules.append(TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal(str(inst.get("minQty", "0.0001"))),
                max_order_size=Decimal(str(inst.get("maxQty", "999999"))),
                min_price_increment=Decimal(str(inst.get("tickSize", "0.01"))),
                min_base_amount_increment=Decimal(str(inst.get("stepSize", "0.0001"))),
                min_notional_size=Decimal(str(inst.get("minNotional", "1"))),
            ))
        except Exception as e:
            self.logger().debug(f"Error parsing rule for {inst}: {e}")

    return rules
```

#### Order placement and cancellation

```python
async def _place_order(
    self, order_id, trading_pair, amount, trade_type, order_type, price, **kwargs
) -> Tuple[str, float]:
    symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
    payload = {
        "symbol":    symbol,
        "side":      CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
        "type":      CONSTANTS.ORDER_TYPE_LIMIT,
        "price":     str(price),
        "quantity":  str(amount),
        "clientOrderId": order_id,
    }
    result = await self._api_post(
        path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
        data=payload,
        is_auth_required=True,
    )
    exchange_order_id = str(result["orderId"])
    timestamp = float(result.get("createdAt", 0))   # seconds or ms — normalise
    return exchange_order_id, timestamp


async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
    result = await self._api_delete(
        path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{tracked_order.exchange_order_id}",
        limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
        is_auth_required=True,
    )
    return (result.get("status") or "").upper() in ("CANCELLED", "CANCELED", "SUCCESS")
```

#### Order status and trade updates

```python
async def _request_order_status(self, tracked_order) -> OrderUpdate:
    result = await self._api_get(
        path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{tracked_order.exchange_order_id}",
        limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
        is_auth_required=True,
    )
    status_str = (result.get("status") or "").lower()
    new_state = CONSTANTS.ORDER_STATE.get(status_str)
    if new_state is None:
        raise ValueError(f"Unknown status: {status_str}")

    return OrderUpdate(
        client_order_id=tracked_order.client_order_id,
        exchange_order_id=str(tracked_order.exchange_order_id),
        trading_pair=tracked_order.trading_pair,
        update_timestamp=float(result.get("updatedAt", 0)),
        new_state=new_state,
    )


async def _all_trade_updates_for_order(self, order) -> List[TradeUpdate]:
    # Fetch fill details for a tracked order.
    # If the exchange returns individual trades, create one TradeUpdate per trade.
    # If it only returns aggregate fill data, create one TradeUpdate total.
    ...
```

#### Balance updates

```python
async def _update_balances(self) -> None:
    response = await self._api_get(
        path_url=CONSTANTS.BALANCE_PATH_URL,
        is_auth_required=True,
    )
    # response format varies — adapt to your exchange
    # Typical pattern: list of {"asset": "BTC", "free": "1.0", "locked": "0.1"}
    local_assets = set(self._account_balances.keys())
    remote_assets = set()

    for item in response.get("balances", []):
        asset = item["asset"].upper()
        free   = Decimal(str(item.get("free", 0)))
        locked = Decimal(str(item.get("locked", 0)))
        self._account_balances[asset] = free + locked
        self._account_available_balances[asset] = free
        remote_assets.add(asset)

    for stale in local_assets - remote_assets:
        del self._account_balances[stale]
        del self._account_available_balances[stale]
```

#### User stream event listener

```python
async def _user_stream_event_listener(self):
    """
    Consume events from the user stream queue and update internal state.
    This runs forever in a background task.
    """
    async for event in self._iter_user_event_queue():
        try:
            event_type = event.get("event")

            if event_type == "balance_update":
                # Update balances immediately from the streamed data
                ...

            elif event_type == "order_update":
                for order_data in event.get("data", []):
                    client_id = order_data.get("clientOrderId")
                    tracked = self._order_tracker.all_updatable_orders.get(client_id)
                    if tracked is None:
                        continue
                    status_str = (order_data.get("status") or "").lower()
                    new_state = CONSTANTS.ORDER_STATE.get(status_str)
                    if new_state is None:
                        continue
                    self._order_tracker.process_order_update(OrderUpdate(
                        trading_pair=tracked.trading_pair,
                        update_timestamp=float(order_data.get("updatedAt", 0)),
                        new_state=new_state,
                        client_order_id=client_id,
                        exchange_order_id=str(order_data.get("orderId", "")),
                    ))

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error in user stream listener.", exc_info=True)
            await self._sleep(5.0)
```

#### Fee calculation

```python
def _get_fee(self, base_currency, quote_currency, order_type, order_side,
             amount, price=s_decimal_NaN, is_maker=None) -> TradeFeeBase:
    is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
    return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))
```

#### Exception classification

```python
def _is_request_exception_related_to_time_synchronizer(self, exc):
    return "timestamp" in str(exc).lower()

def _is_order_not_found_during_status_update_error(self, exc):
    return "not found" in str(exc).lower() or "404" in str(exc)

def _is_order_not_found_during_cancelation_error(self, exc):
    return "not found" in str(exc).lower() or "404" in str(exc)
```

---

## 5. Registering the connector

### 5.1  Auto-discovery (automatic — no code needed)

hummingbot scans `hummingbot/connector/exchange/*/` at startup. Your connector
is automatically discovered if:

- `hummingbot/connector/exchange/<name>/__init__.py` exists (even empty)
- `hummingbot/connector/exchange/<name>/<name>_utils.py` exists and exports
  `CENTRALIZED`, `EXAMPLE_PAIR`, `DEFAULT_FEES`, and `KEYS`

**If you forget `__init__.py`, the connector will not appear in `connect`.**

### 5.2  Rate oracle registration

Add your connector as a rate oracle source so other strategies can fetch
prices from it.

**New file:** `hummingbot/core/rate_oracle/sources/<name>_rate_source.py`

```python
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

class YourExchangeRateSource(RateSourceBase):
    @property
    def name(self): return "your_exchange"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token=None):
        self._ensure_exchange()
        tickers = await self._exchange.get_all_pairs_prices()
        return self._extract_prices(tickers, quote_token)

    def _build_exchange(self):
        from hummingbot.connector.exchange.your_exchange.your_exchange_exchange import YourExchange
        return YourExchange(api_key="", api_secret="", trading_required=False)
```

**Edit:** `hummingbot/core/rate_oracle/rate_oracle.py` — add one line:

```python
from hummingbot.core.rate_oracle.sources.your_exchange_rate_source import YourExchangeRateSource
# ...
RATE_ORACLE_SOURCES = {
    # ... existing entries ...
    "your_exchange": YourExchangeRateSource,   # ← add this
}
```

### 5.3  Volume oracle registration

Same pattern as rate oracle.

**New file:** `hummingbot/core/volume_oracle/sources/<name>_volume_source.py`

**Edit:** `hummingbot/core/volume_oracle/volume_oracle.py` — add one import
and one entry in `VOLUME_ORACLE_SOURCES`.

---

## 6. Adding rate oracle and volume oracle support

In your main exchange class, add these two helper methods:

```python
async def get_all_pairs_prices(self) -> List[Dict]:
    """Called by the rate oracle source to fetch current prices."""
    response = await self._api_get(
        path_url=CONSTANTS.TICKER_PATH_URL,
        is_auth_required=False,
    )
    return response if isinstance(response, list) else response.get("data", [])


async def get_all_24h_volume_tickers(self, trading_pairs=None) -> List[Dict]:
    """Called by the volume oracle source to fetch 24h volumes."""
    tickers = await self.get_all_pairs_prices()
    if not trading_pairs:
        return tickers
    requested = {tp.replace("-", "/").upper() for tp in trading_pairs}
    return [t for t in tickers if (t.get("symbol") or "").upper() in requested]
```

---

## 7. Writing tests

Tests live in:
```
test/hummingbot/connector/exchange/<name>/
├── __init__.py
├── test_<name>_auth.py
├── test_<name>_constants.py
├── test_<name>_web_utils.py
├── test_<name>_utils.py
├── test_<name>_api_order_book_data_source.py
├── test_<name>_user_stream_data_source.py
└── test_<name>_exchange.py
```

**All unit tests mock the network.** You never need a live exchange to run
the test suite.

### 7.1  Test file conventions

```python
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from bidict import bidict
from hummingbot.connector.exchange.<name>.<name>_exchange import YourExchange

_VALID_SECRET = "aa" * 32   # 32 bytes = valid Ed25519 key for testing

def _make_exchange(**kwargs) -> YourExchange:
    """Test factory. Override any field with kwargs."""
    defaults = dict(
        your_exchange_api_key="test_key",
        your_exchange_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-USDT"],
        trading_required=False,
    )
    defaults.update(kwargs)
    return YourExchange(**defaults)
```

### 7.2  What to test in each file

| File | What to test |
|------|-------------|
| `test_<name>_auth.py` | Signature is correct; headers are set; empty creds bypass signing |
| `test_<name>_constants.py` | ORDER_STATE has all expected keys; rate limits list non-empty |
| `test_<name>_web_utils.py` | URLs are built correctly; throttler is created |
| `test_<name>_utils.py` | Config map fields exist; fee defaults are valid; helper functions |
| `test_<name>_exchange.py` | Properties return expected values; `_place_order` sends right payload; `_update_balances` parses response correctly; order state transitions work |
| `test_<name>_api_order_book_data_source.py` | Snapshot message is built correctly; trade message is built correctly |
| `test_<name>_user_stream_data_source.py` | Events are put into the output queue; poll interval is respected |

### 7.3  Mocking API calls

Use `unittest.mock.AsyncMock` to mock `_api_get`, `_api_post`, `_api_delete`:

```python
async def test_update_balances(self):
    balance_response = {
        "balances": [
            {"asset": "BTC", "free": "1.5", "locked": "0.5"},
            {"asset": "USDT", "free": "5000.0", "locked": "0.0"},
        ]
    }
    with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = balance_response
        await self.exchange._update_balances()

    self.assertEqual(Decimal("2.0"), self.exchange._account_balances["BTC"])
    self.assertEqual(Decimal("1.5"), self.exchange._account_available_balances["BTC"])
```

### 7.4  Running the tests

```bash
# Run all tests for your connector
python -m pytest test/hummingbot/connector/exchange/<name>/ -v

# Run a specific test file
python -m pytest test/hummingbot/connector/exchange/<name>/test_<name>_exchange.py -v

# Run with coverage
python -m pytest test/hummingbot/connector/exchange/<name>/ --cov=hummingbot/connector/exchange/<name> -v
```

---

## 8. Live verification

Once unit tests pass, verify against the real exchange.

### 8.1  Create a standalone verification script

Place it at `scripts/verify_<name>_connection.py`. Follow the pattern in
`scripts/verify_csx_connection.py`.

The script should:
1. Accept credentials from environment variables
2. Test public endpoints (health, instruments, ticker, order book)
3. Test private endpoints (balance, open orders)
4. Print a clear pass/fail summary

```bash
YOUR_EXCHANGE_API_KEY="..." \
YOUR_EXCHANGE_API_SECRET="..." \
python scripts/verify_<name>_connection.py
```

### 8.2  Sanity-check against the exchange UI

After running the script:

- [ ] Price shown in script matches what you see in the exchange UI
- [ ] Balance shown in script matches your actual account balance
- [ ] Place a small test order via hummingbot; confirm it appears in the UI
- [ ] Cancel the order via hummingbot; confirm it disappears in the UI
- [ ] Fill an order (manually match it in the UI); confirm hummingbot sees it as `FILLED`

---

## 9. Checklist before raising a PR

### Code completeness

- [ ] All 8 connector files created
- [ ] `__init__.py` exists (empty is fine)
- [ ] `_utils.py` exports `CENTRALIZED`, `EXAMPLE_PAIR`, `DEFAULT_FEES`, `KEYS`
- [ ] Rate oracle source created and registered in `rate_oracle.py`
- [ ] Volume oracle source created and registered in `volume_oracle.py`
- [ ] Connector appears in `connect` command output when hummingbot starts

### Order flow

- [ ] `_place_order` creates an order and returns `(exchange_order_id, timestamp)`
- [ ] `_place_cancel` cancels an order and returns `True`
- [ ] `_request_order_status` returns correct `OrderState` for all statuses
- [ ] `_all_trade_updates_for_order` returns fill data for filled orders
- [ ] `_update_balances` correctly sets `_account_balances` and `_account_available_balances`

### Error handling

- [ ] Unknown order status raises `ValueError` (not silently ignored)
- [ ] Order-not-found errors are classified by `_is_order_not_found_*` methods
- [ ] Time synchronizer errors are classified by `_is_request_exception_related_to_time_synchronizer`

### Tests

- [ ] All 7 test files present
- [ ] `python -m pytest test/hummingbot/connector/exchange/<name>/ -v` passes
- [ ] No test relies on real API calls (all mocked)

### Live verification

- [ ] `scripts/verify_<name>_connection.py` exists and all checks pass
- [ ] Balance matches exchange UI
- [ ] Test order placed and cancelled successfully

### Documentation

- [ ] Inline docstrings on non-obvious methods
- [ ] PR description includes: exchange URL, auth method, any special notes

---

## 10. Reference connectors in this repo

When you're stuck, read these connectors. They are fully implemented and
tested.

| Connector | Best reference for |
|---|---|
| `coinswitch` | Ed25519 auth, Socket.IO WebSocket, complex order state mapping |
| `csx` | REST-only market data (no WebSocket), Ed25519 auth, proxy support |
| `coindcx` | Clean minimal implementation, good test coverage |
| `wazirx` | Indian exchange, HMAC auth, straightforward structure |
| `binance` | Most complete reference; complex but battle-tested |
| `kucoin` | WebSocket user stream with token refresh |

**Always use the most similar exchange as your starting template** — copy the
file, rename everything, then adjust the API-specific parts. Do not start
from a blank file.

---

*For proxy server support (exchanges that require IP whitelisting),*
*see [PROXY_SERVER_GUIDE.md](./PROXY_SERVER_GUIDE.md).*
