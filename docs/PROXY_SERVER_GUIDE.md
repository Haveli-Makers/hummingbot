# Proxy Server Support for IP-Whitelisted Exchanges

Some exchanges (e.g. CoinSwitch Kuber / CSX) only accept API requests from a
pre-registered set of IP addresses, and some API keys (e.g. on CoinDCX) can be
bound to an IP. If your hummingbot instance runs on a machine whose IP is not
whitelisted, every API call will be rejected with an authentication or
IP-restriction error.

Connectors with proxy support today: `csx`, `ajaib` and `coindcx`, plus
`coindcx_perpetual` for REST only (its Socket.IO streams still connect
directly, see 3.4).

**The solution:** route the connector's traffic through a proxy server that
has a whitelisted IP.

This guide explains how the proxy infrastructure works and exactly what to
add to any connector to enable it.

---

## Table of Contents

1. [How it works — architecture overview](#1-how-it-works--architecture-overview)
2. [The three files you must edit](#2-the-three-files-you-must-edit)
3. [Complete code for each file](#3-complete-code-for-each-file)
   (3.4 covers streams that open their own connections, such as Socket.IO)
4. [Supported proxy types](#4-supported-proxy-types)
5. [Runtime configuration](#5-runtime-configuration)
6. [Testing your proxy integration](#6-testing-your-proxy-integration)
7. [Troubleshooting](#7-troubleshooting)
8. [Reference: CSX as a worked example](#8-reference-csx-as-a-worked-example)

---

## 1. How it works — architecture overview

hummingbot's HTTP/WebSocket stack is layered as follows:

```
Exchange connector
    └── WebAssistantsFactory
            └── ConnectionsFactory  ← this is where the proxy plugs in
                    └── aiohttp.ClientSession
```

By default, `ConnectionsFactory` creates a plain `aiohttp.ClientSession` with
no proxy. The key insight is that `WebAssistantsFactory` already accepts a
`connections_factory` parameter — you can substitute a
`ProxyConnectionsFactory` that creates a session pre-wired to a proxy.

```
Without proxy (default)                With proxy
────────────────────────────           ──────────────────────────────────────
ConnectionsFactory()                   ProxyConnectionsFactory("socks5://...")
  └── aiohttp.ClientSession()            └── ProxyConnector.from_url(...)
        (uses your machine's IP)               └── aiohttp.ClientSession(
                                                       connector=ProxyConnector
                                                   )
                                               (all traffic exits via proxy IP)
```

**What does NOT change:**
- `RESTConnection` and `WSConnection` — untouched
- `ConnectionsFactory` (the default singleton) — untouched
- Every other connector — untouched

**What changes per connector:** three small additions totalling ~15 lines,
plus the stream files if a stream opens its own connections instead of using
the factory (see [3.4](#34--streams-that-open-their-own-connections-socketio)).

### The shared building block

```
hummingbot/core/web_assistant/connections/proxy_connections_factory.py
```

This file is written once and shared by every connector that needs a proxy.
You never need to modify it.

---

## 2. The three files you must edit

For a connector named `xyz`, add to these three files:

| File | What to add |
|------|-------------|
| `hummingbot/connector/exchange/xyz/xyz_utils.py` | `xyz_proxy_url` field in `XyzConfigMap` |
| `hummingbot/connector/exchange/xyz/xyz_exchange.py` | `xyz_proxy_url` param in `__init__`, passed to factory |
| `hummingbot/connector/exchange/xyz/xyz_web_utils.py` | `proxy_url` param in `build_api_factory`, `_build_connections_factory` helper |
| `xyz_api_order_book_data_source.py`, `xyz_api_user_stream_data_source.py` | Only if a stream uses its own client (e.g. python-socketio) instead of `get_ws_assistant()`: see 3.4 |

To find out whether 3.4 applies, search the connector for `socketio`,
`aiohttp.ClientSession(` or `websockets.connect`. Anything that opens a
connection without the `WebAssistantsFactory` bypasses the proxy.

---

## 3. Complete code for each file

### 3.1  `xyz_utils.py` — add one field to the config map

```python
from pydantic import ConfigDict, Field, SecretStr
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from decimal import Decimal

# ... your existing constants ...

class XyzConfigMap(BaseConnectorConfigMap):
    connector: str = "xyz"

    xyz_api_key: SecretStr = Field(...)          # already exists
    xyz_api_secret: SecretStr = Field(...)        # already exists

    # ── ADD THIS BLOCK ────────────────────────────────────────────────────────
    xyz_proxy_url: SecretStr = Field(
        default=SecretStr(""),                    # empty = no proxy (direct connection)
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route XYZ traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,       # stored encrypted in conf/connectors/xyz.yml
            "is_connect_key": True,  # MUST be True, see below
            "prompt_on_new": True,   # user is asked when running `connect xyz`
        },
    )
    # ─────────────────────────────────────────────────────────────────────────

    model_config = ConfigDict(title="xyz")

KEYS = XyzConfigMap.model_construct()
```

> **Why `is_connect_key: True`?**
> Only `is_connect_key: True` fields are passed to the connector's constructor
> (`Security.api_keys()` → `api_keys_from_connector_config_map()`). With
> `False`, `connect xyz` still asks for the proxy URL and saves it, but the
> connector never receives it, so all traffic silently goes direct.
> The field stays optional anyway: its default is `""`, and a blank value
> means "connect directly".

> **Why `SecretStr`?**
> The proxy URL contains credentials (username, password). Storing it as
> `SecretStr` means hummingbot encrypts it in `conf/connectors/xyz.yml`
> alongside the API keys.

---

### 3.2  `xyz_exchange.py` — accept and forward the URL

```python
class XyzExchange(ExchangePyBase):

    def __init__(
        self,
        xyz_api_key: str,
        xyz_api_secret: str,
        # ── ADD THIS PARAMETER ────────────────────────────────────────────────
        xyz_proxy_url: str = "",
        # ─────────────────────────────────────────────────────────────────────
        balance_asset_limit=None,
        rate_limits_share_pct=Decimal("100"),
        trading_pairs=None,
        trading_required=True,
        domain=CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = xyz_api_key
        self.secret_key = xyz_api_secret
        self._proxy_url = xyz_proxy_url or ""   # ← ADD THIS LINE
        self._domain = domain
        # ... rest of your __init__ ...
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,  # ← ADD THIS LINE
        )

    # ── ADD THIS METHOD ──────────────────────────────────────────────────────
    async def stop_network(self):
        await super().stop_network()
        # A proxied factory owns its own aiohttp session; close it so it doesn't
        # leak ("Unclosed client session"). The default ConnectionsFactory is a
        # shared singleton and must be left alone.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Failed closing the proxy connections factory.", exc_info=True)
```

> **Why `self._proxy_url or None`?**
> An empty string is falsy. Passing `None` explicitly signals "no proxy" to
> `_build_connections_factory`, which then uses the default singleton
> `ConnectionsFactory`.
>
> **Set `self._proxy_url` before `super().__init__()`.** The base class builds
> the web assistants factory and the data sources inside its `__init__`.

---

### 3.3  `xyz_web_utils.py` — route to the right factory

```python
def build_api_factory(
        throttler=None,
        time_synchronizer=None,
        domain=CONSTANTS.DEFAULT_DOMAIN,
        time_provider=None,
        auth=None,
        proxy_url=None,                          # ← ADD THIS PARAMETER
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()

    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        connections_factory=_build_connections_factory(proxy_url),  # ← ADD THIS
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=time_provider or (lambda: _get_server_time()),
            ),
        ],
    )


# ── ADD THIS HELPER FUNCTION ──────────────────────────────────────────────────
def _build_connections_factory(proxy_url):
    """
    Returns a ProxyConnectionsFactory when a proxy URL is provided,
    otherwise falls back to the default singleton ConnectionsFactory.
    The import is intentionally lazy so the proxy library is only
    imported when actually used.
    """
    if proxy_url:
        from hummingbot.core.web_assistant.connections.proxy_connections_factory import (
            ProxyConnectionsFactory,
        )
        return ProxyConnectionsFactory(proxy_url=proxy_url)

    from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
    return ConnectionsFactory()
# ─────────────────────────────────────────────────────────────────────────────
```

> **Why lazy imports?**
> The `if proxy_url:` block is only reached when the user actually configures
> a proxy. `aiohttp_socks` (required by `ProxyConnectionsFactory`) is only
> imported then. Connectors that never use a proxy pay zero overhead.

---

### 3.4  Streams that open their own connections (Socket.IO)

The factory only covers what goes through it: REST requests and streams made
with `get_ws_assistant()`. Some connectors drive a third-party stream client
instead. CoinDCX's order book and user streams, for example, use
`socketio.AsyncClient`, which opens its own connections. With only 3.1 to 3.3,
REST goes through the proxy but those streams still connect from your own IP.

python-socketio accepts an aiohttp session as `http_session`, and engine.io
opens its websocket with it. So give each client a session over the proxy:

```python
# xyz_web_utils.py
def build_proxy_session(proxy_url):
    if not proxy_url:
        return None
    from aiohttp_socks import ProxyConnector
    # The same settings as ProxyConnectionsFactory.
    return aiohttp.ClientSession(connector=ProxyConnector.from_url(proxy_url, rdns=True))


# xyz_api_user_stream_data_source.py (the same in the order book data source)
def _build_client(self, output):
    self._http_session = web_utils.build_proxy_session(self._proxy_url)  # None = direct
    client = socketio.AsyncClient(logger=False, reconnection=False, http_session=self._http_session)
    ...

async def _disconnect(self):
    if self._client is not None:
        ...  # disconnect the client first
        self._client = None
    if self._http_session is not None:
        await self._http_session.close()  # engine.io never closes a session it was given
        self._http_session = None
```

The exchange passes `proxy_url=self._proxy_url or None` to both data sources.

Two engine.io details make this exact shape matter:

- engine.io never closes a session it was given, so the data source must close
  it after each disconnect.
- If the session it was given is already closed when it connects, engine.io
  quietly makes a plain, **direct** `aiohttp.ClientSession()` and carries on.
  So build a new session for every connection (in `_build_client`), and never
  reuse one across reconnects.

Reference: `hummingbot/connector/exchange/coindcx/` (`coindcx_web_utils.py`
and both data sources), tested in
`test/hummingbot/connector/exchange/coindcx/test_coindcx_proxy.py`.

---

## 4. Supported proxy types

The `ProxyConnector.from_url()` call inside `ProxyConnectionsFactory`
auto-detects the proxy type from the URL scheme:

| URL format | Protocol | Authentication |
|---|---|---|
| `socks5://user:pass@host:1080` | SOCKS5 | Username + password |
| `socks5://host:1080` | SOCKS5 | None |
| `socks4://host:1080` | SOCKS4 | None |
| `http://user:pass@host:8080` | HTTP CONNECT | Username + password |
| `http://host:8080` | HTTP CONNECT | None |

**Recommendation:** Use SOCKS5 when possible. It works at the TCP level,
proxies both REST and WebSocket traffic transparently, and performs DNS
resolution on the proxy server (so your real IP is never exposed even in
DNS queries).

---

## 5. Runtime configuration

### 5.1  Via hummingbot CLI (recommended)

```
connect xyz
```

You will be prompted for all fields in `XyzConfigMap`, including the proxy URL:

```
Enter your XYZ API key
>>> <your API key>

Enter your XYZ API secret
>>> <your 64-char hex secret>

Enter a proxy URL ... or leave blank to connect directly
>>> socks5://proxyuser:proxypass@your.proxy.host:1080
```

The proxy URL is encrypted and stored in `conf/connectors/xyz.yml`.

### 5.2  Via scripts / direct instantiation

```python
from hummingbot.connector.exchange.xyz.xyz_exchange import XyzExchange

# With proxy
exchange = XyzExchange(
    xyz_api_key="your_key",
    xyz_api_secret="your_secret",
    xyz_proxy_url="socks5://user:pass@host:1080",  # ← only this line differs
    trading_pairs=["BTC-INR"],
    trading_required=True,
)

# Without proxy (direct connection)
exchange = XyzExchange(
    xyz_api_key="your_key",
    xyz_api_secret="your_secret",
    # xyz_proxy_url omitted → defaults to "" → no proxy
    trading_pairs=["BTC-INR"],
    trading_required=True,
)
```

### 5.3  Via environment variables (for scripts)

```bash
export XYZ_API_KEY="your_key"
export XYZ_API_SECRET="your_secret"
export XYZ_PROXY_URL="socks5://user:pass@host:1080"
python scripts/verify_xyz_connection.py
```

---

## 6. Testing your proxy integration

### Step 1 — Verify the proxy server itself works

Before running hummingbot, confirm the proxy is reachable and routes to the
correct IP:

```bash
# SOCKS5 proxy
curl --socks5 user:pass@proxy.host:1080 https://api.xyz-exchange.com/health

# HTTP proxy
curl --proxy http://user:pass@proxy.host:8080 https://api.xyz-exchange.com/health
```

The response should come from the exchange, and the exchange should see the
proxy's whitelisted IP.

### Step 2 — Check what IP the exchange sees

Most exchanges provide a "validate keys" or "account info" endpoint. Compare
the IP in the response to the proxy server's IP.

### Step 3 — Run the connector's verification script

```bash
XYZ_API_KEY="..." XYZ_API_SECRET="..." XYZ_PROXY_URL="socks5://..." \
    python scripts/verify_xyz_connection.py
```

### Step 4 — Run unit tests (proxy path)

Unit tests mock all HTTP calls, so no real proxy is needed. They still
exercise the proxy construction path:

```python
# In your test file:
exchange = _make_exchange(xyz_proxy_url="socks5://fake:fake@localhost:1080")
# HTTP calls are mocked — ProxyConnectionsFactory is constructed but never
# actually connects to the fake proxy.
```

```bash
python -m pytest test/hummingbot/connector/exchange/xyz/ -v
```

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ModuleNotFoundError: aiohttp_socks` | Package not installed | `pip install aiohttp-socks` |
| `ConnectionRefusedError` | Proxy host/port wrong | Check proxy URL format and server status |
| `403 Forbidden` from exchange | Proxy's outbound IP not whitelisted | Contact exchange to whitelist the proxy server's IP |
| `401 Unauthorized` from exchange | API credentials wrong (not a proxy issue) | Check API key and secret |
| Proxy works for REST but not WebSocket | SOCKS4 used instead of SOCKS5 | Use `socks5://` — SOCKS4 has limited WebSocket support |
| DNS resolution fails inside proxy | `rdns=False` (default for some setups) | `ProxyConnectionsFactory` already passes `rdns=True` — no action needed |
| Proxy URL saved but not used | The field has `is_connect_key: False`, so it never reaches the constructor | Set `is_connect_key: True` (3.1) |
| Proxy URL saved but not used | Empty string passed as proxy_url | Ensure `self._proxy_url = xyz_proxy_url or ""` and `proxy_url=self._proxy_url or None` |
| REST works through the proxy, but a stream connects direct (or is rejected) | The stream uses its own client (e.g. Socket.IO), which skips the factory | Give it a proxied session (3.4) |
| `Unclosed client session` at shutdown | The proxied factory is never closed | Close it in `stop_network` (3.2) |
| `<exchange> is not ready. Please wait...` every second, after `Connection reset by peer` errors at start | The proxy dropped the first order book read. `OrderBookTracker` makes that read only once, so the connector never gets ready | CoinDCX spot retries the read (5 tries over about 15 s). For other connectors, or if every try fails: `stop`, then `start` again |

---

## 8. Reference: CSX as a worked example

The CSX (CoinSwitch Kuber) connector is the canonical example of this pattern.
Refer to these files:

```
hummingbot/connector/exchange/csx/csx_utils.py       ← csx_proxy_url field
hummingbot/connector/exchange/csx/csx_exchange.py     ← _proxy_url storage + forwarding
hummingbot/connector/exchange/csx/csx_web_utils.py    ← _build_connections_factory helper
scripts/verify_csx_connection.py                      ← real-API verification script
```

For a connector whose streams use Socket.IO (3.4), see CoinDCX spot:

```
hummingbot/connector/exchange/coindcx/coindcx_utils.py                       ← coindcx_proxy_url field
hummingbot/connector/exchange/coindcx/coindcx_exchange.py                    ← passes the proxy to the factory and both streams
hummingbot/connector/exchange/coindcx/coindcx_web_utils.py                   ← _build_connections_factory + build_proxy_session
hummingbot/connector/exchange/coindcx/coindcx_api_order_book_data_source.py  ← a proxied session per Socket.IO connection
hummingbot/connector/exchange/coindcx/coindcx_api_user_stream_data_source.py ← the same for the user stream
```

The shared infrastructure lives at:
```
hummingbot/core/web_assistant/connections/proxy_connections_factory.py
```

---
