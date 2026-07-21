# Proxy Server Support for IP-Whitelisted Exchanges

Some exchanges (e.g. CoinSwitch Kuber / CSX) only accept API requests from a
pre-registered set of IP addresses. If your hummingbot instance runs on a
machine whose IP is not whitelisted, every API call will be rejected with an
authentication or IP-restriction error.

**The solution:** route the connector's traffic through a proxy server that
has a whitelisted IP.

This guide explains how the proxy infrastructure works and exactly what to
add to any connector to enable it.

---

## Table of Contents

1. [How it works — architecture overview](#1-how-it-works--architecture-overview)
2. [The three files you must edit](#2-the-three-files-you-must-edit)
3. [Complete code for each file](#3-complete-code-for-each-file)
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

**What changes per connector:** three small additions totalling ~15 lines.

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
            "is_connect_key": False, # optional — connection works without it
            "prompt_on_new": True,   # user is asked when running `connect xyz`
        },
    )
    # ─────────────────────────────────────────────────────────────────────────

    model_config = ConfigDict(title="xyz")

KEYS = XyzConfigMap.model_construct()
```

> **Why `is_connect_key: False`?**
> `is_connect_key: True` fields are checked when hummingbot validates the
> connection. The proxy URL is optional — leaving it blank is a valid
> configuration — so set it to `False`.

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
```

> **Why `self._proxy_url or None`?**
> An empty string is falsy. Passing `None` explicitly signals "no proxy" to
> `_build_connections_factory`, which then uses the default singleton
> `ConnectionsFactory`.

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
| Proxy URL saved but not used | Empty string passed as proxy_url | Ensure `self._proxy_url = xyz_proxy_url or ""` and `proxy_url=self._proxy_url or None` |

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

The shared infrastructure lives at:
```
hummingbot/core/web_assistant/connections/proxy_connections_factory.py
```

---

*This document was written alongside the `feat/proxy-server-support` PR.*
*For questions, see that PR's description or the inline code comments.*
