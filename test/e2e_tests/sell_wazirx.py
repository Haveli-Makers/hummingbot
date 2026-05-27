#!/usr/bin/env python3
"""
sell_wazirx.py  —  Sell a WazirX asset at near-market price via REST API.

Reads credentials exclusively from test/e2e_tests/.env (never from the shell
environment).  Shows a summary and asks for explicit confirmation before
placing any order.

Usage:
    python test/e2e_tests/sell_wazirx.py
"""

import asyncio
import hashlib
import hmac
import time
from decimal import Decimal
from pathlib import Path

import aiohttp
from dotenv import dotenv_values

# ── Load .env (credentials only from file, never os.environ) ─────────────────

_TEST_DIR = Path(__file__).parent
_ENV_FILE = None
for _cand in (_TEST_DIR / ".env", _TEST_DIR.parent / ".env"):
    if _cand.exists():
        _ENV_FILE = _cand
        break

if _ENV_FILE is None:
    raise FileNotFoundError(
        "No .env file found. "
        "Copy test/e2e_tests/.env.example → test/e2e_tests/.env and fill in values."
    )

_env = dotenv_values(_ENV_FILE)

API_KEY = (_env.get("WAZIRX_CRED_wazirx_api_key") or "").strip()
API_SECRET = (_env.get("WAZIRX_CRED_wazirx_api_secret") or "").strip()
TRADING_PAIR = (_env.get("WAZIRX_TRADING_PAIR") or "BTC-INR").strip()

if not API_KEY or not API_SECRET:
    raise ValueError(
        "WAZIRX_CRED_wazirx_api_key / WAZIRX_CRED_wazirx_api_secret "
        f"not found in {_ENV_FILE}"
    )

BASE_URL = "https://api.wazirx.com/sapi"
SYMBOL = TRADING_PAIR.replace("-", "").lower()  # btcinr
BASE_TOKEN = TRADING_PAIR.split("-")[0]          # BTC
QUOTE_TOKEN = TRADING_PAIR.split("-")[1]         # INR


# ── Auth helper (mirrors wazirx_auth.py exactly) ──────────────────────────────

async def _server_time(session: aiohttp.ClientSession) -> int:
    async with session.get(f"{BASE_URL}/v1/time") as r:
        if r.status == 200:
            data = await r.json()
            return int(data.get("serverTime", int(time.time() * 1000)))
    return int(time.time() * 1000)


def _sign(params: dict, server_ts: int) -> str:
    """
    Build the signed query string the same way wazirx_auth.py does:
      1. Add recvWindow and timestamp to params (in insertion order).
      2. Join as k=v pairs (NO sorting).
      3. HMAC-SHA256 sign the string.
      4. Append &signature=... at the end.
    Returns the final query string ready to use as POST body.
    """
    p = dict(params)
    p["recvWindow"] = 60000
    p["timestamp"] = server_ts
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + f"&signature={sig}"


def _auth_headers() -> dict:
    return {
        "X-Api-Key": API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    async with aiohttp.ClientSession() as session:

        # ── 1. Fetch server time (avoids clock-skew rejections) ──────────────
        server_ts = await _server_time(session)
        print(f"\nServer time : {server_ts}")

        # ── 2. Fetch order book — best bid ────────────────────────────────────
        print(f"Fetching order book for {TRADING_PAIR}...")
        async with session.get(
            f"{BASE_URL}/v1/depth",
            params={"symbol": SYMBOL, "limit": 5},
        ) as r:
            depth = await r.json()

        bids = depth.get("bids", [])
        asks = depth.get("asks", [])
        if not bids:
            print("ERROR: order book has no bids — cannot determine price.")
            return

        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0])) if asks else best_bid

        # Place 0.1 % below best bid → taker order (fills against existing bids)
        sell_price = round(best_bid * Decimal("0.999"))
        print(f"  Best bid  : {best_bid} {QUOTE_TOKEN}")
        print(f"  Best ask  : {best_ask} {QUOTE_TOKEN}")
        print(f"  Sell at   : {sell_price} {QUOTE_TOKEN}  (best_bid × 0.999)")

        # ── 3. Ask for the amount to sell ─────────────────────────────────────
        print()
        default_amt = _env.get("WAZIRX_LIMIT_SELL_AMOUNT") or "0.00004"
        raw = input(
            f"Amount of {BASE_TOKEN} to sell "
            f"[default: {default_amt}]: "
        ).strip()
        sell_amount = Decimal(raw if raw else default_amt)

        if sell_amount <= Decimal("0"):
            print("Amount must be positive. Exiting.")
            return

        est_receive = sell_amount * sell_price

        # ── 4. Confirmation prompt ────────────────────────────────────────────
        print()
        print("=" * 52)
        print(f"  SELL  {sell_amount} {BASE_TOKEN}")
        print(f"  At    {sell_price} {QUOTE_TOKEN}/{BASE_TOKEN}  (limit, taker)")
        print(f"  Get   ~{est_receive:.2f} {QUOTE_TOKEN}  (before fees)")
        print("=" * 52)
        confirm = input("\nType  yes  to place the order: ").strip().lower()

        if confirm != "yes":
            print("Cancelled — no order placed.")
            return

        # ── 5. Place the sell order ───────────────────────────────────────────
        # Refresh server time right before signing to avoid stale timestamps.
        server_ts = await _server_time(session)

        order_params = {
            "symbol": SYMBOL,
            "side": "sell",
            "type": "limit",
            "quantity": f"{sell_amount:f}",
            "price": str(sell_price),
        }
        body = _sign(order_params, server_ts)

        print("\nPlacing SELL order...")
        async with session.post(
            f"{BASE_URL}/v1/order",
            data=body,
            headers=_auth_headers(),
        ) as r:
            result = await r.json()
            status_code = r.status

        print(f"HTTP {status_code}")
        if status_code == 200:
            oid = result.get("id") or result.get("orderId", "N/A")
            cid = result.get("clientOrderId", "N/A")
            st = result.get("status", "N/A")
            filled = result.get("executedQty", "0")
            print(f"  Exchange order ID : {oid}")
            print(f"  Client order ID   : {cid}")
            print(f"  Status            : {st}")
            print(f"  Filled qty        : {filled} {BASE_TOKEN}")
            print()
            if Decimal(str(filled)) >= sell_amount * Decimal("0.99"):
                print(f"Order filled immediately — {sell_amount} {BASE_TOKEN} sold.")
            else:
                print("Order placed (open). It will fill when a buyer matches the price.")
                print("Monitor on WazirX or cancel via the app if needed.")
        else:
            code = result.get("code", "")
            msg = result.get("message", result.get("msg", ""))
            print(f"  Error {code}: {msg}")
            print(f"  Full response: {result}")


if __name__ == "__main__":
    asyncio.run(main())
