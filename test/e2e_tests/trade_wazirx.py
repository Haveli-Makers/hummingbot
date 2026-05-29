#!/usr/bin/env python3
"""
trade_wazirx.py  —  Trade a WazirX asset at near-market price via REST API.
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


async def _get_open_orders(session: aiohttp.ClientSession) -> list:
    """Return open orders for SYMBOL, empty list on any error."""
    ts = await _server_time(session)
    qs = _sign({"symbol": SYMBOL}, ts)
    async with session.get(
        f"{BASE_URL}/v1/openOrders?{qs}",
        headers=_auth_headers(),
    ) as r:
        if r.status == 200:
            data = await r.json()
            return data if isinstance(data, list) else []
    return []


async def _cancel_order(session: aiohttp.ClientSession, order_id: str) -> tuple:
    """Cancel order_id on SYMBOL. Returns (http_status, response_dict)."""
    ts = await _server_time(session)
    body = _sign({"symbol": SYMBOL, "orderId": order_id}, ts)
    async with session.delete(
        f"{BASE_URL}/v1/order",
        data=body,
        headers=_auth_headers(),
    ) as r:
        return r.status, await r.json()


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    async with aiohttp.ClientSession() as session:

        # ── 1. Fetch server time (avoids clock-skew rejections) ──────────────
        server_ts = await _server_time(session)
        print(f"\nServer time : {server_ts}")

        # ── 2. Fetch order book — best bid/ask ───────────────────────────────
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
        if not asks:
            print("ERROR: order book has no asks — cannot determine buy price.")
            return

        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0]))

        print(f"  Best bid  : {best_bid} {QUOTE_TOKEN}")
        print(f"  Best ask  : {best_ask} {QUOTE_TOKEN}")

        # ── 3. Show balances ──────────────────────────────────────────────────
        print("\nFetching your balances...")
        funds_ts = await _server_time(session)
        funds_qs = _sign({}, funds_ts)
        async with session.get(
            f"{BASE_URL}/v1/funds?{funds_qs}",
            headers=_auth_headers(),
        ) as r:
            if r.status == 200:
                resp = await r.json()
                balances = resp if isinstance(resp, list) else resp.get("balances", [])
                found_balances = False
                for entry in balances:
                    asset = entry.get("asset", "").upper()
                    free = Decimal(entry.get("free", "0"))
                    locked = Decimal(entry.get("locked", "0"))
                    total = free + locked
                    if total > 0:
                        lock_note = f"  ← {locked} LOCKED in open order" if locked > 0 else ""
                        print(f"  {asset:<5} : {total}  (Free: {free}, Locked: {locked}){lock_note}")
                        found_balances = True
                if not found_balances:
                    print("  No balances > 0 found.")
            else:
                print(f"  Could not fetch balances (HTTP {r.status}).")

        # ── 4. Show open orders + offer to cancel ────────────────────────────
        print(f"\nFetching open orders for {TRADING_PAIR}...")
        open_orders = await _get_open_orders(session)
        if not open_orders:
            print("  No open orders.")
        else:
            print(f"  {len(open_orders)} open order(s):")
            for o in open_orders:
                oid = o.get("id", "?")
                side = o.get("side", "?").upper()
                qty = o.get("origQty", "?")
                price = o.get("price", "?")
                filled = o.get("executedQty", "0")
                remaining = Decimal(str(qty)) - Decimal(str(filled))
                print(
                    f"    [{oid}] {side} {qty} {BASE_TOKEN} @ {price} {QUOTE_TOKEN}"
                    f"  filled: {filled}  remaining (locked): {remaining}"
                )

            print()
            to_cancel = input(
                "Enter order ID to cancel (or press Enter to skip): "
            ).strip()
            if to_cancel:
                print(f"Cancelling order {to_cancel}...")
                code, result = await _cancel_order(session, to_cancel)
                if code in (200, 201):
                    print(f"  Cancelled: {result.get('status', 'ok')}")
                    # Refresh open orders after cancel
                    open_orders = await _get_open_orders(session)
                    if not open_orders:
                        print("  All orders cancelled.")
                else:
                    msg = result.get("message", result.get("msg", ""))
                    print(f"  Cancel failed (HTTP {code}): {msg}")
                    print(f"  Full response: {result}")
                    return

        # ── 5. Ask for the trade side ─────────────────────────────────────────
        print()
        side_input = input("Do you want to buy or sell? [buy/sell]: ").strip().lower()
        if side_input not in ["buy", "sell"]:
            print("Invalid side. Must be 'buy' or 'sell'. Exiting.")
            return

        # ── 6. Ask for the amount ─────────────────────────────────────────────
        print()
        default_amt = (
            _env.get("WAZIRX_LIMIT_BUY_AMOUNT" if side_input == "buy" else "WAZIRX_LIMIT_SELL_AMOUNT")
            or "0.00004"
        )
        raw = input(
            f"Amount of {BASE_TOKEN} to {side_input} "
            f"[default: {default_amt}]: "
        ).strip()
        try:
            trade_amount = Decimal(raw if raw else default_amt)
        except Exception:
            print("Invalid numeric amount. Exiting.")
            return

        if trade_amount <= Decimal("0"):
            print("Amount must be positive. Exiting.")
            return

        if side_input == "buy":
            trade_price = round(best_ask * Decimal("1.001"))
            est_value = trade_amount * trade_price
            action_desc = "BUY"
            est_desc = f"  Cost  ~{est_value:.2f} {QUOTE_TOKEN}  (before fees)"
            price_desc = f"{trade_price} {QUOTE_TOKEN}  (best_ask × 1.001)"
        else:
            trade_price = round(best_bid * Decimal("0.999"))
            est_value = trade_amount * trade_price
            action_desc = "SELL"
            est_desc = f"  Get   ~{est_value:.2f} {QUOTE_TOKEN}  (before fees)"
            price_desc = f"{trade_price} {QUOTE_TOKEN}  (best_bid × 0.999)"

        # ── 7. Confirmation prompt ────────────────────────────────────────────
        print()
        print("=" * 52)
        print(f"  {action_desc.upper().ljust(4)}  {trade_amount} {BASE_TOKEN}")
        print(f"  At    {price_desc}")
        print(est_desc)
        print("=" * 52)
        confirm = input("\nType  yes  to place the order: ").strip().lower()

        if confirm != "yes":
            print("Cancelled — no order placed.")
            return

        # ── 8. Place the order ────────────────────────────────────────────────
        server_ts = await _server_time(session)

        order_params = {
            "symbol": SYMBOL,
            "side": side_input,
            "type": "limit",
            "quantity": f"{trade_amount:f}",
            "price": str(trade_price),
        }
        body = _sign(order_params, server_ts)

        print(f"\nPlacing {action_desc} order...")
        async with session.post(
            f"{BASE_URL}/v1/order",
            data=body,
            headers=_auth_headers(),
        ) as r:
            result = await r.json()
            status_code = r.status

        # WazirX returns 201 for a newly created order (open/wait) and
        # 200 for orders that fill immediately.  Both are success responses.
        print(f"HTTP {status_code}")
        if status_code in (200, 201):
            oid = result.get("id") or result.get("orderId", "N/A")
            cid = result.get("clientOrderId", "N/A")
            st = result.get("status", "N/A")
            filled = Decimal(str(result.get("executedQty", "0")))
            print(f"  Exchange order ID : {oid}")
            print(f"  Client order ID   : {cid}")
            print(f"  Status            : {st}")
            print(f"  Filled qty        : {filled} {BASE_TOKEN}")
            print()
            past_tense = {"buy": "bought", "sell": "sold"}[side_input]
            if filled >= trade_amount * Decimal("0.99"):
                print(f"✓ Order filled immediately — {trade_amount} {BASE_TOKEN} {past_tense}.")
            elif filled > 0:
                remaining = trade_amount - filled
                print(f"⚡ Partially filled: {filled} {BASE_TOKEN} {past_tense}, {remaining} remaining (open).")
                print(f"  Run the script again and cancel order [{oid}] to free the locked amount.")
            else:
                print(f"⏳ Order is open (status: {st}) — waiting in the book at {trade_price} {QUOTE_TOKEN}.")
                print(f"  Run the script again and cancel order [{oid}] if you want to free the funds.")
        else:
            err_code = result.get("code", "")
            msg = result.get("message", result.get("msg", ""))
            print(f"  Error {err_code}: {msg}")
            print(f"  Full response: {result}")


if __name__ == "__main__":
    asyncio.run(main())
