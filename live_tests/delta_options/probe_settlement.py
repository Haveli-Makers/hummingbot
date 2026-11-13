#!/usr/bin/env python
"""
Read-only probe for Phase 3 of the delta_options connector: what Delta shows about expiry and settlement.

It places no orders and changes nothing on the account. From Delta's live API it reads:
  1. Expired options: their state, settlement time and settlement price; whether Delta still serves an
     expired option by symbol (product and ticker); and whether a "settled" state exists besides "expired".
  2. The live option closest to expiry: its trading status and product specs, for any sign of a last
     trading time.
  3. With --with-keys, the account's own records: option fills by type (a settlement is a fill of type
     "settlement"), wallet transactions by type ("settlement"), how option orders ended (state and
     cancellation reason), and the option positions open now.
Every response is also saved in full to probe_output/settlement_probe_<UTC time>.json.

KEYS (only with --with-keys): the encrypted conf/connectors/delta_perpetual.yml, read as
verify_delta_options.py reads it (asks for the Hummingbot password), or DELTA_API_KEY / DELTA_API_SECRET
with --from-env.

USAGE (in the hummingbot conda env, from any directory)
    python probe_settlement.py                     # public part only, no keys
    python probe_settlement.py --with-keys         # + the account's records
    python probe_settlement.py --with-keys --days 90
"""
import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_delta_options as verify  # noqa: E402  (it also puts the hummingbot repo on sys.path)

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS  # noqa: E402
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative  # noqa: E402

header, info, ok, fail = verify.header, verify.info, verify.ok, verify.fail

WALLET_TRANSACTIONS_PATH_URL = "/v2/wallet/transactions"
ORDER_HISTORY_PATH_URL = "/v2/orders/history"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "probe_output")

# Every response, saved at the end for a closer look than the console gives.
RAW = {}


def _when(value):
    seconds = DeltaOptionsDerivative._parse_timestamp(value)
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%d %b %H:%M:%S UTC") if seconds else str(value)


def _dec(value):
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except InvalidOperation:
        return None


def _compact(record, limit=700):
    """One record on one line: a nested product shrinks to its symbol."""
    shown = dict(record)
    if isinstance(shown.get("product"), dict):
        shown["product"] = shown["product"].get("symbol")
    text = json.dumps(shown, default=str, separators=(",", ":"))
    return text if len(text) <= limit else text[:limit] + " ..."


async def _get(connector, name, path, params=None, auth=False, limit_id=None):
    """One GET. Returns the response, or None and the error; either is kept in RAW under `name`."""
    try:
        response = await connector._api_get(path_url=path, params=params, is_auth_required=auth,
                                            limit_id=limit_id or path)
    except Exception as e:
        RAW[name] = {"error": str(e)}
        return None, str(e)
    RAW[name] = response
    return response, None


async def _pages(connector, name, path, params, auth, limit_id, max_pages):
    """All rows of a cursor-paginated list, up to max_pages. Returns the rows and what stopped it early, if anything."""
    rows, params = [], dict(params)
    problem = None
    for page in range(max_pages):
        try:
            response = await connector._api_get(path_url=path, params=params, is_auth_required=auth,
                                                limit_id=limit_id)
        except Exception as e:
            problem = f"page {page + 1} failed: {str(e)[-200:]}"
            break
        rows.extend(response.get("result") or [])
        after = (response.get("meta") or {}).get("after")
        if not after:
            break
        params["after"] = after
    else:
        problem = f"stopped after {max_pages} pages"
    RAW[name] = {"rows": rows, "problem": problem}
    return rows, problem


def _underlying(product):
    return (product.get("underlying_asset") or {}).get("symbol")


async def expired_options(connector, underlying, res):
    header(f"1. EXPIRED {underlying} OPTIONS — what Delta keeps after expiry")
    rows, problem = await _pages(
        connector, "expired products", CONSTANTS.PRODUCTS_PATH_URL,
        {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES, "states": "expired", "page_size": 100},
        auth=False, limit_id=CONSTANTS.PRODUCTS_PATH_URL, max_pages=60)
    options = [p for p in rows if _underlying(p) == underlying]
    res.check(bool(options), f"{len(rows)} expired options listed, {len(options)} on {underlying}"
              + (f" ({problem})" if problem else ""))
    findings = {}
    if not options:
        return findings

    by_expiry = {}
    for product in options:
        by_expiry.setdefault(product.get("settlement_time"), []).append(product)
    expiries = sorted(by_expiry, key=DeltaOptionsDerivative._parse_timestamp, reverse=True)
    info("most recent expiries: " + ", ".join(f"{_when(t)} ({len(by_expiry[t])})" for t in expiries[:5]))
    info(f"states: {dict(Counter(p.get('state') for p in options))}; trading statuses: "
         f"{dict(Counter(p.get('trading_status') for p in options))}")

    latest = sorted(by_expiry[expiries[0]], key=lambda p: (p.get("contract_type"), _dec(p.get("strike_price")) or 0))
    values = Counter(str(p.get("settlement_price")) for p in latest)
    info(f"latest expiry, {_when(expiries[0])}: {len(latest)} options; settlement_price values: "
         + ", ".join(f"{value} x{count}" for value, count in values.most_common(4))
         + (" ..." if len(values) > 4 else ""))
    findings["one_price_per_expiry"] = len(values) == 1 and next(iter(values)) not in ("None", "")
    findings["settlement_price"] = next(iter(values)) if len(values) == 1 else None
    for contract_type in ("call_options", "put_options"):
        same = [p for p in latest if p.get("contract_type") == contract_type]
        for product in [same[i] for i in sorted({0, len(same) // 2, len(same) - 1}) if same]:
            info(f"  {product.get('symbol')}: strike {product.get('strike_price')}, state {product.get('state')}, "
                 f"settlement_price {product.get('settlement_price')}, settlement_time "
                 f"{product.get('settlement_time')}")
    info(f"one expired product in full: {_compact(latest[0], limit=1200)}")

    # Whether the connector can still look an expired option up later, by symbol.
    symbol = latest[len(latest) // 2].get("symbol")
    product, error = await _get(connector, f"product {symbol}", f"{CONSTANTS.PRODUCTS_PATH_URL}/{symbol}",
                                limit_id=CONSTANTS.PRODUCTS_PATH_URL)
    result = (product or {}).get("result") or {}
    findings["product_by_symbol"] = bool(result)
    info(f"GET /v2/products/{symbol}: " + (f"works (state {result.get('state')}, settlement_price "
                                           f"{result.get('settlement_price')})" if result
                                           else f"fails: {(error or str(product))[-200:]}"))
    ticker, error = await _get(connector, f"ticker {symbol}", CONSTANTS.TICKERS_PATH_URL.format(symbols=symbol),
                               limit_id=CONSTANTS.TICKERS_PATH_URL)
    result = (ticker or {}).get("result")
    shown = result if isinstance(result, dict) else {"result": result}
    info(f"GET /v2/tickers/{symbol}: " + (f"works: {_compact(shown, 400)}" if result
                                          else f"fails: {(error or str(ticker))[-200:]}"))
    settled, error = await _get(connector, "settled products", CONSTANTS.PRODUCTS_PATH_URL,
                                {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES, "states": "settled",
                                 "page_size": 10}, limit_id=CONSTANTS.PRODUCTS_PATH_URL)
    info("states=settled: " + (f"{len((settled or {}).get('result') or [])} options on the first page"
                               if settled else f"fails: {(error or '')[-200:]}"))
    return findings


async def live_option_specs(connector, underlying, res):
    header(f"2. THE LIVE {underlying} OPTION CLOSEST TO EXPIRY — any sign of a last trading time")
    rows, problem = await _pages(
        connector, "live products", CONSTANTS.PRODUCTS_PATH_URL,
        {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES, "states": "live", "page_size": 100},
        auth=False, limit_id=CONSTANTS.PRODUCTS_PATH_URL, max_pages=50)
    options = [p for p in rows if _underlying(p) == underlying]
    res.check(bool(options), f"{len(options)} live {underlying} options" + (f" ({problem})" if problem else ""))
    if not options:
        return
    soonest = min(options, key=lambda p: DeltaOptionsDerivative._parse_timestamp(p.get("settlement_time")))
    hours = (DeltaOptionsDerivative._parse_timestamp(soonest.get("settlement_time")) - time.time()) / 3600
    info(f"{soonest.get('symbol')} expires {_when(soonest.get('settlement_time'))}, in {hours:.1f} h: state "
         f"{soonest.get('state')}, trading_status {soonest.get('trading_status')}")
    info(f"product_specs: {json.dumps(soonest.get('product_specs'), default=str)}")
    timed = {k: v for k, v in soonest.items() if any(w in k for w in ("time", "expir", "settle", "launch", "status"))}
    info(f"fields about time and status: {json.dumps(timed, default=str)}")


async def account_records(connector, days, res):
    start = int((time.time() - days * 86400) * 1e6)
    header(f"3. THE ACCOUNT'S OPTION RECORDS — last {days} days")
    findings = {}
    failed_before = res.failed

    fills, problem = await _pages(
        connector, "fills", CONSTANTS.FILLS_PATH_URL,
        {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES, "start_time": start, "page_size": 50},
        auth=True, limit_id=CONSTANTS.FILLS_PATH_URL, max_pages=40)
    res.check(problem is None or "stopped after" in problem,
              f"{len(fills)} option fills, by type: {dict(Counter(f.get('fill_type') for f in fills))}"
              + (f" ({problem})" if problem else ""))
    settlement_fills = [f for f in fills if f.get("fill_type") == "settlement"]
    findings["settlement_fills"] = len(settlement_fills)
    for fill in settlement_fills[:3]:
        info(f"settlement fill: {_compact(fill)}")
    for fill in [f for f in fills if f.get("fill_type") != "settlement"][:1]:
        info(f"a normal fill, for comparison: {_compact(fill, 500)}")

    transactions, problem = await _pages(
        connector, "wallet transactions", WALLET_TRANSACTIONS_PATH_URL,
        {"start_time": start, "page_size": 50},
        auth=True, limit_id=CONSTANTS.FILLS_PATH_URL, max_pages=40)
    res.check(problem is None or "stopped after" in problem,
              f"{len(transactions)} wallet transactions, by type: "
              f"{dict(Counter(t.get('transaction_type') for t in transactions))}"
              + (f" ({problem})" if problem else ""))
    settlement_entries = [t for t in transactions if t.get("transaction_type") == "settlement"]
    findings["settlement_entries"] = len(settlement_entries)
    for entry in settlement_entries[:3]:
        info(f"settlement entry: {_compact(entry)}")

    orders, problem = await _pages(
        connector, "order history", ORDER_HISTORY_PATH_URL,
        {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES, "start_time": start, "page_size": 50},
        auth=True, limit_id=CONSTANTS.FILLS_PATH_URL, max_pages=40)
    endings = Counter((o.get("state"), o.get("cancellation_reason")) for o in orders)
    res.check(problem is None or "stopped after" in problem,
              f"{len(orders)} option orders, by how they ended: "
              + (", ".join(f"{state}/{reason} x{count}" for (state, reason), count in endings.most_common())
                 or "none")
              + (f" ({problem})" if problem else ""))

    findings["reads_failed"] = res.failed > failed_before
    positions, error = await _get(connector, "positions", CONSTANTS.POSITIONS_PATH_URL,
                                  {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES}, auth=True,
                                  limit_id=CONSTANTS.POSITIONS_PATH_URL)
    open_positions = (positions or {}).get("result") or []
    listed = ", ".join(f"{p.get('product_symbol')} {p.get('size')}" for p in open_positions) or "none"
    info(f"open option positions now: {listed}" + (f" (read failed: {error[-200:]})" if error else ""))
    return findings


def summarize(expired, account, days, path):
    header("WHAT THIS TELLS US")
    if expired:
        if expired.get("one_price_per_expiry"):
            info(f"settlement_price is the same for every option of an expiry ({expired.get('settlement_price')}): "
                 f"the index's settlement price, so each option's payout is worked out from its strike")
        elif expired.get("settlement_price") in ("None", ""):
            info("expired options carry no settlement_price: the payout has to come from the account's records")
        else:
            info("settlement_price differs between options of one expiry: see the examples above for what it is")
        info("an expired option can still be read by symbol" if expired.get("product_by_symbol")
             else "an expired option can no longer be read by symbol: the connector has to keep what it needs")
    if account is not None:
        found = account.get("settlement_fills", 0) + account.get("settlement_entries", 0)
        verdict = (": their format is above" if found
                   else ": some reads failed, so this may be incomplete; run again" if account.get("reads_failed")
                   else ": the hold-through-expiry run is needed to see one")
        info(f"{account.get('settlement_fills', 0)} settlement fills and {account.get('settlement_entries', 0)} "
             f"settlement wallet entries in the last {days} days{verdict}")
    info(f"every response is saved in {path}")


async def run(args, api_key, api_secret, proxy_url):
    res = verify.Results()
    verify._quiet_app_warnings()
    print(verify._c("1", f"delta_options settlement probe — read-only, {args.underlying}"
                         + (" (with keys)" if args.with_keys else " (public data only)")))
    if args.with_keys:
        info(f"proxy: {verify._masked(proxy_url)}" if proxy_url else "proxy: none (direct connection)")
    connector = DeltaOptionsDerivative(
        delta_options_api_key=api_key,
        delta_options_api_secret=api_secret,
        delta_options_proxy_url=proxy_url or "",
        delta_options_dead_man_switch=False,
        trading_pairs=[],
        trading_required=False,
    )
    expired = account = None
    try:
        expired = await expired_options(connector, args.underlying, res)
        await live_option_specs(connector, args.underlying, res)
        if args.with_keys:
            account = await account_records(connector, args.days, res)
    finally:
        await connector.stop_network()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"settlement_probe_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json")
    with open(path, "w") as f:
        json.dump(RAW, f, indent=1, default=str)
    summarize(expired, account, args.days, path)

    header("SUMMARY")
    (ok if res.failed == 0 else fail)(f"{res.passed}/{res.passed + res.failed} reads worked")
    return 0 if res.failed == 0 else 1


def main():
    p = argparse.ArgumentParser(description="Read-only probe of what Delta shows about option expiry and settlement.")
    p.add_argument("--underlying", default="BTC", choices=["BTC", "ETH"])
    p.add_argument("--with-keys", action="store_true", help="also read the account's fills, wallet entries and orders")
    p.add_argument("--from-env", action="store_true", help="with --with-keys: read DELTA_API_KEY / DELTA_API_SECRET")
    p.add_argument("--days", type=int, default=30, help="how far back to read the account's records")
    args = p.parse_args()
    api_key, api_secret, proxy_url = verify.load_credentials(args)
    try:
        raise SystemExit(asyncio.run(run(args, api_key, api_secret, proxy_url)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
