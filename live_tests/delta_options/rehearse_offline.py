"""
Offline rehearsal of verify_delta_options.py's --trade and --fill checks.

The REAL DeltaOptionsDerivative runs against a simulated Delta: REST orders/fills/positions/heartbeats are answered
in-process, the private stream is fed into the connector's user stream queue, and a watchdog cancels orders when a
heartbeat expires. Post-only orders that would cross are rejected, as Delta documents. Nothing touches the network.

    python rehearse_offline.py                # every scenario, ~2 min
    python rehearse_offline.py "trade only"   # scenarios whose name contains the words given
"""
import asyncio
import importlib.util
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

sys.path.insert(0, "/home/vinayak/hm/hummingbot")
_spec = importlib.util.spec_from_file_location(
    "verify", "/home/vinayak/hm/temp/connectors/delta_options/verify_delta_options.py")
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS  # noqa: E402
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative  # noqa
from hummingbot.core.data_type.option_info import OptionInfo  # noqa: E402
from hummingbot.core.data_type.order_book import OrderBook  # noqa: E402
from hummingbot.core.data_type.order_book_row import OrderBookRow  # noqa: E402
from hummingbot.core.utils.async_utils import safe_ensure_future  # noqa: E402

# A 3 s heartbeat instead of 30 s, so the kill switch trips quickly.
CONSTANTS.HEARTBEAT_TTL_MS = 3000
CONSTANTS.HEARTBEAT_INTERVAL = 1.0

SPOT = Decimal("110000")
# Options expiring at noon UTC in two and three days: the connector refuses orders on expired ones.
_NOON = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
LATER, SOONER = _NOON + timedelta(days=3), _NOON + timedelta(days=2)
ATM_SYMBOL, OTM_SYMBOL = f"C-BTC-110000-{LATER:%d%m%y}", f"C-BTC-113400-{LATER:%d%m%y}"
ATM, OTM = f"C_BTC_110000_{LATER:%d%m%y}-USD", f"C_BTC_113400_{LATER:%d%m%y}-USD"
# Streamed by the bot, not traded by the test.
OTHER_SYMBOL, OTHER = f"P-BTC-109000-{SOONER:%d%m%y}", f"P_BTC_109000_{SOONER:%d%m%y}-USD"


def product(symbol, product_id, contract_type="call_options", expiry=f"{LATER:%Y-%m-%dT%H:%M:%SZ}"):
    return {
        "id": product_id, "symbol": symbol, "contract_type": contract_type, "state": "live",
        "strike_price": symbol.split("-")[2], "settlement_time": expiry, "contract_value": "0.001",
        "tick_size": "0.1", "underlying_asset": {"symbol": "BTC"}, "quoting_asset": {"symbol": "USD"},
        "settling_asset": {"symbol": "USD"}, "barrier_price": None,
        "maker_commission_rate": "0.0001", "taker_commission_rate": "0.0001",
        "product_specs": {"premium_commission_rate": 0.035},
    }


PRODUCTS = [product(ATM_SYMBOL, 11), product(OTM_SYMBOL, 12),
            product(OTHER_SYMBOL, 13, "put_options", f"{SOONER:%Y-%m-%dT%H:%M:%SZ}")]
# symbol: (bids, asks, mark)
MARKET = {
    ATM_SYMBOL: ([(Decimal("1100"), 10)], [(Decimal("1150"), 10)], Decimal("1125")),
    OTM_SYMBOL: ([(Decimal("180"), 10)], [(Decimal("195"), 10)], Decimal("187")),
    OTHER_SYMBOL: ([(Decimal("900"), 10)], [(Decimal("950"), 10)], Decimal("925")),
}
# Delta's live bands on 2 Nov ran from 0.1 to about 3x the mark.
WIDE_BANDS = {ATM_SYMBOL: (Decimal("0.1"), Decimal("3300")), OTM_SYMBOL: (Decimal("0.1"), Decimal("560")),
              OTHER_SYMBOL: (Decimal("0.1"), Decimal("2700"))}
TIGHT_BANDS = {ATM_SYMBOL: (Decimal("1000"), Decimal("1300")), OTM_SYMBOL: (Decimal("150"), Decimal("220")),
               OTHER_SYMBOL: (Decimal("850"), Decimal("1000"))}


def now_us():
    return int(time.time() * 1e6)


class FakeDelta:
    def __init__(self, connector, bands, naked_short=True, dead_man_works=True, heartbeat_get_needs_user_id=False,
                 create_twice_fails=True, reject_all=False, heartbeat_broken=False, stream_drop_at_trip=None,
                 sticky_trip=False, stale_heartbeats=0, ttl_zero_trips=False):
        # ttl_zero_trips: a ttl-0 acknowledgment trips the switch instead of switching it off.
        self.ttl_zero_trips = ttl_zero_trips
        # stream_drop_at_trip: None, "comes back" (the stream reconnects 1 s later and the connector re-syncs, as
        # the data source does on re-subscribing) or "for good" — as on 3 Nov, when the proxy dropped both
        # WebSockets the moment the dead-man switch fired.
        # sticky_trip: a tripped heartbeat also cancels orders placed afterwards on its options (Delta's actual
        # behaviour is what the live probe finds out).
        self.stream_drop_at_trip = stream_drop_at_trip
        self.sticky_trip = sticky_trip
        self.stream_down = False
        self.c = connector
        self.bands = bands
        self.orders, self.fills, self.positions, self.heartbeats = {}, [], {}, {}
        self.by_id = {p["id"]: p for p in PRODUCTS}
        self.next_id = 1000
        self.naked_short = naked_short
        self.dead_man_works = dead_man_works
        self.heartbeat_get_needs_user_id = heartbeat_get_needs_user_id
        self.create_twice_fails = create_twice_fails
        self.reject_all = reject_all
        self.heartbeat_broken = heartbeat_broken
        self.balance = Decimal("100")
        self.log = []
        for n in range(stale_heartbeats):  # left tripped by earlier runs that could not switch them off
            self.heartbeats[f"hbstale{n}"] = {"symbols": ["C-BTC-1-011126"], "ttl": 30000, "default_ttl": 30000,
                                              "last_ack": time.time() - 3600, "tripped": True}

    def stream(self, message):
        if not self.stream_down:  # a dropped stream loses what Delta sends meanwhile
            self.c._user_stream_tracker.user_stream.put_nowait(message)

    def _stream_comes_back(self):
        self.stream_down = False
        self.c.resync_after_stream_gap()

    # ── REST ──
    async def post(self, path_url, data=None, is_auth_required=False, **kwargs):
        self.log.append(("POST", path_url, data))
        await asyncio.sleep(0.01)
        if path_url == CONSTANTS.ORDERS_PATH_URL:
            return self._place(data)
        if path_url == CONSTANTS.HEARTBEAT_CREATE_PATH_URL:
            if self.heartbeat_broken:
                raise IOError('HTTP status is 400. Error: {"error":{"code":"bad_schema","context":'
                              '{"schema_errors":[{"code":"unknown_field","path":"default_ttl"}]}}}')
            assert "default_ttl" in data and data["impact"] == "products", data
            if data["heartbeat_id"] in self.heartbeats and self.create_twice_fails:
                raise IOError('HTTP status is 400. Error: {"error":{"code":"heartbeat_already_exists"}}')
            self.heartbeats[data["heartbeat_id"]] = {"symbols": data["product_symbols"], "ttl": 0,
                                                     "default_ttl": data["default_ttl"], "last_ack": time.time(),
                                                     "tripped": False}
            return {"success": True, "result": {"heartbeat_id": data["heartbeat_id"], "status": "created"}}
        if path_url == CONSTANTS.HEARTBEAT_PATH_URL:
            heartbeat = self.heartbeats.get(data["heartbeat_id"])
            if heartbeat is None:
                raise IOError('HTTP status is 400. Error: {"error":{"code":"heartbeat_not_found"}}')
            heartbeat.update(ttl=int(data["ttl"]), last_ack=time.time(), tripped=False)
            if self.ttl_zero_trips and int(data["ttl"]) == 0:
                heartbeat["tripped"] = True
                for order in self.open_orders():
                    if order["product_symbol"] in heartbeat["symbols"]:
                        self._cancel(order, "cancelled_due_to_deadman_switch_trigger")
            # As live on 3 Nov: the expiry in ms, and a boolean.
            return {"success": True, "result": {"heartbeat_timestamp": int(time.time() * 1000) + int(data["ttl"]),
                                                "process_enabled": bool(data["ttl"])}}
        raise AssertionError(f"unexpected POST {path_url}")

    def _place(self, data):
        product_ = self.by_id[data["product_id"]]
        symbol = product_["symbol"]
        size, side, price = data["size"], data["side"], Decimal(data["limit_price"])
        assert data["order_type"] == "limit_order" and isinstance(size, int) and size > 0, data
        if self.reject_all:
            raise IOError('HTTP status is 400. Error: {"error":{"code":"insufficient_margin"},"success":false}')
        held = self.positions.get(symbol, 0)
        if data["reduce_only"]:
            if (side == "sell" and held < size) or (side == "buy" and -held < size):
                raise IOError('HTTP status is 400. Error: {"error":{"code":"reduce_only_violated"}}')
        elif side == "sell" and held - size < 0 and not self.naked_short:
            raise IOError('HTTP status is 400. Error: {"error":{"code":"naked_short_restricted"},"success":false}')
        lower, upper = self.bands[symbol]
        if not lower <= price <= upper:
            raise IOError('HTTP status is 400. Error: {"error":{"code":"out_of_bankruptcy"}}')
        bids, asks = MARKET[symbol][0], MARKET[symbol][1]
        crosses = (side == "buy" and price >= asks[0][0]) or (side == "sell" and price <= bids[0][0])
        if data.get("post_only") and crosses:
            raise IOError('HTTP status is 400. Error: {"error":{"code":"post_only_would_take_liquidity"}}')
        order_id = self.next_id
        self.next_id += 1
        order = {"id": order_id, "product_id": product_["id"], "product_symbol": symbol, "size": size,
                 "unfilled_size": size, "side": side, "limit_price": data["limit_price"], "state": "open",
                 "client_order_id": data["client_order_id"], "reduce_only": data["reduce_only"],
                 "post_only": data.get("post_only", False), "created_at": now_us(), "cancellation_reason": None}
        self.orders[order_id] = order
        asyncio.get_running_loop().call_later(0.05, self._after_place, order_id, crosses)
        return {"success": True, "result": dict(order)}

    def _after_place(self, order_id, crosses):
        order = self.orders[order_id]
        if not crosses:
            self.stream({"type": "orders", "action": "create", "order_id": order_id,
                         "client_order_id": order["client_order_id"], "state": "open", "size": order["size"],
                         "unfilled_size": order["size"], "timestamp": now_us()})
            if self.sticky_trip and any(h["tripped"] and order["product_symbol"] in h["symbols"]
                                        for h in self.heartbeats.values()):
                asyncio.get_running_loop().call_later(
                    0.5, self._cancel, order, "cancelled_due_to_deadman_switch_trigger")
            return
        symbol = order["product_symbol"]
        bids, asks = MARKET[symbol][0], MARKET[symbol][1]
        fill_price = asks[0][0] if order["side"] == "buy" else bids[0][0]
        amount = Decimal(order["size"]) * Decimal("0.001")
        commission = (min(Decimal("0.0001") * SPOT * amount, Decimal("0.035") * fill_price * amount)
                      * Decimal("1.18")).quantize(Decimal("0.00000001"))
        fill = {"id": f"fill-{len(self.fills) + 1}", "order_id": str(order_id), "product_id": order["product_id"],
                "product_symbol": symbol, "size": order["size"], "price": str(fill_price),
                "commission": str(commission), "settling_asset_symbol": "USD", "role": "taker",
                "side": order["side"], "created_at": datetime.now(timezone.utc).isoformat()}
        self.fills.append(fill)
        order.update(state="closed", unfilled_size=0)
        self.positions[symbol] = self.positions.get(symbol, 0) + (order["size"] if order["side"] == "buy"
                                                                  else -order["size"])
        self.balance -= commission
        self.stream({"type": "orders", "action": "update", "order_id": order_id,
                     "client_order_id": order["client_order_id"], "state": "closed", "size": order["size"],
                     "unfilled_size": 0, "timestamp": now_us()})
        self.stream({"type": "v2/user_trades", "sy": symbol, "c": order["client_order_id"], "o": order_id,
                     "s": order["size"], "p": str(fill_price), "f": fill["id"], "r": "t"})
        size = self.positions[symbol]
        if size == 0:
            del self.positions[symbol]
            self.stream({"type": "positions", "action": "delete", "symbol": symbol, "size": 0})
        else:
            self.stream({"type": "positions", "action": "update", "symbol": symbol, "size": size,
                         "entry_price": str(fill_price)})
        self.stream({"type": "margins", "asset_symbol": "USD", "balance": str(self.balance),
                     "available_balance": str(self.balance)})

    async def get(self, path_url, params=None, is_auth_required=False, limit_id=None, **kwargs):
        self.log.append(("GET", path_url, params))
        await asyncio.sleep(0.01)
        if path_url == CONSTANTS.FILLS_PATH_URL:
            ids = {int(x) for x in params["product_ids"].split(",")}
            return {"success": True, "meta": {"after": None},
                    "result": [f for f in self.fills if f["product_id"] in ids]}
        if path_url == CONSTANTS.POSITIONS_PATH_URL:
            return {"success": True, "result": [
                {"product_symbol": s, "size": z, "entry_price": "1",
                 "margin": str((Decimal("0.12") * SPOT * Decimal("0.001") * abs(z)) if z < 0 else Decimal("0")),
                 "liquidation_price": "0", "unrealized_pnl": "0"} for s, z in self.positions.items()]}
        if path_url == CONSTANTS.WALLET_PATH_URL:
            # Resting orders hold margin: a buy its premium, a sell to open a share of the underlying plus premium.
            held = sum((Decimal(o["limit_price"]) if o["side"] == "buy"
                        else Decimal("0.10") * SPOT + Decimal(o["limit_price"])) * o["size"] * Decimal("0.001")
                       for o in self.open_orders())
            return {"success": True, "result": [{"asset_symbol": "USD", "balance": str(self.balance),
                                                 "available_balance": str(self.balance - held)}]}
        if path_url == CONSTANTS.HEARTBEAT_PATH_URL:
            if self.heartbeat_get_needs_user_id:
                raise IOError('HTTP status is 400. Error: {"error":{"code":"bad_schema","context":"user_id"}}')
            wanted = (params or {}).get("heartbeat_id")
            return {"success": True, "result": [
                {"id": n, "heartbeat_id": k, "impact": "products", "product_symbols": v["symbols"],
                 "default_ttl": v["default_ttl"], "config": {"1": {"action": "cancel_orders", "unhealthy_count": 1}},
                 "user_id": 1, "state": "tripped" if v["tripped"] else "off" if v["ttl"] == 0 else "on"}
                for n, (k, v) in enumerate(self.heartbeats.items()) if wanted in (None, k)]}
        if path_url == CONSTANTS.ORDERS_PATH_URL:
            ids = {int(x) for x in params["product_ids"].split(",")}
            states = set(params["states"].split(","))
            return {"success": True, "meta": {"after": None}, "result": [
                dict(o) for o in self.orders.values() if o["product_id"] in ids and o["state"] in states]}
        if path_url.startswith("/v2/orders/"):
            order = self.orders.get(int(path_url.rsplit("/", 1)[1]))
            if order is None:
                raise IOError('HTTP status is 404. Error: {"error":{"code":"not_found"}}')
            return {"success": True, "result": dict(order)}
        raise AssertionError(f"unexpected GET {path_url}")

    async def delete(self, path_url, data=None, is_auth_required=False, **kwargs):
        self.log.append(("DELETE", path_url, data))
        await asyncio.sleep(0.01)
        order = self.orders.get(data["id"])
        if order is None or order["state"] != "open":
            raise IOError('HTTP status is 400. Error: {"error":{"code":"open_order_not_found"},"success":false}')
        self._cancel(order, "cancelled_by_user")
        return {"success": True, "result": dict(order)}

    def _cancel(self, order, reason):
        order.update(state="cancelled", cancellation_reason=reason)
        self.stream({"type": "orders", "action": "update", "order_id": order["id"],
                     "client_order_id": order["client_order_id"], "state": "cancelled", "size": order["size"],
                     "unfilled_size": order["size"], "timestamp": now_us()})

    async def watchdog(self):
        while True:
            await asyncio.sleep(0.2)
            for heartbeat in self.heartbeats.values():
                expired = time.time() > heartbeat["last_ack"] + heartbeat["ttl"] / 1000
                if heartbeat["ttl"] and not heartbeat["tripped"] and expired:
                    heartbeat["tripped"] = True
                    if self.stream_drop_at_trip:
                        self.stream_down = True
                        if self.stream_drop_at_trip == "comes back":
                            asyncio.get_running_loop().call_later(1.0, self._stream_comes_back)
                    if self.dead_man_works:
                        for order in self.orders.values():
                            if order["state"] == "open" and order["product_symbol"] in heartbeat["symbols"]:
                                self._cancel(order, "cancelled_due_to_deadman_switch_trigger")

    def open_orders(self):
        return [o for o in self.orders.values() if o["state"] == "open"]


async def scenario(name, trade=True, fill=True, held_other=False, bands=None, **fake_settings):
    print(f"\n\n######## SCENARIO: {name} ########")
    bands = bands or WIDE_BANDS
    verify._CAPTURED.clear()
    connector = DeltaOptionsDerivative(
        "key", "secret", delta_options_allow_selling=True, delta_options_max_short_contracts=1,
        delta_options_dead_man_switch=True, trading_pairs=[ATM, OTM, OTHER], trading_required=True)
    connector._initialize_trading_pair_symbols_from_exchange_info(PRODUCTS)
    for rule in await connector._format_trading_rules(PRODUCTS):
        connector._trading_rules[rule.trading_pair] = rule
    for symbol, (bids, asks, mark) in MARKET.items():
        pair = await connector.trading_pair_associated_to_exchange_symbol(symbol)
        lower, upper = bands[symbol]
        connector._options_trading.initialize_option_info(OptionInfo(
            trading_pair=pair, mark_price=mark, underlying_price=SPOT, price_band_lower=lower, price_band_upper=upper))
        book = OrderBook()
        book.apply_snapshot([OrderBookRow(float(p), float(s) * 0.001, 1) for p, s in bids],
                            [OrderBookRow(float(p), float(s) * 0.001, 1) for p, s in asks], 1)
        connector.order_book_tracker._order_books[pair] = book
    fake = FakeDelta(connector, bands, **fake_settings)
    connector._api_post, connector._api_get, connector._api_delete = fake.post, fake.get, fake.delete
    if held_other:
        fake.positions[OTHER_SYMBOL] = 2
    await connector._update_balances()
    await connector._update_positions()
    connector._user_stream_event_listener_task = safe_ensure_future(connector._user_stream_event_listener())
    connector._heartbeat_task = safe_ensure_future(connector._heartbeat_loop())
    # The status poll runs when the connector re-syncs; nothing ticks the connector here, so no timed polls happen.
    connector._update_time_synchronizer = AsyncMock()
    polling = safe_ensure_future(connector._status_polling_loop())
    watchdog = safe_ensure_future(fake.watchdog())

    class Args:
        pass
    args = Args()
    args.trade, args.fill, args.slippage, args.yes = trade, fill, 0.05, True
    res = verify.Results()
    instruments = [connector.get_instrument(p) for p in (ATM, OTM, OTHER)]
    started = time.time()
    try:
        await verify.run_trading(connector, res, args, instruments, SPOT)
    finally:
        await connector._stop_heartbeat()
        # A stray second heartbeat loop would switch the heartbeat back on within an interval (1 s here).
        await asyncio.sleep(2.5)
        watchdog.cancel()
        polling.cancel()
        connector._user_stream_event_listener_task.cancel()
    print(f"\n  >> {name}: {res.passed} passed, {res.failed} failed in {time.time() - started:.1f}s")
    leftover = fake.open_orders()
    positions = {s: z for s, z in fake.positions.items() if s != OTHER_SYMBOL}
    sent = [(d["side"], d["limit_price"], d.get("post_only", False), d["reduce_only"])
            for method, path, d in fake.log if method == "POST" and path == CONSTANTS.ORDERS_PATH_URL]
    print(f"  >> orders sent to fake Delta (side, price, post_only, reduce_only): {sent}")
    print(f"  >> fills on fake Delta: {len(fake.fills)}; open orders left: {leftover}")
    print(f"  >> test positions left on fake Delta: {positions}; other position: {fake.positions.get(OTHER_SYMBOL)}")
    if verify._CAPTURED:
        verify._show_captured()
    return res, leftover, positions, fake


async def main():
    verify._install_log_capture()
    verify._quiet_app_warnings()  # as run() does
    only = sys.argv[1:]
    plans = {
        "trade only": dict(fill=False, stale_heartbeats=2),
        "trade, sticky trip": dict(fill=False, sticky_trip=True),
        "trade, ttl 0 trips the switch": dict(fill=False, ttl_zero_trips=True),
        "trade, tight band": dict(fill=False, bands=TIGHT_BANDS),
        "trade, naked shorts refused": dict(fill=False, naked_short=False),
        "trade, dead-man switch does nothing": dict(fill=False, dead_man_works=False),
        "trade, every order rejected": dict(fill=False, reject_all=True),
        "trade, heartbeat setup refused": dict(fill=False, heartbeat_broken=True),
        "trade, stream drops at the trip and comes back": dict(fill=False, stream_drop_at_trip="comes back"),
        "trade, stream drops at the trip for good": dict(fill=False, stream_drop_at_trip="for good"),
        "fill, user holds another option": dict(trade=False, held_other=True),
        "trade + fill": dict(),
    }
    outcomes = {}
    for name, settings in plans.items():
        if not only or any(word in name for word in only):
            outcomes[name] = await scenario(name, **settings)
    print("\n\n######## SUMMARY ########")
    for name, (res, leftover, positions, fake) in outcomes.items():
        ttls = [h["ttl"] for h in fake.heartbeats.values()]
        print(f"  {name}: {res.passed} passed / {res.failed} failed; fills {len(fake.fills)}, leftover orders "
              f"{len(leftover)}, test positions {positions}, other position {fake.positions.get(OTHER_SYMBOL)}, "
              f"heartbeat ttl after stop {ttls}")


if __name__ == "__main__":
    asyncio.run(main())
