"""
Offline rehearsal of hold_through_expiry.py. Nothing touches the network.

`buy` runs the REAL DeltaOptionsDerivative against rehearse_offline.py's simulated Delta (orders, fills, positions,
heartbeats answered in-process); the option chain, the clock and the startup are stubbed. `check` reads a simulated
settled Delta: before the expiry, and after it (one settlement fill and wallet entry for the in-the-money call,
nothing for the worthless one). `buy --stay` runs through an expiry a few seconds away, against a simulated Delta
that settles each position as the real one did on 10 Nov, with the bot's trade recorder writing a real SQLite file.
Files go to a new temporary folder (inside $REHEARSE_DIR if set), never to probe_output/.

    python rehearse_hold.py
"""
import asyncio
import builtins
import json
import os
import sys
import tempfile
import time
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import hold_through_expiry as hold  # noqa: E402
import rehearse_offline as sim  # noqa: E402  (its FakeDelta; it also puts the hummingbot repo on sys.path)

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS  # noqa: E402
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative  # noqa
from hummingbot.core.data_type.option_info import OptionInfo  # noqa: E402
from hummingbot.core.data_type.order_book import OrderBook  # noqa: E402
from hummingbot.core.data_type.order_book_row import OrderBookRow  # noqa: E402
from hummingbot.core.utils.async_utils import safe_ensure_future  # noqa: E402

SPOT = Decimal("100000")
NOW = time.time()
CLOSE = datetime.fromtimestamp(NOW + 600, tz=timezone.utc).replace(second=0, microsecond=0)  # too close to use
NEXT = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)


def _symbol(kind, strike, expiry):
    return f"{kind}-BTC-{strike}-{expiry:%d%m%y}"


def _product(kind, strike, expiry, product_id):
    return sim.product(_symbol(kind, strike, expiry), product_id, "call_options" if kind == "C" else "put_options",
                       f"{expiry:%Y-%m-%dT%H:%M:%SZ}")


# symbol: (bids, asks, mark), sizes in contracts. The nearest in-the-money strike has no seller, so the next is bought.
BOOKS = {
    (98500, NEXT): ([(Decimal("1450"), 5)], [], Decimal("1500")),
    (98000, NEXT): ([(Decimal("1980"), 5)], [(Decimal("2060"), 5)], Decimal("2020")),
    (99000, NEXT): ([(Decimal("990"), 5)], [(Decimal("1060"), 5)], Decimal("1025")),
    (102000, NEXT): ([(Decimal("8"), 5)], [(Decimal("12"), 5)], Decimal("10")),
    (101500, NEXT): ([(Decimal("18"), 5)], [(Decimal("24"), 5)], Decimal("21")),
    (103000, NEXT): ([(Decimal("1"), 5)], [(Decimal("3"), 5)], Decimal("2")),
    (97000, NEXT): ([(Decimal("2950"), 5)], [(Decimal("3050"), 5)], Decimal("3000")),
    (100000, NEXT): ([(Decimal("380"), 5)], [(Decimal("420"), 5)], Decimal("400")),
    (98500, CLOSE): ([(Decimal("1480"), 5)], [(Decimal("1520"), 5)], Decimal("1500")),
    (102000, CLOSE): ([(Decimal("0.1"), 5)], [(Decimal("0.5"), 5)], Decimal("0.3")),
}
PRODUCTS = [_product("C", strike, expiry, 100 + n) for n, (strike, expiry) in enumerate(BOOKS)]
PRODUCTS.append(_product("P", 98000, NEXT, 999))
MARKET = {_symbol("C", strike, expiry): book for (strike, expiry), book in BOOKS.items()}
MARKET[_symbol("P", 98000, NEXT)] = ([(Decimal("90"), 5)], [(Decimal("110"), 5)], Decimal("100"))
BANDS = {symbol: (Decimal("0.1"), mark * 3) for symbol, (_, _, mark) in MARKET.items()}
sim.PRODUCTS, sim.MARKET, sim.SPOT = PRODUCTS, MARKET, SPOT
FAKES = []
# The options the simulated Delta lists now; a scenario can swap in another set.
WORLD = {"products": PRODUCTS, "market": MARKET, "bands": BANDS, "settle_index": None}


def use_world(products, market, bands, settle_index=None):
    WORLD.update(products=products, market=market, bands=bands, settle_index=settle_index)
    sim.PRODUCTS, sim.MARKET = products, market


def soon_world(seconds):
    """Calls expiring a few seconds from now, for --stay."""
    expiry = datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc).replace(microsecond=0)
    books = {
        98500: ([(Decimal("1950"), 5)], [(Decimal("2010"), 5)], Decimal("1980")),
        98000: ([(Decimal("2450"), 5)], [(Decimal("2510"), 5)], Decimal("2480")),
        99000: ([(Decimal("1450"), 5)], [(Decimal("1510"), 5)], Decimal("1480")),
        102000: ([(Decimal("1"), 5)], [(Decimal("2"), 5)], Decimal("1.5")),
        101500: ([(Decimal("2"), 5)], [(Decimal("3"), 5)], Decimal("2.5")),
        103000: ([(Decimal("0.1"), 5)], [(Decimal("0.5"), 5)], Decimal("0.3")),
    }
    products = [_product("C", strike, expiry, 500 + n) for n, strike in enumerate(books)]
    market = {_symbol("C", strike, expiry): book for strike, book in books.items()}
    bands = {symbol: (Decimal("0.1"), mark * 3) for symbol, (_, _, mark) in market.items()}
    return products, market, bands


class SettlingDelta(sim.FakeDelta):
    """
    Also settles each position at its option's expiry, as Delta did on 10 Nov 2026: a settlement fill at the option's
    value (worthless ones too), the position deleted on the stream, then the wallet update.
    """

    def __init__(self, connector, bands, settle_index):
        super().__init__(connector, bands)
        self.settle_index = settle_index
        self.settled = {}
        self._task = safe_ensure_future(self._settle_when_due())

    def stop(self):
        self._task.cancel()

    @staticmethod
    def _expiry(product):
        return datetime.fromisoformat(product["settlement_time"].replace("Z", "+00:00")).timestamp()

    def _value(self, product):
        strike = Decimal(product["strike_price"])
        if product["contract_type"] == "call_options":
            return max(self.settle_index - strike, Decimal("0"))
        return max(strike - self.settle_index, Decimal("0"))

    async def _settle_when_due(self):
        while True:
            await asyncio.sleep(0.1)
            for product in sim.PRODUCTS:
                symbol = product["symbol"]
                if symbol in self.settled or time.time() < self._expiry(product) + 0.3:
                    continue
                value = self.settled[symbol] = self._value(product)
                size = self.positions.pop(symbol, 0)
                if size == 0:
                    continue
                amount = Decimal(abs(size)) * Decimal("0.001")
                commission = (min(Decimal("0.0001") * SPOT * amount, Decimal("0.035") * value * amount)
                              * Decimal("1.18")).quantize(Decimal("0.00000001"))
                self.fills.append({
                    "id": f"settle-{symbol}", "fill_type": "settlement", "order_id": "a1b2c3", "role": "taker",
                    "product_id": product["id"], "product_symbol": symbol, "size": abs(size),
                    "side": "sell" if size > 0 else "buy", "price": str(value), "commission": str(commission),
                    "settling_asset_symbol": "USD", "created_at": datetime.now(timezone.utc).isoformat()})
                self.balance += (value * amount if size > 0 else -value * amount) - commission
                self.stream({"type": "positions", "action": "delete", "symbol": symbol, "size": 0})
                self.stream({"type": "margins", "asset_symbol": "USD", "balance": str(self.balance),
                             "available_balance": str(self.balance)})

    async def get(self, path_url, params=None, is_auth_required=False, limit_id=None, **kwargs):
        if path_url.startswith(CONSTANTS.PRODUCTS_PATH_URL + "/"):
            symbol = path_url.rsplit("/", 1)[1]
            product = next(p for p in sim.PRODUCTS if p["symbol"] == symbol)
            settled = symbol in self.settled
            return {"success": True, "result": dict(product, state="expired" if settled else "live",
                                                    settlement_price=str(self.settled[symbol]) if settled else None)}
        return await super().get(path_url, params, is_auth_required, limit_id, **kwargs)


class DummyClock:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def add_iterator(self, iterator):
        pass

    async def run(self):
        await asyncio.sleep(3600)


async def fake_scout():
    connector = DeltaOptionsDerivative("", "", trading_pairs=[], trading_required=False)
    connector._initialize_trading_pair_symbols_from_exchange_info(WORLD["products"])
    return connector.get_option_chain("BTC"), SPOT


async def fake_wait_ready(connector, res, timeout=120):
    """What a real start does, done by hand against the simulated Delta."""
    products, market, bands = WORLD["products"], WORLD["market"], WORLD["bands"]
    connector._initialize_trading_pair_symbols_from_exchange_info(products)
    for rule in await connector._format_trading_rules(products):
        connector._trading_rules[rule.trading_pair] = rule
    for symbol, (bids, asks, mark) in market.items():
        pair = await connector.trading_pair_associated_to_exchange_symbol(symbol)
        if pair not in connector.trading_pairs:
            continue
        lower, upper = bands[symbol]
        connector._options_trading.initialize_option_info(OptionInfo(
            trading_pair=pair, mark_price=mark, underlying_price=SPOT, price_band_lower=lower, price_band_upper=upper))
        book = OrderBook()
        book.apply_snapshot([OrderBookRow(float(p), float(s) * 0.001, 1) for p, s in bids],
                            [OrderBookRow(float(p), float(s) * 0.001, 1) for p, s in asks], 1)
        connector.order_book_tracker._order_books[pair] = book
    if WORLD["settle_index"] is not None:
        fake = SettlingDelta(connector, bands, WORLD["settle_index"])
    else:
        fake = sim.FakeDelta(connector, bands)
    FAKES.append(fake)
    connector._api_post, connector._api_get, connector._api_delete = fake.post, fake.get, fake.delete
    await connector._update_balances()
    await connector._update_positions()
    connector._user_stream_event_listener_task = safe_ensure_future(connector._user_stream_event_listener())
    connector._heartbeat_task = safe_ensure_future(connector._heartbeat_loop())
    connector._update_time_synchronizer = AsyncMock()
    # As start_network does; stop_network cancels both.
    connector._expiry_watch_task = safe_ensure_future(connector._expiry_watch_loop())
    connector._status_polling_task = safe_ensure_future(connector._status_polling_loop())
    res.check(True, f"connector ready (simulated) with {len(connector.trading_pairs)} options")


def _iso(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


class SettledDelta:
    """Answers `check`'s reads, before the expiry (settled=False) or after it."""

    def __init__(self, state, buy_fills, settled):
        self.state, self.settled = state, settled
        self.balance = Decimal(state["balance_after"])
        self.fills = [dict(f, fill_type="normal") for f in buy_fills]
        self.entries = []
        for f in buy_fills:
            created = f["created_at"]
            self.entries.append({"transaction_type": "commission", "amount": f"-{f['commission']}",
                                 "asset_symbol": "USD", "product_id": f["product_id"], "created_at": created,
                                 "meta_data": {"fill_uuid": f["id"], "product_symbol": f["product_symbol"]}})
            self.entries.append({"transaction_type": "cashflow", "amount": str(-Decimal(f["price"]) / 1000),
                                 "asset_symbol": "USD", "product_id": f["product_id"], "created_at": created,
                                 "meta_data": {"product_symbol": f["product_symbol"]}})
        self.entries.append({"transaction_type": "funding", "amount": "0.001", "asset_symbol": "USD", "product_id": 27,
                             "created_at": _iso(state["expiry"] + 1), "meta_data": {"product_symbol": "BTCUSD"}})
        self.prices = {}
        settle_index = SPOT + 500
        for option in state["options"]:
            strike = Decimal(option["strike"])
            self.prices[option["symbol"]] = max(settle_index - strike, Decimal("0"))
        self.positions = {o["symbol"]: 1 for o in state["options"]}
        if settled:
            self.positions = {}
            for option in state["options"]:
                price = self.prices[option["symbol"]]
                if price > 0:  # the in-the-money one: a settlement fill and a wallet entry
                    self.fills.append({"id": "settle-1", "fill_type": "settlement", "side": "sell", "size": 1,
                                       "price": str(price), "commission": "0", "order_id": None,
                                       "product_id": option["product_id"], "product_symbol": option["symbol"],
                                       "created_at": _iso(state["expiry"] + 3)})
                    self.entries.append({"transaction_type": "settlement", "amount": str(price / 1000),
                                         "asset_symbol": "USD", "product_id": option["product_id"],
                                         "created_at": _iso(state["expiry"] + 3), "balance": "x",
                                         "meta_data": {"product_symbol": option["symbol"]}})
                    self.balance += price / 1000

    async def get(self, path_url, params=None, is_auth_required=False, limit_id=None, **kwargs):
        await asyncio.sleep(0.01)
        if path_url.startswith(CONSTANTS.PRODUCTS_PATH_URL + "/"):
            symbol = path_url.rsplit("/", 1)[1]
            return {"success": True, "result": {
                "symbol": symbol, "state": "expired" if self.settled else "live",
                "settlement_time": _iso(self.state["expiry"]),
                "settlement_price": str(self.prices[symbol]) if self.settled else None}}
        if path_url == CONSTANTS.FILLS_PATH_URL:
            ids = {int(x) for x in params["product_ids"].split(",")}
            fills = [f for f in self.fills if f["product_id"] in ids]
            return {"success": True, "meta": {"after": None}, "result": fills}
        if path_url == hold.WALLET_TRANSACTIONS_PATH_URL:
            return {"success": True, "meta": {"after": None}, "result": self.entries}
        if path_url == CONSTANTS.POSITIONS_PATH_URL:
            return {"success": True, "result": [{"product_symbol": s, "size": z} for s, z in self.positions.items()]}
        if path_url == CONSTANTS.WALLET_PATH_URL:
            return {"success": True, "result": [{"asset_symbol": "USD", "balance": str(self.balance),
                                                 "available_balance": str(self.balance)}]}
        raise AssertionError(f"unexpected GET {path_url}")


async def run_check_against(simulated):
    original = hold.DeltaOptionsDerivative

    class Connector(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._api_get = simulated.get

    hold.DeltaOptionsDerivative = Connector
    hold.RAW.clear()
    try:
        return await hold.run_check(Namespace(state=simulated.state_path), "key", "secret", "")
    finally:
        hold.DeltaOptionsDerivative = original


async def main():
    scratch = tempfile.mkdtemp(prefix="rehearse_hold_", dir=os.environ.get("REHEARSE_DIR"))
    hold.OUTPUT_DIR, hold.LATEST = scratch, os.path.join(scratch, "hold_latest.json")
    hold.scout, hold.Clock, hold.verify.wait_ready = fake_scout, DummyClock, fake_wait_ready
    DeltaOptionsDerivative.ready = property(lambda self: True)
    # A real Clock's ticks set the connector's time; without one it stays NaN, and the trade recorder refuses events.
    DeltaOptionsDerivative.current_timestamp = property(lambda self: time.time())
    outcomes = {}

    print("\n\n######## SCENARIO: buy, answered 'no' ########")
    real_input = builtins.input
    builtins.input = lambda prompt="": "no"
    try:
        code = await hold.run_buy(Namespace(again=False, yes=False, stay=False), "key", "secret", "")
    finally:
        builtins.input = real_input
    sent = [d for m, p, d in FAKES[-1].log if m == "POST" and p == CONSTANTS.ORDERS_PATH_URL]
    outcomes["no"] = (code == 1 and not sent and not os.path.exists(hold.LATEST), f"exit {code}, orders {len(sent)}")

    print("\n\n######## SCENARIO: buy ########")
    code = await hold.run_buy(Namespace(again=False, yes=True, stay=False), "key", "secret", "")
    fake = FAKES[-1]
    sent = [(d["side"], d["limit_price"], d["reduce_only"], d.get("post_only", False))
            for m, p, d in fake.log if m == "POST" and p == CONSTANTS.ORDERS_PATH_URL]
    state = json.load(open(hold.LATEST))
    bought = [(o["trading_pair"], o["fill_price"], o["amount"]) for o in state["options"]]
    ttls = [h["ttl"] for h in fake.heartbeats.values()]
    print(f"  >> orders sent: {sent}\n  >> bought: {bought}\n  >> positions on fake Delta: {fake.positions}"
          f"\n  >> open orders left: {fake.open_orders()}\n  >> heartbeat ttl after stop: {ttls}")
    want = [f"C_BTC_98000_{NEXT:%d%m%y}-USD", f"C_BTC_102000_{NEXT:%d%m%y}-USD"]
    outcomes["buy"] = (code == 0 and [b[0] for b in bought] == want and len(sent) == 2 and not fake.open_orders()
                       and ttls == [0] and all(not r and not p for _, _, r, p in sent),
                       f"exit {code}, bought {[b[0] for b in bought]}")

    print("\n\n######## SCENARIO: buy again before the expiry (refused) ########")
    count = len(FAKES)
    code = await hold.run_buy(Namespace(again=False, yes=True, stay=False), "key", "secret", "")
    outcomes["again refused"] = (code == 1 and len(FAKES) == count, f"exit {code}")

    print("\n\n######## SCENARIO: check before the expiry ########")
    simulated = SettledDelta(state, fake.fills, settled=False)
    simulated.state_path = hold.LATEST
    code = await run_check_against(simulated)
    outcomes["check early"] = (code == 0, f"exit {code}")

    print("\n\n######## SCENARIO: check after the expiry ########")
    shift = state["expiry"] - (time.time() - 20 * 60)
    past = dict(state, expiry=state["expiry"] - shift, bought_at=state["bought_at"] - shift)
    fills = [dict(f, created_at=_iso(datetime.fromisoformat(f["created_at"]).timestamp() - shift)) for f in fake.fills]
    past_path = os.path.join(scratch, "hold_past.json")
    json.dump(past, open(past_path, "w"))
    simulated = SettledDelta(past, fills, settled=True)
    simulated.state_path = past_path
    code = await run_check_against(simulated)
    saved = sorted(n for n in os.listdir(scratch) if n.startswith("hold_check_"))
    outcomes["check settled"] = (code == 0 and len(saved) >= 1, f"exit {code}, saved {saved}")

    print("\n\n######## SCENARIO: every in-the-money candidate without a seller ########")
    for strike in (98000, 99000):
        MARKET[_symbol("C", strike, NEXT)] = (MARKET[_symbol("C", strike, NEXT)][0], [], Decimal("1000"))
    code = await hold.run_buy(Namespace(again=True, yes=True, stay=False), "key", "secret", "")
    sent = [d for m, p, d in FAKES[-1].log if m == "POST" and p == CONSTANTS.ORDERS_PATH_URL]
    outcomes["no seller"] = (code == 1 and not sent, f"exit {code}, orders {len(sent)}")

    print("\n\n######## SCENARIO: buy --stay with the expiry a day away (refused) ########")
    count = len(FAKES)
    code = await hold.run_buy(Namespace(again=True, yes=True, stay=True), "key", "secret", "")
    outcomes["stay refused"] = (code == 1 and len(FAKES) == count, f"exit {code}")

    print("\n\n######## SCENARIO: buy --stay through an expiry 8 seconds away ########")
    use_world(*soon_world(8), settle_index=SPOT + 500)
    hold.MIN_LEAD_MINUTES = hold.STOP_BEFORE_EXPIRY_MINUTES = hold.BUY_MARGIN = 0
    hold.STAY_STATUS_INTERVAL, hold.STAY_AFTER_SETTLEMENT, hold.STAY_TIMEOUT = 2, 2, 60
    hold.STREAM_LOG_FROM = 2  # the buys come seconds before this expiry; keep them in the "buy fills" part
    DeltaOptionsDerivative.EXPIRY_CHECK_INTERVAL = 0.5
    CONSTANTS.SETTLEMENT_READ_DELAY, CONSTANTS.SETTLEMENT_POLL_INTERVAL = 0.5, 1.0
    started = time.time()
    code = await hold.run_buy(Namespace(again=True, yes=True, stay=True), "key", "secret", "")
    FAKES[-1].stop()
    stay_files = sorted(n for n in os.listdir(scratch) if n.startswith("hold_stay_"))
    saved = json.load(open(os.path.join(scratch, next(n for n in stay_files if n.endswith(".json")))))
    outcomes["stay"] = (code == 0 and any(n.endswith(".sqlite") for n in stay_files) and len(saved["settled"]) == 2
                        and len(saved["trade_records"]) == 4,
                        f"exit {code} after {time.time() - started:.0f}s, settled {len(saved['settled'])}, "
                        f"trade records {len(saved['trade_records'])}, stream messages {len(saved['stream'])}")

    print("\n\n######## SUMMARY ########")
    for name, (passed, detail) in outcomes.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name}: {detail}")
    print(f"  files in {scratch}: {sorted(os.listdir(scratch))}")


if __name__ == "__main__":
    asyncio.run(main())
