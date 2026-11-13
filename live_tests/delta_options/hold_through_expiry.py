#!/usr/bin/env python
"""
Hold-through-expiry test of the delta_options connector (Phase 3): how Delta settles an option.

    buy    Buys 1 contract (0.001 BTC) each of two BTC calls of the soonest expiry the bot can still open:
           one about 1.5% in the money, which should settle with a value, and one about 2% out of the money,
           which should expire worthless. It shows the prices and the cost, and asks before buying. Nothing is
           sold or closed afterwards: both positions are left to expire. It trades through the real connector,
           with the dead-man switch on, as a bot would.
    check  Read-only. After the expiry, reads what Delta did with the two options: each one's state and
           settlement price, every fill and wallet entry on them since the buy, whether the positions are gone,
           and the balance. Run it again later if something is not there yet.
    buy --stay
           Buys as above, then keeps the connector running through the expiry, as a bot would, and checks what
           the connector does by itself: OptionExpired for each option; OptionSettled for each one held, matching
           Delta's settlement fill (price, size, fee, id); the positions gone and the balance updated; and both
           the buy and the settlement in the bot's trade records (a Hummingbot trades database, written to
           probe_output/). It also logs what Delta's private stream sent around the expiry. Run it between 16:00
           and 16:55 IST on the day; it ends a minute or so after the 17:30 IST expiry. Ctrl-C is safe: what was
           bought is saved first, so `check` still works.

TIMES (IST). Delta's BTC options expire every day at 17:30 IST. The connector opens no new position in an
option's last 30 minutes, so `buy` must run before about 16:55 IST to use that day's expiry (later, it picks the
next day's and says so). Run `check` from about 17:45 IST.

COST. The in-the-money call costs about 1-2 USD, which comes back at settlement unless BTC falls below its
strike by the expiry. The out-of-the-money call costs cents. Fees are about 0.01 USD per buy. Worst case: both
premiums.

FILES. `buy` saves what it bought to probe_output/hold_<UTC time>.json and probe_output/hold_latest.json; `check`
reads hold_latest.json (or --state FILE) and saves every response to probe_output/hold_check_<UTC time>.json.
`buy --stay` also writes probe_output/hold_stay_<UTC time>.json (the connector's events, Delta's records, the private
stream's messages) and probe_output/hold_stay_<UTC time>.sqlite (the trade records).

KEYS. As verify_delta_options.py: the encrypted conf/connectors/delta_perpetual.yml (asks for the Hummingbot
password; the proxy comes too), or DELTA_API_KEY / DELTA_API_SECRET (+ DELTA_PROXY_URL) with --from-env.

USAGE (in the hummingbot conda env, from any directory)
    python hold_through_expiry.py buy
    python hold_through_expiry.py check
    python hold_through_expiry.py buy --stay
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
from hummingbot.core.clock import Clock, ClockMode  # noqa: E402
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType  # noqa: E402
from hummingbot.core.data_type.in_flight_order import OrderState  # noqa: E402
from hummingbot.core.data_type.option_info import OptionType  # noqa: E402
from hummingbot.core.event.event_forwarder import EventForwarder  # noqa: E402
from hummingbot.core.event.events import MarketEvent  # noqa: E402
from hummingbot.core.utils.async_utils import safe_ensure_future  # noqa: E402
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest  # noqa: E402

header, info, ok, fail = verify.header, verify.info, verify.ok, verify.fail

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "probe_output")
LATEST = os.path.join(OUTPUT_DIR, "hold_latest.json")
WALLET_TRANSACTIONS_PATH_URL = "/v2/wallet/transactions"

STOP_BEFORE_EXPIRY_MINUTES = 30  # the connector's default: no new positions in an option's last 30 minutes
MIN_LEAD_MINUTES = STOP_BEFORE_EXPIRY_MINUTES + 5
ITM_TARGET, ITM_MIN = Decimal("0.015"), Decimal("0.01")  # strike about 1.5% below spot, at least 1%
OTM_TARGET, OTM_MIN = Decimal("0.02"), Decimal("0.015")  # strike about 2% above spot, at least 1.5%
CANDIDATES = 3  # strikes streamed per role, in case the nearest one has no seller
SLIPPAGE = Decimal("0.02")  # the buy limit is up to 2% above the ask; it fills at the ask
FILL_TIMEOUT = 30  # seconds; a buy not filled by then is cancelled
BUY_MARGIN = 30  # seconds: no buy starts closer than this to the closing window

# --stay
STAY_MAX_WAIT = 3 * 3600  # refuses an expiry further away than this
STAY_STATUS_INTERVAL = 60  # seconds between status lines
STAY_AFTER_SETTLEMENT = 30  # seconds to stay after the last settlement, for positions and balances to refresh
STAY_TIMEOUT = 20 * 60  # stops waiting this long after the expiry (the connector itself gives up after 15 min)
STREAM_LOG_FROM = 120  # private-stream messages are kept from this many seconds before the expiry

# Every response of `check`, saved at the end for a closer look than the console gives.
RAW = {}


def ist(timestamp, seconds=False):
    return datetime.fromtimestamp(timestamp, tz=verify.IST).strftime(
        "%H:%M:%S IST, %d %b" if seconds else "%H:%M IST, %d %b")


def _dec(value):
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except InvalidOperation:
        return None


def _seconds(value):
    return DeltaOptionsDerivative._parse_timestamp(value)


def _compact(record, limit=1500):
    """One record on one line: a nested product shrinks to its symbol."""
    shown = dict(record)
    if isinstance(shown.get("product"), dict):
        shown["product"] = shown["product"].get("symbol")
    text = json.dumps(shown, default=str, separators=(",", ":"))
    return text if len(text) <= limit else text[:limit] + " ..."


def _moneyness(instrument, spot):
    share = (instrument.strike - spot) / spot
    return f"strike {instrument.strike}, {abs(share):.1%} {'below' if share < 0 else 'above'} spot"


def _read_state(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _write_state(state):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"hold_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json")
    for target in (path, LATEST):
        with open(target, "w") as f:
            json.dump(state, f, indent=1, default=str)
    return path


# ── buy ───────────────────────────────────────────────────────────────────────

async def scout():
    """Delta's live BTC options and the BTC spot price, read without keys."""
    connector = DeltaOptionsDerivative("", "", trading_pairs=[], trading_required=False)
    try:
        await connector.trading_pair_symbol_map()
        chain = connector.get_option_chain("BTC")
        if not chain:
            return [], None
        first = chain[0].trading_pair
        spot = Decimal(str((await connector._orderbook_ds._request_tickers([first]))[first]["spot_price"]))
        return chain, spot
    finally:
        await connector.stop_network()


def choose_candidates(chain, spot, now):
    """
    The soonest expiry the connector can still open positions in, and for it the calls nearest to 1.5% in the money
    and 2% out of the money, a few of each, nearest first. Returns (expiry, in the money, out of the money, the
    expiries skipped as too close).
    """
    calls = [i for i in chain if i.option_type == OptionType.CALL]
    expiries = sorted({i.expiry_timestamp for i in calls})
    too_close = [e for e in expiries if e - now < MIN_LEAD_MINUTES * 60]
    usable = [e for e in expiries if e - now >= MIN_LEAD_MINUTES * 60]
    if not usable:
        return None, [], [], too_close
    expiry = usable[0]
    same = [i for i in calls if i.expiry_timestamp == expiry]
    itm = sorted((i for i in same if i.strike <= spot * (1 - ITM_MIN)),
                 key=lambda i: abs(i.strike - spot * (1 - ITM_TARGET)))[:CANDIDATES]
    otm = sorted((i for i in same if i.strike >= spot * (1 + OTM_MIN)),
                 key=lambda i: abs(i.strike - spot * (1 + OTM_TARGET)))[:CANDIDATES]
    return expiry, itm, otm, too_close


def _first_with_seller(connector, candidates):
    for instrument in candidates:
        bid, ask = verify._book(connector, instrument.trading_pair)
        if ask is not None:
            return instrument, bid, ask
    return None, None, None


def _fee_estimate(connector, pair, amount, price):
    base, quote = pair.split("-")
    fee = connector.get_fee(base, quote, OrderType.LIMIT, TradeType.BUY, PositionAction.OPEN, amount, price,
                            is_maker=False)
    return fee.percent * price * amount


async def _buy_one(connector, events, res, role, instrument):
    """Buys 1 contract with a limit order up to 2% above the ask; cancels it if it has not filled in time."""
    pair = instrument.trading_pair
    one = connector.contract_size(pair)
    price = connector._crossing_price(pair, True, SLIPPAGE)
    if price.is_nan():
        res.check(False, f"{role}: no price to buy {pair} at")
        return None
    cid = connector.buy(pair, one, OrderType.LIMIT, price, position_action=PositionAction.OPEN)
    started = time.time()
    while time.time() - started < FILL_TIMEOUT:
        if sum((f.amount for f in events.fills_of(cid)), Decimal("0")) >= one:
            break
        if verify._state(connector, cid) in (OrderState.FAILED, OrderState.CANCELED):
            break
        await asyncio.sleep(1)
        if time.time() - started > 10:
            await connector._update_order_status()  # recovers a fill the private stream missed
    if verify._state(connector, cid) not in verify.TERMINAL:
        info(f"{role}: not filled in {FILL_TIMEOUT}s; cancelling it")
        connector.cancel(pair, cid)
        await verify._wait_state(connector, cid, verify.TERMINAL, timeout=15)
    fills = events.fills_of(cid)
    if not fills:
        res.check(False, f"{role}: {pair} not bought ({verify._state(connector, cid)}): {events.why(cid)}")
        return None
    amount = sum((f.amount for f in fills), Decimal("0"))
    average = sum((f.price * f.amount for f in fills), Decimal("0")) / amount
    fee = sum((fee.amount for f in fills for fee in f.trade_fee.flat_fees), Decimal("0"))
    res.check(amount == one, f"{role}: bought {amount} BTC of {pair} at {average} (limit {price}); fee {fee} USD")
    position = await verify._poll_position(connector, pair, want_open=True)
    res.check(position is not None, f"{role}: Delta lists the position: {getattr(position, 'amount', None)} BTC "
                                    f"at {getattr(position, 'entry_price', None)}")
    order = verify._order(connector, cid)
    return {
        "role": role,
        "trading_pair": pair,
        "symbol": await connector.exchange_symbol_associated_to_pair(pair),
        "product_id": connector._product_ids[pair],
        "strike": str(instrument.strike),
        "client_order_id": cid,
        "exchange_order_id": getattr(order, "exchange_order_id", None),
        "amount": str(amount),
        "fill_price": str(average),
        "fee": str(fee),
        "fill_ids": [f.exchange_trade_id for f in fills],
    }


async def buy_both(connector, events, res, expiry, spot, itm, otm, assume_yes):
    """Shows the plan, asks, and buys. Returns the test's state, or None if nothing was bought."""
    header("PLAN — 1 contract of each, left to expire")
    await connector._update_balances()
    await connector._update_positions()
    picks = []
    for role, candidates in (("in the money", itm), ("out of the money", otm)):
        instrument, bid, ask = _first_with_seller(connector, candidates)
        if instrument is None:
            info(f"{role}: none of {', '.join(i.trading_pair for i in candidates)} has a seller right now")
            continue
        picks.append((role, instrument, bid, ask))
    if not picks or picks[0][0] != "in the money":
        res.check(False, "no in-the-money call to buy, so the test would show no settlement with a value; "
                         "nothing bought (try again in a few minutes)")
        return None

    collateral = connector.get_buy_collateral_token(picks[0][1].trading_pair)
    cost = fees = Decimal("0")
    for role, instrument, bid, ask in picks:
        pair = instrument.trading_pair
        one = connector.contract_size(pair)
        mark = connector.get_option_info(pair).mark_price
        intrinsic = max(spot - instrument.strike, Decimal("0"))
        fee = _fee_estimate(connector, pair, one, ask)
        cost, fees = cost + ask * one, fees + fee
        info(f"{role}: {pair} ({_moneyness(instrument, spot)})")
        info(f"    ask {ask}, bid {bid}, mark {mark} USD per BTC. 1 contract = {one} BTC: costs about "
             f"{ask * one:.4f} {collateral} + {fee:.4f} fee; worth {intrinsic * one:.4f} {collateral} at expiry "
             f"if BTC stays at {spot}")
        held = connector.get_position(pair)
        if held is not None:
            info(f"    the account already holds {held.amount} BTC of it; that settles too")
    if len(picks) == 1:
        info("out of the money: skipped, nothing to buy; the test still shows a settlement with a value")
    window = expiry - STOP_BEFORE_EXPIRY_MINUTES * 60
    info(f"expiry {ist(expiry)}, in {(expiry - time.time()) / 60:.0f} min; the bot can open positions until "
         f"{ist(window)}")
    available = connector.get_available_balance(collateral)
    info(f"total about {cost:.4f} {collateral} + {fees:.4f} fees; available {available} {collateral}")
    if not assume_yes:
        # Asked in a thread, so the connector's streams keep running while the prompt waits.
        answer = await asyncio.get_running_loop().run_in_executor(
            None, input, f"  Type 'yes' to buy {'these 2 contracts' if len(picks) == 2 else 'this contract'}: ")
        if answer.strip().lower() != "yes":
            info("nothing bought")
            return None
    if time.time() >= window - BUY_MARGIN:
        res.check(False, f"too late for the {ist(expiry)} expiry now; run `buy` again to use the next one")
        return None

    header("BUYING")
    started = time.time()
    balance_before = connector.get_balance(collateral)
    bought = []
    for role, instrument, bid, ask in picks:
        record = await _buy_one(connector, events, res, role, instrument)
        if record is not None:
            bought.append(record)
    if not bought:
        return None
    await connector._update_balances()
    return {
        "bought_at": started,
        "expiry": expiry,
        "spot_at_buy": str(spot),
        "collateral": collateral,
        "balance_before": str(balance_before),
        "balance_after": str(connector.get_balance(collateral)),
        "options": bought,
    }


# ── buy --stay ────────────────────────────────────────────────────────────────

class OptionEvents:
    """
    The connector's OptionExpired and OptionSettled events, with the time each arrived. It holds the forwarders,
    since event listeners are weakly referenced.
    """

    def __init__(self, connector):
        self.expired, self.settled = [], []
        self.captured_from = len(verify._CAPTURED)
        self._connector = connector
        self._forwarders = [(MarketEvent.OptionExpired, EventForwarder(self._recorder(self.expired))),
                            (MarketEvent.OptionSettled, EventForwarder(self._recorder(self.settled)))]
        for event, forwarder in self._forwarders:
            connector.add_listener(event, forwarder)

    @staticmethod
    def _recorder(log):
        return lambda event: log.append((time.time(), event))

    def close(self):
        for event, forwarder in self._forwarders:
            self._connector.remove_listener(event, forwarder)

    def of(self, log, trading_pair):
        return [(arrived, event) for arrived, event in log if event.trading_pair == trading_pair]


def subscribe_to_older_fills_channel(connector):
    """
    For the faster-fills design: Delta's docs say v2/user_trades is faster than the older user_trades channel and,
    unlike it, carries no commission. The run subscribes to the older one as well, read-only, to see whether it does.
    The connector ignores its messages; a rejected subscription is only logged.
    """
    data_source = connector._user_stream_tracker.data_source
    subscribe = data_source._subscribe_channels

    async def subscribe_also(websocket_assistant):
        await subscribe(websocket_assistant)
        await websocket_assistant.send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": [
            {"name": "user_trades", "symbols": [CONSTANTS.CALL_OPTIONS, CONSTANTS.PUT_OPTIONS]}]}}))

    data_source._subscribe_channels = subscribe_also


class StreamLog:
    """
    The private stream's messages, with the time each arrived: fills and order updates from the start (the buy's,
    to compare with REST for faster fills), and everything from shortly before the expiry on.
    """
    EARLY_TYPES = (CONSTANTS.WS_USER_TRADES_CHANNEL, CONSTANTS.WS_ORDERS_CHANNEL, "user_trades")

    def __init__(self, connector, start):
        self.messages = []
        read = connector._iter_user_event_queue

        async def tapped():
            async for message in read():
                if time.time() >= start or message.get("type") in self.EARLY_TYPES:
                    self.messages.append((time.time(), message))
                yield message

        connector._iter_user_event_queue = tapped


class TradeRecords:
    """The bot's trade records as a Hummingbot run keeps them: a MarketsRecorder writing to its own SQLite file."""

    def __init__(self, connector, path):
        from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
        from hummingbot.client.config.config_helpers import ClientConfigAdapter
        from hummingbot.connector.markets_recorder import MarketsRecorder
        from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType

        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.sql = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                        db_path=path)
        self._recorder = MarketsRecorder(sql=self.sql, markets=[connector], config_file_path="hold_through_expiry.py",
                                         strategy_name="hold_through_expiry",
                                         market_data_collection=MarketDataCollectionConfigMap())
        self._recorder.start()

    def fills(self, trading_pairs):
        from hummingbot.model.trade_fill import TradeFill

        columns = ("symbol", "trade_type", "order_type", "position", "price", "amount", "trade_fee_in_quote",
                   "exchange_trade_id", "order_id", "timestamp")
        with self.sql.get_new_session() as session:
            rows = (session.query(TradeFill).filter(TradeFill.symbol.in_(trading_pairs))
                    .order_by(TradeFill.timestamp).all())
            return [{column: getattr(row, column) for column in columns} for row in rows]

    def stop(self):
        self._recorder.stop()


def _strike(trading_pair):
    return trading_pair.split("_")[2]


def _status_line(connector, expiry, held, option_events):
    now = time.time()
    when = (f"expiry in {(expiry - now) / 60:.0f} min" if now < expiry
            else f"{(now - expiry) / 60:.1f} min after the expiry")
    positions = ", ".join(f"{_strike(pair)}: {getattr(connector.get_position(pair), 'amount', 0)}" for pair in held)
    due = ", ".join(f"{_strike(pair)}{' (held)' if entry.held else ''}"
                    for pair, entry in connector._settlements_due.items()) or "none"
    stream = "up" if verify._private_stream_up(connector) else "DOWN"
    return (f"{ist(now, seconds=True)} · {when} · positions {positions} · private stream {stream} · settlements "
            f"looked for: {due} · OptionExpired {len(option_events.expired)}, OptionSettled "
            f"{len(option_events.settled)}")


def _describe(event):
    if isinstance(getattr(event, "price", None), Decimal):
        fee = sum((f.amount for f in event.trade_fee.flat_fees), Decimal("0"))
        return f"{event.trade_type.name} {event.amount} at {event.price}, fee {fee}, id {event.exchange_trade_id}"
    return f"expiry {ist(event.expiry_timestamp)}"


async def stay_through_expiry(connector, state, option_events):
    """Keeps the connector running, as a bot would, until Delta has settled what was bought, or it is too late."""
    expiry = state["expiry"]
    held = [option["trading_pair"] for option in state["options"]]
    header(f"STAYING — the connector keeps running through the {ist(expiry)} expiry, as a bot would")
    info(f"a status line every {STAY_STATUS_INTERVAL / 60:g} min; this ends {STAY_AFTER_SETTLEMENT}s after the last "
         f"settlement. Ctrl-C is safe: what was bought is saved, and `check` still works.")
    last_status = settled_at = None
    shown = {"OptionExpired": 0, "OptionSettled": 0}
    while True:
        for name, log in (("OptionExpired", option_events.expired), ("OptionSettled", option_events.settled)):
            for arrived, event in log[shown[name]:]:
                info(f"{ist(arrived, seconds=True)} {name} {event.trading_pair}: {_describe(event)}")
            shown[name] = len(log)
        now = time.time()
        settled = {event.trading_pair for _, event in option_events.settled}
        still_looking = [pair for pair in held if pair in connector._settlements_due]
        if settled_at is None and all(pair in settled for pair in held) and not still_looking:
            settled_at = now
        if settled_at is not None and now >= settled_at + STAY_AFTER_SETTLEMENT:
            return
        if now >= expiry + STAY_TIMEOUT:
            info(f"stopped waiting, {STAY_TIMEOUT / 60:g} min after the expiry")
            return
        if last_status is None or now - last_status >= STAY_STATUS_INTERVAL:
            info(_status_line(connector, expiry, held, option_events))
            last_status = now
        await asyncio.sleep(1)


async def check_stay(connector, res, state, option_events, order_events, stream, records, stamp):
    """What the connector did by itself through the expiry, against Delta's own records."""
    expiry, options = state["expiry"], state["options"]
    collateral = state.get("collateral", "USD")
    header("WHAT THE CONNECTOR DID — against Delta's records")
    fills = [fill for fill in await connector._read_fills([int(o["product_id"]) for o in options], expiry - 60)
             if fill.get("fill_type") == CONSTANTS.FILL_TYPE_SETTLEMENT]
    products = {}
    for option in options:
        try:
            response = await connector._api_get(path_url=f"{CONSTANTS.PRODUCTS_PATH_URL}/{option['symbol']}",
                                                limit_id=CONSTANTS.PRODUCTS_PATH_URL)
            products[option["symbol"]] = response.get("result") or {}
        except Exception as e:
            products[option["symbol"]] = {"error": str(e)}
    expected_balance = Decimal(state["balance_after"])
    for option in options:
        pair, symbol, amount = option["trading_pair"], option["symbol"], Decimal(option["amount"])
        info(f"{option['role']}: {pair}")
        expired = option_events.of(option_events.expired, pair)
        res.check(len(expired) == 1 and expired[0][1].expiry_timestamp == expiry,
                  f"  OptionExpired raised once ({len(expired)}), "
                  + (f"{expired[0][0] - expiry:+.1f}s from the expiry" if expired else "never"))
        fill = next((f for f in fills if f.get("product_id") == option["product_id"]
                     or f.get("product_symbol") == symbol), None)
        res.check(fill is not None, f"  Delta's settlement fill: {fill.get('side')} {fill.get('size')} at "
                                    f"{fill.get('price')}, commission {fill.get('commission')}, id {fill.get('id')}"
                  if fill is not None else "  Delta lists no settlement fill for it")
        settled = option_events.of(option_events.settled, pair)
        res.check(len(settled) == 1, f"  OptionSettled raised once ({len(settled)})"
                  + (f", {settled[0][0] - expiry:+.1f}s from the expiry" if settled else ""))
        if settled and fill is not None:
            event = settled[0][1]
            fee = sum((f.amount for f in event.trade_fee.flat_fees), Decimal("0"))
            settlement_price = _dec(products[symbol].get("settlement_price"))
            res.check((event.trade_type, event.amount) == (TradeType.SELL, amount),
                      f"  it sells the {amount} held: {event.trade_type.name} {event.amount}")
            res.check(event.price == _dec(fill.get("price")) == settlement_price,
                      f"  at Delta's settlement price: event {event.price}, fill {fill.get('price')}, product "
                      f"{settlement_price}")
            res.check(fee == _dec(fill.get("commission")),
                      f"  with Delta's settlement fee: event {fee}, fill {fill.get('commission')}")
            res.check(event.exchange_trade_id == str(fill.get("id")),
                      f"  carrying the fill's id {event.exchange_trade_id}")
            expected_balance += event.price * event.amount - fee
        position = connector.get_position(pair)
        res.check(position is None, "  the connector no longer lists the position" if position is None
                  else f"  the connector still lists a position: {position.amount}")
    balance = connector.get_balance(collateral)
    res.check(abs(balance - expected_balance) <= Decimal("0.0001"),
              f"the connector's balance {balance} {collateral} = {state['balance_after']} after the buy + payouts "
              f"- fees ({expected_balance:.8f})")
    still_looking = [o["trading_pair"] for o in options if o["trading_pair"] in connector._settlements_due]
    res.check(not still_looking, "the connector stopped looking for the settlements" if not still_looking
              else f"the connector is still looking for: {', '.join(still_looking)}")
    warnings = [m for m in verify._CAPTURED[option_events.captured_from:]
                if "no settlement" in m.lower() or "could not read delta's records" in m.lower()]
    res.check(not warnings, "no settlement warnings from the connector" if not warnings
              else f"settlement warnings: {warnings}")

    header("TRADE RECORDS — as a bot run stores them")
    rows = records.fills([o["trading_pair"] for o in options])
    for option in options:
        pair = option["trading_pair"]
        mine = [row for row in rows if row["symbol"] == pair]
        opening = [row for row in mine if row["order_type"] != "SETTLEMENT"]
        closing = [row for row in mine if row["order_type"] == "SETTLEMENT"]
        res.check(len(opening) == 1 and len(closing) == 1,
                  f"{pair}: {len(opening)} opening fill and {len(closing)} settlement recorded (1 and 1 expected)")
        if opening and closing:
            buy, settle = opening[0], closing[0]
            res.check((settle["trade_type"], settle["position"]) == ("SELL", "CLOSE"),
                      f"  the settlement closes it: {settle['trade_type']} {settle['position']} {settle['amount']} at "
                      f"{settle['price']}, fee {settle['trade_fee_in_quote']}")
            result = ((Decimal(str(settle["price"])) - Decimal(str(buy["price"]))) * Decimal(str(buy["amount"]))
                      - Decimal(str(buy["trade_fee_in_quote"])) - Decimal(str(settle["trade_fee_in_quote"])))
            info(f"  bought at {buy['price']}, settled at {settle['price']}: {result:+.8f} {collateral} "
                 f"after both fees")
    info(f"the records are in {records.path}")

    # For faster fills (market making): the stream announces a fill seconds before REST, which carries the fee.
    header("BUY FILLS ON THE STREAM — against REST, for faster fills")
    stream_fills = [(arrived, message) for arrived, message in stream.messages
                    if arrived < expiry - STREAM_LOG_FROM and message.get("type") == CONSTANTS.WS_USER_TRADES_CHANNEL]
    for arrived, message in stream_fills[:6]:
        info(f"{ist(arrived, seconds=True)} {_compact(message, 400)}")
    if not stream_fills:
        info("no v2/user_trades message for the buys")
    older = [(arrived, message) for arrived, message in stream.messages
             if arrived < expiry - STREAM_LOG_FROM and message.get("type") == "user_trades"]
    for arrived, message in older[:4]:
        info(f"{ist(arrived, seconds=True)} older user_trades channel: {_compact(message, 500)}")
    if older:
        info("the older user_trades channel carries the commission" if any("commission" in m for _, m in older)
             else "the older user_trades channel has no commission either")
    else:
        info("nothing on the older user_trades channel (rejected, or nothing sent)")
    for option in options:
        for fill_id in option["fill_ids"]:
            match = next(((a, m) for a, m in stream_fills if str(m.get("f")) == fill_id), None)
            event = next((f for f in order_events.fills if f.exchange_trade_id == fill_id), None)
            if match is None:
                info(f"{_strike(option['trading_pair'])}: no stream message carries REST's fill id {fill_id}")
            elif event is not None:
                info(f"{_strike(option['trading_pair'])}: the stream and REST share the fill id; the bot recorded the "
                     f"fill {event.timestamp - match[0]:.1f}s after the stream message, once REST had it")

    header("PRIVATE STREAM — what Delta sent from 2 min before the expiry")
    symbols = [o["symbol"] for o in options]
    about_them = [(arrived, message) for arrived, message in stream.messages
                  if arrived >= expiry - STREAM_LOG_FROM and message.get("type") != "margins"
                  and any(symbol in json.dumps(message, default=str) for symbol in symbols)]
    for arrived, message in about_them[:20]:
        info(f"{arrived - expiry:+.1f}s {message.get('type')}: {_compact(message, 400)}")
    if not about_them:
        info("no message about the two options")
    wallet = [arrived for arrived, message in stream.messages if message.get("type") == "margins"]
    info(f"{len(wallet)} wallet (margins) messages" + (f", from {wallet[0] - expiry:+.1f}s" if wallet else ""))
    # Only what came after the expiry: the buys' own trade messages may be in the log too.
    trades = [message for arrived, message in about_them
              if arrived >= expiry and message.get("type") == CONSTANTS.WS_USER_TRADES_CHANNEL]
    info("Delta announced the settlement on v2/user_trades" if trades
         else "no v2/user_trades message after the expiry: only REST shows the settlement")

    saved = os.path.join(OUTPUT_DIR, f"hold_stay_{stamp}.json")
    with open(saved, "w") as f:
        json.dump({
            "state": state,
            "expired": [(arrived, vars(event)) for arrived, event in option_events.expired],
            "settled": [(arrived, {**vars(event), "trade_fee": event.trade_fee.to_json()})
                        for arrived, event in option_events.settled],
            "settlement_fills": fills,
            "products": products,
            "trade_records": rows,
            "stream": stream.messages,
        }, f, indent=1, default=str)
    info(f"events, Delta's records and the stream messages are saved in {saved}")


async def run_buy(args, api_key, api_secret, proxy_url):
    res = verify.Results()
    verify._install_log_capture()
    verify._quiet_app_warnings()
    print(verify._c("1", "delta_options hold-through-expiry test — BUY (real orders; asks before buying)"))
    info(f"now {ist(time.time())}")
    info(f"proxy: {verify._masked(proxy_url)}" if proxy_url else "proxy: none (direct connection)")
    previous = _read_state(LATEST)
    if previous and previous.get("expiry", 0) > time.time() and not args.again:
        fail(f"a test is already waiting for its expiry: {', '.join(o['trading_pair'] for o in previous['options'])}, "
             f"bought {ist(previous['bought_at'])}, expiring {ist(previous['expiry'])}. Run `check` after that "
             f"expiry, or add --again to buy another pair anyway.")
        return 1

    header("CHOOSING — two BTC calls of the soonest expiry the bot can still open")
    chain, spot = await scout()
    if not chain:
        fail("Delta lists no live BTC options")
        return 1
    info(f"BTC spot {spot}")
    expiry, itm, otm, too_close = choose_candidates(chain, spot, time.time())
    for skipped in too_close:
        info(f"skipping the {ist(skipped)} expiry: the bot opens no positions after "
             f"{ist(skipped - STOP_BEFORE_EXPIRY_MINUTES * 60)}")
    if expiry is None or not itm:
        fail("no in-the-money call found" if expiry is not None else "no expiry far enough away")
        return 1
    info(f"using the {ist(expiry)} expiry")
    if args.stay and expiry - time.time() > STAY_MAX_WAIT:
        fail(f"--stay keeps the bot running until the expiry, and the next one is at {ist(expiry)}, "
             f"{(expiry - time.time()) / 3600:.1f} h away. Run `buy --stay` between 16:00 and 16:55 IST on the day.")
        return 1
    info("in the money, nearest first: " + "; ".join(f"{i.trading_pair} ({_moneyness(i, spot)})" for i in itm))
    info("out of the money, nearest first: " + ("; ".join(f"{i.trading_pair} ({_moneyness(i, spot)})" for i in otm)
                                                or "none"))

    pairs = [i.trading_pair for i in itm + otm]
    connector = DeltaOptionsDerivative(
        delta_options_api_key=api_key,
        delta_options_api_secret=api_secret,
        delta_options_proxy_url=proxy_url or "",
        delta_options_allow_selling=False,
        delta_options_dead_man_switch=True,
        delta_options_stop_before_expiry_minutes=STOP_BEFORE_EXPIRY_MINUTES,
        trading_pairs=pairs,
        trading_required=True,
    )
    # Before the clock starts it: the tap wraps what the connector's user-stream listener reads.
    stream = StreamLog(connector, start=expiry - STREAM_LOG_FROM) if args.stay else None
    if args.stay:
        subscribe_to_older_fills_channel(connector)
    clock = Clock(ClockMode.REALTIME, tick_size=1.0)
    clock.__enter__()
    clock.add_iterator(connector)
    clock_task = safe_ensure_future(clock.run())
    stamp = f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
    events = option_events = records = None
    state = path = None
    try:
        await verify.wait_ready(connector, res)
        if connector.ready:
            events = verify.OrderEvents(connector)
            if args.stay:
                option_events = OptionEvents(connector)
                records = TradeRecords(connector, os.path.join(OUTPUT_DIR, f"hold_stay_{stamp}.sqlite"))
            state = await buy_both(connector, events, res, expiry, spot, itm, otm, args.yes)
            if state is not None:
                # Saved at once, so `check` works even if the stay below is cut short.
                path = _write_state(state)
                info(f"what was bought is saved in {path}")
                if args.stay:
                    await stay_through_expiry(connector, state, option_events)
                    await check_stay(connector, res, state, option_events, events, stream, records, stamp)
        else:
            info("the connector never became ready; nothing was bought")
    finally:
        for listener in (events, option_events):
            if listener is not None:
                listener.close()
        if records is not None:
            records.stop()
        header("SHUTDOWN")
        heartbeat_armed = connector._heartbeat_task is not None
        captured = len(verify._CAPTURED)
        clock_task.cancel()
        try:
            await clock_task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            clock.__exit__(None, None, None)
        except Exception:
            pass
        await connector.stop_network()
        info("clock stopped, network stopped")
        if heartbeat_armed:
            failure = next((m for m in verify._CAPTURED[captured:] if "Could not switch off" in m), None)
            res.check(failure is None, "the dead-man switch was switched off (it only ever cancels open orders; "
                                       "the positions stay)" if failure is None
                      else f"the dead-man switch was not switched off: {failure[-250:]}")

    if verify._CAPTURED:
        verify._show_captured()

    if state is not None:
        header("NEXT")
        info(f"bought: {', '.join(o['trading_pair'] for o in state['options'])}; saved in {path}")
        if args.stay:
            info(f"for Delta's wallet entries as well, run:  python {os.path.abspath(__file__)} check")
        else:
            info(f"leave them alone: Delta settles them at {ist(state['expiry'])}")
            info(f"from {ist(state['expiry'] + 15 * 60)} run:  python {os.path.abspath(__file__)} check")

    header("SUMMARY")
    total = res.passed + res.failed
    (ok if res.failed == 0 and state is not None else fail)(
        f"{res.passed}/{total} checks passed" + ("" if state is not None else "; nothing bought"))
    return 0 if res.failed == 0 and state is not None else 1


# ── check ─────────────────────────────────────────────────────────────────────

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


async def _pages(connector, name, path, params, limit_id, max_pages=10):
    """All rows of a cursor-paginated list. Returns the rows and the error that stopped it, if any."""
    rows, params, problem = [], dict(params), None
    for page in range(max_pages):
        try:
            response = await connector._api_get(path_url=path, params=params, is_auth_required=True,
                                                limit_id=limit_id)
        except Exception as e:
            problem = f"page {page + 1} failed: {str(e)[-200:]}"
            break
        rows.extend(response.get("result") or [])
        after = (response.get("meta") or {}).get("after")
        if not after:
            break
        params["after"] = after
    RAW[name] = {"rows": rows, "problem": problem}
    return rows, problem


async def run_check(args, api_key, api_secret, proxy_url):
    res = verify.Results()
    verify._quiet_app_warnings()
    print(verify._c("1", "delta_options hold-through-expiry test — CHECK (read-only)"))
    path = args.state or LATEST
    state = _read_state(path)
    if state is None:
        fail(f"no test found in {path}; run `buy` first")
        return 1
    now = time.time()
    expiry = state["expiry"]
    info(f"now {ist(now)}; the test bought {', '.join(o['trading_pair'] for o in state['options'])} at "
         f"{ist(state['bought_at'])}; expiry {ist(expiry)}")
    info(f"proxy: {verify._masked(proxy_url)}" if proxy_url else "proxy: none (direct connection)")
    early = now < expiry + 5 * 60
    if early:
        info(f"this is before Delta settles ({ist(expiry)}): what is missing shows as 'not yet'. "
             f"Run again from {ist(expiry + 15 * 60)}")
    RAW["state"] = state

    def expect(cond, message):
        if early and not cond:
            info(f"not yet: {message}")
        else:
            res.check(cond, message)

    options = state["options"]
    ids = [int(o["product_id"]) for o in options]
    start = int((state["bought_at"] - 120) * 1e6)
    connector = DeltaOptionsDerivative(
        delta_options_api_key=api_key,
        delta_options_api_secret=api_secret,
        delta_options_proxy_url=proxy_url or "",
        delta_options_dead_man_switch=False,
        trading_pairs=[],
        trading_required=False,
    )
    products = {}
    try:
        header("READING — from Delta")
        for option in options:
            symbol = option["symbol"]
            response, error = await _get(connector, f"product {symbol}", f"{CONSTANTS.PRODUCTS_PATH_URL}/{symbol}",
                                         limit_id=CONSTANTS.PRODUCTS_PATH_URL)
            res.check(error is None, f"read the product {symbol}" + (f": {error[-200:]}" if error else ""))
            products[symbol] = (response or {}).get("result") or {}
        fills, problem = await _pages(connector, "fills", CONSTANTS.FILLS_PATH_URL,
                                      {"product_ids": ",".join(str(i) for i in ids), "start_time": start,
                                       "page_size": CONSTANTS.FILLS_PAGE_SIZE}, limit_id=CONSTANTS.FILLS_PATH_URL)
        res.check(problem is None, f"{len(fills)} fills on the options since the buy, by type: "
                                   f"{dict(Counter(f.get('fill_type') for f in fills))}"
                  + (f" ({problem})" if problem else ""))
        entries, problem = await _pages(connector, "wallet transactions", WALLET_TRANSACTIONS_PATH_URL,
                                        {"start_time": start, "page_size": 50}, limit_id=CONSTANTS.FILLS_PATH_URL)
        res.check(problem is None, f"{len(entries)} wallet entries since the buy, by type: "
                                   f"{dict(Counter(e.get('transaction_type') for e in entries))}"
                  + (f" ({problem})" if problem else ""))
        positions, error = await _get(connector, "positions", CONSTANTS.POSITIONS_PATH_URL,
                                      {"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES}, auth=True,
                                      limit_id=CONSTANTS.POSITIONS_PATH_URL)
        res.check(error is None, "read the open option positions" + (f": {error[-200:]}" if error else ""))
        balances, error = await _get(connector, "balances", CONSTANTS.WALLET_PATH_URL, auth=True,
                                     limit_id=CONSTANTS.WALLET_PATH_URL)
        res.check(error is None, "read the balance" + (f": {error[-200:]}" if error else ""))
    finally:
        await connector.stop_network()

    open_sizes = {p.get("product_symbol"): p.get("size") for p in (positions or {}).get("result") or []}
    collateral = state.get("collateral", "USD")
    usd = next((b for b in (balances or {}).get("result") or []
                if str(b.get("asset_symbol") or "").upper() == collateral), {})
    balance_now = _dec(usd.get("balance"))
    matched, summary = [], []
    expected_total = Decimal("0")
    for option in options:
        symbol, amount = option["symbol"], Decimal(option["amount"])
        header(f"{option['role'].upper()} — {symbol}: strike {option['strike']}, {option['amount']} BTC bought at "
               f"{option['fill_price']}")
        product = products.get(symbol) or {}
        settlement_price = _dec(product.get("settlement_price"))
        expect(product.get("state") == "expired",
               f"Delta's state for it: {product.get('state')} (settlement time {product.get('settlement_time')})")
        value = settlement_price * amount if settlement_price is not None else None
        expect(settlement_price is not None,
               f"settlement price {settlement_price} USD per BTC: worth {value} {collateral} for the {amount} BTC held"
               if settlement_price is not None else "settlement price not set")
        if value is not None:
            expected_total += value
        expect(symbol not in open_sizes, "the position is gone from Delta" if symbol not in open_sizes
               else f"the position is still open on Delta: size {open_sizes[symbol]}")

        mine = [f for f in fills if f.get("product_symbol") == symbol or f.get("product_id") == option["product_id"]]
        buys = [f for f in mine if str(f.get("order_id")) == str(option["exchange_order_id"])]
        others = [f for f in mine if f not in buys]
        bought_fills = "; ".join(f"{f.get('fill_type')} {f.get('side')} {f.get('size')} at {f.get('price')}, "
                                 f"commission {f.get('commission')}" for f in buys)
        info(f"buy fill(s): {bought_fills or 'none listed'}")
        for f in others:
            info(f"OTHER FILL ({f.get('fill_type')}) at {ist(_seconds(f.get('created_at')), seconds=True)}: "
                 f"{_compact(f)}")
        if not others:
            info("no other fill on it (no settlement fill)" + (" yet" if early else ""))

        on_it = [e for e in entries if e.get("product_id") == option["product_id"]
                 or (e.get("meta_data") or {}).get("product_symbol") == symbol]
        matched.extend(on_it)
        later = [e for e in on_it if _seconds(e.get("created_at")) >= expiry - 60]
        for e in later:
            info(f"wallet entry at {ist(_seconds(e.get('created_at')), seconds=True)}: {e.get('transaction_type')} "
                 f"{e.get('amount')} {e.get('asset_symbol')} (balance after: {e.get('balance')}); "
                 f"meta {_compact(e.get('meta_data') or {}, 600)}")
        if not later:
            info("no wallet entry on it since the expiry" + (" yet" if early else ""))
        summary.append(
            f"{option['role']}: settlement price {settlement_price}; fills after the buy: "
            f"{[f.get('fill_type') for f in others] or 'none'}; wallet entries since the expiry: "
            f"{[(e.get('transaction_type'), e.get('amount')) for e in later] or 'none'}; position "
            f"{'gone' if symbol not in open_sizes else 'still open'}")

    unmatched = [e for e in entries if e not in matched]
    if unmatched:
        header("OTHER WALLET ENTRIES since the buy (not on the test's options)")
        info(f"{len(unmatched)}, by type: {dict(Counter(e.get('transaction_type') for e in unmatched))}")
        for e in [e for e in unmatched if _seconds(e.get("created_at")) >= expiry - 60][:5]:
            info(f"at {ist(_seconds(e.get('created_at')), seconds=True)}: {_compact(e, 600)}")

    header("WHAT DELTA DID")
    for line in summary:
        info(line)
    balance_after_buy = _dec(state.get("balance_after"))
    if balance_now is not None and balance_after_buy is not None:
        change = balance_now - balance_after_buy
        info(f"balance {balance_after_buy} after the buy -> {balance_now} now: {change:+} {collateral}; the settlement "
             f"prices say {expected_total:+} {collateral}")
        expect(abs(change - expected_total) <= Decimal("0.05"),
               f"the balance moved by what the settlement prices say (difference {change - expected_total:+}; "
               f"other trading on the account since the buy would also show here)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    saved = os.path.join(OUTPUT_DIR, f"hold_check_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json")
    with open(saved, "w") as f:
        json.dump(RAW, f, indent=1, default=str)
    info(f"every response is saved in {saved}")

    header("SUMMARY")
    total = res.passed + res.failed
    (ok if res.failed == 0 else fail)(f"{res.passed}/{total} checks passed"
                                      + (f"; run again from {ist(expiry + 15 * 60)}" if early else ""))
    return 0 if res.failed == 0 else 1


def main():
    p = argparse.ArgumentParser(description="Hold two BTC calls through an expiry and see how Delta settles them.")
    p.add_argument("action", choices=["buy", "check"],
                   help="buy: before about 16:55 IST (asks first); check: read-only, from about 17:45 IST")
    p.add_argument("--from-env", action="store_true",
                   help="read DELTA_API_KEY / DELTA_API_SECRET (+ DELTA_PROXY_URL) instead of the encrypted conf")
    p.add_argument("--yes", action="store_true", help="buy: skip the confirmation prompt")
    p.add_argument("--again", action="store_true", help="buy: even though an earlier test has not expired yet")
    p.add_argument("--stay", action="store_true",
                   help="buy: keep the connector running through the expiry and check what it does by itself "
                        "(run between 16:00 and 16:55 IST; ends a minute or so after 17:30 IST)")
    p.add_argument("--state", help="check: the file `buy` wrote (default: probe_output/hold_latest.json)")
    args = p.parse_args()
    args.with_keys = True
    api_key, api_secret, proxy_url = verify.load_credentials(args)
    run = run_buy if args.action == "buy" else run_check
    try:
        raise SystemExit(asyncio.run(run(args, api_key, api_secret, proxy_url)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
