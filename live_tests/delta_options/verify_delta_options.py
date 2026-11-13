#!/usr/bin/env python
"""
Live check of the delta_options connector: market data (Phase 1) and trading (Phase 2).

Drives the REAL `DeltaOptionsDerivative` through a Hummingbot Clock, the way a
strategy would.

WHAT IT CHECKS
--------------
1. Option chain: the connector reads every live BTC (or ETH) option from Delta,
   and every expiry is at 12:00 UTC.
2. It picks four options to stream: the call and the put nearest to spot, for the
   two nearest expiries.
3. Startup: the connector becomes `ready` (symbol map, order books, option info;
   with --with-keys also balances, trading rules and the private stream, which
   logs in with `key-auth`).
4. Live data for each option: `ob_l2` book messages keep arriving and the book
   matches a REST snapshot; the `ticker` keeps sending mark price, IV and Greeks,
   and they make sense (call delta 0..1, put delta -1..0, mark inside the band).
5. Expiry safety, simulated: one option is marked expired and the public
   WebSocket is dropped. After the reconnect it must be left out of the
   subscription while the others keep streaming.
6. --minutes N: keeps watching, printing a status line every 30 s. Run it across
   12:00 UTC (17:30 IST) to see a real expiry: the expired option goes quiet, the
   WebSocket is dropped once more, and the rest must keep streaming.
7. --trade: no trading, only orders that rest and get cancelled. On the ATM
   call of the later expiry:
     - a post-only BUY at a fifth of the bid and a post-only SELL at 2.5 times
       the ask rest on Delta; each must be confirmed (the connector's state and
       created event, and Delta's own copy of it: side, size, price, post-only,
       open), then cancelled and confirmed again;
     - refused before anything is sent: a MARKET order, a sell over the
       1-contract short limit, a buy above Delta's price band, and (with the
       default setting) any sell to open;
     - kill switch: the bot stops its heartbeats on purpose with a post-only BUY
       resting; Delta must cancel that order about 30 s after the last
       heartbeat (read from Delta over REST), and the connector must show it
       cancelled too, even if its private stream dropped meanwhile. While the
       heartbeat is still tripped, one more post-only BUY shows whether Delta
       goes on cancelling new orders (sticky) or only cancelled once;
     - at the end nothing has filled, Delta lists none of the test orders and
       the balance is unchanged.
   Delta's heartbeat records are listed, and what Delta holds while the orders
   rest is compared with the connector's margin estimate.
8. --fill: two real round trips of 1 contract each,
     a. buy that ATM call while the private stream is paused (the fill must be
        recovered over REST), then close it with the kill switch, flatten(),
        while the stream runs;
     b. sell a ~3% out-of-the-money call to open a short, then buy it back.
   Each fill's commission is compared with the connector's fee estimate, and the
   margin Delta blocks for the short with the connector's margin estimate.
9. Last, with --trade or --fill: the dead-man switch is switched off as a clean
   stop does it, with a post-only BUY resting, and Delta must leave that order
   alone past the 30 s TTL. The switch is then armed again, and the shutdown,
   where the clock's stop and the script's overlap, must switch it off too.

SAFETY
------
* Read-only unless --trade or --fill is given (both imply --with-keys); --fill
  asks for confirmation (skip with --yes). A read-only run sets up no
  dead-man switch either.
* --trade orders cannot fill: they are post-only, which Delta rejects rather
  than let take liquidity, and rest at a fifth of the bid / 2.5 times the ask
  for a few seconds (the kill-switch one for ~30 s). An order the band would
  bring within half / 1.5 times the book is not placed. Each is cancelled.
* --fill trades 1 contract (0.001 BTC) at a time; finally-blocks cancel leftover
  orders and close only the positions the test opened. It needs the two
  options flat to start.
* The connector refuses market orders, and refuses selling to open unless it is
  enabled; this script enables it, capped at 1 contract.

KEYS (only with --with-keys; --trade and --fill imply it)
----------------------------------------------------------
* default:    the encrypted conf/connectors/delta_perpetual.yml (same Delta
              account; asks for the Hummingbot password; the proxy comes too).
              Set by `connect delta_perpetual` in Hummingbot.
* --from-env: DELTA_API_KEY / DELTA_API_SECRET (optional DELTA_PROXY_URL)
* --testnet:  Delta's India testnet; needs demo.delta.exchange keys via --from-env
A bot trading options uses its own keys and settings: `connect delta_options`.

USAGE (in the hummingbot conda env, from any directory)
    python verify_delta_options.py                    # public data, no keys, ~2 min
    python verify_delta_options.py --with-keys        # + private stream, balances, positions
    python verify_delta_options.py --minutes 45       # then watch; start ~17:15 IST to cross an expiry
    python verify_delta_options.py --trade            # post-only orders that rest and get cancelled, ~4 min
    python verify_delta_options.py --fill             # two real 1-contract round trips, asks first
    python verify_delta_options.py --testnet --from-env --trade --fill
    python verify_delta_options.py --underlying ETH
"""
import argparse
import asyncio
import dataclasses
import getpass
import logging
import os
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from urllib.parse import urlsplit

warnings.filterwarnings("ignore")

# ── Locate the hummingbot repo root so this runs from anywhere ────────────────
_d = os.path.dirname(os.path.abspath(__file__))
for _ in range(6):
    _cand = os.path.join(_d, "hummingbot")
    if os.path.isdir(os.path.join(_cand, "hummingbot", "connector")):
        if _cand not in sys.path:
            sys.path.insert(0, _cand)
        break
    _d = os.path.dirname(_d)

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
from hummingbot.logger import NETWORK  # noqa: E402

_USE_COLOR = os.isatty(1)
IST = timezone(timedelta(hours=5, minutes=30))


def _c(code, text):
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def ok(m):
    print(f"  {_c('32', 'PASS')}  {m}")


def fail(m):
    print(f"  {_c('31', 'FAIL')}  {m}")


def info(m):
    print(f"  {_c('36', '·')}  {m}")


def header(t):
    print("\n" + _c("1", f"=== {t} ==="))


class Results:
    def __init__(self):
        self.passed = self.failed = 0

    def check(self, cond, msg):
        (ok if cond else fail)(msg)
        if cond:
            self.passed += 1
        else:
            self.failed += 1
        return cond


async def wait_for(predicate, timeout, interval=1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def when(timestamp):
    utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return f"{utc:%d %b %H:%M} UTC ({utc.astimezone(IST):%H:%M} IST)"


def _masked(url):
    password = urlsplit(url).password
    return url.replace(f":{password}@", ":****@") if password else url


# The connector's own WARNING/ERROR logs and its network restarts, with times, shown at the end for diagnosis.
_CAPTURED = []


class _CaptureHandler(logging.Handler):
    def emit(self, record):
        try:
            message = record.getMessage()
            error = record.exc_info[1] if record.exc_info else None
            if error is not None:
                # Network errors are logged below INFO, with the exchange's answer only in the exception.
                message += f" [{type(error).__name__}: {error}]"
            if record.levelno >= logging.WARNING or error is not None or "Network status" in message:
                stamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%H:%M:%S.%f")[:12]
                _CAPTURED.append(f"{stamp} UTC {record.levelname}: {message}")
        except Exception:
            pass


def _install_log_capture():
    handler = _CaptureHandler(level=NETWORK)
    for name in ("hummingbot.connector", "hummingbot.core.data_type", "asyncio"):
        logging.getLogger(name).addHandler(handler)
    # The network check that stops and restarts the connector logs at INFO; its errors (rejected orders, the
    # dead-man switch) at NETWORK, just below. Those are captured only: the console keeps INFO and above.
    DeltaOptionsDerivative.logger().setLevel(NETWORK)
    for console in logging.getLogger().handlers:
        console.addFilter(lambda record: record.levelno >= logging.INFO)


def _show_captured():
    header("CONNECTOR WARNINGS / ERRORS (for diagnosis)")
    seen = {}  # message: (first time, count); a retry loop repeats the same error
    for entry in _CAPTURED:
        stamp, _, message = entry.partition(" UTC ")
        first, count = seen.get(message, (stamp, 0))
        seen[message] = (first, count + 1)
    for message, (first, count) in seen.items():
        info(f"{first} UTC {message[:300]}" + (f"  (x{count})" if count > 1 else ""))


def _quiet_app_warnings():
    """
    Errors such as a rejected order carry an app warning, and the first one would build the whole Hummingbot client
    (its config, a trading-pair fetch from every exchange, the terminal UI), blocking the event loop for seconds. A
    stub app takes them instead; they are still logged.
    """
    from hummingbot.client.hummingbot_application import HummingbotApplication

    class _NoApp:
        def add_application_warning(self, warning):
            pass

        def notify(self, msg):
            pass

    HummingbotApplication._main_app = _NoApp()


class StreamTap:
    """
    Records when each option last got a book or ticker message, records the symbols of every subscription the
    connector sends, and lets the script drop the public WebSocket.
    """

    def __init__(self, connector):
        self.book_times = {}
        self.ticker_times = {}
        self.subscriptions = []
        self._drop = asyncio.Event()
        self._send = None
        ds = connector._orderbook_ds
        parse_book, parse_ticker, connect, process = (
            ds._parse_order_book_snapshot_message, ds._parse_option_info_message,
            ds._connected_websocket_assistant, ds._process_websocket_messages)

        async def on_book(raw_message, message_queue):
            self.book_times[raw_message.get("sy")] = time.time()
            await parse_book(raw_message=raw_message, message_queue=message_queue)

        async def on_ticker(raw_message, message_queue):
            for entry in raw_message.get("d") or []:
                self.ticker_times[entry.get("s")] = time.time()
            await parse_ticker(raw_message=raw_message, message_queue=message_queue)

        async def on_connect():
            ws = await connect()
            send = ws.send

            async def recording_send(request):
                payload = getattr(request, "payload", None) or {}
                if payload.get("type") == "subscribe":
                    self.subscriptions.append(list(payload["payload"]["channels"][0]["symbols"]))
                await send(request)

            ws.send = recording_send
            self._send = send
            return ws

        async def on_process(websocket_assistant):
            # A drop cancels the reader rather than closing the socket under it, which hummingbot's WebSocket
            # layer reports as an error; the connector then disconnects and reconnects as it would after a drop.
            self._drop = asyncio.Event()
            reading = asyncio.ensure_future(process(websocket_assistant=websocket_assistant))
            dropping = asyncio.ensure_future(self._drop.wait())
            try:
                await asyncio.wait({reading, dropping}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                reading.cancel()
                dropping.cancel()
            await asyncio.gather(reading, dropping, return_exceptions=True)
            if not reading.cancelled() and reading.exception() is not None:
                raise reading.exception()

        ds._parse_order_book_snapshot_message = on_book
        ds._parse_option_info_message = on_ticker
        ds._connected_websocket_assistant = on_connect
        ds._process_websocket_messages = on_process

    def book_age(self, symbol):
        return time.time() - self.book_times.get(symbol, 0)

    def ticker_age(self, symbol):
        return time.time() - self.ticker_times.get(symbol, 0)

    async def resubscribe(self, symbol):
        """Adds a symbol back on the open connection; Delta accepts extra subscribe messages."""
        channels = [{"name": name, "symbols": [symbol]} for name in (
            CONSTANTS.WS_ORDER_BOOK_CHANNEL, CONSTANTS.WS_TRADES_CHANNEL, CONSTANTS.WS_TICKER_CHANNEL)]
        await self._send(WSJSONRequest(payload={"type": "subscribe", "payload": {"channels": channels}}))

    async def reconnect(self, timeout=30):
        """Drops the public WebSocket; returns the symbols of the next subscription, or None."""
        count = len(self.subscriptions)
        self._drop.set()
        if await wait_for(lambda: len(self.subscriptions) > count, timeout=timeout, interval=0.5):
            return self.subscriptions[-1]
        return None


async def pick_options(res, underlying, domain, with_otm_call):
    header(f"OPTION CHAIN — Delta's live {underlying} options")
    scout = DeltaOptionsDerivative("", "", trading_pairs=[], trading_required=False, domain=domain)
    try:
        await scout.trading_pair_symbol_map()
        chain = scout.get_option_chain(underlying)
        expiries = sorted({i.expiry_timestamp for i in chain})
        res.check(len(chain) > 0, f"{len(chain)} live {underlying} options across {len(expiries)} expiries")
        if not chain:
            return [], None
        off_noon = [i for i in chain if i.expiry_timestamp % 86400 != 43200]
        res.check(not off_noon, "every expiry is at 12:00 UTC" if not off_noon
                  else f"{len(off_noon)} options expire at another time, e.g. {off_noon[0]}")
        info("expiries: " + ", ".join(when(e) for e in expiries[:6]) + (" ..." if len(expiries) > 6 else ""))

        first = chain[0].trading_pair
        spot = Decimal(str((await scout._orderbook_ds._request_tickers([first]))[first]["spot_price"]))
        info(f"{underlying} spot {spot}")
        picked = []
        for expiry in expiries[:2]:
            for option_type in (OptionType.CALL, OptionType.PUT):
                same = [i for i in chain if i.expiry_timestamp == expiry and i.option_type == option_type]
                if same:
                    picked.append(min(same, key=lambda i: abs(i.strike - spot)))
        if with_otm_call:
            # For the short of --fill: the call of the later expiry whose strike is nearest above spot + 3%.
            calls = [i for i in chain if i.expiry_timestamp == expiries[min(1, len(expiries) - 1)]
                     and i.option_type == OptionType.CALL and i.strike >= spot * Decimal("1.03")]
            if calls:
                picked.append(min(calls, key=lambda i: i.strike))
        for i in picked:
            hours = (i.expiry_timestamp - time.time()) / 3600
            info(f"streaming {i.trading_pair}: {i.option_type.value} strike {i.strike}, "
                 f"expires {when(i.expiry_timestamp)}, in {hours:.1f} h")
        return picked, spot
    finally:
        await scout.stop_network()


async def wait_ready(connector, res, timeout=120):
    header("STARTUP — waiting for connector.ready")
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        status = connector.status_dict
        if status != last:
            info("  ".join(f"{'ok' if v else '..'} {k}" for k, v in status.items()))
            last = dict(status)
        if all(status.values()):
            break
        await asyncio.sleep(2)
    res.check(connector.ready, f"connector.ready = {connector.ready}")


def _best(book, side):
    entries = list(book.bid_entries() if side == "bid" else book.ask_entries())
    return Decimal(str(entries[0].price)) if entries else None


async def check_live_data(connector, tap, res, instruments, spot, symbols):
    header("LIVE DATA — books and tickers (after 15s of streaming)")
    await asyncio.sleep(15)
    for instrument in instruments:
        pair, symbol = instrument.trading_pair, symbols[instrument.trading_pair]
        res.check(tap.book_age(symbol) < 15, f"{pair}: ob_l2 messages arriving (last {tap.book_age(symbol):.1f}s ago)")
        res.check(tap.ticker_age(symbol) < 12, f"{pair}: ticker arriving (last {tap.ticker_age(symbol):.1f}s ago)")

        option = connector.get_option_info(pair)
        book = connector.get_order_book(pair)
        bid, ask = _best(book, "bid"), _best(book, "ask")
        info(f"{pair}: book {bid} / {ask} | mark {option.mark_price} | IV mark {option.mark_iv} "
             f"(bid {option.bid_iv} / ask {option.ask_iv})")
        info(f"{' ' * len(pair)}  delta {option.delta} gamma {option.gamma} theta {option.theta} "
             f"vega {option.vega} | band {option.price_band_lower}..{option.price_band_upper} | "
             f"OI {option.open_interest}")

        delta_ok = option.delta is not None and (
            0 <= option.delta <= 1 if instrument.option_type == OptionType.CALL else -1 <= option.delta <= 0)
        res.check(delta_ok, f"{pair}: {instrument.option_type.value} delta {option.delta} has the right sign")
        in_band = (option.price_band_lower is None or option.price_band_upper is None
                   or option.price_band_lower <= option.mark_price <= option.price_band_upper)
        res.check(option.mark_price > 0 and in_band,
                  f"{pair}: mark {option.mark_price} is positive and inside the band")
        drift = abs(option.underlying_price - spot) / spot
        res.check(drift < Decimal("0.01"),
                  f"{pair}: underlying {option.underlying_price} matches spot (drift {drift:.3%})")

        rest = await connector._orderbook_ds._order_book_snapshot(pair)
        rest_bid = max((Decimal(p) for p, _ in rest.content["bids"]), default=None)
        rest_ask = min((Decimal(p) for p, _ in rest.content["asks"]), default=None)
        same = (bid, ask) == (rest_bid, rest_ask)
        close = all(a is None and b is None or (a and b and abs(a - b) / b < Decimal("0.02"))
                    for a, b in ((bid, rest_bid), (ask, rest_ask)))
        res.check(same or close, f"{pair}: WebSocket book {bid}/{ask} matches REST {rest_bid}/{rest_ask}")

        price = ask or option.mark_price
        amount = connector.size_to_amount(pair, Decimal("1"))
        base, quote = pair.split("-")
        fee = connector.get_fee(base, quote, OrderType.LIMIT, TradeType.BUY, PositionAction.OPEN, amount, price)
        info(f"{' ' * len(pair)}  fee estimate for 1 contract ({amount} {instrument.underlying}) at {price}: "
             f"{fee.percent * price * amount:.4f} {quote} ({fee.percent:.2%} of the premium, incl. 18% GST)")


async def check_expiry_safety(connector, tap, res, instruments, symbols):
    header("EXPIRY SAFETY — simulated: one option marked expired, then the WebSocket is dropped")
    # The farthest expiry plays the expired one, so today's options stay streamed for --minutes.
    victim = instruments[-1]
    others = [symbols[i.trading_pair] for i in instruments[:-1]]
    victim_symbol = symbols[victim.trading_pair]
    connector._options_trading._instruments[victim.trading_pair] = dataclasses.replace(
        victim, expiry_timestamp=time.time() - 1)
    info(f"{victim.trading_pair} now looks expired to the connector")
    try:
        symbols_sent = await tap.reconnect()
        res.check(symbols_sent is not None, "public WebSocket reconnected and subscribed again")
        if symbols_sent is None:
            return
        res.check(victim_symbol not in symbols_sent,
                  f"{victim.trading_pair} left out of the new subscription ({len(symbols_sent)} symbols sent)")
        await asyncio.sleep(15)
        res.check(all(tap.book_age(s) < 15 for s in others), "the other options kept streaming after the reconnect")
        res.check(tap.book_age(victim_symbol) > 10,
                  f"{victim.trading_pair} went quiet ({tap.book_age(victim_symbol):.0f}s since its last book message)")
    finally:
        connector._options_trading._instruments[victim.trading_pair] = victim
        # Added back on the open connection: a second forced reconnect ~15s later made the proxy drop every
        # connection, twice in a row.
        await tap.resubscribe(victim_symbol)
        resumed = await wait_for(lambda: tap.book_age(victim_symbol) < 5, timeout=20, interval=0.5)
        res.check(resumed, f"after undoing it, {victim.trading_pair} streams again")


async def check_account(connector, res, pairs):
    header("ACCOUNT DATA — private stream (key-auth), balances, positions, trading rules")
    res.check(connector.status_dict.get("user_stream_initialized"), "private stream logged in with key-auth")
    balances = connector.get_all_balances()
    res.check(len(balances) > 0, f"{len(balances)} balance asset(s): "
                                 f"{', '.join(f'{a} {v}' for a, v in list(balances.items())[:4])}")
    res.check(all(pair in connector.trading_rules for pair in pairs), "trading rules loaded for the streamed options")
    positions = connector.account_positions
    info(f"{len(positions)} open option position(s)")
    for pair, position in positions.items():
        info(f"{pair}: {position.amount} @ {position.entry_price} (uPnL {position.unrealized_pnl})")


def _private_stream_up(connector):
    # The user stream holds its WebSocket only while it is connected and logged in.
    return connector._user_stream_tracker.data_source._ws_assistant is not None


async def watch(connector, tap, res, instruments, symbols, minutes, with_keys):
    header(f"WATCH — {minutes} min, a status line every 30s (Ctrl-C stops the run)")
    end = time.time() + minutes * 60
    expired_seen = set()
    while time.time() < end:
        await asyncio.sleep(min(30, max(end - time.time(), 0)))
        parts = []
        for instrument in instruments:
            pair, symbol = instrument.trading_pair, symbols[instrument.trading_pair]
            option = connector.get_option_info(pair)
            label = f"{instrument.option_type.value[0].upper()}{instrument.strike}"
            state = "EXPIRED" if connector.is_expired(pair) else f"mark {option.mark_price} d {option.delta}"
            parts.append(f"{label} book {tap.book_age(symbol):.0f}s tick {tap.ticker_age(symbol):.0f}s {state}")
        if with_keys:
            parts.append(f"private stream {'up' if _private_stream_up(connector) else 'DOWN'}")
        print(f"  {datetime.now(timezone.utc):%H:%M:%S} UTC  " + " | ".join(parts))

        newly_expired = [i for i in instruments if connector.is_expired(i.trading_pair)
                         and i.trading_pair not in expired_seen]
        for instrument in newly_expired:
            expired_seen.add(instrument.trading_pair)
            info(f"{instrument.trading_pair} expired at {when(instrument.expiry_timestamp)}; "
                 f"dropping the WebSocket to check the resubscription")
            symbols_sent = await tap.reconnect()
            res.check(symbols_sent is not None and symbols[instrument.trading_pair] not in symbols_sent,
                      f"real expiry: {instrument.trading_pair} left out after the reconnect ({symbols_sent})")

    live = [i for i in instruments if not connector.is_expired(i.trading_pair)]
    res.check(all(tap.book_age(symbols[i.trading_pair]) < 30 for i in live),
              f"every live option was still streaming at the end ({len(live)} of {len(instruments)} live)")
    if not expired_seen:
        info("no option expired during the watch; start ~15 min before 12:00 UTC (17:30 IST) to see one")


# ── Trading: --trade and --fill ───────────────────────────────────────────────

TERMINAL = {OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED}


class OrderEvents:
    """
    The order events a strategy would get from the connector. It holds the forwarders, since event listeners are
    weakly referenced.
    """

    def __init__(self, connector):
        self.created, self.fills, self.cancels, self.failures = [], [], [], []
        self._connector = connector
        self._forwarders = [(MarketEvent.BuyOrderCreated, EventForwarder(self.created.append)),
                            (MarketEvent.SellOrderCreated, EventForwarder(self.created.append)),
                            (MarketEvent.OrderFilled, EventForwarder(self.fills.append)),
                            (MarketEvent.OrderCancelled, EventForwarder(self.cancels.append)),
                            (MarketEvent.OrderFailure, EventForwarder(self.failures.append))]
        for event, forwarder in self._forwarders:
            connector.add_listener(event, forwarder)

    def close(self):
        for event, forwarder in self._forwarders:
            self._connector.remove_listener(event, forwarder)

    def was_created(self, cid):
        return any(e.order_id == cid for e in self.created)

    def was_cancelled(self, cid):
        return any(e.order_id == cid for e in self.cancels)

    def fills_of(self, cid):
        return [f for f in self.fills if f.order_id == cid]

    def why(self, cid):
        """Why an order failed: Delta's error, or the connector's own refusal."""
        failure = next((f for f in self.failures if f.order_id == cid), None)
        if failure is not None and failure.error_message:
            return failure.error_message[-300:]
        related = [m for m in _CAPTURED if cid and cid in m]
        return related[-1][-300:] if related else "no reason captured"


def _dec(value):
    try:
        return Decimal(str(value)) if value is not None else None
    except Exception:
        return None


def _order(connector, cid):
    return connector._order_tracker.fetch_order(client_order_id=cid)


def _state(connector, cid):
    return getattr(_order(connector, cid), "current_state", None)


async def _wait_state(connector, cid, states, timeout=25):
    # buy() and sell() place the order from a task, so it is tracked a moment later.
    await wait_for(lambda: _state(connector, cid) in states, timeout=timeout, interval=0.25)
    return _state(connector, cid)


async def _delta_order(connector, cid):
    """The order as Delta has it, read by its exchange id."""
    response = await connector._api_get(
        path_url=CONSTANTS.ORDER_BY_ID_PATH_URL.format(order_id=_order(connector, cid).exchange_order_id),
        is_auth_required=True, limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL)
    return response.get("result") or {}


async def _poll_delta_order(connector, cid, timeout, interval=2.0):
    """Reads the order from Delta until it is no longer open; returns the last copy read and the last read error."""
    deadline = time.time() + timeout
    order, error = {}, None
    while time.time() < deadline:
        try:
            order, error = await _delta_order(connector, cid), None
        except Exception as e:
            error = str(e)[-200:]
        if order.get("state") not in (None, "open", "pending"):
            break
        await asyncio.sleep(interval)
    return order, error


def _hms(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%H:%M:%S.%f")[:12] + " UTC"


async def _bot_orders_open_on_delta(connector, pair):
    """The bot's orders that Delta still lists as open on the option (the test's, by their client id prefix)."""
    response = await connector._api_get(
        path_url=CONSTANTS.ORDERS_PATH_URL,
        params={"product_ids": str(connector._product_ids[pair]), "states": "open,pending"}, is_auth_required=True)
    return [o for o in response.get("result") or []
            if str(o.get("client_order_id") or "").startswith(connector.client_order_id_prefix)]


def _on_tick(connector, pair, price, up):
    tick = connector.trading_rules[pair].min_price_increment
    return max((price / tick).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * tick, tick)


def _book(connector, pair):
    book = connector.get_order_book(pair)
    return _best(book, "bid"), _best(book, "ask")


def _resting_price(connector, pair, side):
    """
    A price for a test order that must not fill: a fifth of the bid for a buy, 2.5 times the ask for a sell, kept
    inside Delta's price band. None if the band would bring it within half (buy) or 1.5 times (sell) the book.
    """
    option = connector.get_option_info(pair)
    bid, ask = _book(connector, pair)
    if side == TradeType.BUY:
        reference = bid or option.mark_price
        price = _on_tick(connector, pair, reference / 5, up=False)
        if option.price_band_lower is not None:
            price = max(price, _on_tick(connector, pair, option.price_band_lower, up=True))
        return price if price <= reference / 2 else None
    reference = ask or option.mark_price
    price = _on_tick(connector, pair, reference * Decimal("2.5"), up=True)
    if option.price_band_upper is not None:
        price = min(price, _on_tick(connector, pair, option.price_band_upper, up=False))
    return price if price >= reference * Decimal("1.5") else None


def _trade_pairs(instruments, spot):
    """The ATM call of the later expiry (it can't expire during the test) and, if streamed, the ~3% OTM call."""
    calls = [i for i in instruments if i.option_type == OptionType.CALL]
    if not calls:
        return None, None
    expiry = max(i.expiry_timestamp for i in calls)
    same = [i for i in calls if i.expiry_timestamp == expiry]
    atm = min(same, key=lambda i: abs(i.strike - spot))
    otm = next((i for i in same if i is not atm and i.strike >= spot * Decimal("1.03")), None)
    return atm.trading_pair, otm.trading_pair if otm else None


async def _place_resting(connector, events, res, label, side, pair, amount, price):
    """
    Places a post-only order to open, which Delta rejects rather than let it fill on arrival, and waits until it
    rests or ends. Returns its client id and whether it rests.
    """
    place = connector.buy if side == TradeType.BUY else connector.sell
    cid = place(pair, amount, OrderType.LIMIT_MAKER, price, position_action=PositionAction.OPEN)
    state = await _wait_state(connector, cid, {OrderState.OPEN, OrderState.PARTIALLY_FILLED} | TERMINAL)
    is_open = state == OrderState.OPEN
    exchange_order_id = getattr(_order(connector, cid), "exchange_order_id", None)
    res.check(is_open, f"{label} at {price} rests on Delta ({cid}, Delta id {exchange_order_id})" if is_open
              else f"{label} at {price} did not rest ({state}): {events.why(cid)}")
    if is_open:
        created = await wait_for(lambda: events.was_created(cid), timeout=5, interval=0.25)
        res.check(created, f"{label}: the connector told the strategy it was created "
                           f"({side.name.capitalize()}OrderCreated)")
    return cid, is_open


async def _cancel_leftovers(connector, pair, cids):
    """Safety net: cancels any test order still open. A cancel waits for an order that is still being placed."""
    leftover = [cid for cid in cids
                if cid and _order(connector, cid) is not None and _state(connector, cid) not in TERMINAL]
    if not leftover:
        return
    header("SAFETY — cancelling leftover test orders")
    for cid in leftover:
        connector.cancel(pair, cid)
    for cid in leftover:
        state = await _wait_state(connector, cid, TERMINAL, timeout=20)
        (ok if state in TERMINAL else fail)(f"{cid}: {state}")


async def run_order_checks(connector, res, events, pair):
    header(f"ORDERS — a BUY and a SELL rest on {pair}, are confirmed, then cancelled")
    one = connector.contract_size(pair)
    option = connector.get_option_info(pair)
    bid, ask = _book(connector, pair)
    info(f"book {bid} / {ask} | mark {option.mark_price} | band {option.price_band_lower}..{option.price_band_upper}")
    collateral = connector.get_sell_collateral_token(pair)
    orders = []
    for side in (TradeType.BUY, TradeType.SELL):
        price = _resting_price(connector, pair, side)
        if price is None:
            res.check(False, f"{side.name}: Delta's price band keeps it too close to the market; not placed")
        else:
            orders.append((side, price))
    if not orders:
        return
    info("post-only, 1 contract (" + str(one) + ") each: " + ", ".join(f"{s.name} at {p}" for s, p in orders)
         + "; Delta rejects a post-only order rather than fill it on arrival")
    sell_margin = connector.estimate_short_margin(pair, one, option.mark_price)
    if any(side == TradeType.SELL for side, _ in orders):
        info(f"the SELL would open a short, so Delta blocks margin while it rests: about {sell_margin:.2f} "
             f"{collateral} by the connector's estimate ({connector.get_available_balance(collateral)} available)")
    await connector._update_balances()
    available_before = connector.get_available_balance(collateral)
    placed = []
    try:
        for side, price in orders:
            cid, _ = await _place_resting(connector, events, res, side.name, side, pair, one, price)
            placed.append((side, price, cid))
            if side == TradeType.SELL and "naked_short_restricted" in events.why(cid):
                info("Delta does not let this account sell options it doesn't hold (naked_short_restricted)")
        resting = [(side, price) for side, price, cid in placed if _state(connector, cid) == OrderState.OPEN]
        if resting:
            # What Delta blocks for resting orders calibrates the margin estimate without any fill.
            await asyncio.sleep(2)
            await connector._update_balances()
            held = available_before - connector.get_available_balance(collateral)
            parts = [f"BUY premium {price * one}" if side == TradeType.BUY
                     else f"SELL margin estimate {sell_margin:.4f}" for side, price in resting]
            # Delta margins options by portfolio risk at mark prices, so an order resting far from the mark is credited
            # for its price: a low number here does not mean a filled short needs as little.
            info(f"while {' and '.join(s.name for s, _ in resting)} rest, Delta holds {held:.4f} {collateral} of the "
                 f"{available_before} available (the connector expects about: {', '.join(parts)})")

        header("ORDERS — Delta's copy of each order")
        # Delta's order object has no post_only field (live, and per its API reference); the connector's unit tests
        # check that it is sent.
        for side, price, cid in placed:
            if _state(connector, cid) != OrderState.OPEN:
                continue
            order = await _delta_order(connector, cid)
            as_sent = (order.get("client_order_id") == cid and order.get("side") == side.name.lower()
                       and int(order.get("size") or 0) == 1 and _dec(order.get("limit_price")) == price
                       and not order.get("reduce_only") and order.get("state") == "open")
            res.check(as_sent, f"{side.name}: id {order.get('id')}, {order.get('size')} contract "
                               f"{order.get('side')} at {order.get('limit_price')}, {order.get('order_type')} "
                               f"{order.get('time_in_force')}, reduce_only {order.get('reduce_only')}, "
                               f"{order.get('state')}, unfilled {order.get('unfilled_size')}")

        header("ORDERS — cancel")
        for _, _, cid in placed:
            if _state(connector, cid) not in TERMINAL:
                connector.cancel(pair, cid)
        for side, _, cid in placed:
            if getattr(_order(connector, cid), "exchange_order_id", None) is None:
                continue
            state = await _wait_state(connector, cid, TERMINAL, timeout=20)
            order = await _delta_order(connector, cid)
            res.check(state == OrderState.CANCELED and order.get("state") == "cancelled",
                      f"{side.name} cancelled: connector {state}, Delta {order.get('state')} "
                      f"(reason {order.get('cancellation_reason')}), unfilled {order.get('unfilled_size')}")
            if state == OrderState.CANCELED:
                res.check(events.was_cancelled(cid), f"{side.name}: the connector told the strategy it was "
                                                     f"cancelled (OrderCancelled)")
    finally:
        await _cancel_leftovers(connector, pair, [cid for _, _, cid in placed])


async def run_refusal_checks(connector, res, events, pair):
    header("REFUSALS — orders the connector must stop before they reach Delta")
    one = connector.contract_size(pair)
    option = connector.get_option_info(pair)
    # Far above the market and above the band: even if a refusal failed, Delta would reject it or it would rest.
    high = _on_tick(connector, pair, max(option.mark_price * 3, (option.price_band_upper or 0) * Decimal("1.2")),
                    up=True)
    cases = [
        ("a MARKET buy", "MARKET orders are not supported",
         lambda: connector.buy(pair, one, OrderType.MARKET, high, position_action=PositionAction.OPEN)),
        ("selling 2 contracts to open, over the 1-contract short limit", "-contract limit",
         lambda: connector.sell(pair, 2 * one, OrderType.LIMIT, high, position_action=PositionAction.OPEN)),
    ]
    if option.price_band_upper is not None:
        cases.append((f"a BUY at {high}, above the price band (upper {option.price_band_upper})", "price band",
                      lambda: connector.buy(pair, one, OrderType.LIMIT, high, position_action=PositionAction.OPEN)))
    else:
        info("this option has no price band; the band refusal is not checked")
    cids = []
    try:
        for label, expected, place in cases:
            cid = place()
            cids.append(cid)
            state = await _wait_state(connector, cid, TERMINAL, timeout=10)
            reason = events.why(cid)
            never_sent = getattr(_order(connector, cid), "exchange_order_id", None) is None
            res.check(state == OrderState.FAILED and never_sent and expected in reason,
                      f"{label}: refused, nothing sent ({reason[:150]})")

        # Selling to open is off unless a bot enables it; switched off here for a moment to check the default.
        connector._allow_selling = False
        try:
            cid = connector.sell(pair, one, OrderType.LIMIT, high, position_action=PositionAction.OPEN)
            cids.append(cid)
            state = await _wait_state(connector, cid, TERMINAL, timeout=10)
        finally:
            connector._allow_selling = True
        reason = events.why(cid)
        res.check(state == OrderState.FAILED and "not enabled" in reason,
                  f"with selling off (the default), a 1-contract sell to open is refused ({reason[:150]})")
    finally:
        await _cancel_leftovers(connector, pair, cids)


async def run_kill_switch_check(connector, res, events, pair):
    header("KILL SWITCH — Delta must cancel the bot's orders once its heartbeats stop")
    registered = await wait_for(lambda: connector._heartbeat_id is not None, timeout=30)
    res.check(registered, f"dead-man switch set up on Delta (heartbeat {connector._heartbeat_id})")
    if not registered:
        try:
            await connector._register_heartbeat()
            info("a direct attempt to set it up worked; the connector's loop should pick it up within 10s")
        except Exception as e:
            info(f"setting it up fails: {str(e)[-400:]}")
        return
    # Every set of options gets its own heartbeat, and Delta keeps the records (they show no state).
    records = await _heartbeat_records(connector)
    info(f"heartbeats on the account: {len(records) if isinstance(records, list) else records}")
    for record in records if isinstance(records, list) else []:
        info(f"  {record}")
    try:
        ack = await connector._send_heartbeat(connector._heartbeat_id, ttl=CONSTANTS.HEARTBEAT_TTL_MS)
        expiry = connector._parse_timestamp((ack or {}).get("heartbeat_timestamp"))
        info(f"acknowledgment: {ack}" + (f"; Delta acts if no heartbeat comes by {_hms(expiry)}" if expiry else ""))
    except Exception as e:
        info(f"acknowledgment failed: {str(e)[-200:]}")

    one = connector.contract_size(pair)
    price = _resting_price(connector, pair, TradeType.BUY)
    if price is None:
        res.check(False, "Delta's price band keeps a resting BUY too close to the market; kill switch not checked")
        return
    cid, is_open = await _place_resting(connector, events, res, "resting BUY", TradeType.BUY, pair, one, price)
    if not is_open:
        await _cancel_leftovers(connector, pair, [cid])
        return
    # Stops the heartbeats without switching them off, which is what a crash or a hung bot looks like.
    connector._heartbeat_task.cancel()
    probe_cid = None
    try:
        stopped = time.time()
        info(f"heartbeats stopped at {_hms(stopped)}; Delta should cancel the order about "
             f"{CONSTANTS.HEARTBEAT_TTL_MS // 1000}s after the last one")
        # Delta's own state, read over REST, times the cancel whatever happens to the private stream meanwhile.
        order, error = await _poll_delta_order(connector, cid, timeout=75)
        elapsed = time.time() - stopped
        delta_cancelled = order.get("state") == "cancelled"
        res.check(delta_cancelled,
                  f"Delta cancelled the resting order {elapsed:.0f}s after the heartbeats stopped "
                  f"(reason {order.get('cancellation_reason')})" if delta_cancelled
                  else f"Delta did not cancel it within {elapsed:.0f}s (state {order.get('state')}, reason "
                       f"{order.get('cancellation_reason')})" + (f"; last read failed: {error}" if error else ""))
        if delta_cancelled:
            # The connector hears it on the private stream, or over REST when it re-syncs after a stream drop.
            state = await _wait_state(connector, cid, TERMINAL, timeout=20)
            caught_up = time.time() - stopped
            stream = "up" if _private_stream_up(connector) else "DOWN"
            res.check(state == OrderState.CANCELED,
                      f"the connector shows it cancelled too, {caught_up:.0f}s after the heartbeats stopped "
                      f"(private stream {stream})" if state == OrderState.CANCELED
                      else f"the connector still shows it {state} {caught_up:.0f}s after the heartbeats stopped "
                           f"(private stream {stream})")
            if state == OrderState.CANCELED:
                res.check(events.was_cancelled(cid),
                          "the connector told the strategy it was cancelled (OrderCancelled)")
            # One-shot or sticky? A sticky switch would go on cancelling new orders on these options for as long as
            # the heartbeat stays tripped, as one left behind by a bot that stopped uncleanly does. (3 Nov: one-shot.)
            probe_cid, outcome = await _tripped_heartbeat_probe(connector, events, pair, price)
            info(f"while it is still tripped, a new post-only BUY at {price}: {outcome}")
    finally:
        connector._heartbeat_task = safe_ensure_future(connector._heartbeat_loop())
        await _cancel_leftovers(connector, pair, [cid, probe_cid])
    await asyncio.sleep(1)
    try:
        ack = await connector._send_heartbeat(connector._heartbeat_id, ttl=CONSTANTS.HEARTBEAT_TTL_MS)
        res.check(True, f"heartbeats accepted again after the trip: {ack}")
    except Exception as e:
        res.check(False, f"heartbeats are refused after the trip: {str(e)[-200:]}")


async def _heartbeat_records(connector, heartbeat_id=None):
    """Delta's heartbeat records, compact (product lists as counts), or why they could not be read."""
    try:
        response = await connector._api_get(path_url=CONSTANTS.HEARTBEAT_PATH_URL,
                                            params={"heartbeat_id": heartbeat_id} if heartbeat_id else None,
                                            is_auth_required=True)
    except Exception as e:
        return f"unreadable: {str(e)[-200:]}"
    return [{k: len(v) if isinstance(v, list) else v for k, v in record.items()
             if k not in ("config", "user_id", "contract_types", "underlying_assets")}
            for record in response.get("result") or []]


async def _tripped_heartbeat_probe(connector, events, pair, price):
    """
    Places a post-only BUY while the heartbeat is still tripped and watches it for a few seconds: a one-shot switch
    lets it rest, a sticky one cancels or refuses it. Returns its client id, for the clean-up, and what happened.
    """
    cid = connector.buy(pair, connector.contract_size(pair), OrderType.LIMIT_MAKER, price,
                        position_action=PositionAction.OPEN)
    state = await _wait_state(connector, cid, {OrderState.OPEN} | TERMINAL, timeout=15)
    if state != OrderState.OPEN:
        return cid, f"not accepted ({state}): {events.why(cid)}"
    order, _ = await _poll_delta_order(connector, cid, timeout=8)
    if order.get("state") == "open":
        return cid, "Delta lets it rest, so a tripped heartbeat cancels once (one-shot)"
    return cid, (f"Delta {order.get('state')} it (reason {order.get('cancellation_reason')}), so a tripped heartbeat "
                 f"goes on cancelling new orders (sticky)")


async def run_switch_off_check(connector, res, events, pair):
    """
    A clean stop sends ttl 0. Delta's heartbeat records carry no state, so whether that really switches the dead-man
    switch off (rather than tripping it, at once or at the TTL) shows only in what happens to an order resting then.
    """
    header("DEAD-MAN SWITCH — switched off as a clean stop does; a resting order must be left alone")
    if connector._heartbeat_id is None:
        info("no dead-man switch was set up")
        return
    price = _resting_price(connector, pair, TradeType.BUY)
    if price is None:
        res.check(False, "Delta's price band keeps a resting BUY too close to the market; switch-off not checked")
        return
    cid, is_open = await _place_resting(connector, events, res, "resting BUY", TradeType.BUY, pair,
                                        connector.contract_size(pair), price)
    if not is_open:
        await _cancel_leftovers(connector, pair, [cid])
        return
    try:
        captured = len(_CAPTURED)
        await connector._stop_heartbeat()
        failure = next((m for m in _CAPTURED[captured:] if "Could not switch off" in m), None)
        res.check(failure is None, "Delta accepted ttl 0" if failure is None
                  else f"switch-off failed: {failure[-250:]}")
        if failure is None:
            wait = CONSTANTS.HEARTBEAT_TTL_MS / 1000 + 5
            info(f"watching the order for {wait:.0f}s, past the {CONSTANTS.HEARTBEAT_TTL_MS // 1000}s TTL")
            order, error = await _poll_delta_order(connector, cid, timeout=wait)
            left_alone = order.get("state") == "open"
            res.check(left_alone, f"switched off: Delta left the order open {wait:.0f}s later" if left_alone
                      else f"Delta {order.get('state')} the order after the switch-off (reason "
                           f"{order.get('cancellation_reason')}): a clean stop does not switch the dead-man switch off"
                      + (f"; last read failed: {error}" if error else ""))
    finally:
        await _cancel_leftovers(connector, pair, [cid])
        # Armed again, so the shutdown below has to switch it off too, with the clock's stop and the script's
        # running at the same time, as they did when the proxy session was closed under the switch-off.
        connector._heartbeat_task = safe_ensure_future(connector._heartbeat_loop())


async def _poll_position(connector, pair, want_open, timeout=15):
    """Reads positions over REST until the pair is open (or flat); Delta can list a position a moment late."""
    deadline = time.time() + timeout
    while True:
        await connector._update_positions()
        position = connector.get_position(pair)
        if (position is not None) == want_open or time.time() >= deadline:
            return position
        await asyncio.sleep(2)


def _check_fee(connector, res, label, pair, fills):
    """Delta's commission against the connector's estimate for a taker fill, which budget checks rely on."""
    base, quote = pair.split("-")
    paid = sum((fee.amount for f in fills for fee in f.trade_fee.flat_fees), Decimal("0"))
    expected = sum((connector.get_fee(base, quote, OrderType.LIMIT, f.trade_type, PositionAction.OPEN, f.amount,
                                      f.price, is_maker=False).percent * f.price * f.amount for f in fills),
                   Decimal("0"))
    gap = (paid - expected) / expected if expected else None
    res.check(gap is not None and abs(gap) < Decimal("0.15"),
              f"{label} fee: Delta charged {paid}, the connector estimated {expected:.6f}"
              + (f" ({gap:+.1%})" if gap is not None else ""))


async def _check_margin(connector, res, pair, amount, price):
    """The margin Delta blocks for the short, against the connector's estimate (used by budget checks)."""
    symbol = await connector.exchange_symbol_associated_to_pair(pair)
    response = await connector._api_get(path_url=CONSTANTS.POSITIONS_PATH_URL,
                                        params={"contract_types": CONSTANTS.OPTION_CONTRACT_TYPES},
                                        is_auth_required=True)
    entry = next((e for e in response.get("result") or [] if e.get("product_symbol") == symbol), {})
    fields = ("size", "entry_price", "margin", "liquidation_price")
    info("Delta's position: " + ", ".join(f"{k} {entry.get(k)}" for k in fields))
    margin, estimate = _dec(entry.get("margin")), connector.estimate_short_margin(pair, amount, price)
    covered = margin is not None and estimate >= margin
    res.check(covered, f"margin estimate {estimate:.4f} covers what Delta blocks ({margin})" if covered
              else f"margin estimate {estimate:.4f} is below what Delta blocks ({margin}); raise SHORT_MARGIN_RATE")


async def _close_test_position(connector, pair, slippage):
    position = connector.get_position(pair)
    is_buy = position.amount < 0
    place = connector.buy if is_buy else connector.sell
    return place(pair, abs(position.amount), OrderType.LIMIT, connector._crossing_price(pair, is_buy, slippage),
                 position_action=PositionAction.CLOSE)


async def run_fill_checks(connector, res, events, long_pair, short_pair, slippage, assume_yes):
    header("FILLS — two real round trips of 1 contract")
    one = connector.contract_size(long_pair)
    collateral = connector.get_buy_collateral_token(long_pair)
    held = {pair: position.amount for pair, position in connector.account_positions.items()}
    busy = [pair for pair in (long_pair, short_pair) if pair in held]
    if busy:
        res.check(False, f"{', '.join(busy)} already hold a position; the fill checks need them flat")
        return
    # flatten() closes every position on the connector's options, so it closes the long only if nothing else is held.
    use_flatten = not any(pair in connector.trading_pairs for pair in held)
    long_bid, long_ask = _book(connector, long_pair)
    if long_bid is None or long_ask is None:
        res.check(False, f"{long_pair} has no two-sided book ({long_bid} / {long_ask}); the fill checks are skipped")
        return
    info(f"a. BUY 1 {long_pair} ({one} {connector.get_instrument(long_pair).underlying}) at up to "
         f"{connector._crossing_price(long_pair, True, slippage)} (ask {long_ask}), with the private stream paused; "
         f"then sell it back {'with the kill switch, flatten()' if use_flatten else 'reduce-only'}. "
         f"Cost: the spread, about {(long_ask - long_bid) * one:.4f} {collateral}, plus fees")
    short_bid = short_ask = None
    if short_pair is not None:
        short_bid, short_ask = _book(connector, short_pair)
    if short_bid is None or short_ask is None:
        info(f"b. skipped: {short_pair or 'no ~3% out-of-the-money call'} has no two-sided book")
        short_pair = None
    else:
        info(f"b. SELL 1 {short_pair} to open a short at down to "
             f"{connector._crossing_price(short_pair, False, slippage)} (bid {short_bid}), then buy it back. Margin "
             f"about {connector.estimate_short_margin(short_pair, one, short_bid):.2f} {collateral}; cost: the spread, "
             f"about {(short_ask - short_bid) * one:.4f} {collateral}, plus fees")
    info(f"available: {connector.get_available_balance(collateral)} {collateral}")
    if not assume_yes:
        # Asked in a thread, so the connector's streams keep running while the prompt waits.
        answer = await asyncio.get_running_loop().run_in_executor(
            None, input, "  Type 'yes' to trade these real contracts: ")
        if answer.strip().lower() != "yes":
            info("fill checks skipped")
            return

    balance_before = connector.get_balance(collateral)
    test_orders = []
    listener_paused = False
    try:
        header("FILLS a — buy with the private stream paused (the fill must come from REST)")
        connector._user_stream_event_listener_task.cancel()
        listener_paused = True
        buy_cid = connector.buy(long_pair, one, OrderType.LIMIT, connector._crossing_price(long_pair, True, slippage),
                                position_action=PositionAction.OPEN)
        test_orders.append((long_pair, buy_cid))
        await wait_for(lambda: getattr(_order(connector, buy_cid), "exchange_order_id", None) is not None
                       or _state(connector, buy_cid) in TERMINAL, timeout=20, interval=0.25)
        started = time.time()
        for _ in range(15):
            # The status poll is how missed fills are recovered; run it now instead of waiting for its interval.
            await connector._update_order_status()
            if events.fills_of(buy_cid) or _state(connector, buy_cid) == OrderState.FAILED:
                break
            await asyncio.sleep(2)
        fills = events.fills_of(buy_cid)
        res.check(bool(fills), f"buy fill recovered over REST in {time.time() - started:.1f}s at "
                               f"{[str(f.price) for f in fills]}" if fills
                  else f"no buy fill ({_state(connector, buy_cid)}): {events.why(buy_cid)}")
        if not fills:
            return
        _check_fee(connector, res, "buy", long_pair, fills)
        position = await _poll_position(connector, long_pair, want_open=True)
        res.check(position is not None and position.amount == one,
                  f"long position opened: {getattr(position, 'amount', None)} @ "
                  f"{getattr(position, 'entry_price', None)}")
        connector._user_stream_event_listener_task = safe_ensure_future(connector._user_stream_event_listener())
        listener_paused = False

        header("FILLS a — close it " + ("with the kill switch, flatten()" if use_flatten else "with a reduce-only sell")
               + ", the stream running")
        await asyncio.sleep(2)  # the stream messages queued during the pause are handled first
        if use_flatten:
            close_ids = await connector.flatten(slippage)
            res.check(len(close_ids) == 1, f"flatten() placed {len(close_ids)} closing order(s) (expected 1)")
            close_cid = close_ids[0] if close_ids else None
        else:
            close_cid = await _close_test_position(connector, long_pair, slippage)
        test_orders.append((long_pair, close_cid))
        if close_cid is not None:
            started = time.time()
            closed = await wait_for(lambda: bool(events.fills_of(close_cid)), timeout=20, interval=0.25)
            res.check(closed, f"sell fill recorded {time.time() - started:.1f}s after placing, from the stream"
                      if closed else f"no sell fill ({_state(connector, close_cid)}): {events.why(close_cid)}")
            if closed:
                _check_fee(connector, res, "sell", long_pair, events.fills_of(close_cid))
                order = await _delta_order(connector, close_cid)
                res.check(order.get("reduce_only") is True, f"the closing order was reduce-only on Delta "
                                                            f"({order.get('reduce_only')})")
        flat = await wait_for(lambda: connector.get_position(long_pair) is None, timeout=20, interval=0.5)
        res.check(flat, "long position removed after the close (positions stream)")

        if short_pair is None:
            return
        header(f"FILLS b — sell 1 {short_pair} to open a short")
        sell_cid = connector.sell(short_pair, one, OrderType.LIMIT,
                                  connector._crossing_price(short_pair, False, slippage),
                                  position_action=PositionAction.OPEN)
        test_orders.append((short_pair, sell_cid))
        await wait_for(lambda: bool(events.fills_of(sell_cid))
                       or _state(connector, sell_cid) in {OrderState.FAILED, OrderState.CANCELED},
                       timeout=30, interval=0.25)
        fills = events.fills_of(sell_cid)
        res.check(bool(fills), f"short sell filled at {[str(f.price) for f in fills]}" if fills
                  else f"short sell did not fill ({_state(connector, sell_cid)}): {events.why(sell_cid)}")
        if not fills:
            if "naked_short_restricted" in events.why(sell_cid):
                info("Delta does not let this account sell options it doesn't hold (naked_short_restricted)")
            return
        _check_fee(connector, res, "short sell", short_pair, fills)
        position = await _poll_position(connector, short_pair, want_open=True)
        res.check(position is not None and position.amount == -one,
                  f"short position opened: {getattr(position, 'amount', None)} @ "
                  f"{getattr(position, 'entry_price', None)}")
        await _check_margin(connector, res, short_pair, one, fills[0].price)

        header("FILLS b — buy it back (reduce-only)")
        back_cid = await _close_test_position(connector, short_pair, slippage)
        test_orders.append((short_pair, back_cid))
        started = time.time()
        closed = await wait_for(lambda: bool(events.fills_of(back_cid)), timeout=20, interval=0.25)
        res.check(closed, f"buy-back fill recorded {time.time() - started:.1f}s after placing" if closed
                  else f"no buy-back fill ({_state(connector, back_cid)}): {events.why(back_cid)}")
        if closed:
            _check_fee(connector, res, "buy-back", short_pair, events.fills_of(back_cid))
        flat = await wait_for(lambda: connector.get_position(short_pair) is None, timeout=20, interval=0.5)
        res.check(flat, "short position removed after the buy-back")
    finally:
        if listener_paused:
            connector._user_stream_event_listener_task = safe_ensure_future(connector._user_stream_event_listener())
        for pair, cid in test_orders:
            if cid is not None and _state(connector, cid) not in TERMINAL:
                connector.cancel(pair, cid)
        for pair in (long_pair, short_pair):
            if pair is not None and await _poll_position(connector, pair, want_open=False, timeout=10) is not None:
                header(f"SAFETY — closing the test position on {pair}")
                cid = await _close_test_position(connector, pair, slippage)
                left = await _poll_position(connector, pair, want_open=False, timeout=20)
                (ok if left is None else fail)(f"{pair}: {'closed' if left is None else f'STILL OPEN: {left.amount}'} "
                                               f"(order {cid})")
        fees = sum((fee.amount for f in events.fills for fee in f.trade_fee.flat_fees), Decimal("0"))
        info(f"{len(events.fills)} fill(s), {fees} {collateral} in fees; balance {balance_before} -> "
             f"{connector.get_balance(collateral)} {collateral}")


async def run_trading(connector, res, args, instruments, spot):
    long_pair, short_pair = _trade_pairs(instruments, spot)
    if long_pair is None:
        res.check(False, "no call to trade on")
        return
    events = OrderEvents(connector)
    collateral = connector.get_buy_collateral_token(long_pair)
    try:
        if args.trade:
            await connector._update_balances()
            balance_before = connector.get_balance(collateral)
            await run_order_checks(connector, res, events, long_pair)
            await run_refusal_checks(connector, res, events, long_pair)
            await run_kill_switch_check(connector, res, events, long_pair)
            header("ORDERS — after the order checks")
            res.check(not events.fills, f"nothing filled ({len(events.fills)} fills)")
            left = await _bot_orders_open_on_delta(connector, long_pair)
            res.check(not left, "Delta lists none of the test orders as open" if not left
                      else f"Delta still lists test orders as open: {[o.get('id') for o in left]}")
            await connector._update_balances()
            balance_after = connector.get_balance(collateral)
            res.check(balance_after == balance_before, f"balance unchanged, nothing paid: {balance_before} -> "
                                                       f"{balance_after} {collateral}")
        if args.fill:
            await run_fill_checks(connector, res, events, long_pair, short_pair, Decimal(str(args.slippage)), args.yes)
        await run_switch_off_check(connector, res, events, long_pair)
    finally:
        events.close()


async def run(args, api_key, api_secret, proxy_url):
    res = Results()
    _install_log_capture()
    _quiet_app_warnings()
    trading = args.trade or args.fill
    domain = CONSTANTS.TESTNET_DOMAIN if args.testnet else CONSTANTS.DEFAULT_DOMAIN
    mode = "+".join(name for name, on in (("TRADE", args.trade), ("FILL", args.fill)) if on) or (
        "WITH KEYS" if args.with_keys else "PUBLIC DATA ONLY")
    print(_c("1", f"delta_options LIVE check — {args.underlying} on {domain} ({mode})"))
    if args.with_keys:
        info(f"proxy: {_masked(proxy_url)}" if proxy_url else "proxy: none (direct connection)")

    instruments, spot = await pick_options(res, args.underlying, domain, with_otm_call=trading)
    if not instruments:
        return 1
    pairs = [i.trading_pair for i in instruments]
    connector = DeltaOptionsDerivative(
        delta_options_api_key=api_key,
        delta_options_api_secret=api_secret,
        delta_options_proxy_url=proxy_url or "",
        # Selling to open and the dead-man switch only for the trading checks: a read-only run writes nothing.
        delta_options_allow_selling=trading,
        delta_options_max_short_contracts=1,
        delta_options_dead_man_switch=trading,
        trading_pairs=pairs,
        trading_required=args.with_keys,
        domain=domain,
    )
    tap = StreamTap(connector)

    clock = Clock(ClockMode.REALTIME, tick_size=1.0)
    clock.__enter__()
    clock.add_iterator(connector)
    clock_task = safe_ensure_future(clock.run())
    try:
        await wait_ready(connector, res)
        if connector.ready:
            symbols = {pair: await connector.exchange_symbol_associated_to_pair(pair) for pair in pairs}
            await check_live_data(connector, tap, res, instruments, spot, symbols)
            if args.with_keys:
                await check_account(connector, res, pairs)
            await check_expiry_safety(connector, tap, res, instruments, symbols)
            if trading:
                await run_trading(connector, res, args, instruments, spot)
            if args.minutes:
                await watch(connector, tap, res, instruments, symbols, args.minutes, args.with_keys)
            if args.with_keys:
                up = await wait_for(lambda: _private_stream_up(connector), timeout=30)
                res.check(up, "private stream connected at the end of the run (it reconnects after drops)")
        else:
            info("Connector never became ready — skipping the other checks.")
    finally:
        header("SHUTDOWN")
        heartbeat_armed = connector._heartbeat_task is not None
        captured = len(_CAPTURED)
        clock_task.cancel()
        try:
            await clock_task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            # This stops the connector in the background as well, so two stops overlap, as they can in a bot.
            clock.__exit__(None, None, None)
        except Exception:
            pass
        await connector.stop_network()
        info("clock stopped, network stopped")
        if heartbeat_armed:
            failure = next((m for m in _CAPTURED[captured:] if "Could not switch off" in m), None)
            res.check(failure is None, "the dead-man switch was switched off at shutdown" if failure is None
                      else f"the dead-man switch was not switched off at shutdown: {failure[-250:]}")

    if _CAPTURED:
        _show_captured()

    header("SUMMARY")
    total = res.passed + res.failed
    (ok if res.failed == 0 else fail)(f"{res.passed}/{total} checks passed")
    return 0 if res.failed == 0 else 1


def load_credentials(args):
    """Returns (api key, api secret, proxy url); empty strings without --with-keys."""
    if not args.with_keys:
        return "", "", ""
    if args.from_env:
        key, secret = os.environ.get("DELTA_API_KEY", ""), os.environ.get("DELTA_API_SECRET", "")
        if not (key and secret):
            raise SystemExit("--from-env needs DELTA_API_KEY and DELTA_API_SECRET set.")
        return key, secret, os.environ.get("DELTA_PROXY_URL", "")

    from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
    from hummingbot.client.config.security import Security

    password = getpass.getpass("Hummingbot password (to decrypt conf/connectors/delta_perpetual.yml): ")
    if not Security.login(ETHKeyFileSecretManger(password)):
        raise SystemExit("Wrong hummingbot password.")
    keys = Security.api_keys("delta_perpetual")
    if not keys.get("delta_perpetual_api_key"):
        raise SystemExit("No Delta keys in conf/connectors/delta_perpetual.yml; run `connect delta_perpetual` first.")
    return keys["delta_perpetual_api_key"], keys.get("delta_perpetual_api_secret", ""), \
        keys.get("delta_perpetual_proxy_url", "")


def main():
    p = argparse.ArgumentParser(description="Live check of the delta_options connector.")
    p.add_argument("--underlying", default="BTC", choices=["BTC", "ETH"])
    p.add_argument("--with-keys", action="store_true", help="also check the private stream, balances and positions")
    p.add_argument("--from-env", action="store_true", help="with --with-keys: read DELTA_API_KEY / DELTA_API_SECRET")
    p.add_argument("--minutes", type=float, default=0, help="keep watching this long after the checks")
    p.add_argument("--trade", action="store_true",
                   help="post-only orders far from the market that get confirmed and cancelled, refusals and the "
                        "kill switch; nothing fills")
    p.add_argument("--fill", action="store_true", help="two real 1-contract round trips (asks first)")
    p.add_argument("--yes", action="store_true", help="skip the confirmation prompt of --fill")
    p.add_argument("--slippage", type=float, default=0.05, help="how far --fill orders cross the book (0.05 = 5%%)")
    p.add_argument("--testnet", action="store_true", help="Delta's India testnet (demo account keys via --from-env)")
    args = p.parse_args()
    if args.trade or args.fill:
        args.with_keys = True
    if args.testnet and args.with_keys and not args.from_env:
        p.error("--testnet needs demo.delta.exchange keys: set DELTA_API_KEY / DELTA_API_SECRET and add --from-env")
    api_key, api_secret, proxy_url = load_credentials(args)
    try:
        raise SystemExit(asyncio.run(run(args, api_key, api_secret, proxy_url)))
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
