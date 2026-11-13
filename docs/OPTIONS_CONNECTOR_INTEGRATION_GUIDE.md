# How to Integrate an Options Exchange into Hummingbot

This guide covers what an options connector adds to a normal derivative
connector. Read [NEW_EXCHANGE_INTEGRATION_GUIDE.md](./NEW_EXCHANGE_INTEGRATION_GUIDE.md)
first: authentication, web utils, rate limits, the user stream, registration
and the basic test setup work the same way and are not repeated here.

The reference implementation is `delta_options` (Delta Exchange India, BTC and
ETH options), built and live-tested in November 2026. Every rule below comes
from that work, and section 10 lists the bugs we hit so you don't hit them
again. The design and its phases are in [OPTIONS_FRAMEWORK_PLAN](./OPTIONS_FRAMEWORK_PLAN).

---

## Table of Contents

1. [What's different about options](#1-whats-different-about-options)
2. [What to read from the exchange's documentation](#2-what-to-read-from-the-exchanges-documentation)
3. [The shared options layer — what you get for free](#3-the-shared-options-layer--what-you-get-for-free)
4. [Connector file structure](#4-connector-file-structure)
5. [Step-by-step: the options-specific parts](#5-step-by-step-the-options-specific-parts)
   - 5.1 [Constants](#51-constants)
   - 5.2 [Utils: names, instruments, settings](#52-utils-names-instruments-settings)
   - 5.3 [Order book data source](#53-order-book-data-source)
   - 5.4 [User stream data source](#54-user-stream-data-source)
   - 5.5 [The main class](#55-the-main-class)
6. [Expiry and settlement](#6-expiry-and-settlement)
7. [Using an options connector from a strategy](#7-using-an-options-connector-from-a-strategy)
8. [Writing tests](#8-writing-tests)
9. [Live verification](#9-live-verification)
10. [Pitfalls we hit](#10-pitfalls-we-hit)
11. [Checklist before raising a PR](#11-checklist-before-raising-a-pr)
12. [Reference: the delta\_options connector](#12-reference-the-delta_options-connector)

---

## 1. What's different about options

| | Spot / perpetuals | Options |
|---|---|---|
| Instruments | A fixed list | New ones are listed every day, and each one expires (Delta: daily, weekly and monthly expiries, all at 12:00 UTC) |
| Names | `BASE-QUOTE` | Type, underlying, strike and expiry: Delta's `C-BTC-63800-130626` is a BTC call, strike 63,800, expiring 13 Jun 2026 |
| Sizes | Units of the base asset | Contracts of a fixed size (Delta: 1 contract = 0.001 BTC) |
| Live data | Mark price, funding rate | Mark price, implied volatility (IV), Greeks and a price band, sent by the exchange |
| Buying | Pays the price, or posts margin | Pays the premium up front; can lose at most the premium |
| Selling to open | Posts margin | Posts margin; can lose far more than the premium collected |
| Fees | A share of the order value | Delta: 0.01% of the underlying's value, capped at 3.5% of the premium, plus 18% GST |
| End of life | None | Settled in cash at expiry, without any order of yours |

### The class hierarchy

```
ConnectorBase
    └── ExchangeBase
            └── ExchangePyBase
                    └── OptionDerivativePyBase      ← the shared options layer
                            └── YourOptionsDerivative
```

`OptionDerivativePyBase` adds the options behaviour on top of
`ExchangePyBase`: option data, the option chain, order checks, a selling
guard, a kill switch, margin estimates and expiry handling. Around it, the
options layer provides a market-data base class (`OptionAPIOrderBookDataSource`),
a state holder (`OptionsTrading`), a budget checker (`OptionBudgetChecker`),
two events (`OptionExpired`, `OptionSettled`) and a strategy helper
(`listen_to_option_events`). Section 3 lists them all.

> **v1 scope:** script strategies only, one expiry per run. The options picked
> at startup are fixed for the run, because a running connector can't add
> instruments, so moving to the next expiry is a restart. strategy_v2 is not
> supported: its executors treat a connector as a perpetual only when its name
> contains "perpetual", so they would treat options as spot.

---

## 2. What to read from the exchange's documentation

Answer these on top of the general guide's checklist (its section 2). Delta's
answers are in brackets as a worked example.

### 2.1 Instruments

- [ ] Which endpoint lists the options, and how do you filter to live calls and
      puts? Is it paginated?
      (`GET /v2/products?contract_types=call_options,put_options&states=live`,
      cursor pages of 100; 526 live BTC options on 10 Nov 2026.)
- [ ] How does a name encode type, underlying, strike and expiry?
- [ ] At what time do options expire? Is the expiry time in the product data?
      (`settlement_time`; always 12:00 UTC = 17:30 IST.)
- [ ] What is one contract worth in the underlying, and what is the tick size?
      (`contract_value` 0.001 BTC, `tick_size` 0.1.)
- [ ] Does the same list carry products you must filter out, such as barrier
      options? (Yes: skip any product with a `barrier_price`.)
- [ ] After expiry, does the list drop the option? Can you still read it by
      symbol? (Dropped from `states=live`; `GET /v2/products/<symbol>` still
      works, with `state: expired` and its `settlement_price`.)

### 2.2 Option market data

- [ ] Which channel or endpoint carries mark price, IV and Greeks, and how
      often? Are the Greeks per contract or per unit of the underlying?
      (WebSocket `ticker` every 5 s, per unit; REST `/v2/tickers/<symbols>`.)
- [ ] Does the exchange enforce a price band on each option, and where is it
      published? (In the ticker; an order outside it is rejected. Live bands
      ran from 0.1 to about 3 times the mark.)
- [ ] Are book and trade sizes in contracts or in the underlying? (Contracts.)
- [ ] How many symbols can one WebSocket subscription carry? (100.)
- [ ] What happens when a request or a subscription names an expired option?
      (The REST ticker answers `result: null`, and a failed order book
      snapshot stops the tracker from starting any other book.)

### 2.3 Orders

- [ ] Which order types exist? Is there post-only, reduce-only, time in force?
      (Limit orders with `post_only` and `reduce_only`; we refuse market orders
      because option books are thin.)
- [ ] May this account sell options it doesn't hold (naked shorts)? (Yes, on
      our account; the error otherwise is `naked_short_restricted`.)
- [ ] What is the minimum size, and how are sizes sent? (1 contract, as an
      integer number of contracts.)
- [ ] What does a cancel return for an order that has just filled, or that the
      exchange cancelled itself? (`open_order_not_found`.)
- [ ] Does trading stop before expiry? (No sign of it: 3.4 h before expiry an
      option was fully open, and no product field gives a last trading time.)

### 2.4 Fees and margin

- [ ] Is the fee charged on the premium or on the underlying's value? Is there
      a cap, and taxes on top? Are rates per product?
      (`maker_commission_rate`, `taker_commission_rate`, and the cap in
      `product_specs.premium_commission_rate`; the commission on each fill
      includes GST.)
- [ ] Is there a fee at settlement? (Yes: the trading formula applied at the
      settlement price; zero for an option that expires worthless.)
- [ ] How is the margin for a short option computed, and where can you read
      what is held? (Portfolio margin from stress scenarios. A 1-contract short
      3% out of the money with one day left held 0.47 USD.)

### 2.5 Positions and settlement

- [ ] Are positions net per option? Does the positions endpoint list every
      option position on the account, and drop closed or expired ones? (Yes
      to all.)
- [ ] How is the settlement price set? (The index averaged over the last 30
      minutes before expiry.)
- [ ] What does the exchange write when it settles a position: a fill, a
      wallet entry, both? (A fill of type `settlement` for every position,
      worthless ones included, within a second of expiry. Options that pay out
      also get a `settlement` wallet entry and a `commission` entry.)

### 2.6 Safety features

- [ ] Is there a dead-man switch (cancel-on-disconnect)? What exactly does it
      cancel, and how is it switched off?
      (Heartbeats with a TTL. `impact: products` cancels the account's open
      orders on the listed options, manual ones included. Sending TTL 0
      switches it off. A trip cancels once and does not block later orders.)

### 2.7 Rate limits

- [ ] What does each endpoint weigh, and how many ids fit in one request?
      (20,000 weight per 5 minutes; fills weigh 10, orders 5, reads 3; at most
      10 symbols or ids per request.)

---

## 3. The shared options layer — what you get for free

| File | What it gives you |
|---|---|
| `core/data_type/option_info.py` | `OptionType` (CALL, PUT). `OptionInstrumentInfo`: the static facts (pair, underlying, type, strike, expiry) and `is_expired(timestamp)`. `OptionInfo`: live data (mark, underlying price, bid/ask/mark IV, delta, gamma, theta, vega, rho, open interest, price band). `OptionInfoUpdate`: a partial update in which `None` means "unchanged". |
| `connector/options_trading.py` | `OptionsTrading`: the instruments, the live option info (with an update queue) and the net positions of one connector. |
| `core/data_type/option_api_order_book_data_source.py` | `OptionAPIOrderBookDataSource`: adds an option-info message queue and its listener to the usual data source. Gives an expired option an empty book and leaves it out of the hourly snapshots. |
| `connector/option_derivative_py_base.py` | `OptionDerivativePyBase`: the base class, listed below. |
| `connector/derivative/option_budget_checker.py` | `OptionBudgetChecker`: a buy locks premium and fee, selling to open locks a margin estimate, closing locks nothing. |
| `core/event/events.py` | `MarketEvent.OptionExpired` (220) and `MarketEvent.OptionSettled` (221), with `OptionExpiredEvent` and `OptionSettledEvent`. Numbers 213 to 216 belong to the wallet-transfer events. |
| `connector/markets_recorder.py` | Records an `OptionSettled` event once, as the trade that closes the position, so trade history and PnL show it closed. |
| `strategy/option_events.py` | `listen_to_option_events(strategy, connectors)`: calls `did_expire_option` and `did_settle_option` on a script strategy. |

### What `OptionDerivativePyBase` does for you

- **Checks every order before sending it** (`_order_refusal`), in this order:
  ```
  1. unsupported order type     "MARKET orders are not supported"
  2. expired option             "the option expired at 25 Dec 12:00 UTC"
  3. opening inside the window  "... positions are not opened in its last 30 minutes; closing orders are still accepted"
  4. selling guard              selling to open is not enabled, or the short would exceed the cap
  5. price band                 checked on the price as sent, after rounding to the tick
  ```
  A refused order is tracked and failed with its reason, so the strategy gets
  an `OrderFailure` event, and nothing reaches the exchange.
- **Selling guard.** Selling to open is refused unless `allow_selling` is set,
  and the short is capped at `max_short_contracts` per option, counting open
  sell orders as if they had filled.
- **Kill switch.** `await connector.flatten(slippage=Decimal("0.05"))` cancels
  the connector's open orders, then closes each position on its live options
  with a reduce-only limit order that crosses the book by 5%. The price stays
  inside the band and on the tick grid. Positions on other options, or on
  expired ones that are settling, are left alone.
- **Margin estimate.** `estimate_short_margin(pair, amount, price)` returns
  (max(15% − out-of-the-money share, 10%) × underlying + premium) × amount.
  This is deliberately cautious: on Delta it is about 20 times what Delta
  actually holds, so tune it per exchange.
- **Net positions, no leverage.** Only `PositionMode.ONEWAY`; leverage is
  always 1, and setting it only logs a warning.
- **Lookups.** `get_option_chain(underlying, expiry_timestamp=None)` returns live options
  sorted by expiry, type and strike. Also `get_option_info`, `get_instrument`,
  `get_underlying_price`, `is_expired`, `is_closing_only` and
  `exchange_symbols_for_live_pairs`, which skips expired and delisted options.
- **Readiness.** `status_dict["option_info"]` waits for option info on live
  options only; an expired option never blocks a start.
- **Expiry watch.** Every 5 s it does three things: at the start of the closing
  window it cancels resting orders that would open a position; at expiry it
  raises `OptionExpired` once per option; and it then calls
  `_on_option_expired(pair, position)` with the last position seen before
  expiry.
- **Status polling** reads positions as well as balances and orders.

### What you implement

Everything `ExchangePyBase` needs (general guide, section 4.7), plus:

| Method | What it must do |
|---|---|
| `contract_size(pair)` | Amount of the underlying in one contract |
| `get_buy_collateral_token(pair)` / `get_sell_collateral_token(pair)` | The settling asset, e.g. `USD` |
| `_create_order_book_data_source()` | Return your `OptionAPIOrderBookDataSource` subclass |
| `_place_order(..., position_action)` | Send contracts, and send a CLOSE as reduce-only |
| `_update_positions()` | Fill `self._options_trading` with net positions; remove the ones the exchange stops listing |
| `_get_fee(...)` | The options fee formula, as a share of the premium |
| `supported_order_types()` | Normally `LIMIT` and `LIMIT_MAKER` |
| `_on_option_expired(pair, position)` | Optional, but needed for settlement: find the exchange's settlement record and raise `OptionSettled` (section 6) |

---

## 4. Connector file structure

```
hummingbot/connector/derivative/<name>_options/
├── __init__.py
├── <name>_options_constants.py
├── <name>_options_auth.py
├── <name>_options_web_utils.py
├── <name>_options_utils.py                       ← names, instruments, settings
├── <name>_options_api_order_book_data_source.py  ← books, trades, option info
├── <name>_options_api_user_stream_data_source.py ← orders, fills, positions
└── <name>_options_derivative.py                  ← the main class
```

- Put it under `connector/derivative/`. Auto-discovery picks it up there as a
  derivative; `delta_options` needs no registration anywhere else.
- Don't put "perpetual" in the name: perpetual-specific code recognises
  perpetuals by that word.
- Keep it self-contained. `delta_options` shares Delta's signing and URLs with
  `delta_perpetual` but imports nothing from it, so the two can change
  independently. Copy what you need.

---

## 5. Step-by-step: the options-specific parts

The general guide covers auth and web utils. These sections cover what changes
for options.

### 5.1 Constants

Besides URLs and order states, an options connector needs these (Delta's
values):

```python
CALL_OPTIONS = "call_options"
PUT_OPTIONS = "put_options"
OPTION_CONTRACT_TYPES = f"{CALL_OPTIONS},{PUT_OPTIONS}"
LIVE_STATE = "live"
SETTLEMENT_HOUR_UTC = 12          # expiry time when a product carries none

WS_MAX_SYMBOLS = 100              # the connector refuses more pairs than one subscription holds
MAX_SYMBOLS_PER_REQUEST = 10      # tickers, fills: symbols or ids per request

# REST can lag the stream: a fill signalled on the stream is read again after these delays.
FILL_FETCH_DELAYS = (0.0, 0.5, 1.0, 2.0, 4.0)

# Dead-man switch
HEARTBEAT_TTL_MS = 30000
HEARTBEAT_INTERVAL = 10.0

# Settlement
FILL_TYPE_SETTLEMENT = "settlement"
SETTLEMENT_READ_DELAY = 5.0       # seconds after expiry before the first read
SETTLEMENT_POLL_INTERVAL = 10.0
SETTLEMENT_WAIT = 15 * 60         # how long a position held at expiry waits for its settlement

# Fees: the fallback premium cap when a product carries none, and tax
DEFAULT_PREMIUM_CAP_RATE = Decimal("0.1")
GST_RATE = Decimal("0.18")
```

If the exchange weighs endpoints against one shared quota, use linked limits:

```python
RATE_LIMITS = [RateLimit(limit_id=REST_QUOTA_ID, limit=20000, time_interval=300)] + [
    RateLimit(limit_id=path, limit=20000, time_interval=300,
              linked_limits=[LinkedLimitWeightPair(REST_QUOTA_ID, weight)])
    for path, weight in ((PRODUCTS_PATH_URL, 3), (ORDERS_PATH_URL, 5), (FILLS_PATH_URL, 10), ...)
]
```

### 5.2 Utils: names, instruments, settings

**Names.** Hummingbot splits a pair on its only dash, so turn the exchange's
symbol into `<everything else>-<quote>`:

```python
def trading_pair_from_symbol(symbol: str, quote: str) -> str:
    # C-BTC-63800-130626 quoted in USD becomes C_BTC_63800_130626-USD
    return combine_to_hb_trading_pair(base=symbol.replace("-", "_"), quote=quote.upper())


def symbol_from_trading_pair(trading_pair: str) -> str:
    # And back: C_BTC_63800_130626-USD is C-BTC-63800-130626
    return trading_pair.split("-")[0].replace("_", "-")
```

**Instruments.** Write three helpers:

- `is_live_vanilla_option(product)` keeps live calls and puts and drops
  everything else.
- `instrument_from_product(product, pair)` builds an `OptionInstrumentInfo`.
  The expiry comes from `settlement_time`, or from the symbol's date at
  `SETTLEMENT_HOUR_UTC`.
- `instrument_from_trading_pair(pair)` builds one from the name alone. You need
  it for an option the exchange no longer lists, for example when the bot
  starts after its option expired.

**Settings.** Trading settings belong in the config map next to the keys, so
`connect <name>_options` asks for them and Hummingbot passes them to the
constructor by name. Non-secret settings use `is_secure: False`:

```python
delta_options_allow_selling: bool = Field(
    default=False,
    json_schema_extra={
        "prompt": lambda cm: "Allow selling options to open short positions? (Yes/No)",
        "is_secure": False,
        "is_connect_key": True,
        "prompt_on_new": True,
    },
)
```

| Setting | Default | What it does |
|---|---|---|
| `<name>_allow_selling` | No | Allows selling options to open a short |
| `<name>_max_short_contracts` | 1 | The short cap per option, in contracts (must be above 0) |
| `<name>_dead_man_switch` | Yes | Has the exchange cancel open orders if the bot goes quiet |
| `<name>_stop_before_expiry_minutes` | 30 | No new positions in an option's last N minutes; 0 turns this off |

Validate Yes/No answers strictly: reject "maybe" rather than reading it as No.

### 5.3 Order book data source

Subclass `OptionAPIOrderBookDataSource`:

```python
class YourOptionsAPIOrderBookDataSource(OptionAPIOrderBookDataSource):

    def _is_expired(self, trading_pair: str) -> bool:
        return self._connector.is_expired(trading_pair)

    async def _subscribe_channels(self, ws: WSAssistant):
        # Live options only: one expired name must not break the others' streams.
        symbols = await self._connector.exchange_symbols_for_live_pairs(self._trading_pairs)
        ...

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return {
            CONSTANTS.WS_ORDER_BOOK_CHANNEL: self._snapshot_messages_queue_key,
            CONSTANTS.WS_TRADES_CHANNEL: self._trade_messages_queue_key,
            CONSTANTS.WS_TICKER_CHANNEL: self._option_info_messages_queue_key,  # → _parse_option_info_message
        }.get(event_message.get("type"), "")

    async def _parse_option_info_message(self, raw_message, message_queue: asyncio.Queue):
        # One OptionInfoUpdate per option in the message; leave unknown fields as None.
        message_queue.put_nowait(OptionInfoUpdate(trading_pair=..., mark_price=..., delta=..., ...))

    async def get_option_info(self, trading_pair: str) -> OptionInfo:
        # A complete OptionInfo from REST: the base class reads one per live option at startup.
        ...
```

- Convert every size from contracts to the underlying with
  `self._connector.size_to_amount(pair, size)`: book levels, trades, open
  interest.
- Leave expired options out of `get_last_traded_prices`. A ticker request that
  names one can fail the whole batch.
- The base class already gives expired options an empty book and skips them in
  the hourly snapshots.

### 5.4 User stream data source

This works as in the general guide, with two additions.

- **Subscribe to whole categories, not symbols.** Delta subscribes the
  `orders`, `v2/user_trades` and `positions` channels with
  `["call_options", "put_options"]`. The bot then sees every option position
  on the account, including ones it didn't open.
- **Re-sync after a reconnect.** Exchanges don't replay what they sent while a
  stream was down, such as a dead-man cancel or a fill. Remember the drop and
  ask the connector to read everything over REST once the stream is back:

```python
async def _subscribe_channels(self, websocket_assistant: WSAssistant):
    await websocket_assistant.send(...)  # subscribe
    if self._missed_updates:
        self._missed_updates = False
        self._connector.resync_after_stream_gap()  # sets the status poll off at once

async def _on_user_stream_interruption(self, websocket_assistant):
    self._missed_updates = True
    await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
```

### 5.5 The main class

#### Constructor

Set your own attributes first, then hand the options settings to the base
class. `stop_before_expiry` is in seconds:

```python
super().__init__(
    balance_asset_limit,
    rate_limits_share_pct,
    allow_selling=utils.as_bool(delta_options_allow_selling),
    max_short_contracts=Decimal(str(delta_options_max_short_contracts)),
    stop_before_expiry=float(str(delta_options_stop_before_expiry_minutes)) * 60,
)
```

Refuse more pairs than one WebSocket subscription can hold, with a clear
message.

#### Contract sizes

Hummingbot amounts are in the underlying, and exchange sizes are in contracts.
Convert at the edge, and nowhere else:

```python
def contract_size(self, trading_pair: str) -> Decimal:
    return self._contract_values[trading_pair]

def size_to_amount(self, trading_pair: str, size: Decimal) -> Decimal:
    return size * self._contract_values[trading_pair]
```

In the trading rules, set both `min_order_size` and
`min_base_amount_increment` to the contract value. Orders are then quantized
to whole contracts before they are sent.

#### Instruments and the symbol map

In `_initialize_trading_pair_symbols_from_exchange_info`, build the symbol
map, the instruments, contract sizes, product ids and fee rates, then call
`self._options_trading.set_instruments(instruments)`.

The map is rebuilt from live products on every trading-rules refresh (every
30 minutes), so **the bot's own options must stay known after they expire.**
Cancels, late fills and the settlement still need their ids and sizes. Delta's
`_keep_unlisted_trading_pairs` carries over the old data for the connector's
own pairs that are no longer listed. An option it never saw listed gets its
instrument from its name. `_format_trading_rules` likewise keeps the old rules
of the connector's own unlisted pairs.

Keep a plain dict of pair to symbol as well (`self._symbols`). The base
class's symbol map is held in Cython and can't be read from Python.

#### Placing orders

```python
payload = {
    "product_id": self._product_ids[trading_pair],
    "size": int(amount / self.contract_size(trading_pair)),      # whole contracts
    "side": "buy" if trade_type == TradeType.BUY else "sell",
    "order_type": "limit_order",
    "limit_price": str(price),
    "time_in_force": "gtc",
    "client_order_id": order_id,
    "reduce_only": position_action == PositionAction.CLOSE,      # a close can never flip the position
}
if order_type == OrderType.LIMIT_MAKER:
    payload["post_only"] = True
```

#### Cancelling

- Wait for the exchange order id of an order that is still being placed
  (`await tracked_order.get_exchange_order_id()`). Otherwise a cancel, or the
  kill switch, misses it.
- Return `True` only when the exchange says the order is cancelled. An order
  that filled first comes back as "closed" and keeps its fills.
- If the exchange answers "not found", read the order's real state. It may
  have been cancelled by the exchange (dead-man switch, expiry) or it may have
  filled. Delta's `_closed_order_was_cancelled` does this.

#### Fills

Read fills from whichever source carries the commission. On Delta that is REST
`/v2/fills`: `v2/user_trades` has no commission, so a stream fill only
triggers a REST read, retried at `FILL_FETCH_DELAYS` until the stream's fill
count is recorded. Fills arrived over REST about 2 s after the trade. The
status poll recovers fills the stream missed.

#### Positions

```python
async def _update_positions(self):
    response = await self._api_get(path_url=POSITIONS_PATH_URL,
                                   params={"contract_types": OPTION_CONTRACT_TYPES}, is_auth_required=True)
    open_pairs = set()
    for entry in response.get("result") or []:
        ...  # map the symbol to a pair, skip size 0
        self._options_trading.set_position(trading_pair, Position(
            trading_pair=trading_pair,
            position_side=PositionSide.LONG if size > 0 else PositionSide.SHORT,
            unrealized_pnl=..., entry_price=...,
            amount=self.size_to_amount(trading_pair, size),
            leverage=Decimal("1"),
        ))
        open_pairs.add(trading_pair)
    # A position the exchange stops listing is closed, or its option has expired.
    for trading_pair in set(self.account_positions) - open_pairs:
        self._options_trading.remove_position(trading_pair)
```

The last loop matters. Without it, a closed or expired position stays in the
bot forever, and `flatten()` tries to close it.

#### Fees

Return the fee as a share of the premium, so the base class can multiply it by
price × amount:

```python
# Delta: the lower of (rate × underlying value) and (cap × premium), plus GST.
share_of_premium = premium_cap                       # when a price is unknown
if underlying_price and not price.is_nan() and price > 0:
    share_of_premium = min(rate * underlying_price / price, premium_cap)
return TradeFeeBase.new_perpetual_fee(fee_schema=self.trade_fee_schema(), position_action=position_action,
                                      percent=share_of_premium * (Decimal("1") + GST_RATE))
```

The fee recorded on each fill is the exchange's own commission. On Delta it
matched this estimate to the cent on every live fill.

#### Dead-man switch (if the exchange has one)

- Run a heartbeat loop from `start_network` when trading is required and the
  setting is on. Delta registers one heartbeat per key and option set (its id
  hashes both), so a restarted bot re-arms its own.
- Never start a second loop next to a running one.
- On a clean stop, switch it off (Delta: TTL 0), or the exchange cancels
  orders after the bot has gone.
- Serialize `stop_network` with a lock. The clock's exit stops the network in
  the background while the script may stop it too, and the second stop closed
  the proxy session under the first one's switch-off.

---

## 6. Expiry and settlement

```
expiry − N min   closing window: orders that would open a position are refused, resting ones
                 are cancelled once; closing orders are still accepted (base class)
expiry           OptionExpired, once per option; _on_option_expired(pair, last position seen)
expiry + <1 s    Delta writes a settlement fill for every position, worthless ones included
expiry + 5 s …   the connector reads the fills and raises OptionSettled for each settlement fill
                 MarketsRecorder stores it as the trade that closes the position
```

### What the base class does

The expiry watch runs every 5 s. At expiry it calls
`_on_option_expired(trading_pair, position)` with the position held at expiry.
That is the last one seen before it: Delta settles within a second of expiry,
so its position stream can remove a position before the next check sees it.

### What your connector does in `_on_option_expired`

Find the exchange's settlement record and raise one event per settlement:

```python
self.trigger_event(MarketEvent.OptionSettled, OptionSettledEvent(
    timestamp=...,             # when the exchange settled it
    market=self.name,
    trading_pair=trading_pair,
    trade_type=TradeType.SELL,  # the side that closes the position: SELL for a long, BUY for a short
    amount=amount,             # in the underlying
    price=price,               # the settlement value per unit of the underlying (0 if worthless)
    trade_fee=fee,             # the settlement fee, as a flat fee in the settling asset
    exchange_trade_id=fill_id, # the exchange's id: it makes recording safe to repeat
))
```

How `delta_options` does it, which is a good template:

- **It registers the option as due.** `_on_option_expired` records the expiry,
  whether a position was held, and how long to wait. One background task,
  `_settlement_watch_loop`, handles all due options. `start_network` resumes
  it and `stop_network` pauses it, so a network drop at expiry loses nothing.
- **It reads Delta's fills.** From 5 s after expiry, every 10 s,
  `_look_for_settlements` reads the account's fills on the due options, 10
  options per request, from a minute before expiry. It keeps the fills with
  `fill_type: settlement`.
- **How long it waits.** A position held at expiry is waited for up to 15
  minutes, then reported with a warning. Where nothing was held, one
  successful read is enough.
- **Restarts.** After a restart past expiry, the same read finds the fill
  again, and the fill id stops it being recorded twice. If the connector only
  knows an option by name, it reads the product by symbol first, to get its id
  and contract size.
- **Worthless options are recorded too, at price 0.** Without that, PnL would
  show the position opened and never closed.
- **Settlement fills are invisible to order tracking.** They carry an order id
  the exchange generated, so they must be looked for explicitly.

If your exchange writes no settlement fill, build the event from what it does
write. A wallet entry with the payout plus the position held at expiry is
enough: price = payout ÷ amount, and the entry's id serves as
`exchange_trade_id`.

### How it is recorded

`MarketsRecorder` stores each `OptionSettled` once, keyed by
`exchange_trade_id`, as a `TradeFill`: `order_id = settlement-<id>`,
`order_type = SETTLEMENT`, `position = CLOSE`, with the fee. PnL pairs it with
the opening fill, which `test_markets_recorder.py` checks. In the live test on
10 Nov, the account's balance moved by exactly the payout minus the
settlement fee.

---

## 7. Using an options connector from a strategy

```python
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.strategy.option_events import listen_to_option_events
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

CONNECTOR, PAIR = "delta_options", "C_BTC_100000_251226-USD"


class OptionsSketch(ScriptStrategyBase):
    markets = {CONNECTOR: {PAIR}}            # exact options, chosen before the start

    def __init__(self, connectors):
        super().__init__(connectors)
        # Keep the list on the strategy: connectors hold their listeners weakly.
        self._option_listeners = listen_to_option_events(self, self.connectors.values())
        self._bought = False

    def on_tick(self):
        connector = self.connectors[CONNECTOR]
        if self._bought or not connector.ready:
            return
        info = connector.get_option_info(PAIR)          # mark_price, mark_iv, delta, gamma, theta, vega, ...
        one = connector.contract_size(PAIR)             # one contract, in BTC
        candidate = PerpetualOrderCandidate(
            trading_pair=PAIR, is_maker=True, order_type=OrderType.LIMIT, order_side=TradeType.BUY,
            amount=one, price=info.mark_price, leverage=Decimal("1"))
        if connector.budget_checker.adjust_candidate(candidate, all_or_none=True).amount > 0:
            self.buy(CONNECTOR, PAIR, one, OrderType.LIMIT, info.mark_price, position_action=PositionAction.OPEN)
            self._bought = True

    def did_expire_option(self, event):      # OptionExpiredEvent
        self.logger().info(f"{event.trading_pair} expired")

    def did_settle_option(self, event):      # OptionSettledEvent
        self.logger().info(f"{event.trading_pair} settled at {event.price}, fee {event.trade_fee.flat_fees}")
```

- **Open with `PositionAction.OPEN` and close with `PositionAction.CLOSE`.** A
  close is sent reduce-only.
- **Size budget checks with `PerpetualOrderCandidate`** (leverage 1), and
  pass `position_close=True` for closing orders, which lock nothing.
- **Kill switch:** `safe_ensure_future(connector.flatten())`.
- **Pick the options before the start.** Hummingbot calls a script's
  `init_markets` synchronously, before any connector runs, so
  `delta_options_chain` reads the chain over plain HTTP, without keys:

```python
from hummingbot.connector.derivative.delta_options.delta_options_chain import fetch_option_chain, pick_strangle

@classmethod
def init_markets(cls, config):
    chain, spot = fetch_option_chain("BTC")      # every live BTC option, and BTC's spot price
    call, put = pick_strangle(chain, spot, otm=Decimal("0.02"), min_hours_to_expiry=Decimal("1"))
    cls.markets = {"delta_options": {call.trading_pair, put.trading_pair}}
```

The complete example is `scripts/options_strangle_example.py`. It buys a
strangle on a daily expiry, hedges its delta with `delta_perpetual`, holds it
to settlement, then closes the hedge and reports the result. Stopping it before
expiry closes everything.

---

## 8. Writing tests

Put tests in `test/hummingbot/connector/derivative/<name>_options/`. Here is
what `delta_options` covers, file by file:

| Test file | What it covers |
|---|---|
| `test_<name>_options_utils.py` | Names both ways, the product filter, expiry from the product and from the name, settings defaults and validation, settings reaching the connector |
| `test_<name>_options_api_order_book_data_source.py` | Book, trade and ticker parsing, with sizes converted to the underlying; subscriptions skip expired options; empty book for an expired option; tickers skip expired options |
| `test_<name>_options_api_user_stream_data_source.py` | Login, channel subscription, re-sync after a drop |
| `test_<name>_options_derivative.py` | Symbol map, product paging, trading rules, option chain, the fee formula, balances, positions (including removal), readiness |
| `test_<name>_options_trading.py` | Order payloads (reduce-only, post-only), every refusal, selling guard, price band, margin estimate and budget checker, `flatten()`, cancels (including "not found"), fills over REST, dead-man switch |
| `test_<name>_options_expiry.py` | Expired options stay known, closing window, `OptionExpired` once, the watch survives errors |
| `test_<name>_options_settlement.py` | Settlement fills to `OptionSettled` (long, short, worthless), waits and retries, lookup by symbol after a restart, batching |
| `test/hummingbot/connector/test_markets_recorder.py` | A settlement is recorded once as the closing fill, and PnL closes |
| `test/hummingbot/strategy/test_option_events.py` | The strategy helper calls the handlers |

Patterns that work:

- **Move the clock by patching `_time`:**
  `with patch.object(connector, "_time", return_value=EXPIRY - 29 * 60): ...`
- **Keep `EventLogger`s on `self`.** Connectors hold listeners weakly, so a
  local one stops receiving events.
- **Pin `is_expired` on mocked connectors.** On a `MagicMock` connector,
  `is_expired()` returns a truthy mock, which makes every option look expired.
  Set `connector.is_expired.return_value = False`.
- **Drive background loops by hand.** Call `_check_expiries()` and
  `_look_for_settlements()` directly, and mock `_start_settlement_watch` and
  `_sleep`.
- **Base fixtures on live payloads.** The settlement fill in
  `test_delta_options_settlement.py` is modelled on the one Delta wrote on
  10 Nov.
- **Yield after `connector.buy(...)`.** It places the order from a task, so
  `await asyncio.sleep(0)` a few times before asserting.
- **Use `IsolatedAsyncioWrapperTestCase`** for async tests.

```bash
python -m pytest test/hummingbot/connector/derivative/<name>_options/ -v
python -m pytest test/hummingbot/connector/test_markets_recorder.py test/hummingbot/strategy/test_option_events.py -v

# Coverage without pytest-cov:
python -m coverage run --include="hummingbot/connector/derivative/<name>_options/*" -m pytest test/hummingbot/connector/derivative/<name>_options/
python -m coverage report -m
```

Two tests in `test_markets_recorder.py`, `test_store_market_data` and
`test_store_market_data_calculates_missing_values`, fail without the options
changes as well. They are not caused by options work.

---

## 9. Live verification

Climb this ladder one step at a time. Each step needs the previous one to pass
and an explicit go from whoever owns the account.

| Step | What it proves | Delta cost | Delta result |
|---|---|---|---|
| 1. Public data | Chain read, books and tickers stream, Greeks are sane (call delta 0..1, put −1..0), REST agrees with the stream, a forced reconnect leaves an expired option out | 0 | 36/36, 2 Nov |
| 2. With keys | Private stream login, balances, trading rules, positions | 0 | 35/35, 2 Nov |
| 3. Resting orders | Post-only orders far from the book are confirmed by the exchange and cancelled; every refusal; the dead-man switch cancels a resting order when heartbeats stop; a clean stop switches it off | 0 (nothing fills) | 67/67, 3 Nov |
| 4. Tiny fills | A 1-contract buy recovered over REST with the stream paused; a close by `flatten()`; a short and its buy-back; fees and margin against the estimates | 0.03 USD | 62/62, 3 Nov |
| 5. Settlement probe | Read-only: what the exchange keeps after expiry, and the account's records | 0 | 5/5, 10 Nov |
| 6. Hold through expiry | 1 in-the-money and 1 out-of-the-money contract held through an expiry: settlement fills, fees, wallet entries, balance | 0.006 USD | 13/13, 10 Nov |
| 7. The bot through an expiry | The same, with the connector running through the expiry: it raises `OptionExpired` and `OptionSettled` itself, matching the exchange's records; positions and balance update; the trade records hold the buy and the settlement | about 0.01 USD | not run yet |

Rules for live runs:

- **Rehearse every script offline first,** against a fake exchange that
  answers REST and feeds the private stream in-process.
- **Keep orders as small and as safe as possible.** Use the smallest size.
  Price test orders from the exchange's price band. Make orders that must not
  fill post-only, so the exchange rejects them rather than letting them take
  liquidity.
- **Check the balance before and after.** It must move by exactly the premiums
  and fees.
- **Never print secrets,** and mask the password in proxy URLs.
- **Time expiry runs carefully.** Delta expires at 12:00 UTC (17:30 IST).
  Buy before the closing window opens (17:00 IST by default) and check from
  17:45 IST.

The Delta scripts are outside the repo, in `temp/connectors/delta_options/`:

- `verify_delta_options.py` covers steps 1–4 (`--with-keys`, `--trade`,
  `--fill`).
- `probe_settlement.py` is step 5.
- `hold_through_expiry.py` is step 6 (`buy`, then `check`) and step 7
  (`buy --stay`).
- `rehearse_offline.py` and `rehearse_hold.py` are the offline rehearsals.

---

## 10. Pitfalls we hit

| Symptom | Cause | Fix |
|---|---|---|
| A MARKET order crashed the band check | `Decimal("NaN") < x` raises, and the base class looks up a book price for unpriced orders, which fails on an empty book | Refuse unsupported order types first, before any price check |
| A cancel reported success but the order had filled | The exchange answered "closed" | Count a cancel only when the exchange says "cancelled" |
| An order the dead-man switch cancelled stayed OPEN in the bot | The stream dropped at the moment of the trip, the exchange doesn't replay, and the safety cancel's "not found" left it open | On "not found", read the order's real state; re-sync over REST after every stream reconnect |
| The dead-man switch stayed armed after shutdown | The clock's exit and the script both stopped the network; the second stop closed the proxy session under the first one's switch-off | Serialize `stop_network` with a lock |
| A heartbeat came back after the bot switched it off | A second heartbeat loop was running | Never start a loop next to a running one |
| After an expiry, the connector never became ready again | The order book tracker stops at the first failing snapshot (the expired option), and option-info init retried it forever | Empty book for expired options; readiness and option-info init count live options only |
| `KeyError` on cancels and fills after an expiry | The 30-minute refresh rebuilt the maps from live products only | Keep the connector's own options' ids, sizes and rules after they drop out |
| The symbol map couldn't be read from Python | The base class's map is held in Cython | Keep a plain dict of your own |
| A position settled at expiry could look "not held" (found in review) | Delta settles within a second, so the position stream can remove it before the 5 s check | The base class remembers the last position seen before expiry |
| Order tracking ignores settlement fills | They carry an order id the exchange generated | Read them explicitly after expiry |
| Strategy handlers never fired | An inline `EventForwarder` is garbage-collected, because listeners are held weakly | Keep the forwarders; `listen_to_option_events` returns them |
| Strategies never got option events | Script strategies receive a fixed set of connector events | `listen_to_option_events` |
| Fee estimates tens of times too low | The perpetuals formula applies the rate to the premium | The options formula: the rate on the underlying's value, capped by the premium |
| A clash with the wallet-transfer branch | Event numbers 213 to 216 are taken | Options events use 220 and 221 |
| The shared proxy dropped every connection | Two WebSocket reconnects within seconds | In live scripts, force at most one reconnect |

---

## 11. Checklist before raising a PR

### Instruments and data

- [ ] Only live vanilla calls and puts are listed; pagination is followed
- [ ] Names convert both ways; expiry comes from the product, with a name fallback
- [ ] The connector refuses more pairs than one subscription can hold
- [ ] Sizes are converted from contracts everywhere: books, trades, positions, fills, open interest
- [ ] Option info (mark, IV, Greeks, price band) streams and initialises from REST
- [ ] Subscriptions and ticker requests skip expired options

### Orders and positions

- [ ] Market orders are refused; closing orders are reduce-only
- [ ] A cancel counts only when the exchange confirms it; "not found" reads the real state
- [ ] Fills carry the exchange's commission; the stream triggers REST reads with retries
- [ ] Positions are net, leverage 1, and removed when the exchange stops listing them
- [ ] The private stream re-syncs over REST after a reconnect
- [ ] The fee estimate matches the exchange's real commissions

### Safety

- [ ] Selling to open is off by default and capped per option
- [ ] `flatten()` closes the connector's positions and leaves other options alone
- [ ] Dead-man switch (if the exchange has one): on by default, never two loops, switched off on a clean stop, stops serialized

### Expiry and settlement

- [ ] The connector's own options stay known after they expire
- [ ] An expired option never stops the connector becoming ready
- [ ] `_on_option_expired` raises `OptionSettled` from the exchange's record: longs, shorts and worthless options
- [ ] A restart after expiry records each settlement once
- [ ] A position held at expiry without a settlement record is reported

### Tests and live checks

- [ ] All test files pass, with no real network calls
- [ ] Live steps 1–7 of section 9 have passed, and the results are written up
- [ ] The balance moved by exactly the premiums and fees in every trading run

---

## 12. Reference: the delta\_options connector

| File | Look here for |
|---|---|
| `delta_options_constants.py` | Contract types, linked rate limits, heartbeat and settlement timings |
| `delta_options_utils.py` | Names both ways, product filter, instruments from products and names, the trading settings |
| `delta_options_api_order_book_data_source.py` | `ob_l2`/`trades`/`ticker` parsing, sizes in contracts, the option-info feed, live-only subscriptions |
| `delta_options_api_user_stream_data_source.py` | Key-auth login, category subscriptions, re-sync after a drop |
| `delta_options_derivative.py` | Keeping expired options known, orders and cancels, fills over REST, positions, the fee formula, the dead-man switch, settlement (`_on_option_expired` and the methods after it) |
| `delta_options_chain.py` | Picking options before the start: the chain over plain HTTP, and a strangle picker |
| `connector/option_derivative_py_base.py` | Order refusals, selling guard, `flatten()`, margin estimate, the expiry watch |
| `scripts/options_strangle_example.py` | A complete strategy: buy, hedge with the perpetual, hold to settlement |

**Start from `delta_options`.** Copy it, rename everything, then change the
exchange-specific parts. Read the tests next to each file: they show the
behaviour each rule protects.

---

*For the general connector steps, see [NEW_EXCHANGE_INTEGRATION_GUIDE.md](./NEW_EXCHANGE_INTEGRATION_GUIDE.md).*
*For the options design and its phases, see [OPTIONS_FRAMEWORK_PLAN](./OPTIONS_FRAMEWORK_PLAN).*
*For proxy support, see [PROXY_SERVER_GUIDE.md](./PROXY_SERVER_GUIDE.md).*
