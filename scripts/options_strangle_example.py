"""
Example options strategy for the delta_options connector: buys a strangle on a daily expiry, holds it to settlement and
hedges its delta with the BTC perpetual.

At start it picks the soonest Delta expiry at least `min_hours_to_expiry` away, a call about `otm` above BTC's price
and a put about `otm` below it, and buys `contracts_per_leg` contracts of each (one contract is 0.001 BTC). While they
are held, it keeps the strangle's net delta within `hedge_threshold` BTC with delta_perpetual orders. At expiry Delta
settles both legs; the connector records each settlement as the trade that closes it, and the script then closes the
perpetual hedge and reports the result. Stopping the bot before expiry closes everything.

Run it with a config file (`create --script-config options_strangle_example`), after `connect delta_options` and
`connect delta_perpetual` with keys of the same Delta account. Delta's options expire at 12:00 UTC (17:30 IST); the
connector opens no position in an option's last 30 minutes, so start it at least an hour before.
"""
import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.delta_options.delta_options_chain import fetch_option_chain, pick_strangle
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import OptionExpiredEvent, OptionSettledEvent, OrderFilledEvent
from hummingbot.strategy.option_events import listen_to_option_events
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

IST = timezone(timedelta(hours=5, minutes=30))
RETRY_DELAY = 5  # seconds before an entry or closing order that failed or was cancelled is sent again
ENTRY_ORDER_TIMEOUT = 30  # seconds before an unfilled entry order is cancelled, to be placed again at the new price
SETTLEMENT_WAIT = 15 * 60  # after the expiry, how long to wait for Delta's settlements before closing the hedge anyway
STOP_TIMEOUT = 15  # seconds a stop waits for its closing orders to fill


class OptionsStrangleConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    options_exchange: str = Field("delta_options")
    hedge_exchange: str = Field("delta_perpetual")
    hedge_pair: str = Field("BTC-USD")
    underlying: str = Field("BTC")
    otm: Decimal = Field(Decimal("0.02"), description="Strike distance from spot, e.g. 0.02 = 2%")
    contracts_per_leg: int = Field(1, gt=0)
    min_hours_to_expiry: Decimal = Field(Decimal("1"), description="Skip expiries closer than this")
    entry_slippage: Decimal = Field(Decimal("0.02"), description="Entry limit price up to this far above the ask")
    hedge: bool = Field(True)
    hedge_threshold: Decimal = Field(Decimal("0.0006"), description="Net delta in BTC that triggers a hedge")
    hedge_interval: int = Field(10, gt=0, description="Seconds between hedge checks")
    call_pair: str = Field("", description="Chosen at start when empty")
    put_pair: str = Field("", description="Chosen at start when empty")


def crossing_price(reference: Decimal, is_buy: bool, slippage: Decimal, tick: Decimal,
                   band_lower: Optional[Decimal] = None, band_upper: Optional[Decimal] = None) -> Decimal:
    """
    A limit price that crosses the book by up to `slippage` from `reference` (the best ask for a buy, the best bid for
    a sell), on the tick grid and inside the exchange's price band.
    """
    price = reference * (1 + slippage) if is_buy else reference * (1 - slippage)
    if band_upper is not None:
        price = min(price, band_upper)
    if band_lower is not None:
        price = max(price, band_lower)
    steps = price / tick
    rounded = steps.to_integral_value(rounding=ROUND_CEILING if is_buy else ROUND_FLOOR) * tick
    if band_upper is not None and rounded > band_upper:
        rounded = steps.to_integral_value(rounding=ROUND_FLOOR) * tick
    if band_lower is not None and rounded < band_lower:
        rounded = steps.to_integral_value(rounding=ROUND_CEILING) * tick
    return rounded


def hedge_order(net_delta: Decimal, hedge_position: Decimal, threshold: Decimal,
                contract: Decimal) -> Optional[Tuple[TradeType, Decimal, PositionAction]]:
    """
    The perpetual order that brings the net delta back toward zero once it is `threshold` or more away from it, in
    whole contracts. An order never flips the hedge: the part that reduces it goes first, as a closing order. With a
    threshold above half a contract, what is left after a hedge is below the threshold, so hedges don't flip-flop.
    """
    if contract <= 0 or abs(net_delta) < threshold:
        return None
    side = TradeType.SELL if net_delta > 0 else TradeType.BUY
    amount = max((abs(net_delta) / contract).to_integral_value(rounding=ROUND_HALF_UP), Decimal("1")) * contract
    reduces = (side == TradeType.SELL and hedge_position > 0) or (side == TradeType.BUY and hedge_position < 0)
    if reduces:
        return side, min(amount, abs(hedge_position)), PositionAction.CLOSE
    return side, amount, PositionAction.OPEN


def ist(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=IST).strftime("%H:%M IST, %d %b")


class OptionsStrangleExample(ScriptStrategyBase):
    """
    States: buying → holding (hedging) → settling (waiting for Delta's settlements) → closing hedge → done.
    """

    @classmethod
    def init_markets(cls, config: OptionsStrangleConfig):
        if not (config.call_pair and config.put_pair):
            chain, spot = fetch_option_chain(config.underlying)
            if spot is None:
                raise ValueError(f"Delta lists no live {config.underlying} options.")
            call, put = pick_strangle(chain, spot, config.otm, config.min_hours_to_expiry)
            config.call_pair, config.put_pair = call.trading_pair, put.trading_pair
        cls.markets = {config.options_exchange: {config.call_pair, config.put_pair}}
        if config.hedge:
            cls.markets[config.hedge_exchange] = {config.hedge_pair}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: OptionsStrangleConfig):
        super().__init__(connectors, config)
        self.config = config
        self.legs = [config.call_pair, config.put_pair]
        self.state = "buying"
        self.settlements: Dict[str, OptionSettledEvent] = {}
        self.fills: List[OrderFilledEvent] = []
        self._entry_orders: Dict[str, Tuple[str, float]] = {}  # leg: (client order id, sent at)
        self._close_order: Optional[Tuple[str, float]] = None
        self._next_hedge_check = 0.0
        self._said: Set[str] = set()
        # Connectors hold their listeners weakly, so the strategy keeps them.
        self._option_listeners = listen_to_option_events(self, [connectors[config.options_exchange]])

    # ── Reading the markets ───────────────────────────────────────────────────

    @property
    def options(self) -> ConnectorBase:
        return self.connectors[self.config.options_exchange]

    @property
    def perp(self) -> Optional[ConnectorBase]:
        return self.connectors.get(self.config.hedge_exchange) if self.config.hedge else None

    def expiry(self) -> float:
        return self.options.get_instrument(self.config.call_pair).expiry_timestamp

    def held(self, pair: str) -> Decimal:
        position = self.options.get_position(pair)
        return position.amount if position is not None else Decimal("0")

    def hedge_position(self) -> Decimal:
        if self.perp is None:
            return Decimal("0")
        return sum((p.amount for p in self.perp.account_positions.values() if p.trading_pair == self.config.hedge_pair),
                   Decimal("0"))

    def net_delta(self) -> Optional[Decimal]:
        """The strangle's delta in BTC, plus the hedge; None while a held leg's delta is unknown."""
        total = self.hedge_position()
        for pair in self.legs:
            amount = self.held(pair)
            if amount == 0:
                continue
            delta = self.options.get_option_info(pair).delta
            if delta is None:
                return None
            total += delta * amount
        return total

    def hedge_threshold(self) -> Decimal:
        contract = self.perp.trading_rules[self.config.hedge_pair].min_order_size
        # Above half a contract, a hedge never leaves a remainder that calls for the opposite hedge.
        return max(self.config.hedge_threshold, contract * Decimal("0.6"))

    @staticmethod
    def _best(connector: ConnectorBase, pair: str, is_buy: bool) -> Optional[Decimal]:
        book = connector.get_order_book(pair)
        entries = list(book.ask_entries() if is_buy else book.bid_entries())
        return Decimal(str(entries[0].price)) if entries else None

    def _active(self, exchange: str, order_id: str) -> bool:
        return any(order.client_order_id == order_id for order in self.get_active_orders(exchange))

    def _say_once(self, key: str, message: str):
        if key not in self._said:
            self._said.add(key)
            self.logger().info(message)

    # ── The cycle ─────────────────────────────────────────────────────────────

    def on_tick(self):
        if self.state == "buying":
            self._buy_legs()
        elif self.state == "holding":
            if self.current_timestamp >= self.expiry():
                self.state = "settling"
                self.logger().info(f"The strangle expired at {ist(self.expiry())}; waiting for Delta's settlements.")
            else:
                self._hedge()
        elif self.state == "settling":
            settled = all(pair in self.settlements for pair in self.legs if self._bought(pair))
            if settled or self.current_timestamp >= self.expiry() + SETTLEMENT_WAIT:
                if not settled:
                    self.logger().warning("No settlement recorded for every leg; closing the hedge anyway.")
                self.state = "closing hedge"
        elif self.state == "closing hedge":
            self._close_hedge()

    def _bought(self, pair: str) -> bool:
        return any(f.trading_pair == pair and f.trade_type == TradeType.BUY for f in self.fills)

    def _buy_legs(self):
        missing_any = False
        for pair in self.legs:
            target = self.options.contract_size(pair) * self.config.contracts_per_leg
            missing = target - self.held(pair)
            if missing <= 0:
                continue
            missing_any = True
            if self.options.is_closing_only(pair):
                self._say_once(f"late {pair}", f"Too close to {pair}'s expiry to open a position; holding what is "
                                               f"bought.")
                missing_any = False
                break
            sent = self._entry_orders.get(pair)
            if sent is not None:
                order_id, sent_at = sent
                if self._active(self.config.options_exchange, order_id):
                    if self.current_timestamp - sent_at > ENTRY_ORDER_TIMEOUT:
                        self.cancel(self.config.options_exchange, pair, order_id)
                    continue
                if self.current_timestamp - sent_at < RETRY_DELAY:
                    continue
            ask = self._best(self.options, pair, is_buy=True)
            if ask is None:
                self._say_once(f"no ask {pair}", f"Nobody is selling {pair} yet; waiting.")
                continue
            info = self.options.get_option_info(pair)
            price = crossing_price(ask, True, self.config.entry_slippage,
                                   self.options.trading_rules[pair].min_price_increment,
                                   info.price_band_lower, info.price_band_upper)
            candidate = PerpetualOrderCandidate(trading_pair=pair, is_maker=False, order_type=OrderType.LIMIT,
                                                order_side=TradeType.BUY, amount=missing, price=price,
                                                leverage=Decimal("1"))
            if self.options.budget_checker.adjust_candidate(candidate, all_or_none=True).amount <= 0:
                self._say_once(f"budget {pair}", f"Not enough balance to buy {missing} of {pair} at {price}.")
                continue
            order_id = self.buy(self.config.options_exchange, pair, missing, OrderType.LIMIT, price,
                                position_action=PositionAction.OPEN)
            self._entry_orders[pair] = (order_id, self.current_timestamp)
        if not missing_any:
            self.state = "holding"
            self.logger().info(f"Strangle held: {', '.join(f'{self.held(p)} {p}' for p in self.legs)}. Holding it to "
                               f"settlement at {ist(self.expiry())}.")

    def _hedge(self):
        if self.perp is None or self.current_timestamp < self._next_hedge_check:
            return
        self._next_hedge_check = self.current_timestamp + self.config.hedge_interval
        if self.get_active_orders(self.config.hedge_exchange):
            return  # one hedge order at a time
        net = self.net_delta()
        if net is None:
            return
        rule = self.perp.trading_rules[self.config.hedge_pair]
        order = hedge_order(net, self.hedge_position(), self.hedge_threshold(), rule.min_order_size)
        if order is not None:
            side, amount, action = order
            self.logger().info(f"Net delta {net:+.5f} BTC: hedging with {side.name.lower()} {amount} "
                               f"{self.config.hedge_pair} ({action.name.lower()}).")
            self._send_perp(side, amount, action)

    def _send_perp(self, side: TradeType, amount: Decimal, action: PositionAction) -> Optional[str]:
        is_buy = side == TradeType.BUY
        reference = self._best(self.perp, self.config.hedge_pair, is_buy)
        if reference is None:
            self._say_once("no perp book", f"{self.config.hedge_pair} has no book on that side; not hedging now.")
            return None
        price = crossing_price(reference, is_buy, self.config.entry_slippage,
                               self.perp.trading_rules[self.config.hedge_pair].min_price_increment)
        place = self.buy if is_buy else self.sell
        return place(self.config.hedge_exchange, self.config.hedge_pair, amount, OrderType.LIMIT, price,
                     position_action=action)

    def _close_hedge(self):
        position = self.hedge_position()
        if position == 0:
            self.state = "done"
            self.logger().info(self.result())
            return
        if self._close_order is not None:
            order_id, sent_at = self._close_order
            if self._active(self.config.hedge_exchange, order_id) or self.current_timestamp - sent_at < RETRY_DELAY:
                return
        side = TradeType.SELL if position > 0 else TradeType.BUY
        order_id = self._send_perp(side, abs(position), PositionAction.CLOSE)
        if order_id is not None:
            self._close_order = (order_id, self.current_timestamp)

    # ── Events ────────────────────────────────────────────────────────────────

    def did_fill_order(self, event: OrderFilledEvent):
        self.fills.append(event)

    def did_expire_option(self, event: OptionExpiredEvent):
        self.logger().info(f"{event.trading_pair} expired at {ist(event.expiry_timestamp)}.")

    def did_settle_option(self, event: OptionSettledEvent):
        self.settlements[event.trading_pair] = event
        fee = sum((f.amount for f in event.trade_fee.flat_fees), Decimal("0"))
        self.logger().info(f"{event.trading_pair} settled by Delta: {event.trade_type.name.lower()} {event.amount} at "
                           f"{event.price}, fee {fee}.")

    # ── Result, stop and status ───────────────────────────────────────────────

    @staticmethod
    def _fee(event) -> Decimal:
        return sum((f.amount for f in event.trade_fee.flat_fees), Decimal("0"))

    def result(self) -> str:
        """What the round trip made, from the fills and settlements recorded (fees in the settling asset, USD)."""
        cash = Decimal("0")
        for fill in self.fills:
            value = fill.price * fill.amount
            cash += (value if fill.trade_type == TradeType.SELL else -value) - self._fee(fill)
        for event in self.settlements.values():
            value = event.price * event.amount
            cash += (value if event.trade_type == TradeType.SELL else -value) - self._fee(event)
        return f"Done: the strangle and its hedge made {cash:+.6f} USD after fees."

    async def on_stop(self):
        if self.state == "done" or not self.ready_to_trade:
            return
        before_expiry = time.time() < self.expiry()
        if before_expiry and any(self.held(pair) != 0 for pair in self.legs):
            self.logger().info("Stopping before expiry: closing the strangle.")
            await self.options.flatten()  # cancels the connector's orders, then closes its positions reduce-only
        else:
            for order in self.get_active_orders(self.config.options_exchange):
                self.cancel(self.config.options_exchange, order.trading_pair, order.client_order_id)
        if self.perp is not None:
            for order in self.get_active_orders(self.config.hedge_exchange):
                self.cancel(self.config.hedge_exchange, order.trading_pair, order.client_order_id)
            position = self.hedge_position()
            if position != 0:
                self.logger().info(f"Stopping: closing the {position} {self.config.hedge_pair} hedge.")
                self._send_perp(TradeType.SELL if position > 0 else TradeType.BUY, abs(position),
                                PositionAction.CLOSE)
        deadline = time.time() + STOP_TIMEOUT
        while time.time() < deadline and not self._flat(before_expiry):
            await asyncio.sleep(1)
        self.state = "stopped"
        if not self._flat(before_expiry):
            self.logger().warning("Stopped with positions still open: "
                                  + ", ".join(f"{self.held(p)} {p}" for p in self.legs)
                                  + f", hedge {self.hedge_position()} {self.config.hedge_pair}.")

    def _flat(self, legs_too: bool) -> bool:
        legs_flat = not legs_too or all(self.held(pair) == 0 for pair in self.legs)
        return legs_flat and self.hedge_position() == 0

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        expiry = self.expiry()
        left = expiry - time.time()
        lines = [f"State: {self.state}. Expiry {ist(expiry)}"
                 + (f", in {left / 3600:.1f} h." if left > 0 else ", passed."), "Legs:"]
        for pair in self.legs:
            line = f"  {pair}: held {self.held(pair)}"
            try:
                info = self.options.get_option_info(pair)
                line += f", mark {info.mark_price}, delta {info.delta}, IV {info.mark_iv}"
            except KeyError:
                pass
            if pair in self.settlements:
                line += f", settled at {self.settlements[pair].price}"
            lines.append(line)
        if self.perp is not None:
            lines.append(f"Hedge: {self.hedge_position()} {self.config.hedge_pair}")
            net = self.net_delta()
            if net is not None:
                lines.append(f"Net delta: {net:+.5f} BTC (hedged beyond ±{self.hedge_threshold()})")
        active = [o for name in self.connectors for o in self.get_active_orders(name)]
        lines.append(f"Open orders: {len(active)}")
        return "\n".join(lines)
