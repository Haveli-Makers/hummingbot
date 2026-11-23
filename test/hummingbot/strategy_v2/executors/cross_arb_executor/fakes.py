"""
Fake venue and strategy for the cross-arb executor tests.

Small on purpose: orders only do what a venue can do to them — fill, partly fill, be cancelled,
be refused, or fill AFTER being cancelled, which is the case that matters most here.
"""
from decimal import Decimal
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

S0 = Decimal("0")


def q(value, step: Decimal) -> Decimal:
    """Round down to a multiple of step, the way a venue quantizes."""
    value = Decimal(str(value))
    if step <= 0:
        return value
    return (value // step) * step


class FakeOrder:
    """Just enough of InFlightOrder for TrackedOrder to read it."""

    def __init__(self, order_id: str, side: TradeType, amount: Decimal, price: Decimal,
                 base: str, quote: str, timestamp: float):
        self.client_order_id = order_id
        self.trade_type = side
        self.amount = amount
        self.price = price
        self.base_asset, self.quote_asset = base, quote
        self.executed_amount_base = S0
        self.executed_amount_quote = S0
        self.average_executed_price: Optional[Decimal] = None
        self.current_state = OrderState.OPEN
        self.creation_timestamp = timestamp
        self.last_update_timestamp = timestamp
        self.order_fills: Dict = {}
        self.fees_paid = S0

    # what TrackedOrder / the executor read
    @property
    def is_done(self) -> bool:
        return self.current_state in (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED)

    def cumulative_fee_paid(self, token: str) -> Decimal:
        return self.fees_paid if token == self.quote_asset else S0

    # what the tests do to it
    def fill(self, amount: Optional[Decimal] = None, price: Optional[Decimal] = None,
             fee_pct: Decimal = Decimal("0")):
        amount = self.amount if amount is None else Decimal(str(amount))
        price = self.price if price is None else Decimal(str(price))
        self.executed_amount_base += amount
        self.executed_amount_quote += amount * price
        self.average_executed_price = self.executed_amount_quote / self.executed_amount_base
        self.fees_paid += amount * price * fee_pct / Decimal("100")
        if self.executed_amount_base >= self.amount:
            self.current_state = OrderState.FILLED
        else:
            self.current_state = OrderState.PARTIALLY_FILLED
        return self

    def cancel(self):
        self.current_state = OrderState.CANCELED
        return self

    def fail(self):
        self.current_state = OrderState.FAILED
        return self

    def mark_filled_without_fills(self):
        """A venue that reports FILLED while its trade updates are still in flight."""
        self.current_state = OrderState.FILLED
        return self


class FakeConnector:
    def __init__(self, name: str, trading_pair: str = "SOL-INR", bid: Decimal = Decimal("100"),
                 ask: Decimal = Decimal("101"), balances: Optional[Dict[str, Decimal]] = None,
                 amount_step: Decimal = Decimal("0.01"), price_tick: Decimal = Decimal("0.01"),
                 min_order_size: Decimal = Decimal("0.01"), min_notional: Decimal = Decimal("10")):
        self.name = name
        self.amount_step, self.price_tick = amount_step, price_tick
        self.trading_rules = {trading_pair: TradingRule(
            trading_pair=trading_pair, min_order_size=min_order_size, min_price_increment=price_tick,
            min_base_amount_increment=amount_step, min_notional_size=min_notional)}
        self.balances = balances or {"INR": Decimal("1000000"), "SOL": Decimal("1000")}
        self.prices = {PriceType.BestBid: Decimal(str(bid)), PriceType.BestAsk: Decimal(str(ask))}
        self.orders: Dict[str, FakeOrder] = {}
        self._order_tracker = SimpleNamespace(fetch_order=lambda client_order_id: self.orders.get(client_order_id))

    # connector API the executor uses
    def quantize_order_amount(self, trading_pair: str, amount: Decimal) -> Decimal:
        return q(amount, self.amount_step)

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        return q(price, self.price_tick)

    def get_available_balance(self, asset: str) -> Decimal:
        return self.balances.get(asset, S0)

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        return self.prices[price_type]

    def add_listener(self, *_args, **_kwargs):
        pass

    def remove_listener(self, *_args, **_kwargs):
        pass


class FakeStrategy:
    """Records what was sent, and hands back FakeOrders the tests can fill or cancel."""

    def __init__(self, connectors: Dict[str, FakeConnector], timestamp: float = 1_000.0):
        self.connectors = connectors
        self.current_timestamp = timestamp
        self.sent: List[Tuple[str, str, TradeType, Decimal, Decimal]] = []
        self.cancelled: List[Tuple[str, str, str]] = []
        self.refuse_next: Dict[str, bool] = {}   # connector -> refuse the next order
        self._counter = 0

    def _place(self, connector_name, trading_pair, amount, order_type, price, side) -> str:
        self._counter += 1
        order_id = f"{side.name.lower()}-{self._counter}"
        self.sent.append((connector_name, trading_pair, side, Decimal(str(amount)), Decimal(str(price))))
        base, quote = trading_pair.split("-")
        order = FakeOrder(order_id, side, Decimal(str(amount)), Decimal(str(price)), base, quote,
                          self.current_timestamp)
        if self.refuse_next.pop(connector_name, False):
            order.fail()
        self.connectors[connector_name].orders[order_id] = order
        return order_id

    def buy(self, connector_name, trading_pair, amount, order_type=OrderType.LIMIT, price=None,
            position_action=None) -> str:
        return self._place(connector_name, trading_pair, amount, order_type, price, TradeType.BUY)

    def sell(self, connector_name, trading_pair, amount, order_type=OrderType.LIMIT, price=None,
             position_action=None) -> str:
        return self._place(connector_name, trading_pair, amount, order_type, price, TradeType.SELL)

    def cancel(self, connector_name, trading_pair, order_id):
        """Acknowledge the cancel. Whether the order really stops is up to the test."""
        self.cancelled.append((connector_name, trading_pair, order_id))

    # helpers for tests
    def order(self, order_id: str) -> FakeOrder:
        for connector in self.connectors.values():
            if order_id in connector.orders:
                return connector.orders[order_id]
        raise KeyError(order_id)

    def advance(self, seconds: float):
        self.current_timestamp += seconds
