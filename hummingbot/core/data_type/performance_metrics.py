from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import OrderFilledEvent


@dataclass
class PerformanceMetrics:
    trading_pair: str
    start_base_balance: Decimal
    start_quote_balance: Decimal
    start_price: Decimal
    tds_rate: Decimal = Decimal("0.01")

    def __post_init__(self):
        self._quote_token: str = self.trading_pair.split("-")[1]

        self.num_buys: int = 0
        self.num_sells: int = 0

        self.buy_volume_base: Decimal = Decimal("0")
        self.sell_volume_base: Decimal = Decimal("0")
        self.buy_volume_quote: Decimal = Decimal("0")
        self.sell_volume_quote: Decimal = Decimal("0")

        self.total_tds: Decimal = Decimal("0")
        self.total_fees_quote: Decimal = Decimal("0")

        self.realized_pnl_quote: Decimal = Decimal("0")
        self.unrealized_pnl_quote: Decimal = Decimal("0")

        self.current_price: Decimal = self.start_price

        # FIFO cost basis: deque of [remaining_base, fill_price]
        self._buy_queue: deque = deque()

    # ------------------------------------------------------------------
    # Public update methods
    # ------------------------------------------------------------------

    def update_from_fill(self, event: OrderFilledEvent) -> None:
        quote_value = event.price * event.amount
        fee_quote = self._extract_fee_quote(event.trade_fee, event.price, event.amount)

        if event.trade_type == TradeType.BUY:
            self.num_buys += 1
            self.buy_volume_base += event.amount
            self.buy_volume_quote += quote_value
            self.total_fees_quote += fee_quote
            self._buy_queue.append([event.amount, event.price])
        else:
            tds = quote_value * self.tds_rate
            self.num_sells += 1
            self.sell_volume_base += event.amount
            self.sell_volume_quote += quote_value
            self.total_tds += tds
            self.total_fees_quote += fee_quote
            self.realized_pnl_quote += self._realize_pnl(event.amount, event.price) - fee_quote

    def mark_to_market(self, current_price: Decimal) -> None:
        self.current_price = current_price
        net_base = self.buy_volume_base - self.sell_volume_base
        self.unrealized_pnl_quote = net_base * (current_price - self.start_price)

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def num_trades(self) -> int:
        return self.num_buys + self.num_sells

    @property
    def net_base_held(self) -> Decimal:
        return self.buy_volume_base - self.sell_volume_base

    @property
    def cashflow_quote(self) -> Decimal:
        """Net income actually received: realized PnL minus TDS withheld at source."""
        return self.realized_pnl_quote - self.total_tds

    @property
    def total_pnl_quote(self) -> Decimal:
        return self.realized_pnl_quote + self.unrealized_pnl_quote

    @property
    def start_portfolio_value(self) -> Decimal:
        return self.start_quote_balance + self.start_base_balance * self.start_price

    @property
    def return_pct(self) -> Decimal:
        if self.start_portfolio_value == Decimal("0"):
            return Decimal("0")
        return self.total_pnl_quote / self.start_portfolio_value

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def change_signature(self) -> tuple:
        """
        Stable fingerprint of the fields that only move on a fill, excluding the
        mark-to-market values (current_price, unrealized_pnl_quote, total_pnl_quote,
        return_pct) which tick with the market. Use this for change detection so the
        snapshot is treated as "changed" only when an actual trade alters it.
        """
        return (
            self.num_buys,
            self.num_sells,
            self.buy_volume_quote,
            self.sell_volume_quote,
            self.total_tds,
            self.total_fees_quote,
            self.realized_pnl_quote,
        )

    def to_dict(self) -> Dict[str, float]:
        return {
            "num_buys": self.num_buys,
            "num_sells": self.num_sells,
            "num_trades": self.num_trades,
            "buy_volume_quote": float(self.buy_volume_quote),
            "sell_volume_quote": float(self.sell_volume_quote),
            "total_tds": float(self.total_tds),
            "total_fees_quote": float(self.total_fees_quote),
            "realized_pnl_quote": float(self.realized_pnl_quote),
            "unrealized_pnl_quote": float(self.unrealized_pnl_quote),
            "total_pnl_quote": float(self.total_pnl_quote),
            "cashflow_quote": float(self.cashflow_quote),
            "return_pct": float(self.return_pct),
            "net_base_held": float(self.net_base_held),
            "current_price": float(self.current_price),
            "start_price": float(self.start_price),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _realize_pnl(self, sell_amount: Decimal, sell_price: Decimal) -> Decimal:
        """FIFO-matched PnL for a sell fill against accumulated buy cost basis."""
        remaining = sell_amount
        pnl = Decimal("0")
        while remaining > Decimal("0") and self._buy_queue:
            buy_amount, buy_price = self._buy_queue[0]
            matched = min(remaining, buy_amount)
            pnl += matched * (sell_price - buy_price)
            remaining -= matched
            buy_amount -= matched
            if buy_amount == Decimal("0"):
                self._buy_queue.popleft()
            else:
                self._buy_queue[0][0] = buy_amount
        return pnl

    def _extract_fee_quote(self, trade_fee, price: Decimal, amount: Decimal) -> Decimal:
        """Extract fee amount in quote token from a TradeFeeBase object."""
        fee = Decimal("0")
        if trade_fee.percent and trade_fee.percent_token in (None, self._quote_token):
            fee += trade_fee.percent * price * amount
        for flat_fee in trade_fee.flat_fees:
            if flat_fee.token == self._quote_token:
                fee += flat_fee.amount
        return fee
