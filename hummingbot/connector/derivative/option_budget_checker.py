import typing

from hummingbot.connector.budget_checker import BudgetChecker
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.data_type.trade_fee import TokenAmount

if typing.TYPE_CHECKING:  # avoid circular import problems
    from hummingbot.connector.option_derivative_py_base import OptionDerivativePyBase


class OptionBudgetChecker(BudgetChecker):
    """
    Buying an option locks its premium. Selling one to open locks margin, which the connector estimates
    conservatively; the exchange makes the final call. Closing a position locks nothing.

    Size options orders with `PerpetualOrderCandidate` (leverage 1, `position_close=True` for closing orders).
    """

    def __init__(self, exchange: "OptionDerivativePyBase"):
        super().__init__(exchange)

    def populate_collateral_entries(self, order_candidate: OrderCandidate) -> OrderCandidate:
        order_candidate = super().populate_collateral_entries(order_candidate)
        if (
            order_candidate.order_side == TradeType.SELL
            and not getattr(order_candidate, "position_close", False)
            and order_candidate.order_collateral is not None
        ):
            margin = self._exchange.estimate_short_margin(
                order_candidate.trading_pair, order_candidate.amount, order_candidate.price)
            order_candidate.order_collateral = TokenAmount(order_candidate.order_collateral.token, margin)
        return order_candidate

    def _lock_available_collateral(self, order_candidate: OrderCandidate):
        if not getattr(order_candidate, "position_close", False):
            super()._lock_available_collateral(order_candidate)
