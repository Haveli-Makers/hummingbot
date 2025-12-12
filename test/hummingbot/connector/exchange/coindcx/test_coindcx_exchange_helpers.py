

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType


def test_order_type_mappings_and_error_checks():
    # static methods
    assert CoindcxExchange.coindcx_order_type(OrderType.LIMIT) == CONSTANTS.ORDER_TYPE_LIMIT
    assert CoindcxExchange.coindcx_order_type(OrderType.MARKET) == CONSTANTS.ORDER_TYPE_MARKET
    assert CoindcxExchange.to_hb_order_type(CONSTANTS.ORDER_TYPE_MARKET) == OrderType.MARKET

    # instance-level error checks (use __new__ to avoid heavy init)
    ex = CoindcxExchange.__new__(CoindcxExchange)
    assert ex._is_order_not_found_during_cancelation_error(Exception(str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE)))
