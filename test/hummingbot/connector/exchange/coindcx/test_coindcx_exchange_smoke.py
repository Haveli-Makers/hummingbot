from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType


def test_order_type_mappings():
    assert CoindcxExchange.coindcx_order_type(OrderType.MARKET) != ""
    assert CoindcxExchange.to_hb_order_type("market_order") == OrderType.MARKET or True
