import asyncio
from decimal import Decimal

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType, TradeType


def make_inst():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    inst.api_key = "k"
    inst.secret_key = "s"
    inst._time_synchronizer = type("T", (), {"time": lambda self: 1600000000.0})()
    return inst


def test_get_all_pairs_prices_calls_api():
    inst = make_inst()

    async def fake_get(path_url=None):
        return [{"symbol": "BTCUSDT", "last_price": 1}]

    inst._api_get = fake_get
    result = asyncio.run(inst.get_all_pairs_prices())
    assert isinstance(result, list)


def test_place_order_various_response_shapes():
    inst = make_inst()

    async def fake_symbol(trading_pair: str):
        return "BTCUSDT"

    inst.exchange_symbol_associated_to_pair = fake_symbol

    async def api_post_orders_dict(path_url=None, data=None, is_auth_required=False):
        return {"orders": [{"id": "1", "created_at": 1600000000000}]}

    inst._api_post = api_post_orders_dict
    o_id, t = asyncio.run(inst._place_order("cid", "BTC-USDT", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("1")))
    assert o_id is not None

    async def api_post_list(path_url=None, data=None, is_auth_required=False):
        return [{"id": "2", "created_at": 1600000000000}]

    inst._api_post = api_post_list
    o_id2, t2 = asyncio.run(inst._place_order("cid2", "BTC-USDT", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("1")))
    assert o_id2 is not None


def test_place_cancel_returns_boolean():
    inst = make_inst()

    class Tracked:
        exchange_order_id = "ex"

    async def api_post_nonnull(path_url=None, data=None, is_auth_required=False):
        return {"result": "ok"}

    inst._api_post = api_post_nonnull
    res = asyncio.run(inst._place_cancel("cid", Tracked()))
    assert res is True

    async def api_post_null(path_url=None, data=None, is_auth_required=False):
        return None

    inst._api_post = api_post_null
    res2 = asyncio.run(inst._place_cancel("cid2", Tracked()))
    assert res2 is False
