import asyncio
from decimal import Decimal
import types

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


def make_inst():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    inst._time_synchronizer = type("T", (), {"time": lambda self: 1600000000.0})()
    inst._account_balances = {}
    inst._account_available_balances = {}
    return inst


def test_request_order_status_and_update_balances_and_last_price(monkeypatch):
    inst = make_inst()

    class Tracked:
        client_order_id = "cid"
        exchange_order_id = "exid"
        trading_pair = "BTC-USDT"
        current_state = "open"

    async def api_post(path_url=None, data=None, is_auth_required=False):
        if path_url.endswith("order/status") or "order" in path_url:
            return {"status": "cancelled", "id": "exid", "updated_at": 1600000000000}
        if path_url.endswith("balances"):
            return [{"currency": "BTC", "balance": 1, "locked_balance": 0}]
        if path_url.endswith("trade/history"):
            return [{"order_id": "exid", "id": "t1", "quantity": 1, "price": 2, "fee_amount": 0}]

    inst._api_post = api_post

    # Test request order status
    order_update = asyncio.run(inst._request_order_status(Tracked()))
    assert order_update.exchange_order_id == "exid"

    # Test update balances
    asyncio.run(inst._update_balances())
    assert inst._account_balances.get("BTC") is not None

    # Test get last traded price
    async def api_get(path_url=None):
        return [{"symbol": "BTCUSDT", "last_price": 123.45}]

    inst._api_get = api_get
    async def fake_symbol(trading_pair: str):
        return "BTCUSDT"

    inst.exchange_symbol_associated_to_pair = fake_symbol
    price = asyncio.run(inst._get_last_traded_price("BTC-USDT"))
    assert price == 123.45


def test_all_trade_updates_for_order(monkeypatch):
    inst = make_inst()

    class Order:
        exchange_order_id = "exid"
        trading_pair = "BTC-USDT"
        client_order_id = "cid"
        trade_type = None

    async def api_post(path_url=None, data=None, is_auth_required=False):
        return [{"order_id": "exid", "id": "t1", "quantity": 1, "price": 2, "fee_amount": 0}]

    inst._api_post = api_post

    async def fake_symbol(trading_pair: str):
        return "BTCUSDT"

    inst.exchange_symbol_associated_to_pair = fake_symbol

    result = asyncio.run(inst._all_trade_updates_for_order(Order()))
    assert isinstance(result, list)


def test_initialize_trading_pair_symbols_from_exchange_info():
    inst = make_inst()

    called = {}

    def setter(mapping):
        called['mapping'] = mapping

    inst._set_trading_pair_symbol_map = setter

    from hummingbot.connector.exchange.coindcx import coindcx_utils

    info = {"symbol": "BTCUSDT", "target_currency_short_name": "BTC", "base_currency_short_name": "USDT", "status": "active"}
    inst._initialize_trading_pair_symbols_from_exchange_info(info)
    assert 'mapping' in called
