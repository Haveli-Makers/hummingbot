
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TradeFeeBase


class DummyRA:
    def __init__(self, resp=None, raise_exc=False):
        self._resp = resp
        self._raise = raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute_request(self, *args, **kwargs):
        if self._raise:
            raise Exception("dummy error")
        return self._resp


class DummyFactory:
    def __init__(self, resp=None, raise_exc=False):
        self.resp = resp
        self.raise_exc = raise_exc

    def build_rest_assistant(self):
        return DummyRA(resp=self.resp, raise_exc=self.raise_exc)


@pytest.mark.asyncio
async def test_format_trading_rules_with_list_input():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    rule = {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "10"},
        ],
    }

    rules = await exchange._format_trading_rules([rule])
    assert isinstance(rules, list)


@pytest.mark.asyncio
async def test_format_trading_rules_with_dict_input():
    exchange = WazirxExchange("k", "s", trading_pairs=["ETH-USDT"])
    payload = {"symbols": [
        {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "filters": []}
    ]}

    rules = await exchange._format_trading_rules(payload)
    assert isinstance(rules, list)


@pytest.mark.asyncio
async def test_get_last_traded_prices_list_response():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    # simulate list response
    resp = [{"lastPrice": "123.45"}]
    exchange._web_assistants_factory = DummyFactory(resp=resp)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices["BTC-USDT"] == 123.45


@pytest.mark.asyncio
async def test_get_last_traded_prices_dict_response():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    # simulate dict response
    resp = {"lastPrice": "200.0"}
    exchange._web_assistants_factory = DummyFactory(resp=resp)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices["BTC-USDT"] == 200.0


@pytest.mark.asyncio
async def test_get_last_traded_prices_handles_exceptions():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    exchange._web_assistants_factory = DummyFactory(resp=None, raise_exc=True)

    prices = await exchange.get_last_traded_prices(["BTC-USDT"])
    assert prices["BTC-USDT"] == 0.0


@pytest.mark.asyncio
async def test_all_trade_updates_for_order_and_request_order_status():
    exchange = WazirxExchange("k", "s", trading_pairs=["BTC-USDT"])
    # trades response
    trades_resp = [
        {
            "id": "1",
            "orderId": "42",
            "commissionAsset": "USDT",
            "commission": "0.1",
            "qty": "0.5",
            "quoteQty": "61.5",
            "price": "123.0",
            "time": 1650000000000,
        }
    ]
    exchange._web_assistants_factory = DummyFactory(resp=trades_resp)

    order = SimpleNamespace(client_order_id="c1", exchange_order_id="42", trading_pair="BTC-USDT", trade_type=TradeType.BUY)
    exchange._trade_fee_schema = {}
    orig_new_spot_fee = TradeFeeBase.new_spot_fee
    TradeFeeBase.new_spot_fee = staticmethod(
        lambda fee_schema, trade_type, percent=Decimal(0), percent_token=None, flat_fees=None:
        AddedToCostTradeFee(percent=percent, percent_token=percent_token, flat_fees=flat_fees or [])
    )
    try:
        updates = await exchange._all_trade_updates_for_order(order)
    finally:
        TradeFeeBase.new_spot_fee = orig_new_spot_fee
    assert len(updates) == 1
    tu = updates[0]
    assert tu.trade_id == "1"
    assert float(tu.fill_price) == 123.0
