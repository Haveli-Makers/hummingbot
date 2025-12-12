import asyncio
import types

from decimal import Decimal

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS


def test_is_request_exception_related_to_time_synchronizer_false():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    assert inst._is_request_exception_related_to_time_synchronizer(Exception("any")) is False


def test_is_order_not_found_checks():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    err = Exception(str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE))
    assert inst._is_order_not_found_during_status_update_error(err)

    err2 = Exception(CONSTANTS.UNKNOWN_ORDER_MESSAGE)
    assert inst._is_order_not_found_during_cancelation_error(err2)


def test_format_trading_rules_async():
    async def run():
        inst = CoindcxExchange.__new__(CoindcxExchange)

        async def tpas(self, symbol: str):
            return "BTC-USDT"

        inst.trading_pair_associated_to_exchange_symbol = types.MethodType(tpas, inst)

        rule = {
            "coindcx_name": "BTCUSDT",
            "base_currency_precision": 8,
            "target_currency_precision": 8,
            "min_quantity": 1,
            "max_quantity": 100,
            "step": 1,
            "min_notional": 0.001,
            "status": "active"
        }

        rules = await inst._format_trading_rules(rule)
        assert len(rules) == 1
        tr = rules[0]
        assert tr.trading_pair == "BTC-USDT"

    asyncio.run(run())


def test_order_book_snapshot_calls_snapshot_message(monkeypatch):
    from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=None)

    async def fake_request(trading_pair: str):
        return {"bids": {"1": "1"}, "asks": {"2": "2"}}

    monkeypatch.setattr(ds, "_request_order_book_snapshot", fake_request)

    msg = asyncio.run(ds._order_book_snapshot("BTC-USDT"))
    # Should return an OrderBookMessage-like object
    assert msg is not None
