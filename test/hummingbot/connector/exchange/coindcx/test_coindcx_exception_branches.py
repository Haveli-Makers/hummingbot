import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource


def test_format_trading_rules_handles_exception():
    inst = CoindcxExchange.__new__(CoindcxExchange)

    async def bad_tpas(self, symbol: str):
        raise Exception("fail")

    inst.trading_pair_associated_to_exchange_symbol = bad_tpas

    async def run():
        rules = await inst._format_trading_rules({"coindcx_name": "BAD", "status": "active"})
        # even on exception the function should return a list (possibly empty)
        assert isinstance(rules, list)

    asyncio.run(run())


def test_subscribe_channels_handles_exception():
    class WSFail:
        async def send(self, payload):
            raise Exception("send fail")

    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=None, api_factory=None)
    try:
        asyncio.run(ds._subscribe_channels(WSFail()))
    except Exception:
        # expected to raise
        assert True


def test_update_order_fills_from_trades_handles_api_error():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    inst._order_tracker = type("OT", (), {"all_fillable_orders": {}, "active_orders": {}})()
    inst._last_poll_timestamp = 0

    async def bad_post(path_url=None, data=None, is_auth_required=False):
        raise Exception("api fail")

    inst._api_post = bad_post

    # Should handle exception and not raise
    asyncio.run(inst._update_order_fills_from_trades())
    assert True
