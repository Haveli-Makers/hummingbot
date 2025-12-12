import asyncio

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


def test_get_last_traded_price_returns_zero_on_api_error():
    inst = CoindcxExchange.__new__(CoindcxExchange)

    async def bad_get(path_url=None):
        raise Exception("api error")

    inst._api_get = bad_get
    inst.logger = lambda: type("L", (), {"error": lambda *a, **k: None})()

    async def run():
        p = await inst._get_last_traded_price("BTC-USDT")
        assert p == 0.0

    asyncio.run(run())


def test_format_trading_rules_skips_inactive():
    inst = CoindcxExchange.__new__(CoindcxExchange)

    async def tpas(self, symbol: str):
        return "BTC-USDT"

    inst.trading_pair_associated_to_exchange_symbol = tpas

    # inactive rule should be skipped
    rules = asyncio.run(inst._format_trading_rules({"coindcx_name": "X", "status": "inactive"}))
    assert rules == []
