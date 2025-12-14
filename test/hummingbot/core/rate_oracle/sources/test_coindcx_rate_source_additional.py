import pytest
from aioresponses import aioresponses

from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource


def test_parse_trading_pair():
    src = CoindcxRateSource()
    parsed = src._parse_trading_pair("BTCUSDT")
    assert parsed is not None
    assert parsed["trading_pair"] == "BTC-USDT"


@pytest.mark.asyncio
async def test_get_prices_handles_empty_response(monkeypatch):
    src = CoindcxRateSource()

    async def fake_fetch_markets():
        return {}

    monkeypatch.setattr(src, "_fetch_markets", fake_fetch_markets)

    with aioresponses() as m:
        m.get(src.TICKER_URL, body='[]', status=200)
        prices = await src.get_prices()
        assert prices == {}
