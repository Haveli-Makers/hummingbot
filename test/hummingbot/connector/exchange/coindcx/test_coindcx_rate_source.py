import asyncio
from decimal import Decimal

import pytest

from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource


class _Resp:
    def __init__(self, status: int, json_data):
        self.status = status
        self._json = json_data

    async def json(self):
        return self._json


class _GetCtx:
    def __init__(self, resp: _Resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Session:
    def __init__(self, resp: _Resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *_args, **_kwargs):
        return _GetCtx(self._resp)


class _SessionFactory:
    def __init__(self, resp: _Resp):
        self._resp = resp

    def __call__(self, *args, **kwargs):
        return _Session(self._resp)


@pytest.mark.asyncio
async def test_parse_trading_pair():
    rs = CoindcxRateSource()
    assert rs._parse_trading_pair("BTCUSDT") == {"trading_pair": "BTC-USDT", "base": "BTC", "quote": "USDT"}
    assert rs._parse_trading_pair("ethbtc") == {"trading_pair": "ETH-BTC", "base": "ETH", "quote": "BTC"}
    assert rs._parse_trading_pair("UNKNOWN") is None


@pytest.mark.asyncio
async def test_get_prices_and_bid_ask_prices(monkeypatch):
    rs = CoindcxRateSource()

    # Prepare fake markets response
    markets = [
        {"coindcx_name": "BTCUSDT", "base_currency_short_name": "USDT", "target_currency_short_name": "BTC"},
    ]

    # Mock _fetch_markets to return mapping
    async def fake_fetch_markets():
        return {"BTCUSDT": {"trading_pair": "BTC-USDT", "base": "BTC", "quote": "USDT"}}

    monkeypatch.setattr(rs, "_fetch_markets", fake_fetch_markets)

    # Prepare ticker data
    ticker_list = [
        {"market": "BTCUSDT", "bid": "100", "ask": "110"},
        {"market": "UNKNOWN", "bid": "1", "ask": "2"},
    ]

    resp = _Resp(200, ticker_list)
    monkeypatch.setattr("aiohttp.ClientSession", _SessionFactory(resp))

    prices = await rs.get_prices()
    assert isinstance(prices, dict)
    assert "BTC-USDT" in prices
    assert prices["BTC-USDT"] == (Decimal("100") + Decimal("110")) / Decimal("2")

    bid_asks = await rs.get_bid_ask_prices()
    assert "BTC-USDT" in bid_asks
    entry = bid_asks["BTC-USDT"]
    assert entry["bid"] == Decimal("100")
    assert entry["ask"] == Decimal("110")
    assert entry["mid"] == (Decimal("100") + Decimal("110")) / Decimal("2")

