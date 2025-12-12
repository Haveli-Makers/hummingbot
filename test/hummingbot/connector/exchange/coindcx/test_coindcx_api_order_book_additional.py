import asyncio
import time

import pytest

from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class _ConnectorStub:
    def __init__(self, mapping=None):
        self._mapping = mapping or {}

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        return self._mapping.get(symbol, "BTC-USDT")


def test_channel_originating_message():
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=[], connector=None, api_factory=None)

    trade_event = {"p": "10", "q": "1", "T": 1000}
    assert ds._channel_originating_message(trade_event) != ""

    diff_event = {"bids": {"1": "1"}}
    assert ds._channel_originating_message(diff_event) != ""

    other = {"x": 1}
    assert ds._channel_originating_message(other) == ""


@pytest.mark.asyncio
async def test_parse_trade_and_diff_message_makes_orderbook_messages():
    conn = _ConnectorStub({"BTCUSDT": "BTC-USDT", "B-BTC_USDT": "BTC-USDT"})
    ds = CoinDCXAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=conn, api_factory=None)

    q = asyncio.Queue()

    trade_msg = {"s": "BTCUSDT", "T": int(time.time() * 1000), "p": "10", "q": "0.5", "m": 1}
    await ds._parse_trade_message(trade_msg, q)
    item = q.get_nowait()
    assert item.type == OrderBookMessageType.TRADE

    diff_msg = {"bids": {"1": "1"}, "channel": "B-BTC_USDT@orderbook@20"}
    await ds._parse_order_book_diff_message(diff_msg, q)
    item2 = q.get_nowait()
    assert item2.type == OrderBookMessageType.DIFF
