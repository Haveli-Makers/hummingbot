import time

from hummingbot.connector.exchange.coindcx.coindcx_order_book import CoinDCXOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


def test_snapshot_message_sorts_bids_asks():
    msg = {"bids": {"2": "1", "1": "2"}, "asks": {"3": "1", "4": "2"}}
    ts = time.time()
    obm = CoinDCXOrderBook.snapshot_message_from_exchange(msg, ts, metadata={"trading_pair": "BTC-USDT"})
    assert obm.type == OrderBookMessageType.SNAPSHOT
    assert obm.content["bids"][0][0] == 2.0  # highest bid first
    assert obm.content["asks"][0][0] == 3.0  # lowest ask first


def test_diff_message_and_trade_message_mappings():
    diff = {"bids": {"1": "1"}, "asks": {"2": "2"}, "vs": 123}
    dm = CoinDCXOrderBook.diff_message_from_exchange(diff, time.time(), metadata={"trading_pair": "BTC-USDT"})
    assert dm.type == OrderBookMessageType.DIFF

    # trade message with m truthy (mapped to BUY)
    trade = {"T": 1600000000000, "p": "10", "q": "0.5", "m": 1}
    tm = CoinDCXOrderBook.trade_message_from_exchange(trade, metadata={"trading_pair": "BTC-USDT"})
    assert tm.type == OrderBookMessageType.TRADE
    # m==1 should map to BUY numeric value
    from hummingbot.core.data_type.common import TradeType
    assert tm.content["trade_type"] == float(TradeType.BUY.value)

    # trade message with m falsy (mapped to SELL)
    trade2 = {"T": 1600000000000, "p": "11", "q": "0.6", "m": 0}
    tm2 = CoinDCXOrderBook.trade_message_from_exchange(trade2, metadata={"trading_pair": "BTC-USDT"})
    assert tm2.content["price"] == 11.0
