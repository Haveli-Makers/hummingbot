from unittest import TestCase

from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_order_book import CoinDCXPerpetualOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

# Payload shapes from the FAPI guide (GET /fapi/v1/depth, @depth, @aggTrade).
SNAPSHOT = {
    "lastUpdateId": 437,
    "E": 1780915241661,
    "T": 1780915240114,
    "bids": [["67070.0", "9.866"], ["67069.9", "9.866"]],
    "asks": [["67070.2", "9.866"], ["67070.3", "5.000"]],
}
DIFF = {
    "e": "depthUpdate",
    "E": 1714003200000,
    "s": "BTCUSDT",
    "U": 157,
    "u": 160,
    "pu": 156,
    "b": [["68450.00", "1.250"]],
    "a": [["68450.20", "0.800"], ["68450.30", "0"]],
}
AGG_TRADE = {
    "e": "aggTrade",
    "E": 1714003200000,
    "s": "BTCUSDT",
    "a": 5933014,
    "p": "68450.23",
    "q": "1.500",
    "T": 1714003199500,
    "m": True,
}


class CoinDCXPerpetualOrderBookTests(TestCase):
    def test_snapshot_uses_last_update_id_and_level_arrays(self):
        msg = CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            SNAPSHOT, 1780915241.7, metadata={"trading_pair": "BTC-USDT"})

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-USDT", msg.content["trading_pair"])
        self.assertEqual(437, msg.update_id)
        self.assertEqual([[67070.0, 9.866], [67069.9, 9.866]], msg.content["bids"])
        self.assertEqual([[67070.2, 9.866], [67070.3, 5.0]], msg.content["asks"])
        self.assertEqual(1780915241.7, msg.timestamp)

    def test_diff_carries_the_update_range_and_zero_quantity_levels(self):
        msg = CoinDCXPerpetualOrderBook.diff_message_from_exchange(
            DIFF, metadata={"trading_pair": "BTC-USDT"})

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(157, msg.content["first_update_id"])
        self.assertEqual(160, msg.update_id)
        self.assertEqual([[68450.0, 1.25]], msg.content["bids"])
        # A zero quantity is how the stream deletes a level, so it must survive.
        self.assertEqual([[68450.2, 0.8], [68450.3, 0.0]], msg.content["asks"])
        self.assertEqual(1714003200.0, msg.timestamp)

    def test_diff_without_a_first_update_id_falls_back_to_its_last(self):
        msg = CoinDCXPerpetualOrderBook.diff_message_from_exchange(
            {k: v for k, v in DIFF.items() if k != "U"}, timestamp=1.0, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual(160, msg.content["first_update_id"])

    def test_malformed_levels_are_skipped(self):
        msg = CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            {**SNAPSHOT, "bids": [["1", "2"], ["x", "1"], ["3"]]}, 1.0, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual([[1.0, 2.0]], msg.content["bids"])

    def test_buyer_maker_trade_prints_as_a_sell(self):
        msg = CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            AGG_TRADE, metadata={"trading_pair": "BTC-USDT"})

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(float(TradeType.SELL.value), msg.content["trade_type"])
        self.assertEqual(5933014, msg.content["trade_id"])
        self.assertEqual(68450.23, msg.content["price"])
        self.assertEqual(1.5, msg.content["amount"])
        self.assertEqual(1714003199.5, msg.timestamp)

    def test_taker_buy_prints_as_a_buy(self):
        msg = CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            {**AGG_TRADE, "m": False}, metadata={"trading_pair": "BTC-USDT"})
        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])
