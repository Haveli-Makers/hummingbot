from unittest import TestCase

from hummingbot.connector.exchange.ajaib.ajaib_order_book import AjaibOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class AjaibOrderBookTests(TestCase):
    def test_snapshot_message_from_depth_payload(self):
        depth = {
            "e": "depth",
            "s": "BTC_IDR",
            "bids": [["95000", "10"]],
            "asks": [["96000", "100"]],
        }
        msg = AjaibOrderBook.snapshot_message_from_exchange(
            depth, 1700000000.0, metadata={"trading_pair": "BTC-IDR"})

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-IDR", msg.content["trading_pair"])
        self.assertEqual([["95000", "10"]], msg.content["bids"])
        self.assertEqual([["96000", "100"]], msg.content["asks"])
        # No update id in the payload -> derived from the receive timestamp (ms).
        self.assertEqual(int(1700000000.0 * 1e3), msg.content["update_id"])

    def test_trade_message_from_payload_defaults_to_buy(self):
        trade = {
            "e": "trade",
            "E": 1672515782136,
            "s": "BNB_BTC",
            "t": "d66b4db6-fb03-4d89-baf6-666cc845df52",
            "p": "0.001",
            "q": "100",
        }
        msg = AjaibOrderBook.trade_message_from_exchange(trade, metadata={"trading_pair": "BNB-BTC"})

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual("BNB-BTC", msg.content["trading_pair"])
        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])
        self.assertEqual("d66b4db6-fb03-4d89-baf6-666cc845df52", msg.content["trade_id"])
        self.assertEqual(0.001, msg.content["price"])
        self.assertEqual(100.0, msg.content["amount"])
        self.assertEqual(1672515782136 / 1e3, msg.timestamp)

    def test_trade_message_maker_flag_marks_sell(self):
        trade = {"e": "trade", "E": 1672515782136, "s": "BNB_BTC", "t": "x", "p": "1", "q": "1", "m": True}
        msg = AjaibOrderBook.trade_message_from_exchange(trade, metadata={"trading_pair": "BNB-BTC"})
        self.assertEqual(float(TradeType.SELL.value), msg.content["trade_type"])
