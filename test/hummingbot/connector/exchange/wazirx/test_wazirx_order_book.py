from unittest import TestCase

from hummingbot.connector.exchange.wazirx.wazirx_order_book import WazirxOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class WazirxOrderBookTests(TestCase):

    def test_snapshot_message_from_rest_depth(self):
        snapshot_message = WazirxOrderBook.snapshot_message_from_exchange(
            msg={
                "timestamp": 1640000000000,
                "bids": [["4.00000000", "431.00000000"]],
                "asks": [["4.00000200", "12.00000000"]]
            },
            timestamp=1640000000.5,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", snapshot_message.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(1640000000.5, snapshot_message.timestamp)
        self.assertEqual(1640000000500, snapshot_message.update_id)
        self.assertEqual(-1, snapshot_message.trade_id)
        self.assertEqual(1, len(snapshot_message.bids))
        self.assertEqual(4.0, snapshot_message.bids[0].price)
        self.assertEqual(431.0, snapshot_message.bids[0].amount)
        self.assertEqual(1, len(snapshot_message.asks))
        self.assertEqual(4.000002, snapshot_message.asks[0].price)
        self.assertEqual(12.0, snapshot_message.asks[0].amount)

    def test_snapshot_message_from_depth_stream(self):
        snapshot_message = WazirxOrderBook.snapshot_message_from_exchange(
            msg={
                "E": 1640000000,
                "s": "coinalphahbot",
                "b": [["0.0024", "10"], ["0.0023", "5"]],
                "a": [["0.0026", "100"]]
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(2, len(snapshot_message.bids))
        self.assertEqual(0.0024, snapshot_message.bids[0].price)
        self.assertEqual(10.0, snapshot_message.bids[0].amount)
        self.assertEqual(1, len(snapshot_message.asks))
        self.assertEqual(0.0026, snapshot_message.asks[0].price)

    def test_trade_message_from_exchange(self):
        trade_update = {
            "E": 1631681323000,
            "S": "buy",
            "a": 26946138,
            "b": 26946169,
            "m": True,
            "p": "7.0",
            "q": "15.0",
            "s": "coinalphahbot",
            "t": 17376030
        }

        trade_message = WazirxOrderBook.trade_message_from_exchange(
            msg=trade_update,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", trade_message.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade_message.type)
        self.assertEqual(1631681323.0, trade_message.timestamp)
        self.assertEqual(17376030, trade_message.trade_id)
        self.assertEqual(float(TradeType.SELL.value), trade_message.content["trade_type"])
        self.assertEqual(7.0, trade_message.content["price"])
        self.assertEqual(15.0, trade_message.content["amount"])
