from unittest import TestCase

from hummingbot.connector.exchange.coindcx.coindcx_order_book import CoinDCXOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class CoinDCXOrderBookTests(TestCase):

    def test_snapshot_message_from_rest_book(self):
        # The REST order book has no "vs"; its "timestamp" is CoinDCX's clock in ms.
        snapshot_message = CoinDCXOrderBook.snapshot_message_from_exchange(
            msg={
                "asks": {"7433.7000000000": "46.406000"},
                "bids": {"7405.1000000000": "1.902000"},
                "timestamp": 1795517777466,
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "SOL-INR"}
        )

        self.assertEqual("SOL-INR", snapshot_message.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(1640000000.0, snapshot_message.timestamp)
        self.assertEqual(1795517777466, snapshot_message.update_id)
        self.assertEqual(-1, snapshot_message.trade_id)
        self.assertEqual(1, len(snapshot_message.bids))
        self.assertEqual(7405.1, snapshot_message.bids[0].price)
        self.assertEqual(1.902, snapshot_message.bids[0].amount)
        self.assertEqual(1, len(snapshot_message.asks))
        self.assertEqual(7433.7, snapshot_message.asks[0].price)
        self.assertEqual(46.406, snapshot_message.asks[0].amount)

    def test_snapshot_message_from_stream_uses_ts_not_vs(self):
        snapshot_message = CoinDCXOrderBook.snapshot_message_from_exchange(
            msg={
                "vs": 216476,
                "ts": 1795517780511,
                "asks": {"7445.3000000000": "31.704228"},
                "bids": {"7405.1000000000": "1.902000"},
                "s": "SOLINR",
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "SOL-INR"}
        )

        self.assertEqual(1795517780511, snapshot_message.update_id)

    def test_snapshot_message_without_exchange_time_uses_local_time(self):
        snapshot_message = CoinDCXOrderBook.snapshot_message_from_exchange(
            msg={"asks": {}, "bids": {}},
            timestamp=1640000000.5,
            metadata={"trading_pair": "SOL-INR"}
        )

        self.assertEqual(1640000000500, snapshot_message.update_id)

    def test_diff_message_from_exchange(self):
        diff_msg = CoinDCXOrderBook.diff_message_from_exchange(
            msg={
                "ts": 123456789,
                "vs": 2,
                "bids": {
                    "0.0024": "10"
                },
                "asks": {
                    "0.0026": "100"
                }
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", diff_msg.trading_pair)
        self.assertEqual(OrderBookMessageType.DIFF, diff_msg.type)
        self.assertEqual(1640000000.0, diff_msg.timestamp)
        self.assertEqual(123456789, diff_msg.update_id)
        self.assertEqual(123456789, diff_msg.first_update_id)
        self.assertEqual(-1, diff_msg.trade_id)
        self.assertEqual(1, len(diff_msg.bids))
        self.assertEqual(0.0024, diff_msg.bids[0].price)
        self.assertEqual(10.0, diff_msg.bids[0].amount)
        self.assertEqual(1, len(diff_msg.asks))
        self.assertEqual(0.0026, diff_msg.asks[0].price)
        self.assertEqual(100.0, diff_msg.asks[0].amount)

    def test_trade_message_from_exchange(self):
        trade_update = {
            "T": 1234567890123,
            "p": "0.001",
            "q": "100",
            "m": True,
            "s": "COINALPHAHBOT",
            "pr": "spot"
        }

        trade_message = CoinDCXOrderBook.trade_message_from_exchange(
            msg=trade_update,
            metadata={"trading_pair": "COINALPHA-HBOT"}
        )

        self.assertEqual("COINALPHA-HBOT", trade_message.trading_pair)
        self.assertEqual(OrderBookMessageType.TRADE, trade_message.type)
        self.assertEqual(1234567890.123, trade_message.timestamp)
        self.assertEqual(trade_update.get("T"), trade_message.trade_id)
        # m=True: the buyer was the maker, so the taker sold.
        self.assertEqual(float(TradeType.SELL.value), trade_message.content["trade_type"])

    def test_trade_where_the_buyer_took_liquidity_is_a_buy(self):
        # Recorded from CoinDCX: printed at the best ask with m=0.
        trade_message = CoinDCXOrderBook.trade_message_from_exchange(
            msg={"T": 1795517796000, "p": "7435.4000000000", "q": "0.026739", "m": 0, "s": "I-SOL_INR", "pr": "spot"},
            metadata={"trading_pair": "SOL-INR"}
        )

        self.assertEqual(float(TradeType.BUY.value), trade_message.content["trade_type"])
        self.assertEqual(7435.4, trade_message.content["price"])
        self.assertEqual(0.026739, trade_message.content["amount"])
