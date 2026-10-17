from unittest import TestCase

from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_order_book import (
    CoinswitchPerpetualOrderBook,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

# Order Book docs payload (REST and socket share it).
BOOK = {
    "timestamp": 1732685131699,
    "asks": [["3388.92", "0.10"], ["3388.94", "0.01"]],
    "bids": [["3385.71", "2.16"], ["3385.69", "0.01"], ["bad"]],
    "symbol": "ETHUSDT",
}


class CoinswitchPerpetualOrderBookTests(TestCase):
    def test_snapshot(self):
        message = CoinswitchPerpetualOrderBook.snapshot_message_from_exchange(
            BOOK, 1732685132.0, metadata={"trading_pair": "ETH-USDT"})
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual("ETH-USDT", message.trading_pair)
        self.assertEqual(1732685131699, message.update_id)
        self.assertEqual([3385.71, 3385.69], [row.price for row in message.bids])
        self.assertEqual([0.10, 0.01], [row.amount for row in message.asks])
        self.assertEqual(1732685132.0, message.timestamp)

    def test_snapshot_without_timestamp_uses_the_local_time(self):
        message = CoinswitchPerpetualOrderBook.snapshot_message_from_exchange(
            {"bids": [], "asks": []}, 1700000000.5, metadata={"trading_pair": "ETH-USDT"})
        self.assertEqual(1700000000500, message.update_id)

    def test_trade_side_from_the_maker_flag(self):
        trade = {"E": 1732691693128, "p": 0.39391, "q": 133, "e": "EXCHANGE_2", "s": "DOGEUSDT", "m": True}
        message = CoinswitchPerpetualOrderBook.trade_message_from_exchange(trade, {"trading_pair": "DOGE-USDT"})
        self.assertEqual(OrderBookMessageType.TRADE, message.type)
        self.assertEqual(float(TradeType.SELL.value), message.content["trade_type"])
        self.assertEqual(0.39391, message.content["price"])
        self.assertEqual(133, message.content["amount"])
        self.assertEqual(1732691693128, message.trade_id)
        self.assertAlmostEqual(1732691693.128, message.timestamp)

        buy = CoinswitchPerpetualOrderBook.trade_message_from_exchange({**trade, "m": False}, {"trading_pair": "X"})
        self.assertEqual(float(TradeType.BUY.value), buy.content["trade_type"])
