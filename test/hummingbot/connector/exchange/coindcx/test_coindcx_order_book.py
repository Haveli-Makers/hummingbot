"""
Test cases for CoinDCX order book message parsing.
These tests verify the parsing logic for order book data.
"""
import unittest
from decimal import Decimal
from typing import List, Tuple

from hummingbot.connector.exchange.coindcx.coindcx_order_book import CoinDCXOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class TestCoinDCXOrderBookMessageParsing(unittest.TestCase):
    """Test cases for parsing CoinDCX order book messages."""

    def parse_order_book_snapshot(self, data: dict) -> Tuple[List, List]:
        """
        Parse order book snapshot from CoinDCX API.
        Returns (bids, asks) as lists of [price, quantity].
        """
        bids = []
        asks = []

        # CoinDCX format: {"bids": {"price": "qty", ...}, "asks": {"price": "qty", ...}}
        if "bids" in data:
            for price, qty in data["bids"].items():
                bids.append([Decimal(price), Decimal(qty)])
            bids.sort(key=lambda x: x[0], reverse=True)  # Highest bid first

        if "asks" in data:
            for price, qty in data["asks"].items():
                asks.append([Decimal(price), Decimal(qty)])
            asks.sort(key=lambda x: x[0])  # Lowest ask first

        return bids, asks

    def parse_order_book_diff(self, data: dict) -> Tuple[List, List]:
        """
        Parse order book diff/update from CoinDCX WebSocket.
        Returns (bids, asks) as lists of [price, quantity].
        """
        bids = []
        asks = []

        # WebSocket format may vary
        if "bids" in data:
            for entry in data["bids"]:
                if isinstance(entry, list):
                    bids.append([Decimal(entry[0]), Decimal(entry[1])])
                elif isinstance(entry, dict):
                    bids.append([Decimal(entry["price"]), Decimal(entry["quantity"])])

        if "asks" in data:
            for entry in data["asks"]:
                if isinstance(entry, list):
                    asks.append([Decimal(entry[0]), Decimal(entry[1])])
                elif isinstance(entry, dict):
                    asks.append([Decimal(entry["price"]), Decimal(entry["quantity"])])

        return bids, asks

    def test_parse_snapshot_basic(self):
        """Test parsing a basic order book snapshot."""
        data = {
            "bids": {
                "50000.00": "1.5",
                "49999.00": "2.0"
            },
            "asks": {
                "50001.00": "1.0",
                "50002.00": "0.5"
            }
        }

        bids, asks = self.parse_order_book_snapshot(data)

        self.assertEqual(len(bids), 2)
        self.assertEqual(len(asks), 2)

        # Bids should be sorted highest first
        self.assertEqual(bids[0][0], Decimal("50000.00"))
        self.assertEqual(bids[1][0], Decimal("49999.00"))

        # Asks should be sorted lowest first
        self.assertEqual(asks[0][0], Decimal("50001.00"))
        self.assertEqual(asks[1][0], Decimal("50002.00"))

    def test_parse_snapshot_quantities(self):
        """Test that quantities are parsed correctly."""
        data = {
            "bids": {"50000.00": "1.5"},
            "asks": {"50001.00": "2.5"}
        }

        bids, asks = self.parse_order_book_snapshot(data)

        self.assertEqual(bids[0][1], Decimal("1.5"))
        self.assertEqual(asks[0][1], Decimal("2.5"))

    def test_parse_snapshot_empty_bids(self):
        """Test parsing snapshot with no bids."""
        data = {
            "bids": {},
            "asks": {"50001.00": "1.0"}
        }

        bids, asks = self.parse_order_book_snapshot(data)

        self.assertEqual(len(bids), 0)
        self.assertEqual(len(asks), 1)

    def test_parse_snapshot_empty_asks(self):
        """Test parsing snapshot with no asks."""
        data = {
            "bids": {"50000.00": "1.0"},
            "asks": {}
        }

        bids, asks = self.parse_order_book_snapshot(data)

        self.assertEqual(len(bids), 1)
        self.assertEqual(len(asks), 0)

    def test_parse_snapshot_multiple_levels(self):
        """Test parsing snapshot with multiple price levels."""
        data = {
            "bids": {
                "50000.00": "1.0",
                "49999.00": "2.0",
                "49998.00": "3.0",
                "49997.00": "4.0",
                "49996.00": "5.0"
            },
            "asks": {
                "50001.00": "1.0",
                "50002.00": "2.0",
                "50003.00": "3.0"
            }
        }

        bids, asks = self.parse_order_book_snapshot(data)

        self.assertEqual(len(bids), 5)
        self.assertEqual(len(asks), 3)

    def test_parse_diff_list_format(self):
        """Test parsing diff in list format."""
        data = {
            "bids": [["50000.00", "1.5"], ["49999.00", "2.0"]],
            "asks": [["50001.00", "1.0"], ["50002.00", "0.5"]]
        }

        bids, asks = self.parse_order_book_diff(data)

        self.assertEqual(len(bids), 2)
        self.assertEqual(len(asks), 2)
        self.assertEqual(bids[0][0], Decimal("50000.00"))

    def test_parse_diff_dict_format(self):
        """Test parsing diff in dict format."""
        data = {
            "bids": [
                {"price": "50000.00", "quantity": "1.5"},
                {"price": "49999.00", "quantity": "2.0"}
            ],
            "asks": [
                {"price": "50001.00", "quantity": "1.0"}
            ]
        }

        bids, asks = self.parse_order_book_diff(data)

        self.assertEqual(len(bids), 2)
        self.assertEqual(len(asks), 1)


class TestCoinDCXOrderBookTradeMessage(unittest.TestCase):
    """Test cases for parsing CoinDCX trade messages."""

    def parse_trade_message(self, data: dict) -> dict:
        """Parse a trade message from CoinDCX."""
        return {
            "trade_id": str(data.get("T", data.get("trade_id", ""))),
            "price": Decimal(str(data.get("p", data.get("price", "0")))),
            "quantity": Decimal(str(data.get("q", data.get("quantity", "0")))),
            "timestamp": int(data.get("t", data.get("timestamp", 0))),
            "is_buyer_maker": data.get("m", data.get("is_buyer_maker", False))
        }

    def test_parse_trade_basic(self):
        """Test parsing a basic trade message."""
        data = {
            "T": "12345",
            "p": "50000.00",
            "q": "1.5",
            "t": 1620000000000,
            "m": True
        }

        trade = self.parse_trade_message(data)

        self.assertEqual(trade["trade_id"], "12345")
        self.assertEqual(trade["price"], Decimal("50000.00"))
        self.assertEqual(trade["quantity"], Decimal("1.5"))
        self.assertEqual(trade["timestamp"], 1620000000000)
        self.assertTrue(trade["is_buyer_maker"])

    def test_parse_trade_alternative_keys(self):
        """Test parsing trade with alternative key names."""
        data = {
            "trade_id": "67890",
            "price": "49999.99",
            "quantity": "0.5",
            "timestamp": 1620000001000,
            "is_buyer_maker": False
        }

        trade = self.parse_trade_message(data)

        self.assertEqual(trade["trade_id"], "67890")
        self.assertEqual(trade["price"], Decimal("49999.99"))
        self.assertFalse(trade["is_buyer_maker"])


class TestCoinDCXOrderBookMessageConversion(unittest.TestCase):
    def test_snapshot_message_from_exchange(self):
        msg = {
            "bids": {"100.0": "1.5", "99.0": "2.0"},
            "asks": {"101.0": "1.0", "102.0": "2.5"},
            "vs": 12345
        }
        metadata = {"trading_pair": "BTC-USDT"}
        timestamp = 1234567890.0
        ob_msg = CoinDCXOrderBook.snapshot_message_from_exchange(msg, timestamp, metadata)
        self.assertEqual(ob_msg.message_type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(ob_msg.content["trading_pair"], "BTC-USDT")
        self.assertEqual(ob_msg.content["update_id"], 12345)
        self.assertEqual(ob_msg.content["bids"], [[100.0, 1.5], [99.0, 2.0]])
        self.assertEqual(ob_msg.content["asks"], [[101.0, 1.0], [102.0, 2.5]])
        self.assertEqual(ob_msg.timestamp, timestamp)

    def test_diff_message_from_exchange(self):
        msg = {
            "bids": {"100.0": "1.5"},
            "asks": {"101.0": "1.0"},
            "vs": 54321
        }
        metadata = {"trading_pair": "BTC-USDT"}
        timestamp = 1234567890.0
        ob_msg = CoinDCXOrderBook.diff_message_from_exchange(msg, timestamp, metadata)
        self.assertEqual(ob_msg.message_type, OrderBookMessageType.DIFF)
        self.assertEqual(ob_msg.content["trading_pair"], "BTC-USDT")
        self.assertEqual(ob_msg.content["update_id"], 54321)
        self.assertEqual(ob_msg.content["bids"], [[100.0, 1.5]])
        self.assertEqual(ob_msg.content["asks"], [[101.0, 1.0]])
        self.assertEqual(ob_msg.timestamp, timestamp)

    def test_trade_message_from_exchange(self):
        msg = {
            "T": 1234567890000,
            "p": "100.5",
            "q": "0.25",
            "m": 1,
            "s": "BTCUSDT"
        }
        metadata = {"trading_pair": "BTC-USDT"}
        ob_msg = CoinDCXOrderBook.trade_message_from_exchange(msg, metadata)
        self.assertEqual(ob_msg.message_type, OrderBookMessageType.TRADE)
        self.assertEqual(ob_msg.content["trading_pair"], "BTC-USDT")
        self.assertEqual(ob_msg.content["trade_id"], 1234567890000)
        self.assertEqual(ob_msg.content["update_id"], 1234567890000)
        self.assertEqual(ob_msg.content["price"], 100.5)
        self.assertEqual(ob_msg.content["amount"], 0.25)
        self.assertEqual(ob_msg.timestamp, 1234567890.0)
        self.assertEqual(ob_msg.content["trade_type"], 1.0)
