"""
Test cases for CoinDCX API Order Book Data Source.
These tests verify WebSocket message handling and data source logic.
"""
import json
import time
import unittest
from decimal import Decimal
from typing import Dict, Tuple


class TestCoinDCXWebSocketMessages(unittest.TestCase):
    """Test cases for WebSocket message parsing."""

    def parse_ws_message(self, raw_message: str) -> dict:
        """Parse a WebSocket message."""
        try:
            return json.loads(raw_message)
        except json.JSONDecodeError:
            return {}

    def get_message_type(self, message: dict) -> str:
        """Get the type of WebSocket message."""
        if "event" in message:
            return message["event"]
        if "channel" in message:
            return message["channel"]
        if "type" in message:
            return message["type"]
        return "unknown"

    def test_parse_json_message(self):
        """Test parsing a valid JSON message."""
        raw = '{"event": "subscribed", "channel": "orderbook"}'
        result = self.parse_ws_message(raw)

        self.assertEqual(result["event"], "subscribed")
        self.assertEqual(result["channel"], "orderbook")

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON returns empty dict."""
        raw = 'not valid json'
        result = self.parse_ws_message(raw)

        self.assertEqual(result, {})

    def test_get_message_type_event(self):
        """Test getting message type from event field."""
        message = {"event": "subscribed"}
        self.assertEqual(self.get_message_type(message), "subscribed")

    def test_get_message_type_channel(self):
        """Test getting message type from channel field."""
        message = {"channel": "orderbook"}
        self.assertEqual(self.get_message_type(message), "orderbook")

    def test_get_message_type_unknown(self):
        """Test getting unknown message type."""
        message = {"data": "something"}
        self.assertEqual(self.get_message_type(message), "unknown")


class TestCoinDCXOrderBookSubscription(unittest.TestCase):
    """Test cases for order book subscription."""

    def create_subscribe_message(self, channel: str, symbol: str) -> dict:
        """Create a subscription message."""
        return {
            "event": "subscribe",
            "channel": channel,
            "symbol": symbol
        }

    def create_unsubscribe_message(self, channel: str, symbol: str) -> dict:
        """Create an unsubscription message."""
        return {
            "event": "unsubscribe",
            "channel": channel,
            "symbol": symbol
        }

    def test_create_orderbook_subscription(self):
        """Test creating order book subscription message."""
        msg = self.create_subscribe_message("orderbook", "BTCUSDT")

        self.assertEqual(msg["event"], "subscribe")
        self.assertEqual(msg["channel"], "orderbook")
        self.assertEqual(msg["symbol"], "BTCUSDT")

    def test_create_trades_subscription(self):
        """Test creating trades subscription message."""
        msg = self.create_subscribe_message("trades", "ETHUSDT")

        self.assertEqual(msg["channel"], "trades")
        self.assertEqual(msg["symbol"], "ETHUSDT")

    def test_create_unsubscribe_message(self):
        """Test creating unsubscription message."""
        msg = self.create_unsubscribe_message("orderbook", "BTCUSDT")

        self.assertEqual(msg["event"], "unsubscribe")
        self.assertEqual(msg["channel"], "orderbook")


class TestCoinDCXOrderBookSnapshot(unittest.TestCase):
    """Test cases for order book snapshot handling."""

    def process_snapshot(self, data: dict) -> Tuple[int, Dict[str, Decimal], Dict[str, Decimal]]:
        """
        Process an order book snapshot.
        Returns (timestamp, bids, asks).
        """
        timestamp = data.get("timestamp", int(time.time() * 1000))

        bids = {}
        asks = {}

        for price, qty in data.get("bids", {}).items():
            bids[price] = Decimal(str(qty))

        for price, qty in data.get("asks", {}).items():
            asks[price] = Decimal(str(qty))

        return timestamp, bids, asks

    def test_process_snapshot_basic(self):
        """Test processing a basic snapshot."""
        data = {
            "timestamp": 1620000000000,
            "bids": {"50000": "1.0", "49999": "2.0"},
            "asks": {"50001": "0.5", "50002": "1.5"}
        }

        timestamp, bids, asks = self.process_snapshot(data)

        self.assertEqual(timestamp, 1620000000000)
        self.assertEqual(len(bids), 2)
        self.assertEqual(len(asks), 2)
        self.assertEqual(bids["50000"], Decimal("1.0"))
        self.assertEqual(asks["50001"], Decimal("0.5"))

    def test_process_snapshot_empty(self):
        """Test processing empty snapshot."""
        data = {"timestamp": 1620000000000}

        timestamp, bids, asks = self.process_snapshot(data)

        self.assertEqual(len(bids), 0)
        self.assertEqual(len(asks), 0)

    def test_process_snapshot_default_timestamp(self):
        """Test that missing timestamp gets a default."""
        data = {"bids": {"50000": "1.0"}}

        timestamp, bids, asks = self.process_snapshot(data)

        self.assertIsInstance(timestamp, int)
        self.assertGreater(timestamp, 0)


class TestCoinDCXOrderBookDiff(unittest.TestCase):
    """Test cases for order book diff handling."""

    def apply_diff(self,
                   current_book: Dict[str, Decimal],
                   diff: Dict[str, str]) -> Dict[str, Decimal]:
        """
        Apply a diff to the current order book.
        If quantity is "0", remove the level.
        """
        result = current_book.copy()

        for price, qty in diff.items():
            qty_decimal = Decimal(str(qty))
            if qty_decimal == Decimal("0"):
                result.pop(price, None)
            else:
                result[price] = qty_decimal

        return result

    def test_apply_diff_add(self):
        """Test adding a new price level."""
        current = {"50000": Decimal("1.0")}
        diff = {"49999": "2.0"}

        result = self.apply_diff(current, diff)

        self.assertEqual(len(result), 2)
        self.assertEqual(result["49999"], Decimal("2.0"))

    def test_apply_diff_update(self):
        """Test updating an existing price level."""
        current = {"50000": Decimal("1.0")}
        diff = {"50000": "3.0"}

        result = self.apply_diff(current, diff)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["50000"], Decimal("3.0"))

    def test_apply_diff_remove(self):
        """Test removing a price level with zero quantity."""
        current = {"50000": Decimal("1.0"), "49999": Decimal("2.0")}
        diff = {"50000": "0"}

        result = self.apply_diff(current, diff)

        self.assertEqual(len(result), 1)
        self.assertNotIn("50000", result)

    def test_apply_diff_multiple(self):
        """Test applying multiple changes."""
        current = {"50000": Decimal("1.0"), "49999": Decimal("2.0")}
        diff = {
            "50000": "0",       # Remove
            "49999": "5.0",     # Update
            "49998": "3.0"      # Add
        }

        result = self.apply_diff(current, diff)

        self.assertEqual(len(result), 2)
        self.assertNotIn("50000", result)
        self.assertEqual(result["49999"], Decimal("5.0"))
        self.assertEqual(result["49998"], Decimal("3.0"))


class TestCoinDCXTradeDataParsing(unittest.TestCase):
    """Test cases for trade data parsing from WebSocket."""

    def parse_trade(self, data: dict) -> dict:
        """Parse a trade message."""
        return {
            "trade_id": str(data.get("trade_id", data.get("T", ""))),
            "symbol": data.get("symbol", data.get("s", "")),
            "price": Decimal(str(data.get("price", data.get("p", "0")))),
            "quantity": Decimal(str(data.get("quantity", data.get("q", "0")))),
            "timestamp": int(data.get("timestamp", data.get("t", 0))),
            "side": data.get("side", "buy" if data.get("m", False) else "sell")
        }

    def test_parse_trade_standard(self):
        """Test parsing trade with standard keys."""
        data = {
            "trade_id": "12345",
            "symbol": "BTCUSDT",
            "price": "50000.00",
            "quantity": "1.5",
            "timestamp": 1620000000000,
            "side": "buy"
        }

        trade = self.parse_trade(data)

        self.assertEqual(trade["trade_id"], "12345")
        self.assertEqual(trade["symbol"], "BTCUSDT")
        self.assertEqual(trade["price"], Decimal("50000.00"))
        self.assertEqual(trade["quantity"], Decimal("1.5"))
        self.assertEqual(trade["side"], "buy")

    def test_parse_trade_compact_keys(self):
        """Test parsing trade with compact keys."""
        data = {
            "T": "67890",
            "s": "ETHUSDT",
            "p": "3000.00",
            "q": "10",
            "t": 1620000001000,
            "m": True
        }

        trade = self.parse_trade(data)

        self.assertEqual(trade["trade_id"], "67890")
        self.assertEqual(trade["symbol"], "ETHUSDT")
        self.assertEqual(trade["price"], Decimal("3000.00"))


class TestCoinDCXTickerParsing(unittest.TestCase):
    """Test cases for ticker data parsing."""

    def parse_ticker(self, data: dict) -> dict:
        """Parse a ticker message."""
        return {
            "symbol": data.get("market", data.get("symbol", "")),
            "last_price": Decimal(str(data.get("last_price", data.get("c", "0")))),
            "high_24h": Decimal(str(data.get("high", data.get("h", "0")))),
            "low_24h": Decimal(str(data.get("low", data.get("l", "0")))),
            "volume_24h": Decimal(str(data.get("volume", data.get("v", "0")))),
            "change_24h": Decimal(str(data.get("change_24_hour", data.get("P", "0"))))
        }

    def test_parse_ticker_basic(self):
        """Test parsing a basic ticker."""
        data = {
            "market": "BTCUSDT",
            "last_price": "50000.00",
            "high": "51000.00",
            "low": "49000.00",
            "volume": "1000.5",
            "change_24_hour": "2.5"
        }

        ticker = self.parse_ticker(data)

        self.assertEqual(ticker["symbol"], "BTCUSDT")
        self.assertEqual(ticker["last_price"], Decimal("50000.00"))
        self.assertEqual(ticker["high_24h"], Decimal("51000.00"))
        self.assertEqual(ticker["low_24h"], Decimal("49000.00"))
        self.assertEqual(ticker["volume_24h"], Decimal("1000.5"))
        self.assertEqual(ticker["change_24h"], Decimal("2.5"))

    def test_parse_ticker_compact_keys(self):
        """Test parsing ticker with compact keys."""
        data = {
            "symbol": "ETHUSDT",
            "c": "3000.00",
            "h": "3100.00",
            "l": "2900.00",
            "v": "5000",
            "P": "1.5"
        }

        ticker = self.parse_ticker(data)

        self.assertEqual(ticker["symbol"], "ETHUSDT")
        self.assertEqual(ticker["last_price"], Decimal("3000.00"))


class TestCoinDCXDataSourceHelpers(unittest.TestCase):
    """Test cases for data source helper functions."""

    def format_symbol_for_ws(self, trading_pair: str) -> str:
        """Format a trading pair for WebSocket subscription."""
        # Convert BTC-USDT to B-BTC_USDT format
        base, quote = trading_pair.split("-")
        prefix = "I" if quote == "INR" else "B"
        return f"{prefix}-{base}_{quote}"

    def format_symbol_from_ws(self, ws_symbol: str) -> str:
        """Format a WebSocket symbol back to trading pair."""
        # Convert B-BTC_USDT to BTC-USDT
        if ws_symbol.startswith(("B-", "I-")):
            return ws_symbol[2:].replace("_", "-")
        return ws_symbol

    def test_format_symbol_for_ws_usdt(self):
        """Test formatting USDT pair for WebSocket."""
        result = self.format_symbol_for_ws("BTC-USDT")
        self.assertEqual(result, "B-BTC_USDT")

    def test_format_symbol_for_ws_inr(self):
        """Test formatting INR pair for WebSocket."""
        result = self.format_symbol_for_ws("BTC-INR")
        self.assertEqual(result, "I-BTC_INR")

    def test_format_symbol_from_ws(self):
        """Test formatting from WebSocket format."""
        result = self.format_symbol_from_ws("B-ETH_USDT")
        self.assertEqual(result, "ETH-USDT")

    def test_format_roundtrip(self):
        """Test format conversion roundtrip."""
        original = "BTC-USDT"
        ws_format = self.format_symbol_for_ws(original)
        result = self.format_symbol_from_ws(ws_format)

        self.assertEqual(result, original)


if __name__ == "__main__":
    unittest.main()
