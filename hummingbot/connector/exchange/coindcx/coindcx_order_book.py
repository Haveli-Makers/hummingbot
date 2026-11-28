from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class CoinDCXOrderBook(OrderBook):
    """
    CoinDCX-specific order book implementation.
    Handles conversion of CoinDCX API responses to OrderBookMessage objects.

    Update ids are CoinDCX's clock in milliseconds: "ts" on stream messages and "timestamp" on the REST order book.
    Both carry the same value for the same version of the book, so the tracker can compare REST and stream messages.
    The "vs" counter is not used: the REST book doesn't carry it, and it restarts.
    """

    @staticmethod
    def _update_id(msg: Dict, timestamp: float) -> int:
        exchange_time_ms = msg.get("ts") or msg.get("timestamp")
        return int(exchange_time_ms) if exchange_time_ms else int(timestamp * 1000)

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Convert a CoinDCX order book snapshot into an OrderBookMessage.
        """
        if metadata is None:
            metadata = {}

        bids = []
        asks = []

        if "bids" in msg:
            for price, amount in msg["bids"].items():
                bids.append([float(price), float(amount)])

        if "asks" in msg:
            for price, amount in msg["asks"].items():
                asks.append([float(price), float(amount)])

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": cls._update_id(msg, timestamp),
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            content,
            timestamp,
        )

    @classmethod
    def diff_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Convert a CoinDCX order book differential update into an OrderBookMessage.
        """
        if metadata is None:
            metadata = {}

        bids = []
        asks = []

        if "bids" in msg:
            for price, amount in msg["bids"].items():
                bids.append([float(price), float(amount)])

        if "asks" in msg:
            for price, amount in msg["asks"].items():
                asks.append([float(price), float(amount)])

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": cls._update_id(msg, timestamp),
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(
            OrderBookMessageType.DIFF,
            content,
            timestamp,
        )

    @classmethod
    def trade_message_from_exchange(
            cls,
            msg: Dict,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Convert a CoinDCX trade message into an OrderBookMessage.
        """
        if metadata is None:
            metadata = {}

        ts = float(msg.get("T", 0)) / 1000.0

        content = {
            "trading_pair": metadata.get("trading_pair"),
            # "m" is true when the buyer was the maker, so the taker sold.
            "trade_type": float(TradeType.SELL.value) if msg.get("m") else float(TradeType.BUY.value),
            "trade_id": msg.get("T"),
            "update_id": msg.get("T"),
            "price": float(msg.get("p", 0)),
            "amount": float(msg.get("q", 0))
        }

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            content,
            ts,
        )
