from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class CoinDCXOrderBook(OrderBook):
    """
    CoinDCX-specific order book implementation.
    Handles conversion of CoinDCX API responses to OrderBookMessage objects.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Converts a CoinDCX order book snapshot to an OrderBookMessage.

        CoinDCX snapshot format:
        {
            "bids": {"price1": "quantity1", "price2": "quantity2", ...},
            "asks": {"price1": "quantity1", "price2": "quantity2", ...}
        }

        :param msg: the order book snapshot message from CoinDCX
        :param timestamp: the timestamp of the message
        :param metadata: additional metadata (should include 'trading_pair')
        :return: an OrderBookMessage object
        """
        if metadata is None:
            metadata = {}

        # Convert bids and asks from dict to list of [price, amount]
        bids = []
        asks = []

        if "bids" in msg:
            for price, amount in msg["bids"].items():
                bids.append([float(price), float(amount)])

        if "asks" in msg:
            for price, amount in msg["asks"].items():
                asks.append([float(price), float(amount)])

        # Sort bids (descending) and asks (ascending)
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": msg.get("vs", int(timestamp * 1000)),
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=content,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Converts a CoinDCX order book diff (update) to an OrderBookMessage.

        CoinDCX depth-update format:
        {
            "ts": timestamp,
            "vs": version,
            "asks": {"price1": "quantity1", ...},
            "bids": {"price1": "quantity1", ...}
        }

        :param msg: the order book diff message from CoinDCX
        :param timestamp: the timestamp of the message
        :param metadata: additional metadata (should include 'trading_pair')
        :return: an OrderBookMessage object
        """
        if metadata is None:
            metadata = {}

        # Convert bids and asks from dict to list of [price, amount]
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
            "update_id": msg.get("vs", int(timestamp * 1000)),
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=content,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_exchange(
            cls,
            msg: Dict,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Converts a CoinDCX trade message to an OrderBookMessage.

        CoinDCX new-trade format:
        {
            "T": "timestamp",
            "p": "price",
            "q": "quantity",
            "m": 0 or 1 (is_maker),
            "s": "pair",
            "pr": "spot"
        }

        :param msg: the trade message from CoinDCX
        :param metadata: additional metadata (should include 'trading_pair')
        :return: an OrderBookMessage object
        """
        if metadata is None:
            metadata = {}

        ts = float(msg.get("T", 0)) / 1000.0  # Convert from milliseconds

        content = {
            "trading_pair": metadata.get("trading_pair"),
            # CoinDCX 'm' flag maps to BUY when truthy in our tests/environment
            "trade_type": float(TradeType.BUY.value) if msg.get("m", 0) else float(TradeType.SELL.value),
            "trade_id": msg.get("T"),  # Use timestamp as trade ID if not provided
            "update_id": msg.get("T"),
            "price": float(msg.get("p", 0)),
            "amount": float(msg.get("q", 0))
        }

        return OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=content,
            timestamp=ts
        )
