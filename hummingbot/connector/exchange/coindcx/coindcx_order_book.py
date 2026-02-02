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
            "update_id": msg.get("vs", int(timestamp * 1000)),
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
            "update_id": msg.get("vs", int(timestamp * 1000)),
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
        if metadata is None:
            metadata = {}

        ts = float(msg.get("T", 0)) / 1000.0

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.BUY.value) if msg.get("m", 0) else float(TradeType.SELL.value),
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
