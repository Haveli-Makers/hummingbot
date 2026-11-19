from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class WazirxOrderBook(OrderBook):
    """
    WazirX-specific order book implementation.
    Handles conversion of WazirX API responses to OrderBookMessage objects.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Convert a WazirX order book into an OrderBookMessage.
        Accepts both the REST depth response ("bids"/"asks") and the depth stream payload ("b"/"a").
        WazirX books carry no sequence number, so the update id is the local receive time in milliseconds.
        """
        if metadata is None:
            metadata = {}

        bids = [[float(price), float(amount)] for price, amount in msg.get("bids", msg.get("b", []))]
        asks = [[float(price), float(amount)] for price, amount in msg.get("asks", msg.get("a", []))]

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": int(timestamp * 1000),
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
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
        Convert one trade from the WazirX trade stream into an OrderBookMessage.
        """
        if metadata is None:
            metadata = {}

        ts = float(msg.get("E", 0)) / 1000.0

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.SELL.value) if msg.get("m") else float(TradeType.BUY.value),
            "trade_id": msg.get("t"),
            "update_id": msg.get("t"),
            "price": float(msg.get("p", 0)),
            "amount": float(msg.get("q", 0))
        }

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            content,
            ts,
        )
