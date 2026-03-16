from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class AjaibOrderBook(OrderBook):
    """
    Ajaib-specific order book implementation.
    Handles Binance-compatible order book response format.
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

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": msg.get("lastUpdateId", int(timestamp * 1000)),
            "bids": msg.get("bids", []),
            "asks": msg.get("asks", []),
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

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": msg.get("u", int(timestamp * 1000)),
            "bids": msg.get("b", []),
            "asks": msg.get("a", []),
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

        ts = float(msg.get("T", msg.get("time", 0))) / 1000.0

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.SELL.value) if msg.get("m", False) else float(TradeType.BUY.value),
            "trade_id": msg.get("t", msg.get("id")),
            "update_id": msg.get("T", msg.get("time")),
            "price": float(msg.get("p", msg.get("price", 0))),
            "amount": float(msg.get("q", msg.get("qty", 0))),
        }

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            content,
            ts,
        )
