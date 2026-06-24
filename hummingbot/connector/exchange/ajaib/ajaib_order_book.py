from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class AjaibOrderBook(OrderBook):
    """
    Ajaib order book message parsing.

    The Ajaib partial-book-depth stream (``<symbol>@depth``) pushes a full
    snapshot of the top 20 levels every 500ms, so every depth message is treated
    as a SNAPSHOT rather than an incremental diff.
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
            # Depth payloads carry no update id; use the receive timestamp (ms) so
            # successive snapshots are strictly increasing.
            "update_id": msg.get("lastUpdateId", int(timestamp * 1e3)),
            "bids": msg.get("bids", []),
            "asks": msg.get("asks", []),
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
        if metadata is None:
            metadata = {}

        ts = float(msg.get("E", msg.get("time", 0))) / 1e3

        # Ajaib's trade stream does not expose the aggressor side, so trades are
        # reported as BUY by default.
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.SELL.value) if msg.get("m", False) else float(TradeType.BUY.value),
            "trade_id": msg.get("t", msg.get("id")),
            "update_id": msg.get("E", msg.get("time")),
            "price": float(msg.get("p", msg.get("price", 0))),
            "amount": float(msg.get("q", msg.get("qty", 0))),
        }

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            content,
            ts,
        )
