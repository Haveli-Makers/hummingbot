from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


def _levels(raw_levels: Any) -> List[List[float]]:
    levels: List[List[float]] = []
    for level in raw_levels or []:
        try:
            levels.append([float(level[0]), float(level[1])])
        except (IndexError, TypeError, ValueError):
            continue
    return levels


class CoinswitchPerpetualOrderBook(OrderBook):
    """
    Parses CoinSwitch futures order book and trade payloads.

    The REST ``order_book`` and the ``FETCH_ORDER_BOOK_CS_PRO`` socket event share
    one shape, ``{"symbol", "timestamp", "bids", "asks"}`` with ``[price, qty]``
    string pairs, and every socket push is a FULL snapshot ("replace your local
    book on every event"), so there is no diff message type for this connector.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict[str, Any],
            timestamp: float,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        metadata = metadata or {}
        update_id = msg.get("timestamp") or int(timestamp * 1e3)
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": int(update_id),
            "bids": _levels(msg.get("bids")),
            "asks": _levels(msg.get("asks")),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp)

    @classmethod
    def trade_message_from_exchange(
            cls,
            msg: Dict[str, Any],
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        """
        ``{"E": ms, "p": price, "q": qty, "s": symbol, "e": exchange, "m": bool}``.
        ``m`` is "was the buyer the maker?": a resting buyer means the aggressor
        sold, so the print is a SELL. Trades carry no id; the trade time stands in.
        """
        metadata = metadata or {}
        ts_ms = int(msg.get("E") or 0)
        trade_type = TradeType.SELL if msg.get("m") else TradeType.BUY
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(trade_type.value),
            "trade_id": ts_ms,
            "update_id": ts_ms,
            "price": float(msg.get("p", 0) or 0),
            "amount": float(msg.get("q", 0) or 0),
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, ts_ms * 1e-3)
