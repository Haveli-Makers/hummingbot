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


class WazirxPerpetualOrderBook(OrderBook):
    """
    Parses WazirX futures order book and trade payloads.

    Both the REST depth (``bids``/``asks``) and the ``<symbol>@depth`` stream
    (``b``/``a``) carry a full top-20 book. The docs describe the stream as
    "bids/asks to be updated", but live frames always hold ~20 levels per side
    and never a zero-quantity level, so nothing would ever remove a level that
    left the top 20. Every frame is therefore applied as a SNAPSHOT; there is no
    diff message type for this connector.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict[str, Any],
            timestamp: float,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        metadata = metadata or {}
        # E is the event / response time in ms; both REST and stream books carry it.
        update_id = msg.get("E") or msg.get("T") or int(timestamp * 1e3)
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": int(update_id),
            "bids": _levels(msg.get("bids", msg.get("b"))),
            "asks": _levels(msg.get("asks", msg.get("a"))),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp)

    @classmethod
    def trade_message_from_exchange(
            cls,
            msg: Dict[str, Any],
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        metadata = metadata or {}
        ts_ms = int(msg.get("T") or msg.get("E") or 0)
        # ``m`` is "is the buyer the market maker?": a resting buyer means the
        # aggressor sold, so the print is a SELL.
        trade_type = TradeType.SELL if msg.get("m") else TradeType.BUY
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(trade_type.value),
            # aggTrade carries no trade id; the trade time is the closest stand-in.
            "trade_id": ts_ms,
            "update_id": ts_ms,
            "price": float(msg.get("p", 0) or 0),
            "amount": float(msg.get("q", 0) or 0),
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, ts_ms * 1e-3)
