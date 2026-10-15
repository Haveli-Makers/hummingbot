from typing import Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


def _levels(raw_levels) -> List[List[float]]:
    """``[[price, qty], ...]`` with both as strings, as every FAPI book payload sends them."""
    levels: List[List[float]] = []
    for level in raw_levels or []:
        try:
            levels.append([float(level[0]), float(level[1])])
        except (IndexError, TypeError, ValueError):
            continue
    return levels


class CoinDCXPerpetualOrderBook(OrderBook):
    """
    Parses CoinDCX FAPI order book and trade payloads.

    The book is the Binance-style pair of a REST snapshot (``lastUpdateId``) and
    ``depthUpdate`` diffs chained by ``U``/``u``/``pu``; the update id of every
    message is the exchange's own sequence number, so the tracker can order
    snapshots and diffs against each other.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        """From ``GET /fapi/v1/depth``: ``lastUpdateId``, ``bids``, ``asks``."""
        metadata = metadata or {}
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": int(msg["lastUpdateId"]),
            "bids": _levels(msg.get("bids")),
            "asks": _levels(msg.get("asks")),
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp)

    @classmethod
    def diff_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: Optional[float] = None,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        """
        From a ``depthUpdate`` frame. Levels carry absolute quantities; a zero
        quantity removes the level.
        """
        metadata = metadata or {}
        timestamp = timestamp if timestamp is not None else float(msg.get("E", 0) or 0) * 1e-3
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "first_update_id": int(msg.get("U", msg["u"])),
            "update_id": int(msg["u"]),
            "bids": _levels(msg.get("b")),
            "asks": _levels(msg.get("a")),
        }
        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp)

    @classmethod
    def trade_message_from_exchange(
            cls,
            msg: Dict,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        """
        From an ``aggTrade`` frame. ``m`` says whether the BUYER was the maker, so a
        true ``m`` means the taker sold.
        """
        metadata = metadata or {}
        ts_ms = float(msg.get("T") or msg.get("E") or 0)
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.SELL.value) if msg.get("m") else float(TradeType.BUY.value),
            "trade_id": msg["a"],
            "update_id": int(ts_ms),
            "price": float(msg["p"]),
            "amount": float(msg["q"]),
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, ts_ms * 1e-3)
