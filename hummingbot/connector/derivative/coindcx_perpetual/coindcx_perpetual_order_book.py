from typing import Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


def _levels(raw_levels) -> List[List[float]]:
    """
    CoinDCX sends book sides as a mapping of ``price -> quantity`` (both strings)
    rather than as the usual array of pairs, and the entries are NOT sorted.
    """
    if isinstance(raw_levels, dict):
        entries = raw_levels.items()
    else:
        entries = [(level[0], level[1]) for level in (raw_levels or []) if len(level) >= 2]

    levels: List[List[float]] = []
    for price, amount in entries:
        try:
            levels.append([float(price), float(amount)])
        except (TypeError, ValueError):
            continue
    return levels


class CoinDCXPerpetualOrderBook(OrderBook):
    """
    Parses CoinDCX futures order book / trade payloads.

    The ``depth-snapshot`` stream always carries a full top-N snapshot, so there
    is no diff message type for this connector.
    """

    @classmethod
    def snapshot_message_from_exchange(
            cls,
            msg: Dict,
            timestamp: float,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        metadata = metadata or {}
        # ``vs`` (version) increases monotonically; fall back to the payload/receive
        # timestamp so successive snapshots always advance the update id.
        update_id = msg.get("vs") or msg.get("ts") or int(timestamp * 1e3)

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
            msg: Dict,
            metadata: Optional[Dict] = None,
    ) -> OrderBookMessage:
        metadata = metadata or {}
        ts_ms = float(msg.get("T", 0) or 0)

        # ``m`` flags whether the AGGRESSOR was the maker; a taker buy (m falsy)
        # lifts the ask and prints as a BUY.
        is_maker = bool(msg.get("m"))
        content = {
            "trading_pair": metadata.get("trading_pair"),
            "trade_type": float(TradeType.SELL.value) if is_maker else float(TradeType.BUY.value),
            "trade_id": msg.get("t") or int(ts_ms),
            "update_id": int(ts_ms),
            "price": float(msg.get("p", 0) or 0),
            "amount": float(msg.get("q", 0) or 0),
        }
        return OrderBookMessage(OrderBookMessageType.TRADE, content, ts_ms * 1e-3)
