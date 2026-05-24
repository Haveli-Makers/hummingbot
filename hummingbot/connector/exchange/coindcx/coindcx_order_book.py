import logging
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

logger = logging.getLogger(__name__)


def _parse_price_levels(raw) -> List[List[float]]:
    """Parse CoinDCX bid/ask data into [[price, amount], ...] regardless of format.

    Handles:
      - dict  {"95.55": "10", ...}         (REST API format)
      - list of lists  [["95.55", "10"]]    (possible Socket.IO format)
      - list of dicts  [{"p": "95.55", "q": "10"}]
    """
    levels: List[List[float]] = []
    if isinstance(raw, dict):
        for price, amount in raw.items():
            levels.append([float(price), float(amount)])
    elif isinstance(raw, (list, tuple)):
        for item in raw:
            if isinstance(item, dict):
                p = item.get("p") or item.get("price") or item.get("rate", 0)
                q = item.get("q") or item.get("qty") or item.get("amount") or item.get("vol", 0)
                levels.append([float(p), float(q)])
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                levels.append([float(item[0]), float(item[1])])
            else:
                logger.warning(f"CoinDCX: unrecognised price-level format: {item!r}")
    else:
        logger.warning(f"CoinDCX: unexpected bids/asks type: {type(raw).__name__}")
    return levels


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
        Convert a CoinDCX order book snapshot into an OrderBookMessage.
        """
        if metadata is None:
            metadata = {}

        bids = _parse_price_levels(msg.get("bids", {}))
        asks = _parse_price_levels(msg.get("asks", {}))

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": msg.get("vs", 0),
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

        bids = _parse_price_levels(msg.get("bids", {}))
        asks = _parse_price_levels(msg.get("asks", {}))

        content = {
            "trading_pair": metadata.get("trading_pair"),
            "update_id": msg.get("vs", 0),
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
