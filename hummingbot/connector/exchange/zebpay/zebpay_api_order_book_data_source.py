import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_utils import unwrap_data
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange


class ZebpayAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order-book data source for Zebpay (spot).

    Zebpay spot exposes no WebSocket market-data feed, so this implementation
    polls the public REST orderbook and trades endpoints on a fixed interval
    and injects synthetic SNAPSHOT / TRADE messages into the tracker queues.
    """

    SNAPSHOT_POLL_INTERVAL = 30.0
    ORDERBOOK_DEPTH = 50

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "ZebpayExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    def _time(self) -> float:
        return time.time()

    # ── Public helpers ─────────────────────────────────────────────────────────

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for tp in trading_pairs:
            try:
                price = await self._connector._get_last_traded_price(trading_pair=tp)
                if price and price > 0:
                    prices[tp] = price
            except Exception as exc:
                self.logger().warning(f"Error fetching last price for {tp}: {exc}")
        return prices

    # ── Snapshot helpers ───────────────────────────────────────────────────────

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair
        return await self._connector._api_get(
            path_url=CONSTANTS.ORDERBOOK_PATH_URL,
            params={"symbol": symbol, "limit": self.ORDERBOOK_DEPTH},
            is_auth_required=False,
        )

    @staticmethod
    def _extract_depth_data(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Zebpay orderbook shape: {"bids": [["price","volume"], ...], "asks": [...]}
        (optionally wrapped under "data"). Normalise to [[price, qty], ...].
        """
        data = unwrap_data(response)
        if isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            return {"bids": [], "asks": []}

        def _norm(levels) -> list:
            out = []
            for lvl in levels or []:
                if isinstance(lvl, dict):
                    out.append([lvl.get("price", "0"), lvl.get("volume", lvl.get("quantity", "0"))])
                elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    out.append([str(lvl[0]), str(lvl[1])])
            return out

        return {
            "bids": _norm(data.get("bids", data.get("buy", []))),
            "asks": _norm(data.get("asks", data.get("sell", []))),
        }

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raw = await self._request_order_book_snapshot(trading_pair)
        ts = self._time()
        data = self._extract_depth_data(raw)
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(ts * 1000),
                "bids": data["bids"],
                "asks": data["asks"],
            },
            timestamp=ts,
        )

    # ── Main subscription loop (REST polling) ─────────────────────────────────

    async def listen_for_subscriptions(self):
        while True:
            try:
                snapshot_queue = self._message_queue[self._snapshot_messages_queue_key]
                trade_queue = self._message_queue[self._trade_messages_queue_key]

                for trading_pair in self._trading_pairs:
                    try:
                        raw = await self._request_order_book_snapshot(trading_pair)
                        data = self._extract_depth_data(raw)
                        ts = self._time()
                        snapshot_queue.put_nowait({
                            "trading_pair": trading_pair,
                            "bids": data["bids"],
                            "asks": data["asks"],
                            "timestamp": ts * 1000,
                        })
                    except Exception as exc:
                        self.logger().warning(f"Error fetching Zebpay order book for {trading_pair}: {exc}")

                for trading_pair in self._trading_pairs:
                    try:
                        try:
                            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                        except KeyError:
                            symbol = trading_pair
                        trades_resp = await self._connector._api_get(
                            path_url=CONSTANTS.TRADES_PATH_URL,
                            params={"symbol": symbol},
                            is_auth_required=False,
                        )
                        trades = unwrap_data(trades_resp)
                        trades = trades if isinstance(trades, list) else []
                        for trade in trades:
                            if isinstance(trade, dict):
                                trade["_trading_pair"] = trading_pair
                                trade_queue.put_nowait(trade)
                    except Exception as exc:
                        self.logger().warning(f"Error fetching Zebpay trades for {trading_pair}: {exc}")

                await asyncio.sleep(self.SNAPSHOT_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in Zebpay order-book polling loop. Retrying in 5 s …")
                await asyncio.sleep(5.0)

    # ── Message parsers ────────────────────────────────────────────────────────

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        trading_pair = raw_message.get("trading_pair")
        if not trading_pair:
            return
        ts = float(raw_message.get("timestamp", self._time() * 1000)) / 1000.0
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(ts * 1000),
                "bids": raw_message.get("bids", []),
                "asks": raw_message.get("asks", []),
            },
            timestamp=ts,
        ))

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        trading_pair = raw_message.get("_trading_pair")
        if not trading_pair:
            return
        price = raw_message.get("price", "0")
        qty = raw_message.get("amount", raw_message.get("quantity", "0"))
        trade_id = raw_message.get("id", raw_message.get("tradeId", ""))
        # Zebpay: isBuyerMaker True → the aggressor was a SELLER.
        is_buyer_maker = raw_message.get("isBuyerMaker", str(raw_message.get("side", "")).upper() == "SELL")
        ts_raw = raw_message.get("timestamp", raw_message.get("createdAt", self._time() * 1000))
        message_queue.put_nowait(OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.SELL.value) if is_buyer_maker else float(TradeType.BUY.value),
                "trade_id": str(trade_id),
                "update_id": str(trade_id),
                "price": str(price),
                "amount": str(qty),
            },
            timestamp=float(ts_raw) / 1000.0 if float(ts_raw) > 1e12 else float(ts_raw),
        ))

    async def _parse_order_book_diff_message(self, raw_message: Any, message_queue: asyncio.Queue):
        await self._parse_order_book_snapshot_message(raw_message, message_queue)

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("ZebpayAPIOrderBookDataSource uses REST polling — no WebSocket.")
