import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange


class CsxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order-book data source for CoinSwitch Kuber (CSX).

    CSX does not expose a WebSocket market-data feed, so this implementation
    polls the public REST depth and trades endpoints on a fixed interval and
    injects synthetic SNAPSHOT / TRADE messages into the tracker queues.
    """

    SNAPSHOT_POLL_INTERVAL = 30.0
    TRADE_POLL_INTERVAL = 10.0

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CsxExchange",
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
        # The connector implements the PLURAL _get_last_traded_prices (a single
        # ticker call for all pairs). The singular _get_last_traded_price is not
        # overridden and resolves to ExchangeBase._get_last_traded_price, which
        # raises NotImplementedError — so calling it returned {} every time.
        try:
            return await self._connector._get_last_traded_prices(trading_pairs=trading_pairs)
        except Exception as exc:
            self.logger().warning(f"Error fetching last traded prices for {trading_pairs}: {exc}")
            return {}

    # ── Snapshot helpers ───────────────────────────────────────────────────────

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        try:
            instrument = await self._connector.exchange_symbol_associated_to_pair(
                trading_pair=trading_pair
            )
        except KeyError:
            instrument = trading_pair.replace("-", "/")

        return await self._connector._api_get(
            path_url=CONSTANTS.DEPTH_V2_PATH_URL,
            params={"instrument": instrument},
            is_auth_required=False,
        )

    @staticmethod
    def _extract_depth_data(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Navigate the CSX depth response to the dict that has 'buy'/'sell' keys.

        Actual API shape:
            {"data": [{"buy": [{"price":"...","quantity":"..."}, ...], "sell": [...]}]}

        The bids/asks entries are {"price":"...","quantity":"..."} dicts — convert
        to [[price, qty], ...] so hummingbot's order-book tracker can process them.
        """
        data = response.get("data", response) if isinstance(response, dict) else response
        # "data" is a list with one element
        if isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            return {"buy": [], "sell": []}

        def _normalise(entries: list) -> list:
            result = []
            for e in entries:
                if isinstance(e, dict):
                    result.append([e.get("price", "0"), e.get("quantity", "0")])
                elif isinstance(e, (list, tuple)) and len(e) >= 2:
                    result.append([str(e[0]), str(e[1])])
            return result

        return {
            "buy": _normalise(data.get("buy", data.get("bids", []))),
            "sell": _normalise(data.get("sell", data.get("asks", []))),
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
                "bids": data["buy"],
                "asks": data["sell"],
            },
            timestamp=ts,
        )

    # ── Main subscription loop (REST polling) ─────────────────────────────────

    async def listen_for_subscriptions(self):
        """
        Repeatedly fetches order-book snapshots and recent trades via REST and
        pushes them into the appropriate message queues.
        """
        while True:
            try:
                snapshot_queue = self._message_queue[self._snapshot_messages_queue_key]
                trade_queue = self._message_queue[self._trade_messages_queue_key]

                # Snapshot pass
                for trading_pair in self._trading_pairs:
                    try:
                        raw = await self._request_order_book_snapshot(trading_pair)
                        data = self._extract_depth_data(raw)
                        ts = self._time()
                        msg = {
                            "trading_pair": trading_pair,
                            "bids": data["buy"],
                            "asks": data["sell"],
                            "timestamp": ts * 1000,
                        }
                        snapshot_queue.put_nowait(msg)
                    except Exception as exc:
                        self.logger().warning(
                            f"Error fetching order-book snapshot for {trading_pair}: {exc}"
                        )

                # Trade pass
                for trading_pair in self._trading_pairs:
                    try:
                        try:
                            instrument = await self._connector.exchange_symbol_associated_to_pair(
                                trading_pair=trading_pair
                            )
                        except KeyError:
                            instrument = trading_pair.replace("-", "/")

                        trades_resp = await self._connector._api_get(
                            path_url=CONSTANTS.TRADES_PATH_URL,
                            params={"instrument": instrument},
                            is_auth_required=False,
                        )
                        trades = (
                            trades_resp
                            if isinstance(trades_resp, list)
                            else trades_resp.get("data", [])
                        )
                        for trade in trades:
                            trade["_trading_pair"] = trading_pair
                            trade_queue.put_nowait(trade)
                    except Exception as exc:
                        self.logger().warning(
                            f"Error fetching trades for {trading_pair}: {exc}"
                        )

                await asyncio.sleep(self.SNAPSHOT_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error in CSX order-book polling loop. Retrying in 5 s …"
                )
                await asyncio.sleep(5.0)

    # ── Message parsers ────────────────────────────────────────────────────────

    async def _parse_order_book_snapshot_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        trading_pair = raw_message.get("trading_pair")
        if not trading_pair:
            return

        ts = float(raw_message.get("timestamp", self._time() * 1000)) / 1000.0
        msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(ts * 1000),
                "bids": raw_message.get("bids", []),
                "asks": raw_message.get("asks", []),
            },
            timestamp=ts,
        )
        message_queue.put_nowait(msg)

    async def _parse_trade_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        trading_pair = raw_message.get("_trading_pair")
        if not trading_pair:
            return

        # CSX trade fields (confirmed from browser response):
        # {"price":"...","baseQuantity":"...","isBuyerMaker":true,
        #  "createdAt":{"seconds":1780299395,"nanos":950890000}}
        price = raw_message.get("price") or raw_message.get("p") or "0"
        qty = (raw_message.get("baseQuantity") or raw_message.get("quantity")
               or raw_message.get("q") or "0")
        trade_id = raw_message.get("id") or raw_message.get("tradeId") or ""
        is_buyer_maker = raw_message.get("isBuyerMaker") or raw_message.get("m") or False

        # Timestamp: {"seconds": 1780299395, "nanos": 950890000} → milliseconds
        created_at = raw_message.get("createdAt")
        if isinstance(created_at, dict):
            ts_raw = created_at.get("seconds", 0) * 1000 + created_at.get("nanos", 0) / 1_000_000
        else:
            ts_raw = raw_message.get("timestamp") or raw_message.get("time") or (self._time() * 1000)

        msg = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.SELL.value) if is_buyer_maker else float(TradeType.BUY.value),
                "trade_id": str(trade_id),
                "update_id": str(trade_id),
                "price": str(price),
                "amount": str(qty),
            },
            timestamp=float(ts_raw) / 1000.0,
        )
        message_queue.put_nowait(msg)

    async def _parse_order_book_diff_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        # CSX has no incremental diffs – treat every message as a snapshot
        await self._parse_order_book_snapshot_message(raw_message, message_queue)

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("CsxAPIOrderBookDataSource uses REST polling – no WebSocket.")
