import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import socketio

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_order_book import CoinDCXOrderBook
from hummingbot.connector.exchange.coindcx.coindcx_utils import hb_pair_to_coindcx_pair
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoinDCXExchange


def unwrap_frame(message: Any) -> Optional[Dict[str, Any]]:
    """
    Returns the body of a CoinDCX Socket.IO message as a dict, or None if it has none.

    CoinDCX sends every message as {"event": ..., "data": "<JSON text>"}: "data" is a JSON string, not an object.
    A dict in "data", or a bare body, is accepted too.
    """
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except ValueError:
            return None
    if not isinstance(message, dict):
        return None
    body = message.get("data", message)
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            return None
    return body if isinstance(body, dict) else None


class CoinDCXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for CoinDCX order book and trade data.

    The order book is built from the stream's depth-snapshot messages, which carry the top ORDER_BOOK_DEPTH levels per
    side. CoinDCX sends one about every 3 seconds per pair, right after a depth-update of the same version. The
    depth-updates are not used: OrderBookTracker re-applies its window of past diffs on top of every snapshot, which
    can put back levels that are gone.
    """

    # If no depth-snapshot arrives for this long, the base class refreshes the order books from REST.
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinDCXExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._snapshot_messages_queue_key = CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory
        self._client: Optional[socketio.AsyncClient] = None
        # Trades name the market by its pair code ("I-SOL_INR"); depth messages use its symbol ("SOLINR").
        self._pair_code_to_trading_pair: Dict[str, str] = {
            hb_pair_to_coindcx_pair(trading_pair, ecode=CONSTANTS.ECODE_COINDCX): trading_pair
            for trading_pair in trading_pairs
        }
        self._unknown_symbols: Set[str] = set()

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Returns the last traded price for each trading pair.
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        coindcx_pair = hb_pair_to_coindcx_pair(trading_pair, ecode=CONSTANTS.ECODE_COINDCX)

        params = {
            "pair": coindcx_pair
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

        return data

    async def listen_for_subscriptions(self):
        """
        Connects to Socket.IO and subscribes to order book and trade channels.
        """
        while True:
            try:
                self._client = self._build_client()
                await self._client.connect(CONSTANTS.WSS_URL, transports=["websocket"])
                await self._subscribe_channels(self._client)
                self.logger().info("Subscribed to public order book and trade channels")
                ping_task = asyncio.create_task(self._ping_task())
                try:
                    await self._client.wait()
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
            except asyncio.CancelledError:
                await self._disconnect()
                raise
            except Exception:
                self.logger().exception("Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
                await self._disconnect()
                await asyncio.sleep(5.0)
            finally:
                await self._disconnect()
                await asyncio.sleep(1.0)

    def _build_client(self) -> socketio.AsyncClient:
        """
        Build Socket.IO client with event handlers for order book snapshots and trades.
        """
        client = socketio.AsyncClient(
            logger=False,
            reconnection=False
        )

        @client.event
        async def connect():
            self.logger().info("Connected to CoinDCX order book stream")

        @client.event
        async def disconnect():
            self.logger().warning("CoinDCX order book stream disconnected")

        @client.on(CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE)
        async def on_depth_snapshot(message):
            self._queue_message(message, self._snapshot_messages_queue_key)

        @client.on(CONSTANTS.TRADE_EVENT_TYPE)
        async def on_new_trade(message):
            self._queue_message(message, self._trade_messages_queue_key)

        return client

    def _queue_message(self, message: Any, queue_key: str):
        """
        Queue the message body for the base class's listen_for_* loop, which parses it. Queueing an already parsed
        message would get it parsed twice and dropped.
        """
        body = unwrap_frame(message)
        if body is None:
            self.logger().debug(f"Ignoring CoinDCX stream message without a JSON body: {message!r}")
            return
        self._message_queue[queue_key].put_nowait(body)

    async def _disconnect(self):
        """
        Disconnect the Socket.IO client and clean up resources.
        """
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinDCX order book stream disconnect failed", exc_info=True)
            self._client = None

    async def _subscribe_channels(self, client: socketio.AsyncClient):
        """
        Subscribe to order book and trade channels for all trading pairs.
        """
        for trading_pair in self._trading_pairs:
            coindcx_pair = hb_pair_to_coindcx_pair(trading_pair, ecode=CONSTANTS.ECODE_COINDCX)
            orderbook_channel = f"{coindcx_pair}@orderbook@{CONSTANTS.ORDER_BOOK_DEPTH}"
            trades_channel = f"{coindcx_pair}@trades"
            await client.emit("join", {"channelName": orderbook_channel})
            await asyncio.sleep(0.05)
            await client.emit("join", {"channelName": trades_channel})
            await asyncio.sleep(0.05)

    async def _connected_websocket_assistant(self):
        """
        Placeholder for websocket assistant connection.
        CoinDCX uses Socket.IO instead of traditional WebSocket.
        """
        pass

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Fetch and create an order book snapshot message for the trading pair.
        """
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = CoinDCXOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse and enqueue a trade message from the exchange.
        """
        trading_pair = await self._trading_pair_for(raw_message.get("s"))
        if trading_pair is not None:
            message_queue.put_nowait(CoinDCXOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair}))

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parse and enqueue an order book snapshot message from the stream.
        """
        trading_pair = await self._trading_pair_for(raw_message.get("s"))
        if trading_pair is not None:
            message_queue.put_nowait(CoinDCXOrderBook.snapshot_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair}))

    async def _trading_pair_for(self, symbol: Optional[str]) -> Optional[str]:
        """
        Map a stream message's "s" to a trading pair. It is a pair code ("I-SOL_INR") on trades and a market symbol
        ("SOLINR") on depth messages. Returns None for markets it can't map, with one warning per symbol.
        """
        if not symbol:
            return None
        trading_pair = self._pair_code_to_trading_pair.get(symbol)
        if trading_pair is None:
            try:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except KeyError:
                if symbol not in self._unknown_symbols:
                    self._unknown_symbols.add(symbol)
                    self.logger().warning(f"Ignoring CoinDCX market data for unknown symbol {symbol}.")
                return None
        return trading_pair

    async def _ping_task(self):
        """
        Periodically send ping messages to keep the WebSocket connection alive.
        """
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._client and self._client.connected:
                    try:
                        await self._client.emit("ping", {"data": "Ping message"})
                    except Exception as e:
                        self.logger().debug(f"Error sending ping: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger().debug(f"Ping task error: {e}")
