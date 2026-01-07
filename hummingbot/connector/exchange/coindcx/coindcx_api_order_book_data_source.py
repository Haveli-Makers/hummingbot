import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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


class CoinDCXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for CoinDCX order book and trade data.
    Uses Socket.IO for real-time updates and REST API for snapshots.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinDCXExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory
        self._client: Optional[socketio.AsyncClient] = None

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Returns the last traded price for each trading pair.
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange.

        CoinDCX uses the format: GET /market_data/orderbook?pair=B-BTC_USDT

        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
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
                trade_queue = self._message_queue[self._trade_messages_queue_key]
                diff_queue = self._message_queue[self._diff_messages_queue_key]

                self._client = self._build_client(trade_queue, diff_queue)
                await self._client.connect(CONSTANTS.SOCKET_IO_URL, transports=["websocket"])

                for trading_pair in self._trading_pairs:
                    coindcx_pair = hb_pair_to_coindcx_pair(trading_pair, ecode=CONSTANTS.ECODE_COINDCX)
                    orderbook_channel = f"{coindcx_pair}@orderbook@20"
                    trades_channel = f"{coindcx_pair}@trades"
                    await self._client.emit("join", {"channelName": orderbook_channel})
                    await asyncio.sleep(0.05)
                    await self._client.emit("join", {"channelName": trades_channel})
                    await asyncio.sleep(0.05)

                self.logger().info("Subscribed to public order book and trade channels")
                await self._client.wait()
            except asyncio.CancelledError:
                await self._disconnect()
                raise
            except Exception:
                self.logger().exception("Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
                await self._disconnect()
                await asyncio.sleep(5.0)
            else:
                await self._disconnect()
                await asyncio.sleep(1.0)

    def _build_client(self, trade_queue: asyncio.Queue, diff_queue: asyncio.Queue) -> socketio.AsyncClient:
        """Build Socket.IO client with event handlers for order book and trades."""
        client = socketio.AsyncClient(
            logger=False,
            reconnection=False,
            ssl_verify=False
        )

        @client.event
        async def connect():
            self.logger().info("Connected to CoinDCX order book stream")

        @client.event
        async def disconnect():
            self.logger().warning("CoinDCX order book stream disconnected")

        @client.on(CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE)
        async def on_depth_snapshot(message):
            self.logger().debug(f"Received depth-snapshot: {type(message)}")
            if isinstance(message, dict) and ("bids" in message or "asks" in message):
                await self._parse_order_book_diff_message(message, diff_queue)

        @client.on(CONSTANTS.DIFF_EVENT_TYPE)
        async def on_depth_update(message):
            self.logger().debug(f"Received depth-update: {type(message)}")
            if isinstance(message, dict) and ("bids" in message or "asks" in message):
                await self._parse_order_book_diff_message(message, diff_queue)

        @client.on(CONSTANTS.TRADE_EVENT_TYPE)
        async def on_new_trade(message):
            self.logger().debug(f"Received new-trade: {type(message)}")
            if isinstance(message, dict) and "p" in message and "q" in message:
                await self._parse_trade_message(message, trade_queue)

        return client

    async def _disconnect(self):
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinDCX order book stream disconnect failed", exc_info=True)
            self._client = None

    async def _subscribe_channels(self, ws):
        """
        Deprecated - subscriptions now handled in listen_for_subscriptions via Socket.IO emit.
        Kept for compatibility.
        """
        pass

    async def _connected_websocket_assistant(self):
        """
        Deprecated - Socket.IO connection now handled directly in listen_for_subscriptions.
        Kept for compatibility.
        """
        pass

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves and returns the order book snapshot as an OrderBookMessage.
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
        Parses a trade message from CoinDCX Socket.IO and adds it to the message queue.
        """
        self.logger().debug(f"Received trade message: {raw_message}")
        pair_symbol = raw_message.get("s", "")
        if pair_symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=pair_symbol)
            trade_message = CoinDCXOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses an order book diff message from CoinDCX Socket.IO and adds it to the message queue.
        """
        self.logger().debug(f"Received orderbook message: {raw_message}")
        if "bids" in raw_message or "asks" in raw_message:
            trading_pair = None

            pair_symbol = raw_message.get("s") or raw_message.get("symbol") or ""
            if pair_symbol:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=pair_symbol)
            else:
                channel = raw_message.get("channel", "")
                if "@orderbook" in channel:
                    pair_part = channel.split("@")[0]
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=pair_part)

            if trading_pair:
                order_book_message: OrderBookMessage = CoinDCXOrderBook.diff_message_from_exchange(
                    raw_message, time.time(), {"trading_pair": trading_pair})
                message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determines which channel a message originated from based on its content.
        """
        channel = ""

        if "p" in event_message and "q" in event_message and "T" in event_message:
            channel = self._trade_messages_queue_key
        elif "bids" in event_message or "asks" in event_message:
            channel = self._diff_messages_queue_key

        return channel
