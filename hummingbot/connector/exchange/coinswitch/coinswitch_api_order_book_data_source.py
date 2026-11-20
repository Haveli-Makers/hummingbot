import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import socketio

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange


class CoinswitchAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    OrderBook data source for CoinSwitch using Socket.IO v4.
    """

    SNAPSHOT_POLL_INTERVAL = 30.0

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinswitchExchange',
                 api_factory: WebAssistantsFactory,
                 exchange: str = CONSTANTS.DEFAULT_EXCHANGE,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._exchange = exchange.lower()
        self._domain = domain
        self._client: Optional[socketio.AsyncClient] = None

    def _time(self) -> float:
        return time.time()

    @property
    def _namespace(self) -> str:
        return CONSTANTS.EXCHANGE_NAMESPACE_MAP.get(self._exchange, f"/{self._exchange}")

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        prices = {}
        for trading_pair in trading_pairs:
            try:
                price = await self._connector._get_last_traded_price(trading_pair=trading_pair)
                if price and price > 0:
                    prices[trading_pair] = price
            except Exception as e:
                self.logger().warning(f"Error getting last traded price for {trading_pair}: {e}")
        return prices

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair.replace("-", "/")
        params = {"exchange": self._exchange, "symbol": symbol}
        return await self._connector._api_get(
            path_url=CONSTANTS.DEPTH_PATH_URL,
            params=params,
            is_auth_required=True,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = self._time()
        snapshot_data = snapshot_response.get("data", {})
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(snapshot_timestamp * 1000),
                "bids": snapshot_data.get("bids", []),
                "asks": snapshot_data.get("asks", []),
            },
            timestamp=snapshot_timestamp,
        )

    def _build_client(self) -> socketio.AsyncClient:
        client = socketio.AsyncClient(logger=False, reconnection=False)
        namespace = self._namespace
        snapshot_queue = self._message_queue[self._snapshot_messages_queue_key]
        trade_queue = self._message_queue[self._trade_messages_queue_key]

        @client.event(namespace=namespace)
        async def connect():
            self.logger().info(f"Connected to CoinSwitch Socket.IO namespace {namespace}")

        @client.event(namespace=namespace)
        async def disconnect():
            self.logger().warning(f"CoinSwitch order book stream disconnected (namespace {namespace})")

        @client.on(CONSTANTS.ORDER_BOOK_EVENT_TYPE, namespace=namespace)
        async def on_order_book(message):
            if isinstance(message, dict) and ("bids" in message or "asks" in message):
                snapshot_queue.put_nowait(message)

        @client.on(CONSTANTS.TRADE_EVENT_TYPE, namespace=namespace)
        async def on_trade(message):
            if isinstance(message, dict):
                trade_queue.put_nowait(message)

        return client

    async def _subscribe_channels(self, client: socketio.AsyncClient):
        """Emit subscribe events for every trading pair after connection."""
        namespace = self._namespace
        for trading_pair in self._trading_pairs:
            try:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except KeyError:
                symbol = trading_pair.replace("-", "/")
            ws_pair = symbol.replace("/", ",")
            subscribe_data = {"event": "subscribe", "pair": ws_pair}
            await client.emit(CONSTANTS.ORDER_BOOK_EVENT_TYPE, subscribe_data, namespace=namespace)
            await asyncio.sleep(0.05)
            await client.emit(CONSTANTS.TRADE_EVENT_TYPE, subscribe_data, namespace=namespace)
            await asyncio.sleep(0.05)

    async def _disconnect(self):
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinSwitch order book stream disconnect failed", exc_info=True)
            self._client = None

    async def _ping_task(self):
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._client and self._client.connected:
                    try:
                        await self._client.emit("ping", {}, namespace=self._namespace)
                    except Exception as e:
                        self.logger().debug(f"Error sending ping: {e}")
        except asyncio.CancelledError:
            pass

    async def listen_for_subscriptions(self):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        raw_resp = await self._request_order_book_snapshot(trading_pair)
                        snapshot_data = raw_resp.get("data", {})
                        try:
                            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                        except KeyError:
                            symbol = trading_pair.replace("-", "/")
                        ws_symbol = symbol.replace("/", ",")
                        raw_msg = {
                            "s": ws_symbol,
                            "bids": snapshot_data.get("bids", []),
                            "asks": snapshot_data.get("asks", []),
                            "timestamp": self._time() * 1000,
                        }
                        self._message_queue[self._snapshot_messages_queue_key].put_nowait(raw_msg)
                    except Exception as e:
                        self.logger().warning(f"Error fetching initial snapshot for {trading_pair}: {e}")

                self._client = self._build_client()
                await self._client.connect(
                    CONSTANTS.WS_URL,
                    namespaces=[self._namespace],
                    socketio_path=CONSTANTS.WS_SPOT_SOCKETIO_PATH,
                    transports=["websocket"],
                )
                await self._subscribe_channels(self._client)
                self.logger().info("Subscribed to CoinSwitch order book and trade channels")

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
                raise
            except Exception:
                self.logger().exception("Unexpected error in order book stream. Retrying in 5 seconds...")
                await asyncio.sleep(5.0)
            finally:
                await self._disconnect()
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    raise

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        """
        Parses incoming snapshot messages from Socket.IO and puts them in the snapshot queue.
        """
        symbol = raw_message.get("s", "")
        trading_pair = None
        if symbol:
            rest_symbol = symbol.replace(",", "/")
            try:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=rest_symbol)
            except Exception:
                pass

        if trading_pair is None:
            return

        timestamp = float(raw_message.get("timestamp", self._time() * 1000)) / 1000.0
        msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(timestamp * 1000),
                "bids": raw_message.get("bids", []),
                "asks": raw_message.get("asks", []),
            },
            timestamp=timestamp,
        )
        message_queue.put_nowait(msg)

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        symbol = raw_message.get("s", "")
        if not symbol:
            return
        rest_symbol = symbol.replace(",", "/")
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=rest_symbol)
        except Exception:
            return

        trade_timestamp = float(raw_message.get("E", self._time() * 1000)) / 1000.0
        msg = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.SELL.value) if raw_message.get("m", False) else float(TradeType.BUY.value),
                "trade_id": raw_message.get("t", ""),
                "update_id": raw_message.get("t", ""),
                "price": raw_message.get("p", ""),
                "amount": raw_message.get("q", ""),
            },
            timestamp=trade_timestamp,
        )
        message_queue.put_nowait(msg)

    async def _parse_order_book_diff_message(self, raw_message: Any, message_queue: asyncio.Queue):
        if isinstance(raw_message, dict) and ("bids" in raw_message or "asks" in raw_message):
            await self._parse_order_book_snapshot_message(raw_message, message_queue)

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("CoinswitchAPIOrderBookDataSource uses Socket.IO directly.")
