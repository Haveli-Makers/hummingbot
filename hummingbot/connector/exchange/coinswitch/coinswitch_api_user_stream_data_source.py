import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

import socketio

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.connector.exchange.coinswitch.coinswitch_auth import CoinswitchAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange


class CoinswitchAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for CoinSwitch using Socket.IO v4.
    """

    def __init__(self,
                 auth: Optional[CoinswitchAuth] = None,
                 trading_pairs: Optional[List[str]] = None,
                 connector: Optional['CoinswitchExchange'] = None,
                 api_factory: Optional[WebAssistantsFactory] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_recv_time = 0.0
        self._order_client: Optional[socketio.AsyncClient] = None
        self._balance_client: Optional[socketio.AsyncClient] = None

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def _build_order_client(self, output: asyncio.Queue) -> socketio.AsyncClient:
        client = socketio.AsyncClient(logger=False, reconnection=False)
        api_key = self._auth.api_key if self._auth else ""
        namespace = CONSTANTS.WS_ORDER_UPDATES_NAMESPACE

        @client.event(namespace=namespace)
        async def connect():
            self.logger().info("CoinSwitch order-updates stream connected")
            await client.emit(
                CONSTANTS.ORDER_UPDATE_EVENT_TYPE,
                {"apikey": api_key, "event": "subscribe"},
                namespace=namespace,
            )
            self._last_recv_time = time.time()

        @client.event(namespace=namespace)
        async def disconnect():
            self.logger().warning("CoinSwitch order-updates stream disconnected")

        @client.on(CONSTANTS.ORDER_UPDATE_EVENT_TYPE, namespace=namespace)
        async def on_order_update(message):
            self._last_recv_time = time.time()
            if isinstance(message, dict):
                message["event"] = CONSTANTS.ORDER_UPDATE_EVENT_TYPE
            await output.put(message)

        @client.on("error", namespace=namespace)
        async def on_error(message):
            self.logger().warning(f"CoinSwitch order-updates error: {message}")

        return client

    def _build_balance_client(self, output: asyncio.Queue) -> socketio.AsyncClient:
        client = socketio.AsyncClient(logger=False, reconnection=False)
        api_key = self._auth.api_key if self._auth else ""
        namespace = CONSTANTS.WS_BALANCE_UPDATES_NAMESPACE

        @client.event(namespace=namespace)
        async def connect():
            self.logger().info("CoinSwitch balance-updates stream connected")
            await client.emit(
                CONSTANTS.BALANCE_UPDATE_EVENT_TYPE,
                {"apikey": api_key, "event": "subscribe"},
                namespace=namespace,
            )
            self._last_recv_time = time.time()

        @client.event(namespace=namespace)
        async def disconnect():
            self.logger().warning("CoinSwitch balance-updates stream disconnected")

        @client.on(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE, namespace=namespace)
        async def on_balance_update(message):
            self._last_recv_time = time.time()
            if isinstance(message, dict):
                message["event"] = CONSTANTS.BALANCE_UPDATE_EVENT_TYPE
            await output.put(message)

        @client.on("error", namespace=namespace)
        async def on_error(message):
            self.logger().warning(f"CoinSwitch balance-updates error: {message}")

        return client

    async def _disconnect_all(self):
        for attr in ("_order_client", "_balance_client"):
            client = getattr(self, attr, None)
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    self.logger().debug(f"Error disconnecting {attr}", exc_info=True)
                setattr(self, attr, None)

    async def _ping_task(self):
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                for client, namespace in [
                    (self._order_client, CONSTANTS.WS_ORDER_UPDATES_NAMESPACE),
                    (self._balance_client, CONSTANTS.WS_BALANCE_UPDATES_NAMESPACE),
                ]:
                    if client and client.connected:
                        try:
                            await client.emit("ping", {}, namespace=namespace)
                        except Exception as e:
                            self.logger().debug(f"Error sending ping: {e}")
        except asyncio.CancelledError:
            pass

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        while True:
            try:
                self._order_client = self._build_order_client(output)
                self._balance_client = self._build_balance_client(output)

                await self._order_client.connect(
                    CONSTANTS.WS_URL,
                    namespaces=[CONSTANTS.WS_ORDER_UPDATES_NAMESPACE],
                    socketio_path=CONSTANTS.WS_ORDER_UPDATES_SOCKETIO_PATH,
                    transports=["websocket"],
                )
                await self._balance_client.connect(
                    CONSTANTS.WS_URL,
                    namespaces=[CONSTANTS.WS_BALANCE_UPDATES_NAMESPACE],
                    socketio_path=CONSTANTS.WS_BALANCE_UPDATES_SOCKETIO_PATH,
                    transports=["websocket"],
                )
                self.logger().info("CoinSwitch user stream connections established")

                ping_task = asyncio.create_task(self._ping_task())
                try:
                    await asyncio.gather(
                        self._order_client.wait(),
                        self._balance_client.wait(),
                    )
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream. Retrying in 5 seconds...")
                await asyncio.sleep(5.0)
            finally:
                await self._disconnect_all()
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    raise

    async def _subscribe_to_user_stream(self) -> None:
        pass

    async def _unsubscribe_from_user_stream(self) -> None:
        pass
