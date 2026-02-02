import asyncio
from typing import TYPE_CHECKING, List, Optional

import socketio

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoinDCXExchange


class CoinDCXAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    CoinDCX user stream uses Socket.IO.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinDCXAuth,
                 trading_pairs: List[str],
                 connector: 'CoinDCXExchange',
                 api_factory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: CoinDCXAuth = auth
        self._domain = domain
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs = trading_pairs
        self._client: Optional[socketio.AsyncClient] = None
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                self._client = self._build_client(output)
                await self._client.connect(CONSTANTS.WSS_URL, transports=["websocket"])
                self.logger().info("CoinDCX user stream connected successfully")
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
                await self._disconnect()
                await self._sleep(5.0)
            else:
                await self._disconnect()
                await self._sleep(1.0)

    def _build_client(self, output: asyncio.Queue) -> socketio.AsyncClient:
        client = socketio.AsyncClient(
            logger=False,
            reconnection=False
        )
        auth_payload = self._auth.generate_ws_auth_payload()

        @client.event
        async def connect():
            await client.emit("join", auth_payload)
            self._last_recv_time = self._time()

        @client.event
        async def disconnect():
            self.logger().warning("CoinDCX user stream disconnected")

        @client.on(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE)
        async def on_balance(message):
            await self._handle_message(message, CONSTANTS.BALANCE_UPDATE_EVENT_TYPE, output)

        @client.on(CONSTANTS.ORDER_UPDATE_EVENT_TYPE)
        async def on_order(message):
            await self._handle_message(message, CONSTANTS.ORDER_UPDATE_EVENT_TYPE, output)

        @client.on(CONSTANTS.TRADE_UPDATE_EVENT_TYPE)
        async def on_trade(message):
            await self._handle_message(message, CONSTANTS.TRADE_UPDATE_EVENT_TYPE, output)

        @client.on("error")
        async def on_error(message):
            self.logger().warning(f"CoinDCX user stream error: {message}")

        return client

    async def _handle_message(self, message, event_type: str, output: asyncio.Queue):
        self._last_recv_time = self._time()
        if isinstance(message, dict):
            message["event"] = event_type
        await output.put(message)

    async def _ping_task(self):
        try:
            while True:
                await self._sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._client and self._client.connected:
                    try:
                        await self._client.emit("ping", {"data": "Ping message"})
                    except Exception as e:
                        self.logger().debug(f"Error sending ping: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger().debug(f"Ping task error: {e}")

    async def _disconnect(self):
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinDCX user stream disconnect failed", exc_info=True)
            self._client = None

    async def stop(self):
        await self._disconnect()
        await super().stop()
