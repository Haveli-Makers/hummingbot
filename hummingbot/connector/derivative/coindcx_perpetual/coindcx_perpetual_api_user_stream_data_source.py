import asyncio
import json
from typing import TYPE_CHECKING, Any, List, Optional

import socketio

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_order_book_data_source import unwrap_frame
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_auth import CoinDCXPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


class CoinDCXPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private account stream for CoinDCX futures.

    Joins the authenticated ``coindcx`` Socket.IO channel and republishes the
    ``df-order-update``, ``df-position-update`` and ``balance-update`` events
    onto the connector's user-stream queue, tagged with their event name so the
    connector can dispatch them.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinDCXPerpetualAuth,
                 trading_pairs: List[str],
                 connector: 'CoindcxPerpetualDerivative',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._client: Optional[socketio.AsyncClient] = None
        self._last_recv_time: float = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                self._client = self._build_client(output)
                await self._client.connect(web_utils.wss_url(self._domain), transports=["websocket"])
                await self._client.emit("join", self._auth.generate_ws_auth_payload())
                self.logger().info("Subscribed to CoinDCX futures private account channel.")
                self._last_recv_time = self._time()

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
                self.logger().exception(
                    "Unexpected error while listening to user stream. Retrying in 5 seconds...")
                await self._disconnect()
                await self._sleep(5.0)
            finally:
                await self._disconnect()

    def _build_client(self, output: asyncio.Queue) -> socketio.AsyncClient:
        client = socketio.AsyncClient(logger=False, reconnection=False)

        @client.event
        async def connect():
            self.logger().info("Connected to CoinDCX futures private stream.")

        @client.event
        async def disconnect():
            self.logger().warning("CoinDCX futures private stream disconnected.")

        def _handler(event_type: str):
            async def _on_event(message):
                self._last_recv_time = self._time()
                # Private events deliver a LIST of records, so unwrap_frame (dict
                # only) is tried first and _unwrap_payload covers the list case.
                payload = unwrap_frame(message)
                if payload is None:
                    payload = self._unwrap_payload(message)
                if payload is not None:
                    output.put_nowait({"event": event_type, "data": payload})
            return _on_event

        client.on(CONSTANTS.ORDER_UPDATE_EVENT_TYPE, _handler(CONSTANTS.ORDER_UPDATE_EVENT_TYPE))
        client.on(CONSTANTS.POSITION_UPDATE_EVENT_TYPE, _handler(CONSTANTS.POSITION_UPDATE_EVENT_TYPE))
        client.on(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE, _handler(CONSTANTS.BALANCE_UPDATE_EVENT_TYPE))
        return client

    @staticmethod
    def _unwrap_payload(message: Any) -> Optional[Any]:
        """
        Private events carry a JSON-encoded LIST of records in ``data``.
        """
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except (TypeError, ValueError):
                return None
        if isinstance(message, list):
            return message
        if isinstance(message, dict):
            data = message.get("data")
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (TypeError, ValueError):
                    return None
            if isinstance(data, (list, dict)):
                return data
        return None

    async def _disconnect(self):
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinDCX futures private stream disconnect failed", exc_info=True)
            self._client = None

    async def _ping_task(self):
        try:
            while True:
                await self._sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._client is not None and self._client.connected:
                    try:
                        await self._client.emit("ping", {"data": "Ping message"})
                    except Exception as exception:
                        self.logger().debug(f"Error sending ping: {exception}")
        except asyncio.CancelledError:
            pass

    async def _connected_websocket_assistant(self):
        """CoinDCX uses Socket.IO; the connection is managed in listen_for_user_stream."""
        pass

    async def _subscribe_channels(self, websocket_assistant):
        """Subscription happens via the authenticated ``join`` emit."""
        pass
