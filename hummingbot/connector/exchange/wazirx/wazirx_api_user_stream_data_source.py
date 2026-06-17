import asyncio
import json
import time
from typing import Any, List, Optional

import aiohttp

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class WazirxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for WazirX using the account WebSocket.

    WazirX user-data streams (per https://docs.wazirx.com) work like this:
      1. Obtain an ``auth_key`` from the REST create_auth_token endpoint.
      2. Open a websocket to ``wss://stream.wazirx.com/stream``.
      3. Send a subscribe frame that carries the auth_key:
         ``{"event": "subscribe", "streams": [...], "auth_key": "<key>"}``.
      4. The server pushes messages shaped ``{"data": {...}, "stream": "<name>"}``.

    Subscribed streams:
      • outboundAccountPosition — balance snapshots (data.B = [{a, b, l}, ...])
      • orderUpdate            — order state changes
      • ownTrade               — your own fills

    Each pushed message is forwarded verbatim to the connector's
    ``_user_stream_event_listener``, which routes on the ``stream`` field.

    NOTE: the previous implementation was a REST poller that only fetched open
    orders (unauthenticated) and never balances/fills, so the in-memory balance
    cache was never updated by the user stream. This is the real account WS.
    """

    SUBSCRIBE_STREAMS = ["outboundAccountPosition", "orderUpdate", "ownTrade"]

    def __init__(self, auth: Any, trading_pairs: Optional[List[str]] = None,
                 connector: Optional[ExchangePyBase] = None, api_factory=None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        while True:
            session: Optional[aiohttp.ClientSession] = None
            ping_task: Optional[asyncio.Task] = None
            try:
                # auth_key is required on the subscribe frame for private streams.
                auth_key = await self._auth.get_ws_auth_key()

                session = aiohttp.ClientSession()
                # heartbeat=None: WazirX expects an application-level ping
                # ({"event": "ping"}) rather than a websocket-protocol ping.
                self._ws = await session.ws_connect(CONSTANTS.WSS_URL, heartbeat=None)
                self._last_recv_time = time.time()

                await self._ws.send_json({
                    "event": "subscribe",
                    "streams": self.SUBSCRIBE_STREAMS,
                    "auth_key": auth_key,
                })
                self.logger().info(
                    "WazirX user stream subscribed "
                    "(outboundAccountPosition, orderUpdate, ownTrade)."
                )

                ping_task = asyncio.create_task(self._ping_loop())

                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._last_recv_time = time.time()
                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue
                        # Forward only stream data; ignore subscribe/unsubscribe
                        # acks, pongs and errors (those carry "event", no "stream").
                        if isinstance(data, dict) and data.get("stream"):
                            output.put_nowait(data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Error in WazirX user stream. Reconnecting in 5s.", exc_info=True
                )
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                if self._ws is not None:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None
                if session is not None:
                    await session.close()
                await self._sleep(5.0)

    async def _ping_loop(self):
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._ws is not None and not self._ws.closed:
                    await self._ws.send_json({"event": "ping"})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def stop(self):
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        await super().stop()
