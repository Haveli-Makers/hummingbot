import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS
from hummingbot.connector.exchange.csx.csx_auth import CsxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange


class CsxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User-stream data source for CoinSwitch Kuber (CSX).

    CSX does not provide a WebSocket user-data stream, so this implementation
    polls the private balance and open-orders REST endpoints and emits synthetic
    events that the exchange's ``_user_stream_event_listener`` consumes.
    """

    def __init__(
        self,
        auth: Optional[CsxAuth] = None,
        trading_pairs: Optional[List[str]] = None,
        connector: Optional["CsxExchange"] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs or []
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_recv_time: float = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        """
        Continuously polls balance and open-orders endpoints and pushes
        synthetic events into *output* so the exchange can react quickly to
        fills and balance changes without waiting for the background polling
        interval.
        """
        while True:
            try:
                # ── Balance update ─────────────────────────────────────────
                try:
                    balance_resp = await self._connector._api_get(
                        path_url=CONSTANTS.BALANCE_V2_PATH_URL,
                        is_auth_required=True,
                    )
                    self._last_recv_time = time.time()
                    await output.put({"event": "balance_update", "data": balance_resp})
                except Exception as exc:
                    self.logger().warning(f"CSX balance poll error: {exc}")

                # ── Open orders update ─────────────────────────────────────
                try:
                    orders_resp = await self._connector._api_get(
                        path_url=CONSTANTS.ME_ORDERS_PATH_URL,
                        params={"status": "in:OPEN,PARTIALLY_FILLED"},
                        is_auth_required=True,
                    )
                    self._last_recv_time = time.time()
                    orders = (
                        orders_resp
                        if isinstance(orders_resp, list)
                        else orders_resp.get("orders", orders_resp.get("data", []))
                    )
                    if orders:
                        await output.put({"event": "order_update", "data": orders})
                except Exception as exc:
                    self.logger().warning(f"CSX open-orders poll error: {exc}")

                await asyncio.sleep(CONSTANTS.USER_STREAM_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error in CSX user-stream polling. Retrying in 5 s …"
                )
                await asyncio.sleep(5.0)

    # These are no-ops for REST-polling sources
    async def _subscribe_to_user_stream(self) -> None:
        pass

    async def _unsubscribe_from_user_stream(self) -> None:
        pass
