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
                # CSX requires both `onlyOpen` and `type` query parameters.
                # The connector only places LIMIT orders, so we filter on LIMIT.
                try:
                    orders_resp = await self._connector._api_get(
                        path_url=CONSTANTS.ME_ORDERS_PATH_URL,
                        params={"onlyOpen": "true", "type": CONSTANTS.ORDER_TYPE_LIMIT},
                        is_auth_required=True,
                    )
                    self._last_recv_time = time.time()
                    orders = self._extract_orders(orders_resp)
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

    @staticmethod
    def _extract_orders(orders_resp) -> list:
        """
        Normalise the open-orders response into a flat list of order dicts.

        CSX wraps payloads in "data"; the orders may sit directly in that list
        or under a nested "orders" key:
          [{...}, {...}]                          → as-is
          {"data": [{...}, ...]}                   → data
          {"data": {"orders": [{...}, ...]}}       → data.orders
        """
        if isinstance(orders_resp, list):
            return orders_resp
        if not isinstance(orders_resp, dict):
            return []
        data = orders_resp.get("data", orders_resp)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            orders = data.get("orders", data.get("data", []))
            return orders if isinstance(orders, list) else []
        return []

    # These are no-ops for REST-polling sources
    async def _subscribe_to_user_stream(self) -> None:
        pass

    async def _unsubscribe_from_user_stream(self) -> None:
        pass
