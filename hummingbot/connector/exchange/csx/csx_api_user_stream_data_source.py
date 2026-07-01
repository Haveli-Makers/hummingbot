import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS
from hummingbot.connector.exchange.csx.csx_auth import CsxAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
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
        # Exchange order ids of our in-flight orders seen as OPEN last poll; used
        # to detect orders that have since settled (and dropped out of the
        # onlyOpen=true response) so we can emit their terminal state.
        self._open_order_ids: set = set()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        """
        CSX has no WebSocket, so account data is kept realtime by polling each data
        type on its OWN cadence in concurrent loops — balance, active orders, and
        account trades (cumulative fills) — so a slow data type never delays a fast
        one.
        """
        await safe_gather(
            self._poll_forever(self._poll_balance, output, CONSTANTS.BALANCE_POLL_INTERVAL, "balance"),
            self._poll_forever(self._poll_active_orders, output, CONSTANTS.ACTIVE_ORDERS_POLL_INTERVAL, "active-orders"),
            self._poll_forever(self._poll_account_trades, output, CONSTANTS.ACCOUNT_TRADES_POLL_INTERVAL, "account-trades"),
        )

    async def _poll_forever(self, poll_coro, output: asyncio.Queue, interval: float, label: str) -> None:
        """Run a single data-type poll on a fixed cadence; isolate its failures."""
        while True:
            try:
                await poll_coro(output)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger().warning(f"CSX {label} poll error: {exc}")
            await asyncio.sleep(interval)

    async def _poll_balance(self, output: asyncio.Queue) -> None:
        balance_resp = await self._connector._api_get(
            path_url=CONSTANTS.BALANCE_V2_PATH_URL,
            is_auth_required=True,
        )
        self._last_recv_time = time.time()
        await output.put({"event": "balance_update", "data": balance_resp})

    async def _poll_active_orders(self, output: asyncio.Queue) -> None:
        # CSX requires both `onlyOpen` and `type` query parameters. The connector
        # only places LIMIT orders, so we filter on LIMIT.
        orders_resp = await self._connector._api_get(
            path_url=CONSTANTS.ME_ORDERS_PATH_URL,
            params={"onlyOpen": "true", "type": CONSTANTS.ORDER_TYPE_LIMIT},
            is_auth_required=True,
        )
        self._last_recv_time = time.time()
        orders = self._extract_orders(orders_resp)

        # `onlyOpen=true` hides an order the moment it reaches a terminal state, so a
        # fill/cancel would never surface a terminal order_update via the stream.
        # Fetch the final status of any tracked order that just left the open set.
        orders.extend(await self._fetch_settled_order_updates(orders))

        if orders:
            await output.put({"event": "order_update", "data": orders})

    async def _poll_account_trades(self, output: asyncio.Queue) -> None:
        """
        Realtime account fills: poll each tracked in-flight order's status (which
        carries the cumulative ``filledQuantity``) and emit a ``trade_update`` event.
        The exchange turns each into the incremental fill since the last poll, keyed
        by a trade id unique to the cumulative value, so re-emitting is deduped —
        this surfaces fills far faster than the base ~10s status loop.
        """
        if self._connector is None:
            return
        orders = [o for o in self._connector.in_flight_orders.values() if o.exchange_order_id]
        order_data_list: list = []
        for order in orders:
            try:
                resp = await self._connector._api_get(
                    path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{order.exchange_order_id}",
                    limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
                    is_auth_required=True,
                )
                data = resp if isinstance(resp, dict) and "orderId" in resp else (
                    resp.get("data", resp) if isinstance(resp, dict) else {}
                )
                if isinstance(data, dict) and data:
                    order_data_list.append(data)
            except Exception as exc:
                self.logger().warning(f"CSX account-trades poll error for {order.exchange_order_id}: {exc}")

        if order_data_list:
            self._last_recv_time = time.time()
            await output.put({"event": "trade_update", "data": order_data_list})

    async def _fetch_settled_order_updates(self, open_orders: list) -> list:
        """
        Return order dicts for tracked in-flight orders that just left the open
        set (i.e. settled to a terminal state). Remembers the current open-order
        ids as a side effect. Empty when nothing settled or no connector attached.
        """
        if self._connector is None:
            return []

        tracked_ids = {
            str(o.exchange_order_id)
            for o in self._connector.in_flight_orders.values()
            if o.exchange_order_id
        }
        current_open_ids = {
            str(order.get("orderId") or order.get("order_id") or "")
            for order in open_orders
        }
        current_open_ids.discard("")
        current_open_ids &= tracked_ids  # only follow orders we actually own

        settled: list = []
        for oid in self._open_order_ids - current_open_ids:
            if oid not in tracked_ids:
                continue  # already settled in a prior cycle and no longer tracked
            try:
                resp = await self._connector._api_get(
                    path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{oid}",
                    limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
                    is_auth_required=True,
                )
                data = resp if isinstance(resp, dict) and "orderId" in resp else (
                    resp.get("data", resp) if isinstance(resp, dict) else {}
                )
                if isinstance(data, dict) and data:
                    settled.append(data)
            except Exception as exc:
                self.logger().warning(f"CSX terminal-status poll error for {oid}: {exc}")

        self._open_order_ids = current_open_ids
        return settled

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
