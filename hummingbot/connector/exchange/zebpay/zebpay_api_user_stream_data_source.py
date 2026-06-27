import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.connector.exchange.zebpay.zebpay_utils import raise_for_status, unwrap_data
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange


class ZebpayAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User-stream data source for Zebpay (spot).

    Zebpay spot provides no WebSocket user-data stream, so this implementation
    polls the private balance and open-orders REST endpoints and emits synthetic
    events that the exchange's ``_user_stream_event_listener`` consumes.
    """

    def __init__(
        self,
        auth: Optional[ZebpayAuth] = None,
        trading_pairs: Optional[List[str]] = None,
        connector: Optional["ZebpayExchange"] = None,
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
        # Exchange order ids of our in-flight orders seen as ACTIVE last poll; used
        # to detect orders that have since settled (and dropped out of the
        # status=ACTIVE response) so we can fetch and emit their terminal state.
        self._active_order_ids: set = set()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue) -> None:
        while True:
            try:
                # ── Balance update ─────────────────────────────────────────
                try:
                    balance_resp = await self._connector._api_get(
                        path_url=CONSTANTS.BALANCE_PATH_URL,
                        is_auth_required=True,
                    )
                    self._last_recv_time = time.time()
                    await output.put({"event": "balance_update", "data": balance_resp})
                except Exception as exc:
                    self.logger().warning(f"Zebpay balance poll error: {exc}")

                # ── Open orders update ─────────────────────────────────────
                active_orders: list = []
                active_ids: set = set()
                for trading_pair in self._trading_pairs:
                    try:
                        try:
                            symbol = await self._connector.exchange_symbol_associated_to_pair(
                                trading_pair=trading_pair
                            )
                        except KeyError:
                            symbol = trading_pair
                        orders_resp = await self._connector._api_get(
                            path_url=CONSTANTS.ORDERS_PATH_URL,
                            params={"symbol": symbol, "status": CONSTANTS.ORDER_STATUS_ACTIVE},
                            is_auth_required=True,
                        )
                        self._last_recv_time = time.time()
                        for order in self._extract_orders(orders_resp):
                            active_orders.append(order)
                            oid = str(order.get("orderId") or order.get("id") or "")
                            if oid:
                                active_ids.add(oid)
                    except Exception as exc:
                        self.logger().warning(f"Zebpay open-orders poll error for {trading_pair}: {exc}")

                # status=ACTIVE hides terminal orders, so a filled/cancelled order
                # simply drops out and would never emit a terminal update via the
                # stream. Fetch the final status of tracked orders that just left the
                # active set so terminal transitions don't wait on the slower base
                # status loop.
                settled = await self._fetch_settled_order_updates(active_ids)

                merged = active_orders + settled
                if merged:
                    await output.put({"event": "order_update", "data": merged})

                await asyncio.sleep(CONSTANTS.USER_STREAM_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in Zebpay user-stream polling. Retrying in 5 s …")
                await asyncio.sleep(5.0)

    async def _fetch_settled_order_updates(self, active_ids: set) -> list:
        """
        Return order dicts for tracked in-flight orders that just left the active
        set (i.e. reached a terminal state). Remembers the current active tracked
        ids as a side effect. Empty when nothing settled or no connector is attached.
        """
        if self._connector is None:
            self._active_order_ids = set(active_ids)
            return []

        tracked_ids = {
            str(o.exchange_order_id)
            for o in self._connector.in_flight_orders.values()
            if o.exchange_order_id
        }
        active_tracked = active_ids & tracked_ids

        settled: list = []
        for oid in self._active_order_ids - active_tracked:
            if oid not in tracked_ids:
                continue  # already settled in a prior cycle and no longer tracked
            try:
                resp = await self._connector._api_get(
                    path_url=CONSTANTS.ORDER_PATH_URL,
                    params={"orderId": oid},
                    is_auth_required=True,
                )
                raise_for_status(resp)
                data = unwrap_data(resp)
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict) and data:
                    settled.append(data)
            except Exception as exc:
                self.logger().warning(f"Zebpay terminal-status poll error for {oid}: {exc}")

        self._active_order_ids = active_tracked
        return settled

    @staticmethod
    def _extract_orders(orders_resp) -> list:
        """
        Normalise the orders response into a flat list of order dicts.
        Zebpay paginates: {"data": {"items": [...], "totalNum": N, ...}}.
        """
        data = unwrap_data(orders_resp)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            items = data.get("items", data.get("orders", []))
            return items if isinstance(items, list) else []
        return []

    async def _subscribe_to_user_stream(self) -> None:
        pass

    async def _unsubscribe_from_user_stream(self) -> None:
        pass
