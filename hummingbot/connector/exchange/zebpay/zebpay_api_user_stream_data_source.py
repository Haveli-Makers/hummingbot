import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.connector.exchange.zebpay.zebpay_utils import raise_for_status, unwrap_data
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
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
        """
        Zebpay has no WebSocket, so account data is kept realtime by polling each
        data type on its OWN cadence in concurrent loops — balance, active orders,
        and account trades (fills) — so a slow data type never delays a fast one.
        """
        await safe_gather(
            self._poll_forever(self._poll_balance, output, CONSTANTS.BALANCE_POLL_INTERVAL, "balance"),
            self._poll_forever(self._poll_active_orders, output, CONSTANTS.ACTIVE_ORDERS_POLL_INTERVAL, "active-orders"),
            self._poll_forever(self._poll_account_trades, output, CONSTANTS.ACCOUNT_TRADES_POLL_INTERVAL, "account-trades"),
        )

    async def _poll_forever(self, poll_coro, output: asyncio.Queue, interval: float, label: str) -> None:
        """
        Run a single data-type poll on a fixed cadence; isolate its failures.

        On repeated failures the cadence backs off exponentially (up to
        ``MAX_POLL_BACKOFF_INTERVAL``) and resets on the first success. These loops
        run independently, so without a backoff a sustained auth/connectivity outage
        would have all three of them retrying every 2-3s each — multiplying request
        volume at exactly the moment the exchange is already unhealthy.
        """
        consecutive_failures = 0
        while True:
            try:
                await poll_coro(output)
                consecutive_failures = 0
                delay = interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                delay = min(interval * (2 ** consecutive_failures), CONSTANTS.MAX_POLL_BACKOFF_INTERVAL)
                self.logger().warning(
                    f"Zebpay {label} poll error (attempt {consecutive_failures}, "
                    f"retrying in {delay:.0f}s): {exc}"
                )
            await asyncio.sleep(delay)

    async def _poll_balance(self, output: asyncio.Queue) -> None:
        balance_resp = await self._connector._api_get(
            path_url=CONSTANTS.BALANCE_PATH_URL,
            is_auth_required=True,
        )
        self._last_recv_time = time.time()
        await output.put({"event": "balance_update", "data": balance_resp})

    async def _poll_active_orders(self, output: asyncio.Queue) -> None:
        active_orders: list = []
        active_ids: set = set()

        # One request per tracked pair, fired TOGETHER rather than in sequence: at a
        # 2s cadence a serial loop costs pairs x latency and falls behind its own
        # interval as more pairs are tracked. Per-pair failures are isolated.
        responses = await safe_gather(
            *(self._fetch_active_orders_for_pair(tp) for tp in self._trading_pairs),
            return_exceptions=True,
        )
        for trading_pair, orders_resp in zip(self._trading_pairs, responses):
            if isinstance(orders_resp, Exception):
                self.logger().warning(f"Zebpay open-orders poll error for {trading_pair}: {orders_resp}")
                continue
            self._last_recv_time = time.time()
            for order in self._extract_orders(orders_resp):
                active_orders.append(order)
                oid = str(order.get("orderId") or order.get("id") or "")
                if oid:
                    active_ids.add(oid)

        # status=ACTIVE hides terminal orders, so a filled/cancelled order simply
        # drops out and would never emit a terminal update via the stream. Fetch the
        # final status of tracked orders that just left the active set.
        settled = await self._fetch_settled_order_updates(active_ids)

        merged = active_orders + settled
        if merged:
            await output.put({"event": "order_update", "data": merged})

    async def _fetch_active_orders_for_pair(self, trading_pair: str):
        """GET the ACTIVE orders for one pair. Raises on failure (caller isolates)."""
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair
        return await self._connector._api_get(
            path_url=CONSTANTS.ORDERS_PATH_URL,
            params={"symbol": symbol, "status": CONSTANTS.ORDER_STATUS_ACTIVE},
            is_auth_required=True,
        )

    async def _poll_account_trades(self, output: asyncio.Queue) -> None:
        """
        Realtime account fills: poll each tracked in-flight order's fills and emit a
        ``trade_update`` event. Every Zebpay fill carries a unique id, so
        InFlightOrder.update_with_trade_update dedupes repeats — re-emitting a fill is
        harmless, and this surfaces fills far faster than the base ~10s status loop.
        """
        if self._connector is None:
            return
        orders = [o for o in self._connector.in_flight_orders.values() if o.exchange_order_id]
        if not orders:
            return

        # Fire the per-order requests TOGETHER. Sequentially this is N x latency per
        # cycle (worse through the IP-whitelist proxy), so past a handful of orders the
        # loop falls behind its own 2s cadence; the connector throttler still bounds
        # the actual request rate. Per-order failures are isolated.
        responses = await safe_gather(
            *(
                self._connector._api_get(
                    path_url=CONSTANTS.ORDER_FILLS_PATH_URL,
                    params={"orderId": order.exchange_order_id},
                    is_auth_required=True,
                )
                for order in orders
            ),
            return_exceptions=True,
        )

        fills_by_order: list = []
        for order, resp in zip(orders, responses):
            try:
                if isinstance(resp, Exception):
                    raise resp
                raise_for_status(resp)
                data = unwrap_data(resp)
                fills = data.get("fills", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                if fills:
                    fills_by_order.append({"orderId": str(order.exchange_order_id), "fills": fills})
            except Exception as exc:
                self.logger().warning(f"Zebpay account-trades poll error for {order.exchange_order_id}: {exc}")

        if fills_by_order:
            self._last_recv_time = time.time()
            await output.put({"event": "trade_update", "data": fills_by_order})

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
