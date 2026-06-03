import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.connector.exchange.zebpay.zebpay_utils import unwrap_data
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

                # ── Open orders update (per trading pair) ──────────────────
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
                        orders = self._extract_orders(orders_resp)
                        if orders:
                            await output.put({"event": "order_update", "data": orders})
                    except Exception as exc:
                        self.logger().warning(f"Zebpay open-orders poll error for {trading_pair}: {exc}")

                await asyncio.sleep(CONSTANTS.USER_STREAM_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in Zebpay user-stream polling. Retrying in 5 s …")
                await asyncio.sleep(5.0)

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
