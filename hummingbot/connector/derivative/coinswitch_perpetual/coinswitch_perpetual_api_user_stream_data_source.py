import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional

from hummingbot.connector.derivative.coinswitch_perpetual import coinswitch_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_auth import CoinswitchPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
        CoinswitchPerpetualDerivative,
    )

BALANCE_CHANNEL = "balance"
POSITIONS_CHANNEL = "positions"
ORDERS_CHANNEL = "orders"


class CoinswitchPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Account updates for CoinSwitch PRO futures, by REST polling.

    The futures sockets are market data only ("user-data sockets aren't
    available on Futures today"), so wallet, positions and orders are polled,
    each on its own cadence in its own loop, and pushed to the connector as
    events:

    * ``{"channel": "balance", "data": <wallet_balance response>}``
    * ``{"channel": "positions", "trading_pair": ..., "data": <positions response>}``
    * ``{"channel": "orders", "data": [{"order": <order>, "working": bool | None}]}``

    The cadences keep each loop at ~60-75% of its endpoint's 20-per-minute
    budget. Every successful poll refreshes ``last_recv_time``, which keeps the
    base class's own status poll at its slow (2 minute) interval.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: CoinswitchPerpetualAuth,
                 trading_pairs: List[str],
                 connector: "CoinswitchPerpetualDerivative",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_recv_time: float = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        await safe_gather(
            self._poll_forever(self._poll_balance, output, lambda: CONSTANTS.BALANCE_POLL_INTERVAL, "wallet"),
            self._poll_forever(self._poll_positions, output, self._positions_poll_interval, "positions"),
            self._poll_forever(self._poll_orders, output, lambda: CONSTANTS.OPEN_ORDERS_POLL_INTERVAL, "orders"),
        )

    def _positions_poll_interval(self) -> float:
        """One positions call per pair per cycle, held at ``POSITIONS_CALLS_PER_MINUTE``."""
        per_cycle = max(len(self._trading_pairs), 1)
        return max(CONSTANTS.MIN_POSITIONS_POLL_INTERVAL, 60.0 * per_cycle / CONSTANTS.POSITIONS_CALLS_PER_MINUTE)

    async def _poll_forever(self,
                            poll: Callable[[asyncio.Queue], Awaitable[None]],
                            output: asyncio.Queue,
                            interval: Callable[[], float],
                            label: str):
        """
        Runs one poll on its cadence. Consecutive failures back off exponentially
        (capped at ``MAX_POLL_BACKOFF_INTERVAL``) so an outage does not have every
        loop retrying at full speed; the first success resets the cadence.
        """
        consecutive_failures = 0
        while True:
            try:
                await poll(output)
                consecutive_failures = 0
                delay = interval()
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                consecutive_failures += 1
                delay = min(interval() * (2 ** consecutive_failures), CONSTANTS.MAX_POLL_BACKOFF_INTERVAL)
                self.logger().warning(
                    f"CoinSwitch futures {label} poll failed (attempt {consecutive_failures}, "
                    f"retrying in {delay:.0f}s): {exception}")
            await self._sleep(delay)

    async def _poll_balance(self, output: asyncio.Queue):
        response = await self._connector._request_wallet_balance()
        self._last_recv_time = self._time()
        output.put_nowait({"channel": BALANCE_CHANNEL, "data": response})

    async def _poll_positions(self, output: asyncio.Queue):
        for trading_pair in self._trading_pairs:
            response = await self._connector._request_positions(trading_pair)
            self._last_recv_time = self._time()
            output.put_nowait({"channel": POSITIONS_CHANNEL, "trading_pair": trading_pair, "data": response})

    async def _poll_orders(self, output: asyncio.Queue):
        """Skipped while nothing is in flight, so an idle bot spends none of the order budgets."""
        orders = [order for order in self._connector.in_flight_orders.values() if order.exchange_order_id]
        if not orders:
            return
        snapshots = await self._connector._fetch_order_snapshots(orders)
        self._last_recv_time = self._time()
        if snapshots:
            output.put_nowait({
                "channel": ORDERS_CHANNEL,
                "data": [{"order": order, "working": working} for order, working in snapshots],
            })

    # REST polling only: the websocket hooks of the base class are never used.
    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("CoinSwitch futures has no private socket; account data is polled.")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        raise NotImplementedError("CoinSwitch futures has no private socket; account data is polled.")

    async def _process_event_message(self, event_message: Any, queue: asyncio.Queue):
        pass
