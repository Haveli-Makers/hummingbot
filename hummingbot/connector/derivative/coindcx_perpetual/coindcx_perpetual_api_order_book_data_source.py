import asyncio
from collections import deque
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_order_book import CoinDCXPerpetualOrderBook
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )

# Key of the internal message that asks the snapshot loop to rebuild one book.
RESYNC_KEY = "resync_trading_pair"


class _BookSync:
    """
    Where one pair's diff stream stands against its snapshot (guide §9).

    Until ``synced``, diffs are held in ``buffer``; once a snapshot is known they
    are released from the first one that goes past it, and from then on each diff
    must chain onto the previous one through ``pu``.
    """

    def __init__(self):
        self.snapshot_id: Optional[int] = None
        self.last_update_id: Optional[int] = None
        self.synced = False
        self.buffer: Deque[Dict[str, Any]] = deque(maxlen=CONSTANTS.MAX_BUFFERED_DIFFS)
        self.resync_pending = False
        self.last_resync_request = 0.0

    def reset_stream(self):
        """The stream restarted: whatever comes next must be re-checked against a snapshot."""
        self.synced = False
        self.last_update_id = None
        self.buffer.clear()


def _payload(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    The event itself. Raw ``/ws`` streams deliver it bare; a combined stream
    would wrap it as ``{"stream": ..., "data": {...}}``. The guide shows neither
    wrapper, so both are accepted.
    """
    data = message.get("data") if "stream" in message else None
    return data if isinstance(data, dict) else message


class CoinDCXPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book, trade and funding feeds from the CoinDCX FAPI.

    One multiplexed public WebSocket carries ``<symbol>@depth`` diffs,
    ``<symbol>@aggTrade`` trades and ``<symbol>@markPrice`` funding updates for
    every pair. The order book follows the guide's local order book protocol: a
    REST snapshot, diffs past its ``lastUpdateId``, and a fresh snapshot whenever
    the ``pu`` chain breaks.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoindcxPerpetualDerivative',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._book_syncs: Dict[str, _BookSync] = {trading_pair: _BookSync() for trading_pair in trading_pairs}
        # The tracker's diff queue, captured so diffs held for a snapshot can be
        # released the moment the snapshot lands rather than on the next diff.
        self._diff_output: Optional[asyncio.Queue] = None

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        return await self._connector.build_funding_info(trading_pair)

    # ---- REST snapshot -------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return await self._connector._api_get(
            path_url=CONSTANTS.DEPTH_PATH_URL,
            params={"symbol": symbol, "limit": CONSTANTS.ORDER_BOOK_SNAPSHOT_LIMIT},
        )

    async def _fetch_snapshot_message(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        return CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            snapshot, self._time(), metadata={"trading_pair": trading_pair})

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_message = await self._fetch_snapshot_message(trading_pair)
        self._on_snapshot(trading_pair, snapshot_message.update_id)
        return snapshot_message

    # ---- WebSocket -------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=web_utils.wss_url(CONSTANTS.PUBLIC_WS_PATH, self._domain),
                         ping_timeout=CONSTANTS.PUBLIC_WS_HEARTBEAT_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            streams = []
            for trading_pair in self._trading_pairs:
                symbol = (await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)).lower()
                streams.extend(symbol + stream for stream in (CONSTANTS.DIFF_DEPTH_STREAM,
                                                              CONSTANTS.AGG_TRADE_STREAM,
                                                              CONSTANTS.MARK_PRICE_STREAM))
            await ws.send(WSJSONRequest({"method": CONSTANTS.SUBSCRIBE_METHOD, "params": streams, "id": 1}))
            self.logger().info(f"Subscribed to CoinDCX order book, trade and mark price streams: {streams}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to CoinDCX public streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = _payload(event_message).get("e")
        if event_type == CONSTANTS.DIFF_EVENT_TYPE:
            return self._diff_messages_queue_key
        if event_type == CONSTANTS.TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key
        if event_type == CONSTANTS.MARK_PRICE_EVENT_TYPE:
            return self._funding_info_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any],
                                                   websocket_assistant: WSAssistant):
        if "error" in event_message:
            self.logger().error(f"CoinDCX public stream rejected a request: {event_message}")
        elif "result" in event_message:
            self.logger().debug(f"CoinDCX public stream acknowledged a request: {event_message}")
        else:
            self.logger().debug(f"Ignoring an unrecognised CoinDCX public stream frame: {event_message}")

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)
        # Diffs sent while disconnected are gone, so every book must be re-checked
        # against a snapshot; the first diff after reconnecting triggers it.
        for book_sync in self._book_syncs.values():
            book_sync.reset_stream()

    # ---- Parsers ---------------------------------------------------------------

    async def _trading_pair_for(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        except KeyError:
            self.logger().debug(f"Ignoring a frame for unknown CoinDCX symbol {symbol}.")
            return None
        return trading_pair if trading_pair in self._book_syncs else None

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        self._diff_output = output
        await super().listen_for_order_book_diffs(ev_loop, output)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = _payload(raw_message)
        trading_pair = await self._trading_pair_for(data.get("s"))
        if trading_pair is None:
            return
        self._diff_output = message_queue
        book_sync = self._book_syncs[trading_pair]

        if not book_sync.synced:
            book_sync.buffer.append(data)
            self._release_buffered(trading_pair)
            return
        if not self._chains(book_sync, data):
            book_sync.buffer.append(data)
            self._request_resync(
                trading_pair, f"diff pu={data.get('pu')} does not follow u={book_sync.last_update_id}")
            return
        self._release(trading_pair, data)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = _payload(raw_message)
        trading_pair = await self._trading_pair_for(data.get("s"))
        if trading_pair is None:
            return
        message_queue.put_nowait(CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            data, metadata={"trading_pair": trading_pair}))

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = _payload(raw_message)
        trading_pair = await self._trading_pair_for(data.get("s"))
        if trading_pair is None:
            return
        message_queue.put_nowait(FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=Decimal(str(data["i"])) if data.get("i") is not None else None,
            mark_price=Decimal(str(data["p"])) if data.get("p") is not None else None,
            next_funding_utc_timestamp=int(float(data["T"]) * 1e-3) if data.get("T") else None,
            rate=Decimal(str(data["r"])) if data.get("r") is not None else None,
        ))

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        The stream sends no snapshots; this queue only carries resync requests from
        the diff parser. Each one rebuilds that pair's book from a fresh REST snapshot.
        """
        trading_pair = raw_message.get(RESYNC_KEY)
        book_sync = self._book_syncs.get(trading_pair)
        if book_sync is None:
            return
        wait = book_sync.last_resync_request + CONSTANTS.MIN_RESYNC_INTERVAL - self._time()
        if wait > 0:
            await self._sleep(wait)
        book_sync.last_resync_request = self._time()
        try:
            snapshot_message = await self._fetch_snapshot_message(trading_pair)
        finally:
            book_sync.resync_pending = False
        # Snapshot first, then the diffs that follow it: the tracker replays recent
        # diffs over a new snapshot, so this order is the cheaper one to apply.
        message_queue.put_nowait(snapshot_message)
        self._on_snapshot(trading_pair, snapshot_message.update_id)

    # ---- Local order book protocol (guide §9) ----------------------------------

    @staticmethod
    def _chains(book_sync: _BookSync, diff: Dict[str, Any]) -> bool:
        if "pu" not in diff or book_sync.last_update_id is None:
            return True
        return int(diff["pu"]) == book_sync.last_update_id

    def _on_snapshot(self, trading_pair: str, update_id: int):
        book_sync = self._book_syncs.get(trading_pair)
        if book_sync is None:
            return
        book_sync.snapshot_id = int(update_id)
        if not book_sync.synced:
            self._release_buffered(trading_pair)

    def _release_buffered(self, trading_pair: str):
        """
        Line the held diffs up with the snapshot and release them: drop the ones it
        already covers, check the first one picks up straight after it, then release
        the rest while the ``pu`` chain holds.
        """
        book_sync = self._book_syncs[trading_pair]
        if book_sync.snapshot_id is None or self._diff_output is None:
            return
        buffer = book_sync.buffer
        while buffer and int(buffer[0]["u"]) <= book_sync.snapshot_id:
            buffer.popleft()
        if not buffer:
            return
        first_update_id = int(buffer[0].get("U", buffer[0]["u"]))
        if first_update_id > book_sync.snapshot_id + 1:
            self._request_resync(
                trading_pair,
                f"the stream resumes at U={first_update_id}, past the snapshot at {book_sync.snapshot_id}")
            return
        book_sync.synced = True
        book_sync.last_update_id = None
        while buffer:
            diff = buffer[0]
            if not self._chains(book_sync, diff):
                self._request_resync(
                    trading_pair, f"held diff pu={diff.get('pu')} does not follow u={book_sync.last_update_id}")
                return
            buffer.popleft()
            self._release(trading_pair, diff)

    def _release(self, trading_pair: str, diff: Dict[str, Any]):
        self._diff_output.put_nowait(CoinDCXPerpetualOrderBook.diff_message_from_exchange(
            diff, timestamp=self._time(), metadata={"trading_pair": trading_pair}))
        self._book_syncs[trading_pair].last_update_id = int(diff["u"])

    def _request_resync(self, trading_pair: str, reason: str):
        book_sync = self._book_syncs[trading_pair]
        book_sync.synced = False
        book_sync.snapshot_id = None
        if book_sync.resync_pending:
            return
        book_sync.resync_pending = True
        self.logger().warning(f"Rebuilding the {trading_pair} order book from a new snapshot: {reason}.")
        self._message_queue[self._snapshot_messages_queue_key].put_nowait({RESYNC_KEY: trading_pair})
