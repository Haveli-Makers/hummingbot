import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import socketio

from hummingbot.connector.derivative.coinswitch_perpetual import (
    coinswitch_perpetual_constants as CONSTANTS,
    coinswitch_perpetual_utils as utils,
    coinswitch_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_order_book import (
    CoinswitchPerpetualOrderBook,
)
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
        CoinswitchPerpetualDerivative,
    )


class CoinswitchPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book, trade and funding-info feeds for CoinSwitch PRO futures.

    Everything streams over one Socket.IO v4 connection to ``ws.coinswitch.co``
    (handshake path ``/pro/realtime-rates-socket/futures/exchange_2``, namespace
    ``/exchange_2``), one subscription per event and symbol:

    * ``FETCH_ORDER_BOOK_CS_PRO`` — a FULL book on every push, applied as a snapshot;
    * ``FETCH_TRADES_CS_PRO`` — public trade prints;
    * ``FETCH_TICKER_INFO_CS_PRO`` — 24h stats plus mark price (``p``), index price
      (``i``), funding rate (``r``) and next funding time (``T``), which feed the
      funding info.

    The sockets are unauthenticated. REST is used for the initial book and the
    initial funding info; both fall back to the first socket frame when REST
    fails (the futures REST endpoints are documented as signed, so a connector
    without keys may be refused).
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: "CoinswitchPerpetualDerivative",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._client: Optional[socketio.AsyncClient] = None
        self._last_frame_time: float = 0.0
        # "BTCUSDT" -> "BTC-USDT", filled when subscribing.
        self._ws_symbol_to_pair: Dict[str, str] = {}
        # Latest socket book and funding info per pair, for the REST fallbacks.
        self._latest_books: Dict[str, Dict[str, Any]] = {}
        self._latest_funding_info: Dict[str, FundingInfo] = {}
        self._book_received: Dict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self._funding_received: Dict[str, asyncio.Event] = defaultdict(asyncio.Event)

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        try:
            return await self._connector.build_funding_info(trading_pair)
        except asyncio.CancelledError:
            raise
        except Exception as rest_error:
            funding_info = await self._wait_for_frame(self._latest_funding_info, self._funding_received, trading_pair)
            if funding_info is None:
                raise
            self.logger().warning(
                f"REST ticker failed for {trading_pair} ({rest_error}); using the socket ticker's funding info.")
            return funding_info

    # ---- REST snapshot -------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_PATH_URL,
            params={"exchange": CONSTANTS.EXCHANGE, "symbol": symbol, "l2Orderbook": "true"},
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        try:
            snapshot = utils.unwrap_data(await self._request_order_book_snapshot(trading_pair))
            if not isinstance(snapshot, dict) or not ("bids" in snapshot or "asks" in snapshot):
                raise IOError(f"Unexpected order book response for {trading_pair}: {snapshot}")
        except asyncio.CancelledError:
            raise
        except Exception as rest_error:
            snapshot = await self._wait_for_frame(self._latest_books, self._book_received, trading_pair)
            if snapshot is None:
                raise
            self.logger().warning(
                f"REST order book failed for {trading_pair} ({rest_error}); starting from the socket snapshot.")
        return CoinswitchPerpetualOrderBook.snapshot_message_from_exchange(
            snapshot, self._time(), metadata={"trading_pair": trading_pair})

    async def _wait_for_frame(self, cache: Dict[str, Any], events: Dict[str, asyncio.Event], trading_pair: str):
        if trading_pair in cache:
            return cache[trading_pair]
        try:
            await asyncio.wait_for(events[trading_pair].wait(), timeout=CONSTANTS.WS_FIRST_FRAME_TIMEOUT)
        except asyncio.TimeoutError:
            return None
        return cache.get(trading_pair)

    # ---- socket ----------------------------------------------------------------

    def _build_client(self) -> socketio.AsyncClient:
        client = socketio.AsyncClient(reconnection=False, logger=False, engineio_logger=False)
        namespace = CONSTANTS.WS_NAMESPACE
        client.on("connect", self._on_connect, namespace=namespace)
        client.on("disconnect", self._on_disconnect, namespace=namespace)
        client.on(CONSTANTS.ORDER_BOOK_EVENT, self._on_order_book, namespace=namespace)
        client.on(CONSTANTS.TRADES_EVENT, self._on_trades, namespace=namespace)
        client.on(CONSTANTS.TICKER_EVENT, self._on_ticker, namespace=namespace)
        return client

    async def listen_for_subscriptions(self):
        """
        Keeps one Socket.IO connection subscribed to every pair. Reconnection is
        handled here rather than by the client, so a reconnect always re-subscribes.
        """
        while True:
            client: Optional[socketio.AsyncClient] = None
            try:
                client = self._build_client()
                self._client = client
                await client.connect(
                    web_utils.wss_url(self._domain),
                    namespaces=[CONSTANTS.WS_NAMESPACE],
                    socketio_path=CONSTANTS.WS_SOCKETIO_PATH,
                    transports=["websocket"],
                    wait_timeout=CONSTANTS.WS_CONNECT_TIMEOUT,
                )
                await self._subscribe_channels(client)
                await self._watch_connection(client)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    f"Unexpected error in the CoinSwitch futures market data stream. "
                    f"Reconnecting in {CONSTANTS.WS_RECONNECT_DELAY:.0f}s...")
            finally:
                await self._disconnect(client)
            await self._sleep(CONSTANTS.WS_RECONNECT_DELAY)

    async def _subscribe_channels(self, client: socketio.AsyncClient):
        try:
            for trading_pair in self._trading_pairs:
                symbol = (await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)).upper()
                self._ws_symbol_to_pair[symbol] = trading_pair
                payload = {"event": CONSTANTS.SUBSCRIBE_EVENT, "pair": symbol}
                for event in (CONSTANTS.ORDER_BOOK_EVENT, CONSTANTS.TRADES_EVENT, CONSTANTS.TICKER_EVENT):
                    await client.emit(event, payload, namespace=CONSTANTS.WS_NAMESPACE)
            self.logger().info(
                f"Subscribed to CoinSwitch futures order book, trades and ticker for {self._trading_pairs}.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to CoinSwitch futures public streams.")
            raise

    async def _watch_connection(self, client: socketio.AsyncClient):
        """Returns when the socket disconnects or stays silent for ``WS_SILENCE_TIMEOUT``."""
        self._last_frame_time = self._time()
        while client.connected:
            await self._sleep(1.0)
            if self._time() - self._last_frame_time > CONSTANTS.WS_SILENCE_TIMEOUT:
                self.logger().warning(
                    f"No CoinSwitch futures market data for {CONSTANTS.WS_SILENCE_TIMEOUT:.0f}s; reconnecting.")
                return
        self.logger().warning("The CoinSwitch futures market data socket disconnected.")

    async def _disconnect(self, client: Optional[socketio.AsyncClient]):
        if client is None:
            return
        try:
            await client.disconnect()
        except Exception:
            self.logger().debug("CoinSwitch futures socket disconnect failed.", exc_info=True)
        if self._client is client:
            self._client = None

    async def _on_connect(self):
        self.logger().info(f"Connected to the CoinSwitch futures socket namespace {CONSTANTS.WS_NAMESPACE}.")

    async def _on_disconnect(self, *args):
        self.logger().debug(f"CoinSwitch futures socket namespace disconnected {args}.")

    async def _on_order_book(self, payload: Any):
        self._last_frame_time = self._time()
        book = utils.unwrap_data(utils.parse_socket_payload(payload))
        if isinstance(book, dict) and ("bids" in book or "asks" in book):
            self._message_queue[self._snapshot_messages_queue_key].put_nowait(book)

    async def _on_trades(self, payload: Any):
        self._last_frame_time = self._time()
        trades = utils.unwrap_data(utils.parse_socket_payload(payload))
        for trade in trades if isinstance(trades, list) else [trades]:
            if isinstance(trade, dict):
                self._message_queue[self._trade_messages_queue_key].put_nowait(trade)

    async def _on_ticker(self, payload: Any):
        """The push is keyed by symbol (``{"BTCUSDT": {...}}``) so one frame can carry several symbols."""
        self._last_frame_time = self._time()
        tickers = utils.unwrap_data(utils.parse_socket_payload(payload))
        if isinstance(tickers, dict):
            records = [tickers] if "s" in tickers else list(tickers.values())
        elif isinstance(tickers, list):
            records = tickers
        else:
            records = []
        for record in records:
            if isinstance(record, dict):
                self._message_queue[self._funding_info_messages_queue_key].put_nowait(record)

    # ---- parsers -------------------------------------------------------------

    async def _trading_pair_for(self, symbol: Any) -> Optional[str]:
        if not symbol:
            return None
        symbol = str(symbol).upper()
        trading_pair = self._ws_symbol_to_pair.get(symbol)
        if trading_pair is not None:
            return trading_pair
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        except KeyError:
            return None

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair_for(raw_message.get("symbol") or raw_message.get("s"))
        if trading_pair is None:
            return
        self._latest_books[trading_pair] = raw_message
        self._book_received[trading_pair].set()
        message_queue.put_nowait(CoinswitchPerpetualOrderBook.snapshot_message_from_exchange(
            raw_message, self._time(), metadata={"trading_pair": trading_pair}))

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Every book push is a full snapshot; nothing arrives as a diff.
        pass

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._trading_pair_for(raw_message.get("s"))
        if trading_pair is None:
            return
        message_queue.put_nowait(CoinswitchPerpetualOrderBook.trade_message_from_exchange(
            raw_message, metadata={"trading_pair": trading_pair}))

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Ticker push: ``p`` mark price, ``i`` index price, ``r`` funding rate and
        ``T`` next funding time (ms). Fields the frame leaves empty are left out
        of the update rather than zeroed.
        """
        trading_pair = await self._trading_pair_for(raw_message.get("s"))
        if trading_pair is None:
            return
        mark_price = utils.to_decimal(raw_message.get("p"))
        index_price = utils.to_decimal(raw_message.get("i"))
        rate = raw_message.get("r")
        next_funding_ms = int(utils.to_decimal(raw_message.get("T")))
        update = FundingInfoUpdate(
            trading_pair=trading_pair,
            mark_price=mark_price if mark_price > 0 else None,
            index_price=index_price if index_price > 0 else None,
            rate=utils.to_decimal(rate) if rate not in (None, "") else None,
            next_funding_utc_timestamp=int(next_funding_ms * 1e-3) if next_funding_ms > 0 else None,
        )
        if update.mark_price is None and update.index_price is None and update.rate is None:
            return
        if None not in (update.mark_price, update.index_price, update.rate, update.next_funding_utc_timestamp):
            self._latest_funding_info[trading_pair] = FundingInfo(
                trading_pair=trading_pair,
                index_price=update.index_price,
                mark_price=update.mark_price,
                next_funding_utc_timestamp=update.next_funding_utc_timestamp,
                rate=update.rate,
            )
            self._funding_received[trading_pair].set()
        message_queue.put_nowait(update)
