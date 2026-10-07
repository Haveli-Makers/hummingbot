import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.wazirx_perpetual import (
    wazirx_perpetual_constants as CONSTANTS,
    wazirx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_order_book import WazirxPerpetualOrderBook
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative


class WazirxPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book, trade and funding-info feeds for WazirX futures.

    Everything streams over one plain-JSON websocket (``fstreamx.wazirx.com``):

    * ``<symbol>@depth``  — a full top-20 book about once a second, applied as a
      snapshot (see ``WazirxPerpetualOrderBook``);
    * ``<symbol>@aggTrade`` — public trades;
    * ``!markPrice@arr`` — mark price, index price, funding rate and next
      funding time for every contract, about once a second.

    REST is only used for the initial book, funding info at start-up, and the
    hourly snapshot fallback the base class performs when the stream is silent.
    Frames are ``{"data": {...}, "stream": "<name>"}``; control replies
    (connected / subscribed / pong / error) carry ``event`` instead.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: "WazirxPerpetualDerivative",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._requested_streams: List[str] = []
        # lowercase exchange symbol ("btcinr") -> Hummingbot pair ("BTC-INR")
        self._ws_symbol_to_pair: Dict[str, str] = {}

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        return await self._connector.build_funding_info(trading_pair)

    # ---- REST snapshot -------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params={"symbol": symbol, "limit": CONSTANTS.ORDER_BOOK_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        return WazirxPerpetualOrderBook.snapshot_message_from_exchange(
            snapshot, self._time(), metadata={"trading_pair": trading_pair})

    # ---- websocket -----------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=web_utils.wss_url(self._domain), ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            streams: List[str] = []
            for trading_pair in self._trading_pairs:
                symbol = (await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)).lower()
                self._ws_symbol_to_pair[symbol] = trading_pair
                streams.append(CONSTANTS.DEPTH_STREAM.format(symbol=symbol))
                streams.append(CONSTANTS.TRADE_STREAM.format(symbol=symbol))
            streams.append(CONSTANTS.MARK_PRICE_STREAM)
            self._requested_streams = streams

            await ws.send(WSJSONRequest(payload={"event": CONSTANTS.SUBSCRIBE_EVENT, "streams": streams}))
            self.logger().info(f"Subscribed to WazirX futures public streams: {streams}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to WazirX futures public streams.")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        # The connection is only valid for 30 minutes unless the application-level
        # ping is sent; protocol pings (the heartbeat) keep it from idling, this
        # keeps it from expiring.
        ping_task = safe_ensure_future(self._ping_loop(websocket_assistant))
        try:
            await super()._process_websocket_messages(websocket_assistant=websocket_assistant)
        finally:
            ping_task.cancel()

    async def _ping_loop(self, websocket_assistant: WSAssistant):
        while True:
            await self._sleep(CONSTANTS.WS_PING_INTERVAL)
            try:
                await websocket_assistant.send(WSJSONRequest(payload={"event": CONSTANTS.PING_EVENT}))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().debug("Failed to send the WazirX futures stream ping.", exc_info=True)
                return

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        stream = str(event_message.get("stream", ""))
        if stream.endswith(CONSTANTS.DEPTH_STREAM_SUFFIX):
            return self._snapshot_messages_queue_key
        if stream.endswith(CONSTANTS.TRADE_STREAM_SUFFIX):
            return self._trade_messages_queue_key
        if stream == CONSTANTS.MARK_PRICE_STREAM:
            return self._funding_info_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any],
                                                   websocket_assistant: WSAssistant):
        event = event_message.get("event")
        if event == CONSTANTS.ERROR_EVENT:
            self.logger().warning(f"WazirX futures public stream error: {event_message.get('data')}")
        elif event == CONSTANTS.SUBSCRIBED_EVENT:
            # An unknown symbol is acknowledged with "streams": null rather than
            # an error, so the ack is the only place a typo shows up.
            accepted = set((event_message.get("data") or {}).get("streams") or [])
            missing = [stream for stream in self._requested_streams if stream not in accepted]
            if missing:
                self.logger().warning(f"WazirX futures did not acknowledge streams {missing}.")

    # ---- parsers -------------------------------------------------------------

    async def _trading_pair_for(self, ws_symbol: str) -> Optional[str]:
        if not ws_symbol:
            return None
        trading_pair = self._ws_symbol_to_pair.get(ws_symbol.lower())
        if trading_pair is not None:
            return trading_pair
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=ws_symbol.upper())
        except KeyError:
            return None

    @staticmethod
    def _symbol_of(raw_message: Dict[str, Any], suffix: str) -> str:
        data = raw_message.get("data") or {}
        symbol = data.get("s") if isinstance(data, dict) else None
        if symbol:
            return str(symbol)
        stream = str(raw_message.get("stream", ""))
        return stream[:-len(suffix)] if stream.endswith(suffix) else ""

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data")
        if not isinstance(data, dict):
            return
        trading_pair = await self._trading_pair_for(self._symbol_of(raw_message, CONSTANTS.DEPTH_STREAM_SUFFIX))
        if trading_pair is None:
            return
        message_queue.put_nowait(WazirxPerpetualOrderBook.snapshot_message_from_exchange(
            data, self._time(), metadata={"trading_pair": trading_pair}))

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data")
        if not isinstance(data, dict):
            return
        trading_pair = await self._trading_pair_for(self._symbol_of(raw_message, CONSTANTS.TRADE_STREAM_SUFFIX))
        if trading_pair is None:
            return
        message_queue.put_nowait(WazirxPerpetualOrderBook.trade_message_from_exchange(
            data, metadata={"trading_pair": trading_pair}))

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        ``!markPrice@arr`` carries every contract in one array; only the
        subscribed pairs are forwarded. ``p`` is the mark price, ``i`` the index
        price, ``r`` the funding rate and ``T`` the next funding time (ms).
        """
        records = raw_message.get("data")
        if isinstance(records, dict):
            records = [records]
        for record in records or []:
            if not isinstance(record, dict):
                continue
            trading_pair = self._ws_symbol_to_pair.get(str(record.get("s", "")).lower())
            if trading_pair is None:
                continue
            try:
                message_queue.put_nowait(FundingInfoUpdate(
                    trading_pair=trading_pair,
                    index_price=Decimal(str(record["i"])),
                    mark_price=Decimal(str(record["p"])),
                    next_funding_utc_timestamp=int(int(record["T"]) * 1e-3),
                    rate=Decimal(str(record["r"])),
                ))
            except (KeyError, TypeError, ValueError, ArithmeticError):
                self.logger().debug(f"Skipping malformed mark-price record {record}.")
