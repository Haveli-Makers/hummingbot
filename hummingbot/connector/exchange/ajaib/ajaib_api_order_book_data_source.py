import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_web_utils as web_utils
from hummingbot.connector.exchange.ajaib.ajaib_order_book import AjaibOrderBook
from hummingbot.connector.exchange.ajaib.ajaib_utils import hb_pair_to_ajaib_symbol
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order book data source for Ajaib.

    The book is seeded from ``GET /v1/depth`` and kept current by the
    ``<SYMBOL>@depth20`` partial-book stream, which delivers a FULL snapshot of
    the top levels rather than a diff -- so every depth message goes to the
    snapshot queue.

    Two things about that stream are easy to get wrong, and both were:
      * the symbol is matched case-sensitively and must be UPPERCASE;
      * the stream is ``@depth20`` (a partial book), while bare ``@depth`` is a
        top-of-book feed whose payload the snapshot parser cannot read.

    Ajaib also suppresses any depth event repeating the previous ``UpdateId``,
    so a quiet market legitimately goes silent for long stretches. Do not treat
    a gap between frames as a dropped connection.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'AjaibExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Seed the book from GET /v1/depth, so the tracker does not report ready
        while holding 0 bids / 0 asks and a strategy quote against nothing.

        The order book tracker calls this exactly ONCE per pair and never
        retries: if it raises, that book is never initialised and the connector
        can never become ready. Both calls below go through the allowlisted
        proxy, which drops connections intermittently, so:

          * a transient failure (the request, or the symbol map failing to load)
            is retried with backoff, and as a last resort the book starts empty
            and the @depth20 stream populates it -- never a dead connector;
          * a pair that is genuinely not listed fails fast with a message that
            says so, since no amount of retrying can help.
        """
        attempts = CONSTANTS.DEPTH_SNAPSHOT_MAX_ATTEMPTS
        delay = 1.0
        error: Any = None
        for attempt in range(1, attempts + 1):
            try:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                snapshot = await self._connector._api_get(
                    path_url=CONSTANTS.DEPTH_PATH_URL,
                    params={"symbol": symbol, "limit": CONSTANTS.DEPTH_SNAPSHOT_LIMIT},
                    is_auth_required=True,
                )
                return AjaibOrderBook.snapshot_message_from_exchange(
                    snapshot, time.time(), metadata={"trading_pair": trading_pair})
            except asyncio.CancelledError:
                raise
            except KeyError:
                if self._connector.trading_pair_symbol_map_ready():
                    raise ValueError(
                        f"{trading_pair} is not listed on Ajaib: it is absent from /v1/exchange-info. "
                        f"Check the pair, or whether it has been delisted.") from None
                error = "the symbol map is unavailable (the exchange-info request failed)"
            except Exception as exception:
                error = exception
            if attempt < attempts:
                self.logger().warning(
                    f"Order book snapshot for {trading_pair} failed (attempt {attempt}/{attempts}): "
                    f"{error}. Retrying in {delay:.0f}s.")
                await self._sleep(delay)
                delay *= 2

        self.logger().warning(
            f"Could not seed the {trading_pair} order book after {attempts} attempts ({error}). "
            f"Starting it empty; the @{CONSTANTS.WS_DEPTH_STREAM_SUFFIX} stream will populate it.")
        return AjaibOrderBook.snapshot_message_from_exchange(
            {"bids": [], "asks": []}, time.time(), metadata={"trading_pair": trading_pair})

    async def _get_listen_key(self) -> str:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.LISTEN_KEY_PATH_URL, domain=self._domain),
            method=RESTMethod.POST,
            data={},
            throttler_limit_id=CONSTANTS.LISTEN_KEY_PATH_URL,
            is_auth_required=True,
        )
        return response["listenKey"]

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # Market data is NOT served on a bare /ws -- that is rejected 401. Every
        # stream, public ones included, is reached at /ws/<listenKey>, so the
        # order book needs a key exactly as the user stream does.
        #
        # Ajaib issues ONE listen key per API key, not per mint: every POST
        # returns the same string and simply pushes its expiry out to now+60min.
        # It is also safe to hold several connections on that one key at once
        # (verified live: two concurrent connections both stayed up for 90s).
        # So this shares the key with the user stream rather than owning it,
        # and the user stream's 30-minute keepalive keeps it alive for both.
        #
        # Never DELETE this key: being account-global, deleting it drops every
        # connection using it -- the user stream as well as this one.
        listen_key = await self._get_listen_key()
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=f"{web_utils.wss_url(self._domain)}{CONSTANTS.WS_PUBLIC_PATH}/{listen_key}",
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            params = []
            for trading_pair in self._trading_pairs:
                # UPPERCASE. Ajaib matches the symbol case-sensitively: verified
                # live, BTC_IDR@kline_1m delivers and btc_idr@kline_1m is silent.
                symbol = hb_pair_to_ajaib_symbol(trading_pair)
                params.append(f"{symbol}@{CONSTANTS.WS_DEPTH_STREAM_SUFFIX}")
                params.append(f"{symbol}@{CONSTANTS.WS_TRADE_EVENT_TYPE}")

            subscribe_request = WSJSONRequest(payload={
                "method": "SUBSCRIBE",
                "params": params,
                "id": int(time.time()),
            })
            await ws.send(subscribe_request)
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = event_message.get("e")
        if event_type == CONSTANTS.WS_DEPTH_EVENT_TYPE:
            return self._snapshot_messages_queue_key
        if event_type == CONSTANTS.WS_TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key
        return ""

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        symbol = raw_message.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            trade_message = AjaibOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        symbol = raw_message.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            snapshot_message = AjaibOrderBook.snapshot_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(snapshot_message)
