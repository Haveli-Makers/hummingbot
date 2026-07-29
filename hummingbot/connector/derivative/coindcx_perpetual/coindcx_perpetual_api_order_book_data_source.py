import asyncio
import json
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import socketio

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_utils as utils,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_order_book import CoinDCXPerpetualOrderBook
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


def unwrap_frame(message: Any) -> Optional[Dict[str, Any]]:
    """
    Normalise a CoinDCX Socket.IO payload into a dict.

    Frames arrive double-wrapped as ``{"event": ..., "data": "<json string>"}``
    — note ``data`` is a JSON *string*, not an object as the docs suggest — but
    plain dicts are also accepted so callers/tests can pass either shape.
    """
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except (TypeError, ValueError):
            return None
    if not isinstance(message, dict):
        return None

    data = message.get("data", message)
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            return None
    return data if isinstance(data, dict) else None


class CoinDCXPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book, trade and funding-info feeds for CoinDCX futures.

    CoinDCX speaks Socket.IO rather than plain websockets, so this data source
    drives a ``socketio.AsyncClient`` directly and overrides
    ``listen_for_subscriptions`` instead of using ``WSAssistant``.
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
        self._client: Optional[socketio.AsyncClient] = None
        # depth-snapshot frames identify the instrument by market symbol
        # ("BTCUSDT"), unlike every other endpoint which uses "B-BTC_USDT".
        self._market_symbol_to_trading_pair: Dict[str, str] = {
            utils.hb_pair_to_market_symbol(tp): tp for tp in trading_pairs
        }

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ---- REST snapshot -------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        coindcx_pair = utils.hb_pair_to_coindcx_pair(trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.order_book_url(coindcx_pair=coindcx_pair, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        return CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            snapshot, time.time(), metadata={"trading_pair": trading_pair})

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        return await self._connector.build_funding_info(trading_pair)

    # ---- Socket.IO subscriptions --------------------------------------------

    async def listen_for_subscriptions(self):
        while True:
            try:
                self._client = self._build_client()
                await self._client.connect(web_utils.wss_url(self._domain), transports=["websocket"])
                await self._subscribe_channels(self._client)
                ping_task = asyncio.create_task(self._ping_task())
                try:
                    await self._client.wait()
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
            except asyncio.CancelledError:
                await self._disconnect()
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
                await self._disconnect()
                await self._sleep(5.0)
            finally:
                await self._disconnect()

    def _build_client(self) -> socketio.AsyncClient:
        client = socketio.AsyncClient(logger=False, reconnection=False)
        snapshot_queue = self._message_queue[self._snapshot_messages_queue_key]
        trade_queue = self._message_queue[self._trade_messages_queue_key]
        funding_queue = self._message_queue[self._funding_info_messages_queue_key]

        @client.event
        async def connect():
            self.logger().info("Connected to CoinDCX futures public stream.")

        @client.event
        async def disconnect():
            self.logger().warning("CoinDCX futures public stream disconnected.")

        # These queues feed the base class's listen_for_* loops, which call
        # _parse_*_message themselves, so they must receive the RAW payload.
        # Enqueueing an already-parsed message would make the consumer parse it
        # twice and drop every frame.
        @client.on(CONSTANTS.DEPTH_SNAPSHOT_EVENT_TYPE)
        async def on_depth(message):
            data = unwrap_frame(message)
            if data is not None:
                snapshot_queue.put_nowait(data)

        @client.on(CONSTANTS.TRADE_EVENT_TYPE)
        async def on_trade(message):
            data = unwrap_frame(message)
            if data is not None:
                trade_queue.put_nowait(data)

        @client.on(CONSTANTS.CURRENT_PRICES_EVENT_TYPE)
        async def on_prices(message):
            data = unwrap_frame(message)
            if data is not None:
                funding_queue.put_nowait(data)

        return client

    async def _subscribe_channels(self, client: socketio.AsyncClient):
        for trading_pair in self._trading_pairs:
            coindcx_pair = utils.hb_pair_to_coindcx_pair(trading_pair)
            await client.emit("join", {"channelName": CONSTANTS.ORDER_BOOK_CHANNEL.format(
                pair=coindcx_pair, depth=CONSTANTS.ORDER_BOOK_DEPTH)})
            await self._sleep(0.05)
            await client.emit("join", {"channelName": CONSTANTS.TRADES_CHANNEL.format(pair=coindcx_pair)})
            await self._sleep(0.05)
        await client.emit("join", {"channelName": CONSTANTS.CURRENT_PRICES_CHANNEL})
        self.logger().info(f"Subscribed to CoinDCX futures order book, trade and price channels for "
                           f"{self._trading_pairs}.")

    async def _disconnect(self):
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                self.logger().debug("CoinDCX futures stream disconnect failed", exc_info=True)
            self._client = None

    async def _ping_task(self):
        try:
            while True:
                await self._sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                if self._client is not None and self._client.connected:
                    try:
                        await self._client.emit("ping", {"data": "Ping message"})
                    except Exception as exception:
                        self.logger().debug(f"Error sending ping: {exception}")
        except asyncio.CancelledError:
            pass

    # ---- Parsers -------------------------------------------------------------

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        market_symbol = raw_message.get("s", "")
        trading_pair = self._market_symbol_to_trading_pair.get(market_symbol)
        if trading_pair is None:
            # Only one pair subscribed? Then an unlabelled frame is unambiguous.
            if market_symbol or len(self._trading_pairs) != 1:
                return
            trading_pair = self._trading_pairs[0]

        message_queue.put_nowait(CoinDCXPerpetualOrderBook.snapshot_message_from_exchange(
            raw_message, time.time(), metadata={"trading_pair": trading_pair}))

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        coindcx_pair = raw_message.get("s", "")
        if not coindcx_pair:
            return
        try:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=coindcx_pair)
        except Exception:
            return
        message_queue.put_nowait(CoinDCXPerpetualOrderBook.trade_message_from_exchange(
            raw_message, metadata={"trading_pair": trading_pair}))

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        ``currentPrices@futures#update`` carries a ``prices`` map keyed by CoinDCX
        pair with the mark price (``mp``) and funding rate (``fr``). Fields are
        only present when they change, so each is applied independently.
        """
        prices = raw_message.get("prices")
        if not isinstance(prices, dict):
            return

        for coindcx_pair, info in prices.items():
            if not isinstance(info, dict):
                continue
            trading_pair = utils.coindcx_pair_to_hb_pair(coindcx_pair)
            if trading_pair not in self._trading_pairs:
                continue

            mark_price = info.get("mp")
            rate = info.get("fr")
            if mark_price is None and rate is None:
                continue

            message_queue.put_nowait(FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(mark_price)) if mark_price is not None else None,
                mark_price=Decimal(str(mark_price)) if mark_price is not None else None,
                next_funding_utc_timestamp=self._connector.next_funding_timestamp(trading_pair),
                rate=Decimal(str(rate)) if rate is not None else None,
            ))
