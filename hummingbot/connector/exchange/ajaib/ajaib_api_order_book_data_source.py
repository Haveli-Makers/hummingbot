import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_web_utils as web_utils
from hummingbot.connector.exchange.ajaib.ajaib_order_book import AjaibOrderBook
from hummingbot.connector.exchange.ajaib.ajaib_utils import hb_pair_to_ajaib_symbol
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibAPIOrderBookDataSource(OrderBookTrackerDataSource):

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

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = hb_pair_to_ajaib_symbol(trading_pair)
        params = {"symbol": symbol, "limit": 100}

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
            is_auth_required=True,
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = AjaibOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        streams = []
        for trading_pair in self._trading_pairs:
            symbol = hb_pair_to_ajaib_symbol(trading_pair).lower()
            streams.append(f"{symbol}@depth@100ms")
            streams.append(f"{symbol}@trade")

        url = f"{CONSTANTS.WSS_URL}/stream?streams={'/'.join(streams)}"
        await ws.connect(ws_url=url)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data", raw_message)
        symbol = data.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            trade_message = AjaibOrderBook.trade_message_from_exchange(
                data, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data", raw_message)
        symbol = data.get("s", "")
        if symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            order_book_message = AjaibOrderBook.diff_message_from_exchange(
                data, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    async def listen_for_subscriptions(self):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._process_websocket_messages(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _process_websocket_messages(self, ws: WSAssistant):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if isinstance(data, dict):
                stream = data.get("stream", "")
                if "@trade" in stream:
                    await self._parse_trade_message(data, self._message_queue.get("trade", asyncio.Queue()))
                elif "@depth" in stream:
                    await self._parse_order_book_diff_message(data, self._message_queue.get("depth", asyncio.Queue()))
