import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS, coindcx_web_utils as web_utils
from hummingbot.connector.exchange.coindcx.coindcx_order_book import CoinDCXOrderBook
from hummingbot.connector.exchange.coindcx.coindcx_utils import hb_pair_to_coindcx_pair
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoinDCXExchange


class CoinDCXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for CoinDCX order book and trade data.
    Uses CoinDCX WebSocket for real-time updates and REST API for snapshots.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinDCXExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Returns the last traded price for each trading pair.
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange.

        CoinDCX uses the format: GET /market_data/orderbook?pair=B-BTC_USDT

        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        # Convert HB pair to CoinDCX pair format
        coindcx_pair = hb_pair_to_coindcx_pair(trading_pair)

        params = {
            "pair": coindcx_pair
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        CoinDCX WebSocket channels:
        - {pair}@orderbook@{depth} for order book updates (e.g., B-BTC_USDT@orderbook@20)
        - {pair}@trades for trade updates (e.g., B-BTC_USDT@trades)

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                coindcx_pair = hb_pair_to_coindcx_pair(trading_pair)

                # Subscribe to order book depth
                orderbook_channel = f"{coindcx_pair}@orderbook@20"
                subscribe_orderbook = {
                    "channelName": orderbook_channel
                }
                await ws.send(WSJSONRequest(payload={"type": "join", **subscribe_orderbook}))

                # Subscribe to trades
                trades_channel = f"{coindcx_pair}@trades"
                subscribe_trades = {
                    "channelName": trades_channel
                }
                await ws.send(WSJSONRequest(payload={"type": "join", **subscribe_trades}))

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates a new WebSocket connection to CoinDCX.
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
            message_timeout=None
        )
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves and returns the order book snapshot as an OrderBookMessage.
        """
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = CoinDCXOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses a trade message from CoinDCX WebSocket and adds it to the message queue.
        """
        # Extract trading pair from the message
        pair_symbol = raw_message.get("s", "")
        if pair_symbol:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=pair_symbol)
            trade_message = CoinDCXOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses an order book diff message from CoinDCX WebSocket and adds it to the message queue.
        """
        if "bids" in raw_message or "asks" in raw_message:
            channel = raw_message.get("channel", "")
            trading_pair = None

            if "@orderbook" in channel:
                pair_part = channel.split("@")[0]
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=pair_part)

            if trading_pair:
                order_book_message: OrderBookMessage = CoinDCXOrderBook.diff_message_from_exchange(
                    raw_message, time.time(), {"trading_pair": trading_pair})
                message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Determines which channel a message originated from based on its content.
        """
        channel = ""

        if "p" in event_message and "q" in event_message and "T" in event_message:
            channel = self._trade_messages_queue_key
        elif "bids" in event_message or "asks" in event_message:
            channel = self._diff_messages_queue_key

        return channel
