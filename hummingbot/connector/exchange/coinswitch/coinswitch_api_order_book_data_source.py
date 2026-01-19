import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

_logger = logging.getLogger(__name__)


class CoinswitchAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    OrderBook data source for CoinSwitch API.
    Uses REST polling since CoinSwitch doesn't provide WebSocket streams.
    """

    SNAPSHOT_POLL_INTERVAL = 5.0

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'CoinswitchExchange',
                 api_factory: WebAssistantsFactory,
                 exchange: str = CONSTANTS.DEFAULT_EXCHANGE,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Initialize the CoinSwitch order book data source.

        Args:
            trading_pairs: List of trading pairs
            connector: The exchange connector
            api_factory: WebAssistantsFactory for API calls
            exchange: The exchange to use (coinswitchx, wazirx, c2c1, c2c2)
            domain: The domain (default: com)
        """
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._exchange = exchange
        self._domain = domain

    def _time(self) -> float:
        """Return current timestamp."""
        return time.time()

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        """
        Get the last traded prices for trading pairs using the connector.

        Args:
            trading_pairs: List of trading pairs
            domain: Optional domain
        """
        prices = {}
        for trading_pair in trading_pairs:
            try:
                price = await self._connector._get_last_traded_price(trading_pair=trading_pair)
                if price and price > 0:
                    prices[trading_pair] = price
            except Exception as e:
                _logger.warning(f"Error getting last traded price for {trading_pair}: {e}")
        return prices

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair.replace("-", "/")

        params = {
            "exchange": self._exchange,
            "symbol": symbol,
        }
        data = await self._connector._api_get(
            path_url=CONSTANTS.DEPTH_PATH_URL,
            params=params,
            is_auth_required=True,
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Returns an OrderBookMessage with the order book snapshot.

        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: OrderBookMessage with the snapshot
        """
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = self._time()

        snapshot_data = snapshot_response.get("data", {})

        snapshot_msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(snapshot_timestamp * 1000),
                "bids": snapshot_data.get("bids", []),
                "asks": snapshot_data.get("asks", []),
            },
            timestamp=snapshot_timestamp
        )
        return snapshot_msg

    async def listen_for_subscriptions(self):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot_msg = await self._order_book_snapshot(trading_pair)
                        self._message_queue[self._diff_messages_queue_key].put_nowait(snapshot_msg)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        _logger.warning(f"Error fetching order book snapshot for {trading_pair}: {e}")

                await asyncio.sleep(self.SNAPSHOT_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception("Unexpected error in listen_for_subscriptions. Retrying...")
                await asyncio.sleep(1.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                message = await message_queue.get()
                output.put_nowait(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception("Unexpected error when processing order book diffs from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order snapshot events queue.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception("Unexpected error in listen_for_order_book_snapshots")
                await asyncio.sleep(1.0)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listen for trades.

        :param ev_loop: the event loop the method will run in
        :param output: Queue to output trades
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        try:
                            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                        except KeyError:
                            symbol = trading_pair.replace("-", "/")

                        params = {
                            "exchange": self._exchange,
                            "symbol": symbol,
                        }
                        response = await self._connector._api_get(
                            path_url=CONSTANTS.TRADES_PATH_URL,
                            params=params,
                            is_auth_required=True,
                        )

                        if response and "data" in response:
                            for trade in response.get("data", []):
                                trade_timestamp = float(trade.get("E", trade.get("time", self._time() * 1000))) / 1000.0
                                message = OrderBookMessage(
                                    message_type=OrderBookMessageType.TRADE,
                                    content={
                                        "trading_pair": trading_pair,
                                        "trade_type": float(TradeType.BUY.value) if not trade.get("m", False) else float(TradeType.SELL.value),
                                        "trade_id": trade.get("t", trade.get("id", "")),
                                        "update_id": trade.get("t", trade.get("id", "")),
                                        "price": trade.get("p", trade.get("price", "")),
                                        "amount": trade.get("q", trade.get("qty", trade.get("quantity", ""))),
                                    },
                                    timestamp=trade_timestamp
                                )
                                output.put_nowait(message)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        _logger.warning(f"Error fetching trades for {trading_pair}: {e}")

                await asyncio.sleep(self.SNAPSHOT_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception("Unexpected error in listen_for_trades. Retrying...")
                await asyncio.sleep(1.0)

    async def _parse_order_book_diff_message(self, raw_message: Any, message_queue: asyncio.Queue):
        """
        Parses raw order book diff messages. For CoinSwitch, these are already OrderBookMessage objects.
        """
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)
        else:
            pass

    async def _connected_websocket_assistant(self):
        return None

    async def _subscribe_channels(self, ws):
        pass

    async def _process_websocket_messages(self, websocket_assistant):
        pass

    async def _on_order_stream_interruption(self, websocket_assistant=None):
        """
        Handle stream interruption.
        """
        pass
