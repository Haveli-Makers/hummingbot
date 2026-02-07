import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinswitch import (
    coinswitch_constants as CONSTANTS,
    coinswitch_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_api_order_book_data_source import (
    CoinswitchAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_api_user_stream_data_source import (
    CoinswitchAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coinswitch.coinswitch_auth import CoinswitchAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

_logger = logging.getLogger(__name__)


class CoinswitchExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(self,
                 coinswitch_api_key: str,
                 coinswitch_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 exchange: str = CONSTANTS.DEFAULT_EXCHANGE,
                 ):
        """
        Initialize the CoinSwitch exchange connector.

        Args:
            coinswitch_api_key: API key for CoinSwitch
            coinswitch_api_secret: API secret for CoinSwitch
            balance_asset_limit: Optional balance limits
            rate_limits_share_pct: Rate limit percentage
            trading_pairs: List of trading pairs
            trading_required: Whether trading is required
            domain: API domain (default: com)
            exchange: Exchange to use (coinswitchx, wazirx, c2c1, c2c2)
        """
        self.api_key = coinswitch_api_key
        self.secret_key = coinswitch_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._exchange = exchange
        self._last_trades_poll_timestamp = 1.0

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def coinswitch_order_type(order_type: OrderType) -> str:
        """Convert Hummingbot OrderType to CoinSwitch order type string."""
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def to_hb_order_type(coinswitch_type: str) -> OrderType:
        """Convert CoinSwitch order type string to Hummingbot OrderType."""
        return OrderType.LIMIT

    async def get_all_pairs_prices(self) -> Dict[str, Any]:
        """
        Get all trading pairs prices from CoinSwitch.
        Returns:
            Dictionary with ticker data
        """
        params = {"exchange": self._exchange}
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_ALL_PATH_URL,
            params=params,
            is_auth_required=True,
        )
        return response

    @property
    def authenticator(self):
        """Get the authenticator for API requests."""
        return CoinswitchAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer
        )

    @property
    def name(self) -> str:
        """Get the connector name."""
        return "coinswitch"

    @property
    def rate_limits_rules(self):
        """Get the rate limits rules."""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        """Get the API domain."""
        return self._domain

    @property
    def client_order_id_max_length(self):
        """Get the maximum client order ID length."""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        """Get the client order ID prefix."""
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        """Get the trading rules request path."""
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        """Get the trading pairs request path."""
        return CONSTANTS.ACTIVE_COINS_PATH_URL

    @property
    def check_network_request_path(self):
        """Get the check network request path."""
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_rules_rest_request_path(self):
        """Get the REST request path for trading rules."""
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        """Get the list of trading pairs."""
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Whether cancel requests are synchronous in the exchange."""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Whether trading is required."""
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        """Get supported order types. LIMIT_MAKER is treated as LIMIT on CoinSwitch."""
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """Check if exception is related to time synchronizer."""
        error_description = str(request_exception)
        is_time_synchronizer_related = "timestamp" in error_description.lower()
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if error is due to order not found during status update."""
        return "not found" in str(status_update_exception).lower() or "does not exist" in str(status_update_exception).lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if error is due to order not found during cancellation."""
        return "not found" in str(cancelation_exception).lower() or "does not exist" in str(cancelation_exception).lower()

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory."""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Create order book data source."""
        return CoinswitchAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self._domain,
            api_factory=self._web_assistants_factory,
            exchange=self._exchange,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Create user stream data source."""
        return CoinswitchAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """Initialize trading pair symbols from exchange info (ticker data)."""
        mapping = bidict()
        try:
            tickers_data = exchange_info.get("data", {})

            if isinstance(tickers_data, dict):
                for symbol in tickers_data.keys():
                    try:
                        if "/" in symbol:
                            parts = symbol.split("/")
                        elif "-" in symbol:
                            parts = symbol.split("-")
                        else:
                            continue

                        if len(parts) == 2:
                            base, quote = parts
                            hb_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                            mapping[symbol] = hb_pair
                    except Exception as e:
                        _logger.debug(f"Error parsing symbol {symbol}: {e}")

        except Exception as e:
            _logger.error(f"Error initializing trading pair symbols: {e}")

        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        params = {"exchange": self._exchange}
        exchange_info = await self._api_get(
            path_url=CONSTANTS.TICKER_ALL_PATH_URL,
            params=params,
            is_auth_required=True,
        )
        return exchange_info

    async def _make_trading_rules_request(self) -> Any:
        params = {"exchange": self._exchange}
        exchange_info = await self._api_get(
            path_url=CONSTANTS.TICKER_ALL_PATH_URL,
            params=params,
            is_auth_required=True,
        )
        return exchange_info

    async def _import_trading_pairs(self):
        """Import trading pairs from the exchange."""
        return {}

    async def _get_trading_pairs(self) -> List[str]:
        """
        Get list of trading pairs from the exchange.
        Returns:
            List of trading pairs
        """
        try:
            url = web_utils.build_api_url(CONSTANTS.ACTIVE_COINS_PATH_URL)
            params = {"exchange": self._exchange}

            response = await self._rest_assistant.execute_request(
                method="GET",
                url=url,
                params=params,
                is_auth_required=True,
            )

            if response and "data" in response:
                coins = response.get("data", {}).get(self._exchange.lower(), [])
                return coins

        except Exception as e:
            _logger.error(f"Error fetching trading pairs: {e}")

        return []

    async def _get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        Get last traded prices for trading pairs.
        Args:
            trading_pairs: List of trading pairs
        Returns:
            Dictionary mapping trading pair to price
        """
        prices = {}

        for trading_pair in trading_pairs:
            try:
                url = web_utils.build_api_url(CONSTANTS.TICKER_PATH_URL)
                params = {
                    "exchange": self._exchange,
                    "symbol": trading_pair
                }

                response = await self._rest_assistant.execute_request(
                    method="GET",
                    url=url,
                    params=params,
                    is_auth_required=True,
                )

                if response and "data" in response:
                    ticker_data = response.get("data", {})
                    ticker_key = trading_pair.upper()
                    if ticker_key in ticker_data:
                        ticker = ticker_data[ticker_key]
                        price = float(ticker.get("lastPrice", 0))
                        prices[trading_pair] = price

            except Exception as e:
                _logger.warning(f"Error fetching price for {trading_pair}: {e}")

        return prices

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType, order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        """
        Place an order on the exchange.

        Args:
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            trade_type: Buy or sell
            order_type: Order type
            price: Order price

        Returns:
            Tuple of (exchange_order_id, timestamp)
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        payload = {
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "symbol": symbol,
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            "price": float(price),
            "quantity": float(amount),
            "exchange": self._exchange,
            "client_order_id": order_id,
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=payload,
            is_auth_required=True,
        )

        exchange_order_id = str(order_result.get("data", {}).get("order_id", order_result.get("order_id", "")))
        transact_time = float(order_result.get("data", {}).get("created_time", 0)) / 1000.0
        if transact_time == 0:
            transact_time = self._time_synchronizer.time()

        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on the exchange.
        Args:
            order_id: Client order ID
            tracked_order: The tracked in-flight order
        Returns:
            True if cancelled successfully, False otherwise
        """
        cancel_data = {
            "order_id": tracked_order.exchange_order_id,
        }

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=cancel_data,
            is_auth_required=True,
        )

        if cancel_result.get("success") or cancel_result.get("data", {}).get("status") == "CANCELLED":
            return True
        return False

    async def _get_trading_fees(self) -> Dict[str, TradeFeeBase]:
        """
        Get trading fees from the exchange.

        Returns:
            Dictionary of trading fees
        """
        trading_fees = {}

        try:
            url = web_utils.build_api_url(CONSTANTS.TRADING_FEE_PATH_URL)
            params = {"exchange": self._exchange}

            response = await self._rest_assistant.execute_request(
                method="GET",
                url=url,
                params=params,
                is_auth_required=True,
            )

            if response and "data" in response:
                fee_data = response.get("data", {}).get(self._exchange.lower(), {})

                for asset, fee_info in fee_data.items():
                    maker_fee = Decimal(str(fee_info.get("maker_fee_after_discount", 0)))
                    taker_fee = Decimal(str(fee_info.get("taker_fee_after_discount", 0)))

                    trading_fees[asset] = DeductedFromReturnsTradeFee(
                        token=TokenAmount(asset),
                        maker=maker_fee,
                        taker=taker_fee,
                    )

        except Exception as e:
            _logger.error(f"Error fetching trading fees: {e}")

        return trading_fees

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Get fee for a trade.
        Args:
            base_currency: Base asset
            quote_currency: Quote asset
            order_type: Order type
            order_side: Buy or sell
            amount: Order amount
            price: Order price
            is_maker: Whether this is a maker order

        Returns:
            Trade fee
        """
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_balances(self) -> None:
        """Update account balances from the exchange."""
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        try:
            response = await self._api_get(
                path_url=CONSTANTS.GET_PORTFOLIO_PATH_URL,
                is_auth_required=True,
            )

            if response and "data" in response:
                balance_data = response.get("data", [])

                if isinstance(balance_data, dict):
                    balance_data = [{"currency": k, **v} if isinstance(v, dict) else {"currency": k, "main_balance": v}
                                    for k, v in balance_data.items()]

                for asset_data in balance_data:
                    if isinstance(asset_data, dict):
                        asset = asset_data.get("currency", asset_data.get("coin", "")).upper()
                        free = Decimal(str(asset_data.get("main_balance", asset_data.get("free", asset_data.get("available", 0)))))
                        locked = Decimal(str(asset_data.get("blocked_balance_order", asset_data.get("locked", asset_data.get("blocked", 0)))))
                        total = free + locked

                        if asset:
                            self._account_balances[asset] = total
                            self._account_available_balances[asset] = free
                            remote_asset_names.add(asset)

            asset_names_to_remove = local_asset_names.difference(remote_asset_names)
            for asset_name in asset_names_to_remove:
                del self._account_available_balances[asset_name]
                del self._account_balances[asset_name]

        except Exception as e:
            _logger.error(f"Error updating balances: {e}", exc_info=True)

    async def _update_order_status(self) -> None:
        """Update order status from the exchange."""
        pass

    def _parse_order_response(self, order_response: Dict) -> Tuple[str, InFlightOrder]:
        """
        Parse order response from exchange.
        Args:
            order_response: Order response from exchange
        """
        exchange_order_id = order_response.get("order_id")
        trading_pair = order_response.get("symbol")

        in_flight_order = InFlightOrder(
            client_order_id=None,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY if order_response.get("side", "").lower() == "buy" else TradeType.SELL,
            price=Decimal(str(order_response.get("price", 0))),
            amount=Decimal(str(order_response.get("orig_qty", 0))),
            creation_timestamp=float(order_response.get("created_time", 0)) / 1000.0,
        )

        return exchange_order_id, in_flight_order

    def _parse_trade_update(self, trade: Dict) -> TradeUpdate:
        """
        Parse trade update from exchange.
        Args:
            trade: Trade data from exchange
        Returns:
            Trade update
        """
        return TradeUpdate(
            trade_id=trade.get("trade_id"),
            client_order_id=None,
            exchange_order_id=trade.get("order_id"),
            trading_pair=trade.get("symbol"),
            fill_side=TradeType.BUY if trade.get("is_buyer", True) else TradeType.SELL,
            fill_price=Decimal(str(trade.get("price", 0))),
            fill_amount=Decimal(str(trade.get("qty", 0))),
            fee=self._get_fee(
                base=trade.get("symbol", "").split("/")[0],
                quote=trade.get("symbol", "").split("/")[1],
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY if trade.get("is_buyer", True) else TradeType.SELL,
                amount=Decimal(str(trade.get("qty", 0))),
                price=Decimal(str(trade.get("price", 0))),
            ),
            fill_timestamp=float(trade.get("time", 0)) / 1000.0,
        )

    async def _update_trading_fees(self):
        """
        Update trading fees from the exchange.
        """
        pass

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events and process them.
        This functions runs in background continuously processing the events received from the exchange.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")

                if event_type == "balance_update":
                    balance_data = event_message.get("data", [])
                    for asset_data in balance_data:
                        asset = asset_data.get("currency", "").upper()
                        free = Decimal(str(asset_data.get("main_balance", 0)))
                        locked = Decimal(str(asset_data.get("blocked_balance_order", 0)))
                        total = free + locked
                        self._account_balances[asset] = total
                        self._account_available_balances[asset] = free

                elif event_type == "order_update":
                    orders_data = event_message.get("data", [])
                    for order_data in orders_data:
                        client_order_id = order_data.get("client_order_id")
                        exchange_order_id = str(order_data.get("order_id", ""))
                        status = order_data.get("status", "")

                        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                        if tracked_order is not None:
                            new_state = CONSTANTS.ORDER_STATE.get(status)
                            if new_state is not None:
                                order_update = OrderUpdate(
                                    trading_pair=tracked_order.trading_pair,
                                    update_timestamp=float(order_data.get("updated_time", 0)) / 1000.0,
                                    new_state=new_state,
                                    client_order_id=client_order_id,
                                    exchange_order_id=exchange_order_id,
                                )
                                self._order_tracker.process_order_update(order_update=order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from ticker data.
        Args:
            exchange_info_dict: Ticker data dictionary from TICKER_ALL_PATH_URL
        Returns:
            List of trading rules
        """
        trading_rules = []
        try:
            tickers_data = exchange_info_dict.get("data", {})

            if isinstance(tickers_data, dict):
                for symbol, ticker_info in tickers_data.items():
                    try:
                        if "/" in symbol:
                            base, quote = symbol.split("/")
                        elif "-" in symbol:
                            base, quote = symbol.split("-")
                        else:
                            continue

                        trading_pair = f"{base}-{quote}"

                        base_precision = 8
                        quote_precision = 2 if quote == "INR" else 8

                        min_qty = Decimal("0.00000001")
                        min_notional = Decimal("1") if quote == "INR" else Decimal("0.0001")

                        step_size = Decimal(10) ** -base_precision
                        tick_size = Decimal(10) ** -quote_precision

                        trading_rules.append(
                            TradingRule(
                                trading_pair=trading_pair,
                                min_order_size=min_qty,
                                min_price_increment=tick_size,
                                min_base_amount_increment=step_size,
                                min_notional_size=min_notional,
                            )
                        )
                    except Exception as e:
                        _logger.debug(f"Error parsing trading rule for {symbol}: {e}")

        except Exception as e:
            _logger.error(f"Error formatting trading rules: {e}")

        return trading_rules

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Get all trade updates for an order.
        """
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                params = {
                    "order_id": order.exchange_order_id,
                }

                response = await self._api_get(
                    path_url=CONSTANTS.GET_ORDER_PATH_URL,
                    params=params,
                    is_auth_required=True,
                )

                if response and "data" in response:
                    order_data = response.get("data", {})
                    trades = order_data.get("trades", [])

                    for trade in trades:
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=trade.get("fee_asset", ""),
                            flat_fees=[TokenAmount(
                                amount=Decimal(str(trade.get("fee", 0))),
                                token=trade.get("fee_asset", "")
                            )]
                        )
                        trade_update = TradeUpdate(
                            trade_id=str(trade.get("trade_id", "")),
                            client_order_id=order.client_order_id,
                            exchange_order_id=str(order.exchange_order_id),
                            trading_pair=order.trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(str(trade.get("qty", 0))),
                            fill_quote_amount=Decimal(str(trade.get("qty", 0))) * Decimal(str(trade.get("price", 0))),
                            fill_price=Decimal(str(trade.get("price", 0))),
                            fill_timestamp=float(trade.get("time", 0)) / 1000.0,
                        )
                        trade_updates.append(trade_update)

            except Exception as e:
                _logger.error(f"Error fetching trade updates for order {order.exchange_order_id}: {e}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from the exchange.
        """
        try:
            params = {"order_id": tracked_order.exchange_order_id}

            response = await self._api_get(
                path_url=CONSTANTS.GET_ORDER_PATH_URL,
                params=params,
                is_auth_required=True,
            )

            if response and "data" in response:
                order_data = response.get("data", {})
                status_str = order_data.get("status", "")
                new_state = CONSTANTS.ORDER_STATE.get(status_str)

                order_update = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=str(tracked_order.exchange_order_id),
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=float(order_data.get("updated_time", 0)) / 1000.0,
                    new_state=new_state,
                )

                return order_update

        except Exception as e:
            _logger.error(f"Error requesting order status for {tracked_order.exchange_order_id}: {e}")

        return None

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get the last traded price for a trading pair.
        """
        try:
            params = {
                "exchange": self._exchange,
                "symbol": trading_pair
            }

            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL,
                params=params,
            )

            if response and "data" in response:
                ticker_data = response.get("data", {})
                if trading_pair.upper() in ticker_data:
                    ticker = ticker_data[trading_pair.upper()]
                    return float(ticker.get("lastPrice", 0))

        except Exception as e:
            _logger.error(f"Error fetching last traded price for {trading_pair}: {e}")

        return 0.0
