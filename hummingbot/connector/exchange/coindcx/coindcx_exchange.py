import asyncio
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coindcx import (
    coindcx_constants as CONSTANTS,
    coindcx_utils,
    coindcx_web_utils as web_utils,
)
from hummingbot.connector.exchange.coindcx.coindcx_api_order_book_data_source import CoinDCXAPIOrderBookDataSource
from hummingbot.connector.exchange.coindcx.coindcx_api_user_stream_data_source import CoinDCXAPIUserStreamDataSource
from hummingbot.connector.exchange.coindcx.coindcx_auth import CoinDCXAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _field(data: Dict[str, Any], short_key: str, long_key: str) -> Any:
    """A message field under its short stream key ("p") or, if that is missing, its long REST name ("price")."""
    value = data.get(short_key)
    return value if value is not None else data.get(long_key)


class CoindcxExchange(ExchangePyBase):
    """
    CoinDCX exchange connector implementation.
    Supports spot trading on CoinDCX exchange.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    # A fill is recorded once per trade id, so it must be recorded with its fee the first time. The stream host this
    # connector uses (CONSTANTS.WSS_URL) sends the fee ("f") in its trade updates; the older host sent only "T", "c",
    # "m", "o", "p", "q", "s" and "t" (both seen live on 28 Nov 2026). A fill the stream reports without a fee is read
    # from REST right away, which has it. If REST doesn't have the trade yet, it is read again after
    # STREAM_FILL_REST_DELAY seconds, then twice as long, up to STREAM_FILL_REST_ATTEMPTS reads; after that the
    # stream's fill is recorded with the estimated fee.
    STREAM_FILL_REST_ATTEMPTS = 3
    STREAM_FILL_REST_DELAY = 0.5
    # A failed network check (a REST call every 10 s) makes hummingbot stop and restart all of the connector's
    # networking: both streams, the order books and every polling loop. Through a proxy a single call can fail now and
    # then (28 Nov 2026: three restarts in three minutes), so while connected, a failure is reported only once this many
    # checks in a row have failed.
    NETWORK_CHECK_FAILURES_TO_REPORT = 2

    web_utils = web_utils

    def __init__(self,
                 coindcx_api_key: str,
                 coindcx_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 coindcx_proxy_url: str = "",
                 ):
        self.api_key = coindcx_api_key
        self.secret_key = coindcx_api_secret
        # Optional proxy for API keys bound to a whitelisted IP: REST and both Socket.IO streams go through it.
        # Set before super().__init__(), which builds the web assistants factory and the data sources.
        self._proxy_url = coindcx_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_coindcx_timestamp = 1.0
        # Each market's pair code ("I-ETH_INR", "B-BTC_USDT"), read with the symbol map: see exchange_pair_code.
        self._pair_code_for_trading_pair: Dict[str, str] = {}
        # Stream fills waiting to be recorded from REST: client order id -> trade id -> [the stream's fill, REST reads].
        self._stream_fills: Dict[str, Dict[str, list]] = {}
        self._stream_fill_tasks: Dict[str, asyncio.Task] = {}
        self._failed_network_checks = 0  # in a row: see NETWORK_CHECK_FAILURES_TO_REPORT
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def coindcx_order_type(order_type: OrderType) -> str:
        """
        Converts Hummingbot OrderType to CoinDCX order type string.
        """
        if order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def coindcx_side(trade_type: TradeType) -> str:
        """
        Converts Hummingbot TradeType to CoinDCX side string.
        """
        return CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

    @staticmethod
    def to_hb_order_type(coindcx_type: str) -> OrderType:
        """
        Converts CoinDCX order type string to Hummingbot OrderType.
        """
        if coindcx_type == CONSTANTS.ORDER_TYPE_MARKET:
            return OrderType.MARKET
        return OrderType.LIMIT

    @property
    def authenticator(self):
        """
        Get the authenticator instance for CoinDCX API.
        """
        return CoinDCXAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        """
        Get the name of this connector.
        """
        return "coindcx"

    @property
    def rate_limits_rules(self):
        """
        Get the rate limit rules for CoinDCX API.
        """
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        """
        Get the domain for this connector.
        """
        return self._domain

    @property
    def client_order_id_max_length(self):
        """
        Get the maximum length for client order IDs.
        """
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        """
        Get the prefix for client order IDs.
        """
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        """
        Get the API path for trading rules request.
        """
        return CONSTANTS.MARKETS_DETAILS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        """
        Get the API path for trading pairs request.
        """
        return CONSTANTS.MARKETS_DETAILS_PATH_URL

    @property
    def check_network_request_path(self):
        """
        Get the API path for checking network connectivity.
        """
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs(self):
        """
        Get the list of trading pairs for this connector.
        """
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """
        Check if cancel requests are synchronous on this exchange.
        """
        return True

    @property
    def is_trading_required(self) -> bool:
        """
        Check if trading functionality is required for this connector.
        """
        return self._trading_required

    @property
    def status_dict(self):
        """
        Override to add debug logging for connector ready state.
        """
        sd = {
            "symbols_mapping_initialized": self.trading_pair_symbol_map_ready(),
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or len(self._account_balances) > 0,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True,
            "user_stream_initialized": self._is_user_stream_initialized(),
        }
        self.logger().debug(f"status_dict: {sd}")
        return sd

    def supported_order_types(self):
        """
        Get the list of order types supported by this connector.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def start_network(self):
        """
        Start all required tasks and perform initial balance fetch to ensure connector becomes ready.
        """
        await super().start_network()
        if self.is_trading_required:
            try:
                await self._update_balances()
            except Exception as e:
                self.logger().warning(f"Failed to fetch initial balances: {e}")

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Returns the prices for all trading pairs.
        """
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_PATH_URL)
        return pairs_prices

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, str]]:
        all_tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return all_tickers
        filter_symbols = {tp.replace("-", "").upper() for tp in trading_pairs}
        found = {item.get("market", "").upper() for item in all_tickers if isinstance(item, dict)}
        for sym in filter_symbols:
            if sym not in found:
                self.logger().warning(f"Skipping {sym}: symbol not found on {self.name}")
        return [item for item in all_tickers if isinstance(item, dict) and item.get("market", "").upper() in filter_symbols]

    async def get_markets_details(self) -> List[Dict[str, Any]]:
        """
        Returns markets details (metadata) for all trading pairs.
        """
        markets = await self._api_get(path_url=CONSTANTS.MARKETS_DETAILS_PATH_URL)
        return markets

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """
        CoinDCX doesn't require strict time synchronization like Binance.
        """
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) or CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        exc_str = str(cancelation_exception)
        return (
            str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in exc_str
            or CONSTANTS.ORDER_NOT_EXIST_MESSAGE in exc_str
            or (str(CONSTANTS.INVALID_REQUEST_ERROR_CODE) in exc_str
                and CONSTANTS.INVALID_REQUEST_MESSAGE in exc_str)
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """
        Create a web assistants factory for making API requests.
        """
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """
        Create an order book data source for tracking order book updates.
        """
        return CoinDCXAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            proxy_url=self._proxy_url or None)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """
        Create a user stream data source for tracking account updates.
        """
        return CoinDCXAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
            proxy_url=self._proxy_url or None,
        )

    async def stop_network(self):
        """
        Stop the network tasks, then close the proxy session if this connector has one.
        """
        await super().stop_network()
        # A proxied factory owns its own aiohttp session. The default factory is a shared singleton and must be left
        # alone.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Failed closing the CoinDCX proxy connections factory.", exc_info=True)

    async def check_network(self) -> NetworkStatus:
        """
        Hummingbot's network check (a REST call), but a single failure while connected isn't reported: see
        NETWORK_CHECK_FAILURES_TO_REPORT. The call gets a second less than hummingbot allows the whole check, so that a
        slow call counts as a failure here instead of timing the check out.
        """
        try:
            await asyncio.wait_for(self._make_network_check_request(),
                                   timeout=max(1.0, self.check_network_timeout - 1.0))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._failed_network_checks += 1
            if (self.network_status is NetworkStatus.CONNECTED
                    and self._failed_network_checks < self.NETWORK_CHECK_FAILURES_TO_REPORT):
                self.logger().info(f"CoinDCX network check failed ({str(e) or type(e).__name__}); staying connected "
                                   f"unless the next check fails too.")
                return NetworkStatus.CONNECTED
            return NetworkStatus.NOT_CONNECTED
        self._failed_network_checks = 0
        return NetworkStatus.CONNECTED

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculate the trading fee for an order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Places an order on CoinDCX.

        CoinDCX order creation format:
        {
            "side": "buy",
            "order_type": "limit_order",
            "market": "SNTBTC",
            "price_per_unit": 0.03244,
            "total_quantity": 400,
            "timestamp": timeStamp,
            "client_order_id": "2022.02.14-btcinr_1"
        }
        """
        order_result = None
        type_str = CoindcxExchange.coindcx_order_type(order_type)
        side_str = CoindcxExchange.coindcx_side(trade_type)
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        api_params = {
            "market": symbol,
            "side": side_str,
            "total_quantity": float(amount),
            "order_type": type_str,
            "client_order_id": order_id
        }

        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            api_params["price_per_unit"] = float(price)

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)

            if isinstance(order_result, dict) and "orders" in order_result:
                order_data = order_result["orders"][0]
            elif isinstance(order_result, list):
                order_data = order_result[0]
            else:
                order_data = order_result

            o_id = str(order_data.get("id", order_id))
            created_at = order_data.get("created_at", "")
            if isinstance(created_at, (int, float)):
                transact_time = created_at / 1e3
            else:
                transact_time = self._time_synchronizer.time()

        except IOError as e:
            error_description = str(e)
            is_server_overloaded = "status is 503" in error_description
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise

        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancels an order on CoinDCX.

        CoinDCX cancel order format:
        {
            "id": "ead19992-43fd-11e8-b027-bb815bcb14ed",
            "timestamp": timestamp
        }
        """
        api_params = {}

        if tracked_order.exchange_order_id:
            api_params["id"] = tracked_order.exchange_order_id
        else:
            api_params["client_order_id"] = order_id

        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)

        if cancel_result is not None:
            return True
        return False

    @property
    def is_edit_order_supported_by_exchange(self) -> bool:
        """CoinDCX supports native order price editing via POST /exchange/v1/orders/edit."""
        return True

    async def _place_edit(
        self,
        client_order_id: str,
        exchange_order_id: str,
        trading_pair: str,
        new_price: Decimal,
        new_amount: Decimal,
        **kwargs
    ) -> Tuple[str, str, float]:
        """
        Calls the CoinDCX native order-edit endpoint.

        Returns (new_client_order_id, new_exchange_order_id, update_timestamp).
        The client_order_id stays the same on CoinDCX native edits.
        """
        api_params = {
            "id": exchange_order_id,
            "price_per_unit": float(new_price),
        }
        edit_result = await self._api_post(
            path_url=CONSTANTS.ORDER_EDIT_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )
        if isinstance(edit_result, list):
            order_data = edit_result[0]
        else:
            order_data = edit_result
        new_exchange_id = str(order_data.get("id", exchange_order_id))
        updated_at = order_data.get("updated_at", "")
        if isinstance(updated_at, (int, float)):
            timestamp = updated_at / 1e3
        else:
            timestamp = self._time_synchronizer.time()
        return client_order_id, new_exchange_id, timestamp

    async def _update_trading_rules(self):
        """Override to add logging for trading rules update."""
        exchange_info = await self._make_trading_rules_request()
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Formats trading rules from CoinDCX markets_details response.

        CoinDCX market details format:
        {
            "coindcx_name": "SNMBTC",
            "base_currency_short_name": "BTC",
            "target_currency_short_name": "SNM",
            "min_quantity": 1,
            "max_quantity": 90000000,
            "min_price": 5.66e-7,
            "max_price": 0.0000566,
            "min_notional": 0.001,
            "base_currency_precision": 8,
            "target_currency_precision": 0,
            "step": 1,
            "status": "active"
        }
        """
        trading_pair_rules = exchange_info_dict if isinstance(exchange_info_dict, list) else [exchange_info_dict]
        retval = []

        for rule in filter(coindcx_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                symbol = rule.get("symbol", rule.get("coindcx_name", ""))
                try:
                    trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                except KeyError:
                    # A second market for a pair the symbol map gives to another market (see
                    # _initialize_trading_pair_symbols_from_exchange_info): its rules would replace the right ones.
                    continue
                except ValueError:
                    base = rule.get("base_currency_short_name", "")
                    target = rule.get("target_currency_short_name", "")
                    if base and target:
                        trading_pair = f"{target}-{base}"
                    else:
                        trading_pair = coindcx_utils.coindcx_pair_to_hb_pair(symbol)

                base_precision = int(rule.get("base_currency_precision", 8))
                price_increment = Decimal(10) ** (-base_precision)

                target_precision = int(rule.get("target_currency_precision", 8))
                quantity_increment = Decimal(10) ** (-target_precision)
                step_size = Decimal(str(rule.get("step", 1)))

                if quantity_increment <= 0:
                    quantity_increment = Decimal("1e-8")

                if step_size > 0:
                    quantity_increment = step_size
                elif step_size <= 0 and quantity_increment <= 0:
                    quantity_increment = Decimal("1e-8")

                min_order_size = Decimal(str(rule.get("min_quantity", 0)))
                max_order_size = Decimal(str(rule.get("max_quantity", 1e9)))
                min_notional = Decimal(str(rule.get("min_notional", 0)))

                if min_order_size <= 0:
                    min_order_size = quantity_increment if quantity_increment > 0 else Decimal("1e-8")

                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_order_size,
                        max_order_size=max_order_size,
                        min_price_increment=price_increment,
                        min_base_amount_increment=quantity_increment,
                        min_notional_size=min_notional
                    )
                )
            except Exception as e:
                self.logger().warning(f"Error parsing trading pair rule {rule.get('symbol', rule.get('coindcx_name', ''))}: {e}")

        return retval

    async def _status_polling_loop_fetch_updates(self):
        """
        Fetch account and order updates during status polling loop.
        """
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update trading fees from the exchange.
        """
        pass

    async def _user_stream_event_listener(self):
        """
        Processes the events received from the exchange through the user stream.
        Events include balance updates, order updates, and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event")

                if event_type == CONSTANTS.ORDER_UPDATE_EVENT_TYPE:
                    if "data" not in event_message:
                        self.logger().warning(f"Order update event missing 'data' field: {event_message}")
                        continue
                    order_data = event_message.get("data")

                    if isinstance(order_data, str):
                        try:
                            order_data = json.loads(order_data)
                        except json.JSONDecodeError as e:
                            self.logger().warning(f"Failed to parse order update data: {e}")
                            continue

                    if isinstance(order_data, list):
                        for order in order_data:
                            await self._process_order_update(order)
                    else:
                        await self._process_order_update(order_data)

                elif event_type == CONSTANTS.TRADE_UPDATE_EVENT_TYPE:
                    if "data" not in event_message:
                        self.logger().warning(f"Trade update event missing 'data' field: {event_message}")
                        continue
                    trade_data = event_message.get("data")

                    if isinstance(trade_data, str):
                        try:
                            trade_data = json.loads(trade_data)
                        except json.JSONDecodeError as e:
                            self.logger().warning(f"Failed to parse trade update data: {e}")
                            continue

                    if isinstance(trade_data, list):
                        for trade in trade_data:
                            await self._process_trade_update(trade)
                    else:
                        await self._process_trade_update(trade_data)

                elif event_type == CONSTANTS.BALANCE_UPDATE_EVENT_TYPE:
                    if "data" not in event_message:
                        self.logger().warning(f"Balance update event missing 'data' field: {event_message}")
                        continue
                    balance_data = event_message.get("data")

                    if isinstance(balance_data, str):
                        try:
                            balance_data = json.loads(balance_data)
                        except json.JSONDecodeError as e:
                            self.logger().warning(f"Failed to parse balance update data: {e}")
                            continue

                    if isinstance(balance_data, list):
                        for balance in balance_data:
                            self._process_balance_update(balance)
                    else:
                        self._process_balance_update(balance_data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_order_update(self, order_data: Dict[str, Any]):
        """
        Processes an order update message from the user stream.
        """
        if not isinstance(order_data, dict):
            return

        client_order_id = order_data.get("client_order_id", "")
        exchange_order_id = str(order_data.get("id", ""))

        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is None and exchange_order_id:
            tracked_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is not None:
            client_order_id = tracked_order.client_order_id
            status = order_data.get("status", "")
            new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)
            # Milliseconds per CoinDCX's docs, which also show ISO text for the same field elsewhere.
            update_time = coindcx_utils.timestamp_in_seconds(order_data.get("updated_at"))

            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=update_time or self._time_synchronizer.time(),
                new_state=new_state,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
            )
            self._order_tracker.process_order_update(order_update=order_update)

    async def _process_trade_update(self, trade_data: Dict[str, Any]):
        """
        Processes a trade-update message from the user stream: a fill of an order.

        CoinDCX sends it with short keys: "o" order id, "c" client order id, "t" trade id, "s" symbol, "p" price,
        "q" quantity, "T" time in ms, "m" buyer is maker, and on the documented stream host also "f" fee, "e" exchange
        code and "x" status. The long names of the REST trade history ("order_id", "price", ...) are read too.
        A fill with a fee is recorded at once; one without is recorded from REST (see STREAM_FILL_REST_ATTEMPTS).
        Fills of orders this connector doesn't track are ignored.
        """
        if not isinstance(trade_data, dict):
            return

        client_order_id = str(_field(trade_data, "c", "client_order_id") or "")
        exchange_order_id = str(_field(trade_data, "o", "order_id") or "")

        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked_order is None and exchange_order_id:
            tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is None:
            return

        fee_amount = _field(trade_data, "f", "fee_amount")
        if fee_amount is not None:
            self._order_tracker.process_trade_update(
                self._trade_update_from_stream(tracked_order, trade_data, Decimal(str(fee_amount))))
            return
        trade_id = str(_field(trade_data, "t", "id") or "")
        if trade_id in tracked_order.order_fills:
            return
        self._stream_fills.setdefault(tracked_order.client_order_id, {})[trade_id] = [trade_data, 0]
        task = self._stream_fill_tasks.get(tracked_order.client_order_id)
        if task is None or task.done():
            self._stream_fill_tasks[tracked_order.client_order_id] = safe_ensure_future(
                self._record_stream_fills(tracked_order))

    async def _record_stream_fills(self, order: InFlightOrder):
        """
        Records from REST, with their fees, the fills the stream reported for this order without one; see
        STREAM_FILL_REST_ATTEMPTS. Fills the stream reports while this runs are taken in as well.
        """
        pending = self._stream_fills.setdefault(order.client_order_id, {})
        delay = self.STREAM_FILL_REST_DELAY
        while pending:
            for trade_update in await self._all_trade_updates_for_order(order):
                self._order_tracker.process_trade_update(trade_update)
            for trade_id, entry in list(pending.items()):
                entry[1] += 1
                if trade_id in order.order_fills:
                    del pending[trade_id]
                elif entry[1] >= self.STREAM_FILL_REST_ATTEMPTS:
                    self.logger().warning(
                        f"Fill {trade_id} of order {order.client_order_id} isn't in CoinDCX's trade history after "
                        f"{entry[1]} reads; recording it from the stream with an estimated fee.")
                    self._order_tracker.process_trade_update(self._trade_update_from_stream(order, entry[0], None))
                    del pending[trade_id]
            if pending:
                await self._sleep(delay)
                delay = min(delay * 2, 4 * self.STREAM_FILL_REST_DELAY)
        self._stream_fills.pop(order.client_order_id, None)
        self._stream_fill_tasks.pop(order.client_order_id, None)

    def _quote_fee(self, order: InFlightOrder, fee_amount: Decimal) -> TradeFeeBase:
        """A fill's fee as CoinDCX reports it: in the quote currency (live, 28 Nov 2026: 0.05% of the INR value)."""
        return TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=fee_amount, token=order.quote_asset)])

    def _trade_update_from_stream(self, order: InFlightOrder, trade_data: Dict[str, Any],
                                  fee_amount: Optional[Decimal]) -> TradeUpdate:
        """A trade-update message as a fill of the order; with the estimated fee if fee_amount is None."""
        price = Decimal(str(_field(trade_data, "p", "price") or 0))
        amount = Decimal(str(_field(trade_data, "q", "quantity") or 0))
        if fee_amount is None:
            fee = self._get_fee(order.base_asset, order.quote_asset, order.order_type, order.trade_type, amount, price)
        else:
            fee = self._quote_fee(order, fee_amount)
        fill_time = coindcx_utils.timestamp_in_seconds(_field(trade_data, "T", "timestamp"))
        return TradeUpdate(
            trade_id=str(_field(trade_data, "t", "id") or ""),
            client_order_id=order.client_order_id,
            exchange_order_id=str(_field(trade_data, "o", "order_id") or "") or order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=amount,
            fill_quote_amount=amount * price,
            fill_price=price,
            fill_timestamp=fill_time or self._time_synchronizer.time(),
        )

    def _trade_update_from_rest(self, order: InFlightOrder, trade: Dict[str, Any]) -> TradeUpdate:
        """A trade from the REST trade history as a fill of the order."""
        price = Decimal(str(trade.get("price", 0)))
        amount = Decimal(str(trade.get("quantity", 0)))
        fill_time = coindcx_utils.timestamp_in_seconds(trade.get("timestamp"))
        return TradeUpdate(
            trade_id=str(trade.get("id", "")),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=self._quote_fee(order, Decimal(str(trade.get("fee_amount", 0)))),
            fill_base_amount=amount,
            fill_quote_amount=amount * price,
            fill_price=price,
            fill_timestamp=fill_time or self._time_synchronizer.time(),
        )

    @staticmethod
    def _trade_history_request(orders: List[InFlightOrder], symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        A trade history request for these orders' fills: the newest first, from a minute before the oldest of them
        was created. CoinDCX returns the oldest trades first unless asked otherwise, so on an account with many trades
        a plain request never reaches the new ones.
        """
        request: Dict[str, Any] = {"limit": 100, "sort": "desc"}
        created = [order.creation_timestamp for order in orders if order.creation_timestamp]
        if created:
            request["from_timestamp"] = int((min(created) - 60) * 1000)
        if symbol:
            request["symbol"] = symbol
        return request

    async def _sync_after_user_stream_gap(self):
        """
        Reads over REST what the user stream may have missed while it reconnected: the balances, and the status and
        fills of the orders this connector tracks. CoinDCX doesn't resend stream messages, and while the stream is up
        the regular REST check runs only every couple of minutes.
        """
        try:
            await self._update_balances()
            await self._update_order_status()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Could not read the account over REST after the user stream reconnected: {e}")

    def _process_balance_update(self, balance_data: Dict[str, Any]):
        """
        Processes a balance update message from the user stream.
        """
        if not isinstance(balance_data, dict):
            return

        asset_name = balance_data.get("currency_short_name", "") or balance_data.get("currency", "")
        free_balance = Decimal(str(balance_data.get("balance", 0)))
        locked_balance = Decimal(str(balance_data.get("locked_balance", 0)))
        total_balance = free_balance + locked_balance

        self._account_available_balances[asset_name] = free_balance
        self._account_balances[asset_name] = total_balance

    async def _update_order_fills_from_trades(self):
        """
        Backup measure to get filled events with trade ID for orders,
        in case CoinDCX's user stream events are not working.
        """
        small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
        long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

        fillable = list(self._order_tracker.all_fillable_orders.values())
        if fillable and (long_interval_current_tick > long_interval_last_tick
                         or (self.in_flight_orders and small_interval_current_tick > small_interval_last_tick)):

            self._last_trades_poll_coindcx_timestamp = self._time_synchronizer.time()

            try:
                trades = await self._api_post(
                    path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL,
                    data=self._trade_history_request(fillable),
                    is_auth_required=True
                )
                trades_by_order_id: Dict[str, List[Dict[str, Any]]] = {}
                for trade in trades if isinstance(trades, list) else []:
                    trades_by_order_id.setdefault(str(trade.get("order_id", "")), []).append(trade)
                for tracked_order in fillable:
                    for trade in trades_by_order_id.get(tracked_order.exchange_order_id, []):
                        self._order_tracker.process_trade_update(self._trade_update_from_rest(tracked_order, trade))

            except Exception as e:
                self.logger().error(f"Error fetching trade history: {e}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        The order's fills, from the REST trade history.
        """
        if order.exchange_order_id is None:
            return []
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            trades = await self._api_post(
                path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL,
                data=self._trade_history_request([order], symbol),
                is_auth_required=True
            )
        except Exception as e:
            self.logger().error(f"Error fetching trades for order: {e}")
            return []
        return [self._trade_update_from_rest(order, trade) for trade in (trades if isinstance(trades, list) else [])
                if str(trade.get("order_id", "")) == order.exchange_order_id]

    async def get_all_account_trades(self, start_time: Optional[int] = None,
                                     end_time: Optional[int] = None,
                                     limit: int = 100,
                                     trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fetches raw fills for this account directly from CoinDCX's account-wide
        trade history endpoint.
        """
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["from_timestamp"] = start_time
        if end_time is not None:
            params["to_timestamp"] = end_time

        try:
            trades = await self._api_post(
                path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL,
                data=params,
                is_auth_required=True,
            )
            return trades if isinstance(trades, list) else []
        except Exception as e:
            self.logger().error(f"Error fetching account-wide trade history: {e}")
            return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Requests the status of an order from CoinDCX.

        CoinDCX order status request format:
        {
            "id": "ead19992-43fd-11e8-b027-bb815bcb14ed",
            "timestamp": timestamp
        }
        """
        api_params = {}

        if tracked_order.exchange_order_id:
            api_params["id"] = tracked_order.exchange_order_id
        else:
            api_params["client_order_id"] = tracked_order.client_order_id

        updated_order_data = await self._api_post(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            data=api_params,
            is_auth_required=True)

        status = updated_order_data.get("status", "")
        new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)
        update_time = coindcx_utils.timestamp_in_seconds(updated_order_data.get("updated_at"))

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data.get("id", "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_time or self._time_synchronizer.time(),
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        """
        Fetches and updates account balances from CoinDCX.

        CoinDCX balances response format:
        [
            {
                "currency": "BTC",
                "balance": 1.167,
                "locked_balance": 2.1
            }
        ]
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_post(
            path_url=CONSTANTS.USER_BALANCES_PATH_URL,
            data={},
            is_auth_required=True)

        if isinstance(account_info, list):
            balances = account_info
        else:
            balances = account_info.get("balances", [])

        for balance_entry in balances:
            asset_name = balance_entry.get("currency", "")
            free_balance = Decimal(str(balance_entry.get("balance", 0)))
            locked_balance = Decimal(str(balance_entry.get("locked_balance", 0)))
            total_balance = free_balance + locked_balance

            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initializes the trading pair symbol map from exchange info.

        CoinDCX market details format:
        {
            "coindcx_name": "SNMBTC",
            "symbol": "SNMBTC",
            "base_currency_short_name": "BTC",
            "target_currency_short_name": "SNM",
            ...
        }

        CoinDCX can list one pair on two routes: on 27 Nov 2026, for example, ASR-USDT was both "ASRUSDT" (ecode B)
        and "ASR_USDT" (ecode G). A trading pair maps to one symbol only (a duplicate used to make the whole map
        fail, so the connector never got ready), so the plain BASEQUOTE symbol wins, otherwise the first one listed.

        Each chosen market's pair code (its "pair" field, e.g. "I-ETH_INR") is kept too: see exchange_pair_code.
        """
        symbol_for_pair: Dict[str, str] = {}
        pair_code_for_pair: Dict[str, str] = {}
        markets_list = exchange_info if isinstance(exchange_info, list) else [exchange_info]

        for symbol_data in filter(coindcx_utils.is_exchange_information_valid, markets_list):
            symbol = symbol_data.get("symbol", symbol_data.get("coindcx_name", ""))
            base = symbol_data.get("target_currency_short_name", "")
            quote = symbol_data.get("base_currency_short_name", "")

            if symbol and base and quote:
                trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                chosen = symbol_for_pair.get(trading_pair)
                if chosen is None or (symbol == f"{base}{quote}" and chosen != f"{base}{quote}"):
                    symbol_for_pair[trading_pair] = symbol
                    pair_code_for_pair.pop(trading_pair, None)
                    if symbol_data.get("pair"):
                        pair_code_for_pair[trading_pair] = symbol_data["pair"]

        self._pair_code_for_trading_pair = pair_code_for_pair
        self._set_trading_pair_symbol_map(bidict({symbol: pair for pair, symbol in symbol_for_pair.items()}))

    async def exchange_pair_code(self, trading_pair: str) -> str:
        """
        CoinDCX's code for a pair's market, e.g. "I-ETH_INR" or "B-BTC_USDT": what its order book endpoint and its
        stream channels take. It is the market's "pair" field in markets_details, as CoinDCX's docs say to use. For a
        market without one, the usual code is guessed (coindcx_utils.default_pair_code).
        """
        await self.trading_pair_symbol_map()  # reads markets_details, pair codes included, if not done yet
        return self._pair_code_for_trading_pair.get(trading_pair) or coindcx_utils.default_pair_code(trading_pair)

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        Each pair's last traded price, from one read of CoinDCX's ticker. A pair the ticker has no price for is left
        out. (The markets list, which this used to read, has no prices: it gave 0 for every pair.)
        """
        tickers = await self.get_all_pairs_prices()
        last_price_for_symbol = {
            ticker.get("market"): ticker.get("last_price")
            for ticker in (tickers if isinstance(tickers, list) else []) if isinstance(ticker, dict)
        }
        prices: Dict[str, float] = {}
        for trading_pair in trading_pairs:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            try:
                prices[trading_pair] = float(last_price_for_symbol[symbol])
            except (KeyError, TypeError, ValueError):
                continue
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        The pair's last traded price, from CoinDCX's ticker. Raises KeyError if the ticker has none for it.
        """
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]
