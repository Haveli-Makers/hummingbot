import asyncio
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
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CoindcxExchange(ExchangePyBase):
    """
    CoinDCX exchange connector implementation.
    Supports spot trading on CoinDCX exchange.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 coindcx_api_key: str,
                 coindcx_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = coindcx_api_key
        self.secret_key = coindcx_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_coindcx_timestamp = 1.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def coindcx_order_type(order_type: OrderType) -> str:
        """
        Converts Hummingbot OrderType to CoinDCX order type string.
        """
        if order_type == OrderType.LIMIT or order_type == OrderType.LIMIT_MAKER:
            return CONSTANTS.ORDER_TYPE_LIMIT
        elif order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET
        return CONSTANTS.ORDER_TYPE_LIMIT

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
        return CoinDCXAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "coindcx"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS_DETAILS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_DETAILS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Returns the prices for all trading pairs.
        """
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_PATH_URL)
        return pairs_prices

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
        # Consider only 404/Order not found as "order not found" during cancel
        exc_str = str(cancelation_exception)
        return (
            str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in exc_str
            or CONSTANTS.ORDER_NOT_EXIST_MESSAGE in exc_str
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinDCXAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinDCXAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
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
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        api_params = {
            "market": symbol,
            "side": side_str,
            "total_quantity": float(amount),
            "order_type": type_str,
            "client_order_id": order_id
        }

        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            api_params["price_per_unit"] = float(price)

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)

            # CoinDCX returns orders in a list
            if isinstance(order_result, dict) and "orders" in order_result:
                order_data = order_result["orders"][0]
            elif isinstance(order_result, list):
                order_data = order_result[0]
            else:
                order_data = order_result

            o_id = str(order_data.get("id", order_id))
            # Parse timestamp from created_at field
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
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=rule.get("symbol", rule.get("coindcx_name", ""))
                )

                min_order_size = Decimal(str(rule.get("min_quantity", 0)))
                max_order_size = Decimal(str(rule.get("max_quantity", 1e9)))
                step_size = Decimal(str(rule.get("step", 1)))
                min_notional = Decimal(str(rule.get("min_notional", 0)))

                if min_order_size <= 0:
                    raise ValueError("Invalid min_order_size")

                base_precision = int(rule.get("base_currency_precision", 8))
                price_increment = Decimal(10) ** (-base_precision)

                target_precision = int(rule.get("target_currency_precision", 8))
                quantity_increment = Decimal(10) ** (-target_precision)
                if step_size > 0:
                    quantity_increment = step_size

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
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")

        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange.
        CoinDCX typically charges 0.1% maker and 0.1% taker fees.
        """
        pass

    async def _user_stream_event_listener(self):
        """
        Processes the events received from the exchange through the user stream.
        Events include balance updates, order updates, and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event", event_message.get("e", ""))

                if event_type in ["order-update", CONSTANTS.ORDER_UPDATE_EVENT_TYPE]:
                    order_data = event_message.get("data", event_message)

                    if isinstance(order_data, list):
                        for order in order_data:
                            await self._process_order_update(order)
                    else:
                        await self._process_order_update(order_data)

                elif event_type in ["trade-update", CONSTANTS.TRADE_UPDATE_EVENT_TYPE]:
                    trade_data = event_message.get("data", event_message)

                    if isinstance(trade_data, list):
                        for trade in trade_data:
                            await self._process_trade_update(trade)
                    else:
                        await self._process_trade_update(trade_data)

                elif event_type in ["balance-update", CONSTANTS.BALANCE_UPDATE_EVENT_TYPE]:
                    balance_data = event_message.get("data", event_message)

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
        client_order_id = order_data.get("client_order_id", order_data.get("c", ""))
        exchange_order_id = str(order_data.get("id", order_data.get("o", "")))

        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is not None:
            status = order_data.get("status", "")
            new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)

            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=order_data.get("updated_at", self._time_synchronizer.time() * 1000) / 1000,
                new_state=new_state,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
            )
            self._order_tracker.process_order_update(order_update=order_update)

    async def _process_trade_update(self, trade_data: Dict[str, Any]):
        """
        Processes a trade update message from the user stream.
        """
        client_order_id = trade_data.get("c", trade_data.get("client_order_id", ""))
        exchange_order_id = str(trade_data.get("o", trade_data.get("order_id", "")))

        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked_order is not None:
            fee_amount = Decimal(str(trade_data.get("f", trade_data.get("fee_amount", 0))))

            trading_pair = tracked_order.trading_pair
            base, quote = trading_pair.split("-")
            fee_token = quote

            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=tracked_order.trade_type,
                flat_fees=[TokenAmount(amount=fee_amount, token=fee_token)]
            )

            fill_price = Decimal(str(trade_data.get("p", trade_data.get("price", 0))))
            fill_amount = Decimal(str(trade_data.get("q", trade_data.get("quantity", 0))))

            trade_update = TradeUpdate(
                trade_id=str(trade_data.get("t", trade_data.get("id", ""))),
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fee=fee,
                fill_base_amount=fill_amount,
                fill_quote_amount=fill_amount * fill_price,
                fill_price=fill_price,
                fill_timestamp=trade_data.get("T", trade_data.get("timestamp", self._time_synchronizer.time() * 1000)) / 1000,
            )
            self._order_tracker.process_trade_update(trade_update)

    def _process_balance_update(self, balance_data: Dict[str, Any]):
        """
        Processes a balance update message from the user stream.
        """
        asset_name = balance_data.get("currency", balance_data.get("a", ""))
        free_balance = Decimal(str(balance_data.get("balance", balance_data.get("f", 0))))
        locked_balance = Decimal(str(balance_data.get("locked_balance", balance_data.get("l", 0))))
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

        if (long_interval_current_tick > long_interval_last_tick
                or (self.in_flight_orders and small_interval_current_tick > small_interval_last_tick)):

            self._last_trades_poll_coindcx_timestamp = self._time_synchronizer.time()

            # Fetch trade history
            try:
                trade_history_params = {
                    "limit": 100
                }

                trades = await self._api_post(
                    path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL,
                    data=trade_history_params,
                    is_auth_required=True
                )

                if trades:
                    for trade in trades:
                        order_id = str(trade.get("order_id", ""))

                        for tracked_order in self._order_tracker.all_fillable_orders.values():
                            if tracked_order.exchange_order_id == order_id:
                                fee_amount = Decimal(str(trade.get("fee_amount", 0)))
                                trading_pair = tracked_order.trading_pair
                                base, quote = trading_pair.split("-")

                                fee = TradeFeeBase.new_spot_fee(
                                    fee_schema=self.trade_fee_schema(),
                                    trade_type=tracked_order.trade_type,
                                    flat_fees=[TokenAmount(amount=fee_amount, token=quote)]
                                )

                                trade_update = TradeUpdate(
                                    trade_id=str(trade.get("id", "")),
                                    client_order_id=tracked_order.client_order_id,
                                    exchange_order_id=order_id,
                                    trading_pair=trading_pair,
                                    fee=fee,
                                    fill_base_amount=Decimal(str(trade.get("quantity", 0))),
                                    fill_quote_amount=Decimal(str(trade.get("quantity", 0))) * Decimal(str(trade.get("price", 0))),
                                    fill_price=Decimal(str(trade.get("price", 0))),
                                    fill_timestamp=trade.get("timestamp", self._time_synchronizer.time() * 1000) / 1000,
                                )
                                self._order_tracker.process_trade_update(trade_update)
                                break

            except Exception as e:
                self.logger().error(f"Error fetching trade history: {e}")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Retrieves all trade updates for a specific order.
        """
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                trade_history_params = {
                    "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair),
                    "limit": 100
                }

                all_fills_response = await self._api_post(
                    path_url=CONSTANTS.TRADE_HISTORY_ACCOUNT_PATH_URL,
                    data=trade_history_params,
                    is_auth_required=True
                )

                for trade in all_fills_response:
                    if str(trade.get("order_id", "")) == order.exchange_order_id:
                        fee_amount = Decimal(str(trade.get("fee_amount", 0)))
                        trading_pair = order.trading_pair
                        base, quote = trading_pair.split("-")

                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            flat_fees=[TokenAmount(amount=fee_amount, token=quote)]
                        )

                        trade_update = TradeUpdate(
                            trade_id=str(trade.get("id", "")),
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(str(trade.get("quantity", 0))),
                            fill_quote_amount=Decimal(str(trade.get("quantity", 0))) * Decimal(str(trade.get("price", 0))),
                            fill_price=Decimal(str(trade.get("price", 0))),
                            fill_timestamp=trade.get("timestamp", self._time_synchronizer.time() * 1000) / 1000,
                        )
                        trade_updates.append(trade_update)

            except Exception as e:
                self.logger().error(f"Error fetching trades for order: {e}")

        return trade_updates

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

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data.get("id", "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data.get("updated_at", self._time_synchronizer.time() * 1000) / 1000,
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
        """
        mapping = bidict()
        markets_list = exchange_info if isinstance(exchange_info, list) else [exchange_info]

        for symbol_data in filter(coindcx_utils.is_exchange_information_valid, markets_list):
            symbol = symbol_data.get("symbol", symbol_data.get("coindcx_name", ""))
            base = symbol_data.get("target_currency_short_name", "")
            quote = symbol_data.get("base_currency_short_name", "")

            if symbol and base and quote:
                trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                mapping[symbol] = trading_pair

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Gets the last traded price for a trading pair.
        """
        try:
            exchange_info = await self._api_get(
                path_url=CONSTANTS.MARKETS_DETAILS_PATH_URL
            )

            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

            if isinstance(exchange_info, list):
                for market in exchange_info:
                    if market.get("symbol", market.get("coindcx_name", "")) == symbol:
                        last_price = market.get("last_price", market.get("last", 0))
                        return float(last_price)

            return 0.0
        except Exception as e:
            self.logger().error(f"Error getting last traded price: {e}")
            return 0.0
