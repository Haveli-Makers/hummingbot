import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.wazirx import (
    wazirx_constants as CONSTANTS,
    wazirx_utils,
    wazirx_web_utils as web_utils,
)
from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import WazirxAPIUserStreamDataSource
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class WazirxExchange(ExchangePyBase):
    """
    WazirX exchange connector (spot).
    Minimal implementation covering REST trading (place/cancel/get orders), balances, order book snapshots and a polling user-stream fallback.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 wazirx_api_key: str,
                 wazirx_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = wazirx_api_key
        self.secret_key = wazirx_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def authenticator(self):
        return WazirxAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "wazirx"

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
        return CONSTANTS.TICKERS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.TICKERS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.TICKERS_PATH_URL

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
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "Order does not exist" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "Order does not exist" in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return WazirxAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return WazirxAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        symbol = trading_pair.replace("-", "").lower()
        data = {
            "symbol": symbol,
            "side": trade_type.name.lower(),
            "type": order_type.name.lower(),
            "quantity": f"{amount:f}",
        }
        if order_type.is_limit_type():
            data["price"] = f"{price:f}"

        # Use web assistant for signed request in production. Here perform simple POST.
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.CREATE_ORDER_PATH_URL}"
        async with self._web_assistants_factory.build_rest_assistant() as ra:
            try:
                resp = await ra.execute_request(url=url, method="POST", json=data)
                order_id = str(resp.get("orderId", ""))
                executed_amount = float(resp.get("executedQty", 0))
                return order_id, executed_amount
            except Exception as e:
                raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = tracked_order.trading_pair.replace("-", "").lower()
        api_params = {
            "symbol": symbol,
            "orderId": order_id,
        }
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.CANCEL_ORDER_PATH_URL}"
        async with self._web_assistants_factory.build_rest_assistant() as ra:
            try:
                resp = await ra.execute_request(url=url, method="DELETE", params=api_params)
                return resp.get("status") == "CANCELED"
            except Exception as e:
                return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info
        """
        trading_pair_rules = exchange_info_dict.get("symbols", [])
        retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = rule.get("symbol", "").upper()
                if not trading_pair:
                    continue
                
                # Convert to hummingbot format
                base_asset = rule.get("baseAsset", "")
                quote_asset = rule.get("quoteAsset", "")
                if base_asset and quote_asset:
                    hb_trading_pair = f"{base_asset}-{quote_asset}"
                else:
                    continue

                filters = rule.get("filters", [])
                price_filter = next((f for f in filters if f.get("filterType") == "PRICE_FILTER"), {})
                lot_size_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), {})
                min_notional_filter = next((f for f in filters if f.get("filterType") in ["MIN_NOTIONAL", "NOTIONAL"]), {})

                min_order_size = Decimal(lot_size_filter.get("minQty", "0"))
                tick_size = Decimal(price_filter.get("tickSize", "0"))
                step_size = Decimal(lot_size_filter.get("stepSize", "0"))
                min_notional = Decimal(min_notional_filter.get("minNotional", "0"))

                retval.append(
                    TradingRule(hb_trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=tick_size,
                                min_base_amount_increment=step_size,
                                min_notional_size=min_notional))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                # Process WazirX user stream events
                event_type = event_message.get("event")
                
                if event_type == "orderUpdate":
                    # Handle order updates
                    order_data = event_message.get("order", {})
                    client_order_id = order_data.get("clientOrderId")
                    
                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message.get("timestamp", 0) / 1000,
                            new_state=CONSTANTS.ORDER_STATE.get(order_data.get("status"), OrderState.UNKNOWN),
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_data.get("orderId", "")),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "balanceUpdate":
                    # Handle balance updates
                    balance_data = event_message.get("balance", {})
                    asset_name = balance_data.get("asset")
                    free_balance = Decimal(balance_data.get("free", "0"))
                    total_balance = Decimal(balance_data.get("total", "0"))
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            symbol = order.trading_pair.replace("-", "").lower()
            url = f"{CONSTANTS.REST_URL}/api/v3/myTrades"
            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id
            }
            
            async with self._web_assistants_factory.build_rest_assistant() as ra:
                try:
                    resp = await ra.execute_request(url=url, method="GET", params=params)
                    
                    for trade in resp:
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=trade.get("commissionAsset", ""),
                            flat_fees=[TokenAmount(amount=Decimal(trade.get("commission", "0")), 
                                                  token=trade.get("commissionAsset", ""))]
                        )
                        trade_update = TradeUpdate(
                            trade_id=str(trade.get("id", "")),
                            client_order_id=order.client_order_id,
                            exchange_order_id=str(trade.get("orderId", "")),
                            trading_pair=order.trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade.get("qty", "0")),
                            fill_quote_amount=Decimal(trade.get("quoteQty", "0")),
                            fill_price=Decimal(trade.get("price", "0")),
                            fill_timestamp=trade.get("time", 0) / 1000,
                        )
                        trade_updates.append(trade_update)
                except Exception as e:
                    self.logger().error(f"Error fetching trade updates for order {order.client_order_id}: {e}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = tracked_order.trading_pair.replace("-", "").lower()
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_STATUS_PATH_URL}"
        params = {
            "symbol": symbol,
            "orderId": tracked_order.exchange_order_id
        }
        
        async with self._web_assistants_factory.build_rest_assistant() as ra:
            resp = await ra.execute_request(url=url, method="GET", params=params)
            
            new_state = CONSTANTS.ORDER_STATE.get(resp.get("status"), OrderState.UNKNOWN)
            
            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(resp.get("orderId", "")),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=resp.get("updateTime", 0) / 1000,
                new_state=new_state,
            )

            return order_update

    async def _update_balances(self):
        url = f"{CONSTANTS.REST_URL}{CONSTANTS.USER_BALANCES_PATH_URL}"
        
        async with self._web_assistants_factory.build_rest_assistant() as ra:
            try:
                resp = await ra.execute_request(url=url, method="GET")
                
                balances = resp.get("balances", [])
                for balance_entry in balances:
                    asset_name = balance_entry.get("asset", "")
                    free_balance = Decimal(balance_entry.get("free", "0"))
                    total_balance = Decimal(balance_entry.get("free", "0")) + Decimal(balance_entry.get("locked", "0"))
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance
            except Exception as e:
                self.logger().error(f"Error updating balances: {e}")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in exchange_info.get("symbols", []):
            symbol = symbol_data.get("symbol", "").lower()
            base_asset = symbol_data.get("baseAsset", "")
            quote_asset = symbol_data.get("quoteAsset", "")
            if symbol and base_asset and quote_asset:
                hb_trading_pair = f"{base_asset}-{quote_asset}"
                mapping[symbol] = hb_trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """
        Return a dictionary with trading_pair as key and the current price as value for each trading pair passed as
        parameter.
        """
        result = {}
        
        for trading_pair in trading_pairs:
            try:
                symbol = trading_pair.replace("-", "").lower()
                url = f"{CONSTANTS.REST_URL}{CONSTANTS.TICKERS_PATH_URL}"
                params = {"symbol": symbol}
                
                async with self._web_assistants_factory.build_rest_assistant() as ra:
                    resp = await ra.execute_request(url=url, method="GET", params=params)
                    
                    # Assuming the response has ticker data with lastPrice
                    if isinstance(resp, list) and len(resp) > 0:
                        ticker_data = resp[0]
                        last_price = float(ticker_data.get("lastPrice", "0"))
                        result[trading_pair] = last_price
                    elif isinstance(resp, dict):
                        last_price = float(resp.get("lastPrice", "0"))
                        result[trading_pair] = last_price
                        
            except Exception as e:
                self.logger().error(f"Error fetching last traded price for {trading_pair}: {e}")
                result[trading_pair] = 0.0
                
        return result
