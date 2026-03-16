import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.ajaib import (
    ajaib_constants as CONSTANTS,
    ajaib_utils,
    ajaib_web_utils as web_utils,
)
from hummingbot.connector.exchange.ajaib.ajaib_api_order_book_data_source import AjaibAPIOrderBookDataSource
from hummingbot.connector.exchange.ajaib.ajaib_api_user_stream_data_source import AjaibAPIUserStreamDataSource
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class AjaibExchange(ExchangePyBase):
    """
    Ajaib exchange connector implementation.
    Supports spot trading on Ajaib (Binance-compatible API with Ed25519 auth).
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 ajaib_api_key: str,
                 ajaib_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = ajaib_api_key
        self.secret_key = ajaib_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_timestamp = 1.0
        self._keys_configured = bool(ajaib_api_key and ajaib_api_secret)
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def ajaib_order_type(order_type: OrderType) -> str:
        if order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET
        if order_type == OrderType.LIMIT_MAKER:
            return CONSTANTS.ORDER_TYPE_LIMIT_MAKER
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def ajaib_side(trade_type: TradeType) -> str:
        return CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

    @staticmethod
    def to_hb_order_type(ajaib_type: str) -> OrderType:
        if ajaib_type == CONSTANTS.ORDER_TYPE_MARKET:
            return OrderType.MARKET
        if ajaib_type == CONSTANTS.ORDER_TYPE_LIMIT_MAKER:
            return OrderType.LIMIT_MAKER
        return OrderType.LIMIT

    @property
    def authenticator(self):
        return AjaibAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "ajaib"

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
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

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

    async def start_network(self):
        await super().start_network()
        if self.is_trading_required:
            try:
                await self._update_balances()
            except Exception as e:
                self.logger().warning(f"Failed to fetch initial balances: {e}")

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Fetches book ticker data (best bid/ask) for all symbols.
        Uses the Binance-compatible /v1/ticker/bookTicker endpoint.
        """
        if not self._keys_configured:
            self.logger().warning("Ajaib API keys not configured. Cannot fetch prices.")
            return []
        pairs_prices = await self._api_get(
            path_url=CONSTANTS.TICKER_BOOK_PATH_URL,
            is_auth_required=True,
        )
        return pairs_prices

    async def _make_trading_pairs_request(self) -> Any:
        if not self._keys_configured:
            self.logger().warning("Ajaib API keys not configured. Skipping exchange info request.")
            return {"symbols": []}
        return await self._api_get(path_url=self.trading_pairs_request_path, is_auth_required=True)

    async def _make_trading_rules_request(self) -> Any:
        if not self._keys_configured:
            return {"symbols": []}
        return await self._api_get(path_url=self.trading_rules_request_path, is_auth_required=True)

    async def _make_network_check_request(self):
        if not self._keys_configured:
            return
        await self._api_get(path_url=self.check_network_request_path, is_auth_required=True)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
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
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return AjaibAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return AjaibAPIUserStreamDataSource(
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
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        api_params = {
            "symbol": symbol,
            "side": AjaibExchange.ajaib_side(trade_type),
            "type": AjaibExchange.ajaib_order_type(order_type),
            "quantity": str(amount),
            "newClientOrderId": order_id,
        }

        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            api_params["price"] = str(price)
            api_params["timeInForce"] = "GTC"

        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)

        o_id = str(order_result.get("orderId", order_id))
        transact_time = order_result.get("time", self._time_synchronizer.time() * 1000) / 1000

        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        api_params = {"symbol": symbol}
        if tracked_order.exchange_order_id:
            api_params["orderId"] = tracked_order.exchange_order_id
        else:
            api_params["origClientOrderId"] = order_id

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True)

        return cancel_result is not None

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        symbols = exchange_info_dict.get("symbols", []) if isinstance(exchange_info_dict, dict) else exchange_info_dict
        retval = []

        for rule in filter(ajaib_utils.is_exchange_information_valid, symbols):
            try:
                symbol = rule.get("symbol", "")
                base = rule.get("baseAsset", "")
                quote = rule.get("quoteAsset", "")

                if not (symbol and base and quote):
                    continue

                trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)

                min_order_size = Decimal("0")
                max_order_size = Decimal("0")
                min_price_increment = Decimal("0")
                min_base_amount_increment = Decimal("0")
                min_notional = Decimal("0")

                for f in rule.get("filters", []):
                    filter_type = f.get("filterType", "")
                    if filter_type == "PRICE_FILTER":
                        min_price_increment = Decimal(str(f.get("tickSize", "0")))
                    elif filter_type == "LOT_SIZE":
                        min_order_size = Decimal(str(f.get("minQty", "0")))
                        max_order_size = Decimal(str(f.get("maxQty", "0")))
                        min_base_amount_increment = Decimal(str(f.get("stepSize", "0")))
                    elif filter_type == "MIN_NOTIONAL":
                        min_notional = Decimal(str(f.get("minNotional", "0")))

                retval.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_order_size,
                        max_order_size=max_order_size,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=min_base_amount_increment,
                        min_notional_size=min_notional,
                    )
                )
            except Exception as e:
                self.logger().warning(f"Error parsing trading rule for {rule.get('symbol', '')}: {e}")

        return retval

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("e", "")
                if event_type == "executionReport":
                    client_order_id = event_message.get("c", "")
                    exchange_order_id = str(event_message.get("i", ""))
                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)

                    if tracked_order is not None:
                        status = event_message.get("X", "")
                        new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message.get("T", self._time_synchronizer.time() * 1000) / 1000,
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=exchange_order_id,
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                        # Check for fills
                        fill_qty = Decimal(str(event_message.get("l", "0")))
                        if fill_qty > 0:
                            fill_price = Decimal(str(event_message.get("L", "0")))
                            fee_amount = Decimal(str(event_message.get("n", "0")))
                            fee_token = event_message.get("N", "")

                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                flat_fees=[TokenAmount(amount=fee_amount, token=fee_token)]
                            )

                            trade_update = TradeUpdate(
                                trade_id=str(event_message.get("t", "")),
                                client_order_id=client_order_id,
                                exchange_order_id=exchange_order_id,
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=fill_qty,
                                fill_quote_amount=fill_qty * fill_price,
                                fill_price=fill_price,
                                fill_timestamp=event_message.get("T", self._time_synchronizer.time() * 1000) / 1000,
                            )
                            self._order_tracker.process_trade_update(trade_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if order.exchange_order_id is not None:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
                all_fills_response = await self._api_get(
                    path_url=CONSTANTS.TRADES_PATH_URL,
                    params={"symbol": symbol, "orderId": order.exchange_order_id},
                    is_auth_required=True)

                for trade in all_fills_response:
                    fee_amount = Decimal(str(trade.get("commission", 0)))
                    fee_token = trade.get("commissionAsset", "")

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        flat_fees=[TokenAmount(amount=fee_amount, token=fee_token)]
                    )

                    trade_update = TradeUpdate(
                        trade_id=str(trade.get("id", "")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(str(trade.get("qty", 0))),
                        fill_quote_amount=Decimal(str(trade.get("quoteQty", 0))),
                        fill_price=Decimal(str(trade.get("price", 0))),
                        fill_timestamp=trade.get("time", self._time_synchronizer.time() * 1000) / 1000,
                    )
                    trade_updates.append(trade_update)
            except Exception as e:
                self.logger().error(f"Error fetching trades for order: {e}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        params = {"symbol": symbol}
        if tracked_order.exchange_order_id:
            params["orderId"] = tracked_order.exchange_order_id
        else:
            params["origClientOrderId"] = tracked_order.client_order_id

        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            params=params,
            is_auth_required=True)

        status = updated_order_data.get("status", "")
        new_state = CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data.get("orderId", "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data.get("updateTime", self._time_synchronizer.time() * 1000) / 1000,
            new_state=new_state,
        )
        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.PORTFOLIO_PATH_URL,
            is_auth_required=True)

        balances = account_info.get("balances", []) if isinstance(account_info, dict) else []

        for balance_entry in balances:
            asset_name = balance_entry.get("asset", "")
            free_balance = Decimal(str(balance_entry.get("free", 0)))
            locked_balance = Decimal(str(balance_entry.get("locked", 0)))
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

        Ajaib exchange-info format:
        {
            "symbols": [
                {
                    "symbol": "BTC_USDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    ...
                }
            ]
        }
        """
        mapping = bidict()

        symbols = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else exchange_info
        if isinstance(symbols, list):
            for symbol_data in filter(ajaib_utils.is_exchange_information_valid, symbols):
                symbol = symbol_data.get("symbol", "")
                base = symbol_data.get("baseAsset", "")
                quote = symbol_data.get("quoteAsset", "")

                if symbol and base and quote:
                    trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                    mapping[symbol] = trading_pair

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            klines = await self._api_get(
                path_url=CONSTANTS.KLINES_PATH_URL,
                params={"symbol": symbol, "interval": "1m", "limit": 1},
                is_auth_required=True)

            if klines and len(klines) > 0:
                # Kline format: [open_time, open, high, low, close, volume, close_time]
                return float(klines[0][4])  # close price
            return 0.0
        except Exception as e:
            self.logger().error(f"Error getting last traded price: {e}")
            return 0.0
