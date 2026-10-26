import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.wazirx import (
    wazirx_constants as CONSTANTS,
    wazirx_utils,
    wazirx_web_utils as web_utils,
)
from hummingbot.connector.exchange.wazirx.wazirx_api_key_pool import WazirxApiKeyPool
from hummingbot.connector.exchange.wazirx.wazirx_api_order_book_data_source import WazirxAPIOrderBookDataSource
from hummingbot.connector.exchange.wazirx.wazirx_api_user_stream_data_source import WazirxAPIUserStreamDataSource
from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth, WazirxTimestampSequence
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class WazirxAPIError(IOError):
    """A signed request that WazirX answered with an HTTP error status."""

    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


class WazirxExchange(ExchangePyBase):
    """
    WazirX exchange connector for spot trading.

    WazirX rate-limits each API key separately, so the connector accepts up to five
    comma-separated keys from one account and rotates signed requests across them.
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
        """
        Initialize the WazirX exchange connector.

        ``wazirx_api_key`` and ``wazirx_api_secret`` each take one value, or up to five
        comma-separated values matched by position.
        """
        self._api_credentials = wazirx_utils.parse_api_credentials(wazirx_api_key, wazirx_api_secret)
        self.api_key, self.secret_key = self._api_credentials[0]
        self._timestamp_sequence = WazirxTimestampSequence()
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._key_pool = WazirxApiKeyPool(
            auths=[self._auth] + [self._create_auth(key, secret) for key, secret in self._api_credentials[1:]],
            rate_limits=CONSTANTS.RATE_LIMITS,
        )

        if trading_required and trading_pairs:
            for pair in trading_pairs:
                parts = pair.split("-")
                if len(parts) == 2:
                    for asset in parts:
                        self._account_balances[asset] = Decimal("0")
                        self._account_available_balances[asset] = Decimal("0")

    @property
    def authenticator(self):
        return self._create_auth(self.api_key, self.secret_key)

    def _create_auth(self, api_key: str, secret_key: str) -> WazirxAuth:
        return WazirxAuth(
            api_key=api_key,
            secret_key=secret_key,
            time_provider=self._time_synchronizer,
            timestamp_sequence=self._timestamp_sequence,
        )

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
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

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
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL)
        return pairs_prices

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, str]]:
        if not trading_pairs:
            return await self._api_get(path_url=CONSTANTS.TICKERS_PATH_URL)
        results = []
        for tp in trading_pairs:
            base, quote = tp.split("-", 1)
            symbol = f"{base.lower()}{quote.lower()}"
            try:
                resp = await self._api_get(
                    path_url=CONSTANTS.TICKER_24HR_PATH_URL,
                    params={"symbol": symbol},
                )
                if isinstance(resp, dict):
                    results.append(resp)
                elif isinstance(resp, list):
                    results.extend(resp)
            except Exception:
                self.logger().warning(f"Skipping {tp}: symbol not found on {self.name}")
        return results

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_msg = str(request_exception)
        return "2098" in error_msg or "out of receiving window" in error_msg.lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "Order does not exist" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "Order does not exist" in str(cancelation_exception)

    def _is_user_stream_initialized(self):
        return True

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

    async def _wazirx_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False
    ) -> Dict[str, Any]:
        """
        Make an authenticated or unauthenticated request to the WazirX API.

        Signed requests rotate across the configured API keys (see _signed_request).
        """
        params = params or {}
        if is_auth_required:
            return await self._signed_request(method=method.upper(), path=path, params=params)

        url = f"{CONSTANTS.REST_URL}{path}"
        async with aiohttp.ClientSession() as session:
            headers = {}
            if method.upper() == "GET":
                async with session.get(url, params=params, headers=headers) as response:
                    return await self._handle_response(response, method, url)
            elif method.upper() == "POST":
                body = urlencode(params)
                async with session.post(url, data=body, headers=headers) as response:
                    return await self._handle_response(response, method, url)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: Dict[str, Any],
        key_index: Optional[int] = None,
    ) -> Any:
        """
        Signs and sends a request with the next API key that has room on this endpoint.

        On HTTP 429 the key rests on the endpoint and the request is retried once with
        another key. A 429 means WazirX did not process the request, so a retried order
        can't be placed twice. On HTTP 418 (IP ban) all signed requests pause. On error
        2098 the clock is resynced and the request is retried once. ``key_index`` pins
        the request to one key, with no retry on another key.
        """
        url = f"{CONSTANTS.REST_URL}{path}"
        # /v1/order serves create, query and cancel, and each has its own limit
        limit_id = CONSTANTS.ORDER_LIMIT_IDS.get(method, path) if path == CONSTANTS.ORDER_STATUS_PATH_URL else path
        rate_limited_keys: List[int] = []
        clock_resynced = False
        while True:
            index = await self._key_pool.acquire(limit_id, exclude=rate_limited_keys, index=key_index)
            auth = self._key_pool.auth(index)
            status, retry_after, payload = await self._send_signed_request(
                auth=auth, method=method, url=url, params=params)
            key_label = self._key_pool.label(index)
            order_id = params.get("orderId") or (payload.get("id") if status < 400 and isinstance(payload, dict) else None)
            order_note = f" order {order_id}" if order_id else ""
            self.logger().debug(
                f"WazirX {method} {path}{order_note} used API {key_label} ({auth.api_key[:4]}...): HTTP {status}")
            if status < 400:
                return payload

            error = WazirxAPIError(
                f"Error executing request {method} {url} with WazirX API {key_label}. "
                f"HTTP status is {status}. Error: {payload}",
                status=status,
            )
            if status == CONSTANTS.RATE_LIMITED_HTTP_STATUS:
                self._key_pool.rest(
                    index, limit_id, self._retry_after_seconds(retry_after, CONSTANTS.DEFAULT_RATE_LIMIT_COOLDOWN))
                rate_limited_keys.append(index)
                if key_index is None and len(rate_limited_keys) == 1 and len(self._key_pool.active_indexes) > 1:
                    self.logger().warning(f"WazirX rate-limited API {key_label} on {limit_id}; retrying with another key.")
                    continue
            elif status == CONSTANTS.IP_BANNED_HTTP_STATUS:
                pause = self._retry_after_seconds(retry_after, CONSTANTS.DEFAULT_IP_BAN_COOLDOWN)
                self._key_pool.pause(pause)
                self.logger().error(f"WazirX banned this IP (HTTP 418). Signed requests are paused for {pause:.0f}s.")
            elif not clock_resynced and self._is_request_exception_related_to_time_synchronizer(error):
                clock_resynced = True
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer(pass_on_non_cancelled_error=True)
                continue
            raise error

    async def _send_signed_request(
        self,
        auth: WazirxAuth,
        method: str,
        url: str,
        params: Dict[str, Any],
    ) -> Tuple[int, Optional[str], Any]:
        """
        Sends one signed request. Returns the HTTP status, the Retry-After header, and
        the parsed JSON body, or the raw text for an error status.
        """
        _, query_string = await auth.add_auth_params(params)
        headers = auth.get_headers()
        async with aiohttp.ClientSession() as session:
            if method == "GET":
                request = session.get(f"{url}?{query_string}", headers=headers)
            elif method in ("POST", "DELETE"):
                request = session.request(method, url, data=query_string, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            async with request as response:
                if response.status >= 400:
                    return response.status, response.headers.get("Retry-After"), await response.text()
                return response.status, None, await response.json()

    @staticmethod
    def _retry_after_seconds(retry_after: Optional[str], default: float) -> float:
        try:
            seconds = float(retry_after)
        except (TypeError, ValueError):
            return default
        return seconds if seconds > 0 else default

    async def start_network(self):
        if self.is_trading_required and self._key_pool.size > 1:
            await self._check_api_keys()
        await super().start_network()

    async def _check_api_keys(self):
        """
        Sends one balance request with each API key and leaves the keys WazirX rejects
        out of rotation, so a key without trading permission or IP whitelisting doesn't
        fail every Nth request. A key whose check fails for another reason (timeout,
        5xx, rate limit) stays in. If WazirX rejects every key, all of them stay in and
        the normal error handling reports the problem.
        """
        indexes = list(range(self._key_pool.size))
        try:
            errors = await asyncio.wait_for(
                asyncio.gather(*(self._check_api_key(index) for index in indexes)),
                timeout=CONSTANTS.API_KEY_CHECK_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self.logger().warning("The WazirX API key check timed out; starting with every key.")
            self._key_pool.set_active(indexes)
            return

        rejected = [index for index, error in zip(indexes, errors) if error is not None]
        if len(rejected) == len(indexes):
            self.logger().error("WazirX rejected every API key at start-up; keeping all of them in rotation.")
            self._key_pool.set_active(indexes)
            return
        for index, error in zip(indexes, errors):
            if error is not None:
                self.logger().error(
                    f"WazirX rejected API {self._key_pool.label(index)} at start-up, so it is left out of "
                    f"rotation until the connector restarts. {error}")
        active = [index for index in indexes if index not in rejected]
        self._key_pool.set_active(active)
        self.logger().info(f"Rotating signed WazirX requests across {len(active)} of {len(indexes)} API keys.")

    async def _check_api_key(self, index: int) -> Optional[WazirxAPIError]:
        """Returns WazirX's error if it rejected the key, otherwise None."""
        try:
            await self._signed_request(
                method="GET", path=CONSTANTS.USER_BALANCES_PATH_URL, params={}, key_index=index)
        except asyncio.CancelledError:
            raise
        except WazirxAPIError as error:
            rate_limited = error.status in (CONSTANTS.RATE_LIMITED_HTTP_STATUS, CONSTANTS.IP_BANNED_HTTP_STATUS)
            if error.status < 500 and not rate_limited:
                return error
            self.logger().warning(f"Could not check WazirX API {self._key_pool.label(index)}: {error}")
        except Exception as error:
            self.logger().warning(f"Could not check WazirX API {self._key_pool.label(index)}: {error}")
        return None

    async def _handle_response(self, response: aiohttp.ClientResponse, method: str, url: str) -> Dict[str, Any]:
        if response.status >= 400:
            error_text = await response.text()
            raise IOError(f"Error executing request {method} {url}. HTTP status is {response.status}. Error: {error_text}")
        return await response.json()

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]
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
        Place an order on the WazirX exchange.
        """
        symbol = trading_pair.replace("-", "").lower()

        wazirx_order_type = "limit" if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER] else order_type.name.lower()

        params = {
            "symbol": symbol,
            "side": trade_type.name.lower(),
            "type": wazirx_order_type,
            "quantity": f"{amount:f}",
        }
        if order_type.is_limit_type():
            params["price"] = f"{price:f}"

        try:
            resp = await self._wazirx_request(
                method="POST",
                path=CONSTANTS.CREATE_ORDER_PATH_URL,
                params=params,
                is_auth_required=True
            )
            order_id = str(resp.get("id", resp.get("orderId", "")))
            executed_amount = float(resp.get("executedQty", 0))
            return order_id, executed_amount
        except Exception:
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = tracked_order.trading_pair.replace("-", "").lower()

        exchange_order_id = tracked_order.exchange_order_id
        if not exchange_order_id:
            self.logger().warning(f"Cannot cancel order {order_id}: no exchange order ID available")
            return False

        params = {
            "symbol": symbol,
            "orderId": exchange_order_id,
        }
        try:
            resp = await self._wazirx_request(
                method="DELETE",
                path=CONSTANTS.CANCEL_ORDER_PATH_URL,
                params=params,
                is_auth_required=True
            )
            return resp.get("id") is not None or resp.get("orderId") is not None
        except Exception as e:
            self.logger().warning(f"Cancel order {order_id} failed: {e}")
            return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Format trading rules from exchange info
        """
        if isinstance(exchange_info_dict, list):
            trading_pair_rules = exchange_info_dict
        else:
            trading_pair_rules = exchange_info_dict.get("symbols", [])

        retval: List[TradingRule] = []

        for rule in trading_pair_rules:
            try:
                symbol = rule.get("symbol", "")
                if not symbol:
                    continue

                base_asset = rule.get("baseAsset", "")
                quote_asset = rule.get("quoteAsset", "")
                if not base_asset or not quote_asset:
                    continue

                hb_trading_pair = f"{base_asset.upper()}-{quote_asset.upper()}"

                filters = rule.get("filters", [])
                price_filter = next((f for f in filters if f.get("filterType") == "PRICE_FILTER"), {})
                lot_size_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), {})
                min_notional_filter = next(
                    (f for f in filters if f.get("filterType") in ["MIN_NOTIONAL", "NOTIONAL"]),
                    {},
                )

                try:
                    min_order_size = Decimal(lot_size_filter.get("minQty", "1e-8"))
                except Exception:
                    min_order_size = Decimal("1e-8")

                try:
                    max_order_size = Decimal(lot_size_filter.get("maxQty", "1e8"))
                except Exception:
                    max_order_size = Decimal("1e8")

                try:
                    tick_size = Decimal(price_filter.get("tickSize", "1e-8"))
                except Exception:
                    tick_size = Decimal("1e-8")

                try:
                    step_size = Decimal(lot_size_filter.get("stepSize", "1e-8"))
                except Exception:
                    step_size = Decimal("1e-8")

                try:
                    min_notional = Decimal(min_notional_filter.get("minNotional", "0"))
                except Exception:
                    min_notional = Decimal("0")

                trading_rule = TradingRule(
                    trading_pair=hb_trading_pair,
                    min_order_size=min_order_size,
                    max_order_size=max_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    min_quote_amount_increment=step_size,
                    min_notional_size=min_notional,
                )
                retval.append(trading_rule)
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")

        return retval

    async def _update_trading_fees(self):
        return

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event")
                if event_type == "orderUpdate":
                    order_data = event_message.get("order", {})
                    client_order_id = order_data.get("clientOrderId")
                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        new_state = CONSTANTS.ORDER_STATE.get(
                            order_data.get("status"),
                            OrderState.OPEN,
                        )
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message.get("timestamp", 0) / 1000,
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_data.get("orderId", "")),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "balanceUpdate":
                    balance_data = event_message.get("balance", {})
                    asset_name = balance_data.get("asset")
                    if asset_name is not None:
                        free_balance = Decimal(balance_data.get("free", "0"))
                        locked_balance = Decimal(balance_data.get("locked", "0"))
                        total_balance = free_balance + locked_balance
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

                else:
                    continue

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates: List[TradeUpdate] = []

        if order.current_state in [OrderState.FAILED, OrderState.CANCELED]:
            return trade_updates

        if order.exchange_order_id is not None:
            symbol = order.trading_pair.replace("-", "").lower()
            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id,
            }

            try:
                resp = await self._wazirx_request(
                    method="GET",
                    path=CONSTANTS.MY_TRADES_PATH_URL,
                    params=params,
                    is_auth_required=True
                )

                trades = resp if isinstance(resp, list) else resp.get("trades", [])

                for trade in trades:
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=trade.get("feeCurrency", trade.get("commissionAsset", "")),
                        flat_fees=[
                            TokenAmount(
                                amount=Decimal(trade.get("fee", trade.get("commission", "0"))),
                                token=trade.get("feeCurrency", trade.get("commissionAsset", "")),
                            )
                        ],
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
                error_msg = str(e)
                if "429" in error_msg or "Too many" in error_msg:
                    self.logger().warning(f"Rate limit hit while fetching trade updates for order {order.client_order_id}. Will retry later.")
                else:
                    self.logger().error(f"Error fetching trade updates for order {order.client_order_id}: {e}")

        return trade_updates

    async def get_all_account_trades(self, start_time: Optional[int] = None,
                                     end_time: Optional[int] = None,
                                     limit: int = 500,
                                     trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fetches raw fills for this account from WazirX's ``myTrades`` endpoint,
        across the given (or configured) trading pairs.
        """
        trades: List[Dict[str, Any]] = []
        for trading_pair in trading_pairs or self._trading_pairs or []:
            symbol = trading_pair.replace("-", "").lower()
            params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
            if start_time is not None:
                params["startTime"] = start_time
            if end_time is not None:
                params["endTime"] = end_time

            try:
                resp = await self._wazirx_request(
                    method="GET",
                    path=CONSTANTS.MY_TRADES_PATH_URL,
                    params=params,
                    is_auth_required=True,
                )
                symbol_trades = resp if isinstance(resp, list) else resp.get("trades", [])
                for trade in symbol_trades:
                    if isinstance(trade, dict):
                        trade.setdefault("symbol", symbol)
                        # WazirX's own "symbol" field is the raw concatenated
                        # exchange symbol (e.g. "usdtinr"); surface the
                        # hummingbot-formatted pair too so downstream display
                        # doesn't have to reverse-parse it.
                        trade["trading_pair"] = trading_pair
                        trades.append(trade)
            except Exception as e:
                self.logger().error(f"Error fetching account-wide trade history for {symbol}: {e}")

        return trades

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.current_state in [OrderState.FAILED, OrderState.CANCELED]:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=tracked_order.last_update_timestamp,
                new_state=tracked_order.current_state,
            )

        symbol = tracked_order.trading_pair.replace("-", "").lower()
        params = {
            "symbol": symbol,
            "orderId": tracked_order.exchange_order_id,
        }

        resp = await self._wazirx_request(
            method="GET",
            path=CONSTANTS.ORDER_STATUS_PATH_URL,
            params=params,
            is_auth_required=True
        )

        new_state = CONSTANTS.ORDER_STATE.get(resp.get("status"), OrderState.OPEN)
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(resp.get("id", resp.get("orderId", ""))),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=resp.get("updatedTime", resp.get("updateTime", 0)) / 1000,
            new_state=new_state,
        )
        return order_update

    async def _update_balances(self):
        try:
            resp = await self._wazirx_request(
                method="GET",
                path=CONSTANTS.USER_BALANCES_PATH_URL,
                is_auth_required=True
            )

            if isinstance(resp, list):
                balances = resp
            else:
                balances = resp.get("balances", [])

            for balance_entry in balances:
                asset_name = balance_entry.get("asset", "").upper()
                free_balance = Decimal(balance_entry.get("free", "0"))
                total_balance = Decimal(balance_entry.get("free", "0")) + Decimal(balance_entry.get("locked", "0"))
                self._account_available_balances[asset_name] = free_balance
                self._account_balances[asset_name] = total_balance
        except Exception as e:
            self.logger().warning(f"Error updating balances (will retry): {e}")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        if isinstance(exchange_info, list):
            symbols_data = exchange_info
        else:
            symbols_data = exchange_info.get("symbols", [])

        for symbol_data in symbols_data:
            symbol = symbol_data.get("symbol", "").lower()
            base_asset = symbol_data.get("baseAsset", "")
            quote_asset = symbol_data.get("quoteAsset", "")
            if symbol and base_asset and quote_asset:
                hb_trading_pair = f"{base_asset.upper()}-{quote_asset.upper()}"
                mapping[symbol] = hb_trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        result = {}

        for trading_pair in trading_pairs:
            try:
                symbol = trading_pair.replace("-", "").lower()
                url = f"{CONSTANTS.REST_URL}{CONSTANTS.TICKER_24HR_PATH_URL}"
                params = {"symbol": symbol}

                ra = await self._web_assistants_factory.get_rest_assistant()
                resp = await ra.execute_request(url=url, method=RESTMethod.GET, params=params, throttler_limit_id=CONSTANTS.TICKERS_PATH_URL)

                if isinstance(resp, dict):
                    last_price = float(resp.get("lastPrice", "0"))
                    result[trading_pair] = last_price

            except Exception as e:
                self.logger().error(f"Error fetching last traded price for {trading_pair}: {e}")
                result[trading_pair] = 0.0

        return result
