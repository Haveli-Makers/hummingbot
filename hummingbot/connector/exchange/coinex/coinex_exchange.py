import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS, coinex_web_utils as web_utils
from hummingbot.connector.exchange.coinex.coinex_api_order_book_data_source import CoinexAPIOrderBookDataSource
from hummingbot.connector.exchange.coinex.coinex_api_user_stream_data_source import CoinexAPIUserStreamDataSource
from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _result(response: Any) -> Any:
    """
    CoinEx wraps payloads as {"code": 0, "data": …, "message": "OK"}.
    Raise on a non-zero code; otherwise return `data` (or the response itself
    if there is no envelope).
    """
    if isinstance(response, dict):
        code = response.get("code")
        if code is not None and code != 0:
            raise IOError(f"CoinEx API error (code {code}): {response.get('message') or response}")
        if "data" in response:
            return response["data"]
    return response


class CoinexExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(
        self,
        coinex_api_key: str,
        coinex_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.api_key = coinex_api_key
        self.secret_key = coinex_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        # base asset per exchange symbol (e.g. "BTCUSDT" -> "BTC"), for market-order ccy.
        self._base_ccy_by_symbol: Dict[str, str] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Order-type mapping ──────────────────────────────────────────────────────

    @staticmethod
    def coinex_order_type(order_type: OrderType) -> str:
        if order_type is OrderType.LIMIT_MAKER:
            return CONSTANTS.ORDER_TYPE_MAKER_ONLY
        if order_type is OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_MARKET
        return CONSTANTS.ORDER_TYPE_LIMIT

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def authenticator(self):
        return CoinexAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def name(self) -> str:
        return "coinex"

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
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    # ── Factory methods ────────────────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinexAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ── Exception classification ───────────────────────────────────────────────

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        msg = str(request_exception).lower()
        return "timestamp" in msg or "expired" in msg or ("signature" in msg and "time" in msg)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        msg = str(status_update_exception).lower()
        return "not found" in msg or "does not exist" in msg or "order not exist" in msg

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        msg = str(cancelation_exception).lower()
        return "not found" in msg or "does not exist" in msg or "order not exist" in msg

    # ── Trading-pair initialisation ────────────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        self._base_ccy_by_symbol = {}
        for market in _result(exchange_info) or []:
            try:
                if not isinstance(market, dict):
                    continue
                if str(market.get("status", "online")).lower() not in ("online", ""):
                    continue
                symbol = market.get("market")
                base = market.get("base_ccy")
                quote = market.get("quote_ccy")
                if not symbol or not base or not quote:
                    continue
                hb_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
                mapping[symbol] = hb_pair
                self._base_ccy_by_symbol[symbol] = base.upper()
            except Exception as exc:
                self.logger().debug(f"Error parsing CoinEx market '{market}': {exc}")
        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.MARKETS_PATH_URL, is_auth_required=False)

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(path_url=CONSTANTS.MARKETS_PATH_URL, is_auth_required=False)

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for market in _result(exchange_info) or []:
            try:
                if not isinstance(market, dict):
                    continue
                if str(market.get("status", "online")).lower() not in ("online", ""):
                    continue
                base = market.get("base_ccy")
                quote = market.get("quote_ccy")
                if not base or not quote:
                    continue
                trading_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())

                base_prec = int(market.get("base_ccy_precision", 8))
                quote_prec = int(market.get("quote_ccy_precision", 2))
                min_base_increment = Decimal(10) ** -base_prec
                min_price_increment = Decimal(10) ** -quote_prec
                min_order_size = Decimal(str(market.get("min_amount", min_base_increment)))

                rules.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_order_size,
                        min_price_increment=min_price_increment,
                        min_base_amount_increment=min_base_increment,
                    )
                )
            except Exception as exc:
                self.logger().debug(f"Error parsing CoinEx trading rule for '{market}': {exc}")
        return rules

    # ── Pricing (rate / volume oracle helpers) ─────────────────────────────────

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        response = await self._api_get(path_url=CONSTANTS.TICKER_PATH_URL, is_auth_required=False)
        result = _result(response)
        return [t for t in result if isinstance(t, dict)] if isinstance(result, list) else []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return tickers
        requested = {tp.replace("-", "").upper() for tp in trading_pairs}
        return [t for t in tickers if str(t.get("market", "")).upper() in requested]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            symbol = trading_pair.replace("-", "")
        try:
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_PATH_URL,
                params={"market": symbol},
                is_auth_required=False,
            )
            result = _result(response)
            if isinstance(result, list) and result:
                result = result[0]
            if isinstance(result, dict):
                return float(result.get("last") or 0)
        except Exception as exc:
            self.logger().error(f"Error fetching last traded price for {trading_pair}: {exc}")
        return 0.0

    # ── Order placement & cancellation ────────────────────────────────────────

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        payload: Dict[str, Any] = {
            "market": symbol,
            "market_type": CONSTANTS.MARKET_TYPE_SPOT,
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": self.coinex_order_type(order_type),
            "amount": str(amount),
            "client_id": order_id,
        }
        if order_type is OrderType.MARKET:
            # Interpret `amount` as the base currency quantity.
            base_ccy = self._base_ccy_by_symbol.get(symbol)
            if base_ccy:
                payload["ccy"] = base_ccy
        else:
            payload["price"] = str(price)

        response = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=payload,
            is_auth_required=True,
        )
        order = _result(response)
        if not isinstance(order, dict) or not order.get("order_id"):
            raise IOError(f"CoinEx did not return an order id: {response}")
        exchange_order_id = str(order.get("order_id"))
        transact_time = self._normalize_ts(order.get("created_at")) or self._time_synchronizer.time()
        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        if tracked_order.exchange_order_id is None:
            self.logger().warning(f"Cannot cancel {order_id} yet: no exchange order id (pending creation).")
            return False
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data={
                "market": symbol,
                "market_type": CONSTANTS.MARKET_TYPE_SPOT,
                "order_id": int(tracked_order.exchange_order_id),
            },
            is_auth_required=True,
        )
        order = _result(response)
        if isinstance(order, dict):
            status = str(order.get("status", "")).lower()
            return status in ("canceled", "part_canceled") or order.get("order_id") is not None
        return True

    # ── Order & trade status ───────────────────────────────────────────────────

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            params={"market": symbol, "order_id": int(tracked_order.exchange_order_id)},
            is_auth_required=True,
        )
        order = _result(response)
        if not isinstance(order, dict):
            raise ValueError(f"Unexpected CoinEx order status response: {response}")

        new_state = self._derive_order_state(order)
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._normalize_ts(order.get("updated_at") or order.get("created_at"))
            or self._time_synchronizer.time(),
            new_state=new_state,
        )

    def _derive_order_state(self, order: Dict[str, Any]) -> OrderState:
        status_str = str(order.get("status", "")).lower()
        new_state = CONSTANTS.ORDER_STATE.get(status_str)
        if new_state is not None:
            return new_state
        # Fall back to amounts when status is missing/unknown.
        unfilled = order.get("unfilled_amount")
        filled = order.get("filled_amount")
        try:
            if unfilled is not None and Decimal(str(unfilled)) == 0 and filled is not None and Decimal(str(filled)) > 0:
                return OrderState.FILLED
            if filled is not None and Decimal(str(filled)) > 0:
                return OrderState.PARTIALLY_FILLED
        except Exception:
            pass
        return OrderState.OPEN

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        trade_updates: List[TradeUpdate] = []
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            response = await self._api_get(
                path_url=CONSTANTS.ORDER_DEALS_PATH_URL,
                params={"market": symbol, "order_id": int(order.exchange_order_id)},
                is_auth_required=True,
            )
            deals = _result(response)
            for deal in deals if isinstance(deals, list) else []:
                if not isinstance(deal, dict):
                    continue
                fill_price = Decimal(str(deal.get("price", "0")))
                fill_base = Decimal(str(deal.get("amount", "0")))
                fee_token = str(deal.get("fee_ccy") or order.quote_asset)
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=fee_token,
                    flat_fees=[TokenAmount(amount=Decimal(str(deal.get("fee", "0"))), token=fee_token)],
                )
                trade_updates.append(
                    TradeUpdate(
                        trade_id=str(deal.get("deal_id") or deal.get("id") or f"{order.exchange_order_id}-{len(trade_updates)}"),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(order.exchange_order_id),
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_base,
                        fill_quote_amount=fill_base * fill_price,
                        fill_price=fill_price,
                        fill_timestamp=self._normalize_ts(deal.get("created_at")) or self._time_synchronizer.time(),
                    )
                )
        except Exception as exc:
            self.logger().debug(f"Error fetching CoinEx fills for {order.exchange_order_id}: {exc}")
        return trade_updates

    # ── Balance ────────────────────────────────────────────────────────────────

    async def _update_balances(self) -> None:
        local_assets = set(self._account_balances.keys())
        remote_assets: set = set()
        try:
            response = await self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, is_auth_required=True)
            for entry in _result(response) or []:
                if not isinstance(entry, dict):
                    continue
                asset = str(entry.get("ccy", "")).upper()
                if not asset:
                    continue
                available = Decimal(str(entry.get("available", "0")))
                frozen = Decimal(str(entry.get("frozen", "0")))
                self._account_balances[asset] = available + frozen
                self._account_available_balances[asset] = available
                remote_assets.add(asset)
            for stale in local_assets - remote_assets:
                del self._account_balances[stale]
                del self._account_available_balances[stale]
        except Exception as exc:
            self.logger().error(f"Error updating CoinEx balances: {exc}", exc_info=True)

    # ── Fees ───────────────────────────────────────────────────────────────────

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self):
        pass

    # ── User-stream event listener ─────────────────────────────────────────────

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                method = event.get("method")
                data = event.get("data") or {}

                if method == CONSTANTS.WS_BALANCE_UPDATE:
                    balances = data.get("balance_list", data) if isinstance(data, dict) else data
                    for entry in balances if isinstance(balances, list) else []:
                        if not isinstance(entry, dict):
                            continue
                        asset = str(entry.get("ccy", "")).upper()
                        if not asset:
                            continue
                        available = Decimal(str(entry.get("available", "0")))
                        frozen = Decimal(str(entry.get("frozen", "0")))
                        self._account_balances[asset] = available + frozen
                        self._account_available_balances[asset] = available

                elif method == CONSTANTS.WS_ORDER_UPDATE:
                    self._process_ws_order_event(data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in CoinEx user-stream listener.", exc_info=True)
                await self._sleep(5.0)

    def _process_ws_order_event(self, data: Dict[str, Any]):
        order_info = data.get("order") if isinstance(data.get("order"), dict) else data
        if not isinstance(order_info, dict):
            return
        client_order_id = order_info.get("client_id")
        exchange_order_id = str(order_info.get("order_id", ""))

        tracked = None
        if client_order_id:
            tracked = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked is None and exchange_order_id:
            for o in self._order_tracker.all_updatable_orders.values():
                if o.exchange_order_id == exchange_order_id:
                    tracked = o
                    break
        if tracked is None:
            return

        event_name = str(data.get("event", "")).lower()
        new_state = self._derive_order_state(order_info)
        if event_name == "finish" and new_state not in (OrderState.FILLED, OrderState.PARTIALLY_FILLED):
            # A 'finish' with remaining unfilled size is a cancellation.
            unfilled = order_info.get("unfilled_amount")
            try:
                if unfilled is not None and Decimal(str(unfilled)) > 0:
                    new_state = OrderState.CANCELED
            except Exception:
                pass

        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked.trading_pair,
            update_timestamp=self._normalize_ts(order_info.get("updated_at") or order_info.get("created_at"))
            or self._time_synchronizer.time(),
            new_state=new_state,
            client_order_id=tracked.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    # ── Helpers ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_ts(raw) -> float:
        """CoinEx timestamps are milliseconds since epoch; normalise to seconds."""
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        return value / 1000.0 if value > 1e11 else value
