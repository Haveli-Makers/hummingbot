import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS, csx_web_utils as web_utils
from hummingbot.connector.exchange.csx.csx_api_order_book_data_source import CsxAPIOrderBookDataSource
from hummingbot.connector.exchange.csx.csx_api_user_stream_data_source import CsxAPIUserStreamDataSource
from hummingbot.connector.exchange.csx.csx_auth import CsxAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

_logger = logging.getLogger(__name__)


def _extract_instruments_list(response: Any) -> list:
    """
    Navigate the CSX instruments response to a flat list of instrument objects.

    Actual API shape:
        {"data": {"instruments": [{"instrument": "BTC/INR", "basePrecision": "0.001", ...}, ...]}}

    Also handles simpler shapes returned by tests / other callers:
        ["BTC/INR", ...]          plain list of strings
        [{"symbol": "BTC/INR"}]   plain list of dicts
        {"instruments": [...]}    dict with top-level key
    """
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []

    # Unwrap the outer "data" key if present
    data = response.get("data", response)

    if isinstance(data, dict):
        # {"instruments": [...]} — typical CSX shape inside "data"
        if "instruments" in data:
            return data["instruments"]
        # Unexpected: dict of symbol → object  {"BTC/INR": {...}}
        return list(data.keys())

    if isinstance(data, list):
        return data

    return []


class CsxExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(
        self,
        csx_api_key: str,
        csx_api_secret: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        csx_proxy_url: str = "",
    ):
        self.api_key = csx_api_key
        self.secret_key = csx_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._proxy_url = csx_proxy_url or ""
        self._last_trades_poll_timestamp = 1.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Static helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def csx_order_type(order_type: OrderType) -> str:
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def to_hb_order_type(csx_type: str) -> OrderType:
        return OrderType.LIMIT

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def authenticator(self):
        return CsxAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def name(self) -> str:
        return "csx"

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
        return CONSTANTS.INSTRUMENTS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.INSTRUMENTS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.HEALTH_PATH_URL

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
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    # ── Factory methods ────────────────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CsxAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self._domain,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CsxAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ── Exception classification ───────────────────────────────────────────────

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return "timestamp" in str(request_exception).lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        msg = str(status_update_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        msg = str(cancelation_exception).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    # ── Trading pair initialisation ────────────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        """
        Build the bidict from instrument strings returned by GET /api/v1/public/instrument.

        The endpoint may return:
          - a list of strings: ["BTC/INR", "ETH/INR", ...]
          - a list of dicts:   [{"symbol": "BTC/INR", ...}, ...]
          - a dict with a data key containing either of the above
        """
        mapping = bidict()
        try:
            instruments = _extract_instruments_list(exchange_info)

            for item in instruments:
                try:
                    if isinstance(item, str):
                        symbol = item
                    elif isinstance(item, dict):
                        symbol = item.get("symbol") or item.get("instrument") or item.get("name") or ""
                    else:
                        continue

                    symbol = symbol.strip()
                    if not symbol:
                        continue

                    if "/" in symbol:
                        base, quote = symbol.split("/", 1)
                    elif "-" in symbol:
                        base, quote = symbol.split("-", 1)
                    else:
                        continue

                    hb_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())
                    mapping[symbol] = hb_pair
                except Exception as exc:
                    self.logger().debug(f"Error parsing instrument '{item}': {exc}")

        except Exception as exc:
            self.logger().error(f"Error initialising trading pair symbols: {exc}")

        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(
            path_url=CONSTANTS.INSTRUMENTS_PATH_URL,
            is_auth_required=False,
        )

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(
            path_url=CONSTANTS.INSTRUMENTS_PATH_URL,
            is_auth_required=False,
        )

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        """
        Parse instrument info into TradingRule objects.

        If the endpoint only returns symbol strings (no precision data) sensible
        defaults are applied so the connector still starts up correctly.
        """
        trading_rules: List[TradingRule] = []
        try:
            instruments = _extract_instruments_list(exchange_info)

            for item in instruments:
                try:
                    if isinstance(item, str):
                        symbol = item
                        info: Dict[str, Any] = {}
                    elif isinstance(item, dict):
                        symbol = item.get("symbol") or item.get("instrument") or item.get("name") or ""
                        info = item
                    else:
                        continue

                    symbol = symbol.strip()
                    if not symbol:
                        continue

                    if "/" in symbol:
                        base, quote = symbol.split("/", 1)
                    elif "-" in symbol:
                        base, quote = symbol.split("-", 1)
                    else:
                        continue

                    trading_pair = f"{base.upper()}-{quote.upper()}"

                    # CSX precision fields: basePrecision=step, quotePrecision=tick
                    step = Decimal(str(
                        info.get("basePrecision") or info.get("stepSize")
                        or info.get("step_size") or "0.0001"))
                    tick = Decimal(str(
                        info.get("quotePrecision") or info.get("tickSize")
                        or info.get("tick_size") or "0.01"))
                    min_qty = step   # min order size == one step
                    max_qty = Decimal(str(info.get("maxQuantity") or info.get("max_quantity") or "1000000"))
                    min_notional = Decimal(str(
                        info.get("limitPrecision") or info.get("minNotional")
                        or info.get("min_notional") or "1"))

                    trading_rules.append(
                        TradingRule(
                            trading_pair=trading_pair,
                            min_order_size=min_qty,
                            max_order_size=max_qty,
                            min_price_increment=tick,
                            min_base_amount_increment=step,
                            min_notional_size=min_notional,
                        )
                    )
                except Exception as exc:
                    self.logger().debug(f"Error parsing trading rule for '{item}': {exc}")

        except Exception as exc:
            self.logger().error(f"Error formatting trading rules: {exc}")

        return trading_rules

    # ── Pricing ────────────────────────────────────────────────────────────────

    async def _get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        try:
            response = await self._api_get(
                path_url=CONSTANTS.TICKER_V2_PATH_URL,
                is_auth_required=False,
            )
            tickers = response if isinstance(response, list) else response.get("data", [])
            for ticker in tickers:
                # CSX returns PascalCase field names (Instrument, LastTradedPrice)
                instrument = (ticker.get("Instrument") or ticker.get("instrument")
                              or ticker.get("symbol") or "")
                hb_pair = instrument.replace("/", "-").upper()
                if hb_pair in trading_pairs:
                    raw = (ticker.get("LastTradedPrice") or ticker.get("lastTradedPrice")
                           or ticker.get("last") or 0)
                    prices[hb_pair] = float(raw)
        except Exception as exc:
            self.logger().error(f"Error fetching last traded prices: {exc}")
        return prices

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        """Used by rate-oracle source."""
        response = await self._api_get(
            path_url=CONSTANTS.TICKER_V2_PATH_URL,
            is_auth_required=False,
        )
        return response if isinstance(response, list) else response.get("data", [])

    async def get_all_24h_volume_tickers(
        self, trading_pairs: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Used by volume-oracle source."""
        tickers = await self.get_all_pairs_prices()
        if not trading_pairs:
            return tickers
        requested = {tp.replace("-", "/").upper() for tp in trading_pairs}
        return [t for t in tickers
                if (t.get("Instrument") or t.get("instrument") or "").upper() in requested]

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
        try:
            instrument = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except KeyError:
            instrument = trading_pair.replace("-", "/")

        payload: Dict[str, Any] = {
            "clientOrderId": order_id,
            "instrument": instrument,
            "limitPrice": str(price),
            "quantity": str(amount),
            "quantityType": CONSTANTS.QUANTITY_TYPE_BASE,
            "side": CONSTANTS.SIDE_BUY if trade_type == TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
        }

        result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=payload,
            is_auth_required=True,
        )

        exchange_order_id = str(result.get("orderId", result.get("order_id", "")))
        created_at = float(result.get("createdAt", result.get("created_at", 0)))
        return exchange_order_id, created_at

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        result = await self._api_delete(
            path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{tracked_order.exchange_order_id}",
            limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
            is_auth_required=True,
        )
        status = (result.get("status") or "").upper()
        return result.get("success") is True or status == "CANCELLED" or status == "CANCELED"

    # ── Order & trade status ───────────────────────────────────────────────────

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        result = await self._api_get(
            path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{tracked_order.exchange_order_id}",
            limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
            is_auth_required=True,
        )

        order_data = result if "orderId" in result else result.get("data", result)
        status_str = (order_data.get("status") or "").upper()
        new_state = CONSTANTS.ORDER_STATE.get(status_str)
        if new_state is None:
            raise ValueError(
                f"Unknown CSX order status '{status_str}' for {tracked_order.exchange_order_id}"
            )

        update_ts = float(order_data.get("updatedAt", order_data.get("updated_at", 0)))
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_ts,
            new_state=new_state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []

        trade_updates: List[TradeUpdate] = []
        try:
            result = await self._api_get(
                path_url=f"{CONSTANTS.ORDER_BY_ID_PATH_URL}/{order.exchange_order_id}",
                limit_id=CONSTANTS.ORDER_BY_ID_PATH_URL,
                is_auth_required=True,
            )

            order_data = result if "orderId" in result else result.get("data", result)
            filled_qty = Decimal(str(order_data.get("filledQuantity", "0")))
            filled_quote_qty = Decimal(str(order_data.get("filledQuoteQuantity", "0")))

            if filled_qty <= 0:
                return []

            avg_price = (
                filled_quote_qty / filled_qty
                if filled_qty > 0
                else Decimal(str(order_data.get("averagePrice", "0")))
            )

            is_maker = order.order_type is OrderType.LIMIT_MAKER
            fee_pct = Decimal(str(
                order_data.get("makerFee", 0) if is_maker else order_data.get("takerFee", 0)
            ))
            fee = DeductedFromReturnsTradeFee(percent=fee_pct / Decimal("100"))

            trade_updates.append(
                TradeUpdate(
                    trade_id=str(order.exchange_order_id),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(order.exchange_order_id),
                    trading_pair=order.trading_pair,
                    fee=fee,
                    fill_base_amount=filled_qty,
                    fill_quote_amount=filled_quote_qty,
                    fill_price=avg_price,
                    fill_timestamp=float(order_data.get("updatedAt", 0)),
                )
            )
        except Exception as exc:
            self.logger().error(
                f"Error fetching trade updates for {order.exchange_order_id}: {exc}"
            )

        return trade_updates

    # ── Balance ────────────────────────────────────────────────────────────────

    async def _update_balances(self) -> None:
        local_assets = set(self._account_balances.keys())
        remote_assets: set = set()

        try:
            response = await self._api_get(
                path_url=CONSTANTS.BALANCE_V2_PATH_URL,
                is_auth_required=True,
            )

            balance_data = response if isinstance(response, dict) else {}
            available = balance_data.get("Available") or {}
            locked = balance_data.get("Locked") or {}
            all_assets = set(available.keys()) | set(locked.keys())

            for asset in all_assets:
                free = Decimal(str(available.get(asset, "0")))
                held = Decimal(str(locked.get(asset, "0")))
                key = asset.upper()
                self._account_balances[key] = free + held
                self._account_available_balances[key] = free
                remote_assets.add(key)

            for stale in local_assets - remote_assets:
                del self._account_balances[stale]
                del self._account_available_balances[stale]

        except Exception as exc:
            self.logger().error(f"Error updating CSX balances: {exc}", exc_info=True)

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
        """
        CSX does not expose a dedicated trading-fees endpoint.
        Fees are embedded in order responses (makerFee / takerFee).
        This is a no-op; fee estimation falls back to DEFAULT_FEES.
        """
        pass

    # ── User-stream event listener ─────────────────────────────────────────────

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                event_type = event.get("event")

                if event_type == "balance_update":
                    balance_data = event.get("data") or {}
                    available = balance_data.get("Available") or {}
                    locked = balance_data.get("Locked") or {}
                    all_assets = set(available.keys()) | set(locked.keys())
                    for asset in all_assets:
                        free = Decimal(str(available.get(asset, "0")))
                        held = Decimal(str(locked.get(asset, "0")))
                        key = asset.upper()
                        self._account_balances[key] = free + held
                        self._account_available_balances[key] = free

                elif event_type == "order_update":
                    for order_data in event.get("data") or []:
                        client_order_id = order_data.get("clientOrderId") or order_data.get("client_order_id")
                        exchange_order_id = str(order_data.get("orderId", order_data.get("order_id", "")))
                        status_str = (order_data.get("status") or "").upper()

                        tracked = self._order_tracker.all_updatable_orders.get(client_order_id)
                        if tracked is None:
                            continue

                        new_state = CONSTANTS.ORDER_STATE.get(status_str)
                        if new_state is None:
                            continue

                        order_update = OrderUpdate(
                            trading_pair=tracked.trading_pair,
                            update_timestamp=float(order_data.get("updatedAt", 0)),
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=exchange_order_id,
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in CSX user-stream listener.", exc_info=True)
                await self._sleep(5.0)
