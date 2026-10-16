import asyncio
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_utils as utils,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_order_book_data_source import (
    CoinDCXPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_user_stream_data_source import (
    CoinDCXPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_auth import CoinDCXPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OpenOrder, OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_decimal_0 = Decimal("0")

_ERROR_CODE = re.compile(r'"code"\s*:\s*(-?\d+)')


def error_code(exception: Exception) -> Optional[int]:
    """The FAPI ``code`` carried in a request error's body, if any."""
    match = _ERROR_CODE.search(str(exception))
    return int(match.group(1)) if match else None


class CoindcxPerpetualDerivative(PerpetualDerivativePyBase):
    """
    CoinDCX perpetual futures on the FAPI (Futures API Market Maker Integration
    Guide v1.2): a Binance-style API on REST and WebSocket hosts assigned per account,
    read from ``COINDCX_FAPI_REST_URL`` / ``COINDCX_FAPI_WS_URL``.

    Traits that shape this connector:

    * Orders carry our ``newClientOrderId``, so every order can be queried, cancelled
      and modified by client id even if the create response is lost.
    * Creation, modification and cancellation are asynchronous: the REST call answers
      ``CREATE_IN_PROGRESS`` / ``EDIT_IN_PROGRESS`` / ``CANCEL_IN_PROGRESS`` and the
      outcome arrives on ``ORDER_TRADE_UPDATE`` (or a later query). An order therefore
      stays PENDING_CREATE until the venue confirms it, and a cancel is not trusted
      until the venue reports it done.
    * LIMIT_MAKER maps to ``timeInForce=GTX`` (post-only).
    * One-way positions only. The new-order request documents no ``reduceOnly`` or
      ``positionSide``, so a closing order is a plain opposite order; ``exit_position``
      wraps the venue's reduce-only market close.
    """

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter" = None,
        coindcx_perpetual_api_key: str = None,
        coindcx_perpetual_api_secret: str = None,
        coindcx_perpetual_margin_currency: str = CONSTANTS.DEFAULT_MARGIN_CURRENCY,
        coindcx_perpetual_proxy_url: str = "",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        self.api_key = coindcx_perpetual_api_key or ""
        self.secret_key = coindcx_perpetual_api_secret or ""
        self._margin_currency = utils.normalize_margin_currency(coindcx_perpetual_margin_currency)
        self._proxy_url = coindcx_perpetual_proxy_url or ""
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---- identity / configuration -------------------------------------------

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinDCXPerpetualAuth:
        return CoinDCXPerpetualAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer,
        )

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # A cancel only starts (CANCEL_IN_PROGRESS); the outcome comes later.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def is_edit_order_supported_by_exchange(self) -> bool:
        return True

    @property
    def funding_fee_poll_interval(self) -> int:
        return CONSTANTS.FUNDING_FEE_POLL_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    @property
    def margin_currency(self) -> str:
        """Margin currency the position-exit endpoints act on (USDT or INR)."""
        return self._margin_currency

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].sell_order_collateral_token

    # ---- factories -----------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None,
        )

    def _create_order_book_data_source(self) -> CoinDCXPerpetualAPIOrderBookDataSource:
        return CoinDCXPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinDCXPerpetualAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def stop_network(self):
        await super().stop_network()
        # A proxied factory owns a dedicated aiohttp session; the default factory
        # is a shared singleton and must be left alone.
        if self._proxy_url:
            try:
                await self._web_assistants_factory.close()
            except Exception:
                self.logger().debug("Failed closing the proxy connections factory.", exc_info=True)

    # ---- error classification -------------------------------------------------

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return error_code(request_exception) == CONSTANTS.TIMESTAMP_ERROR_CODE

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # A query can lag the latest state by up to a second, so a brand-new order may
        # briefly read as missing; the tracker only gives up after repeated misses.
        return error_code(status_update_exception) == CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # -2011 means "already filled or unknown": either way nothing is left to
        # cancel, and the next status query tells which.
        return error_code(cancelation_exception) in (CONSTANTS.CANCEL_REJECTED_ERROR_CODE,
                                                     CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE)

    async def _signed_pages(self, path_url: str, data: Dict[str, Any],
                            limit_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read a paged private list (page/limit) to the end, or up to MAX_PAGES."""
        records: List[Dict[str, Any]] = []
        for page in range(1, CONSTANTS.MAX_PAGES + 1):
            batch = await self._api_get(path_url=path_url,
                                        data={**data, "page": page, "limit": CONSTANTS.PAGE_LIMIT},
                                        is_auth_required=True, limit_id=limit_id)
            if not isinstance(batch, list):
                break
            records.extend(entry for entry in batch if isinstance(entry, dict))
            if len(batch) < CONSTANTS.PAGE_LIMIT:
                break
        else:
            self.logger().warning(f"Stopped paging {path_url} after {CONSTANTS.MAX_PAGES} pages.")
        return records

    # ---- exchange info / trading rules ---------------------------------------

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_info in filter(utils.is_exchange_information_valid, exchange_info.get("symbols") or []):
            trading_pair = combine_to_hb_trading_pair(base=symbol_info["baseAsset"], quote=symbol_info["quoteAsset"])
            if trading_pair in mapping.inverse:
                continue
            mapping[symbol_info["symbol"]] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for symbol_info in filter(utils.is_exchange_information_valid, exchange_info.get("symbols") or []):
            try:
                price_filter = utils.symbol_filter(symbol_info, "PRICE_FILTER")
                lot_size = utils.symbol_filter(symbol_info, "LOT_SIZE")
                min_notional = utils.symbol_filter(symbol_info, "MIN_NOTIONAL")
                rules.append(TradingRule(
                    trading_pair=combine_to_hb_trading_pair(base=symbol_info["baseAsset"],
                                                            quote=symbol_info["quoteAsset"]),
                    min_order_size=Decimal(lot_size["minQty"]),
                    max_order_size=Decimal(lot_size["maxQty"]),
                    min_price_increment=Decimal(price_filter["tickSize"]),
                    min_base_amount_increment=Decimal(lot_size["stepSize"]),
                    min_notional_size=Decimal(str(min_notional.get("notional", 0))),
                    # The contract's margin asset backs both sides.
                    buy_order_collateral_token=symbol_info.get("marginAsset") or symbol_info["quoteAsset"],
                    sell_order_collateral_token=symbol_info.get("marginAsset") or symbol_info["quoteAsset"],
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {symbol_info.get('symbol')}.")
        return rules

    async def _update_trading_fees(self):
        """The FAPI publishes no per-instrument fees; ``_get_fee`` uses the default schema."""
        pass

    # ---- fees ---------------------------------------------------------------

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker if is_maker is not None else (order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER))
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent=self.estimate_fee_pct(is_maker),
            percent_token=CONSTANTS.FEE_CURRENCY,
        )

    # ---- orders --------------------------------------------------------------

    @staticmethod
    def _decimal_string(value: Decimal, increment: Optional[Decimal] = None) -> str:
        """
        A decimal as the string the API wants, at the instrument's precision: the
        guide says trailing zeros are significant for the configured precision.
        """
        if increment is None or increment <= s_decimal_0:
            return format(value.normalize(), "f")
        exponent = min(increment.normalize().as_tuple().exponent, 0)
        return format(value.quantize(Decimal(1).scaleb(exponent)), "f")

    async def _submit_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                            order_type: OrderType, price: Decimal) -> Dict[str, Any]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rule = self._trading_rules.get(trading_pair)
        body: Dict[str, Any] = {
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": CONSTANTS.ORDER_TYPE_MARKET if order_type is OrderType.MARKET else CONSTANTS.ORDER_TYPE_LIMIT,
            "quantity": self._decimal_string(amount, rule.min_base_amount_increment if rule else None),
            "newClientOrderId": order_id,
        }
        if order_type is not OrderType.MARKET:
            body["price"] = self._decimal_string(price, rule.min_price_increment if rule else None)
            body["timeInForce"] = (CONSTANTS.TIME_IN_FORCE_GTX if order_type is OrderType.LIMIT_MAKER
                                   else CONSTANTS.TIME_IN_FORCE_GTC)
        response = await self._api_post(path_url=CONSTANTS.ORDER_PATH_URL, data=body, is_auth_required=True,
                                        limit_id=CONSTANTS.CREATE_ORDER_LIMIT_ID)
        if not isinstance(response, dict) or not response.get("orderId"):
            raise IOError(f"Unexpected response creating order {order_id}: {response}")
        return response

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           position_action: PositionAction = PositionAction.NIL,
                           **kwargs) -> Tuple[str, float]:
        # The new-order request has no reduceOnly/positionSide field, so opening and
        # closing orders are sent the same way; in one-way mode they net.
        response = await self._submit_order(order_id, trading_pair, amount, trade_type, order_type, price)
        return str(response["orderId"]), self._time_synchronizer.time()

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        """
        Place the order and record what the venue said. A create answers
        CREATE_IN_PROGRESS, so the order stays PENDING_CREATE (rather than the base
        class's OPEN) until the stream or a query confirms it is live.
        """
        response = await self._submit_order(order.client_order_id, order.trading_pair, order.amount,
                                            order.trade_type, order.order_type, order.price)
        exchange_order_id = str(response["orderId"])
        self._order_tracker.process_order_update(OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=self._time_synchronizer.time(),
            new_state=CONSTANTS.ORDER_STATE.get(str(response.get("status")), OrderState.PENDING_CREATE),
        ))
        return exchange_order_id

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_delete(path_url=CONSTANTS.ORDER_PATH_URL,
                                          data={"symbol": symbol, "origClientOrderId": order_id},
                                          is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID)
        if isinstance(response, dict) and response.get("status") in CONSTANTS.CANCEL_ACCEPTED_STATUSES:
            return True
        raise IOError(f"Unexpected response cancelling order {order_id}: {response}")

    async def _place_edit(self,
                          client_order_id: str,
                          exchange_order_id: str,
                          trading_pair: str,
                          new_price: Decimal,
                          new_amount: Decimal,
                          **kwargs) -> Tuple[str, str, float]:
        """Native modify (PUT /fapi/v1/order). Ids are unchanged; the edit completes asynchronously."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rule = self._trading_rules.get(trading_pair)
        response = await self._api_put(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data={
                "symbol": symbol,
                "origClientOrderId": client_order_id,
                "quantity": self._decimal_string(new_amount, rule.min_base_amount_increment if rule else None),
                "price": self._decimal_string(new_price, rule.min_price_increment if rule else None),
            },
            is_auth_required=True, limit_id=CONSTANTS.MODIFY_ORDER_LIMIT_ID)
        if not isinstance(response, dict) or not response.get("orderId"):
            raise IOError(f"Unexpected response modifying order {client_order_id}: {response}")
        return client_order_id, str(response["orderId"]), self._time_synchronizer.time()

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        order = await self._api_get(path_url=CONSTANTS.ORDER_PATH_URL,
                                    data={"symbol": symbol, "origClientOrderId": tracked_order.client_order_id},
                                    is_auth_required=True, limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID)
        return self._order_update_from_order(order, tracked_order)

    def _order_update_from_order(self, order: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        """From a REST order object or an ORDER_TRADE_UPDATE ``o`` block."""
        status = str(order.get("status") or order.get("X") or "")
        timestamp = order.get("updateTime") or order.get("T") or order.get("time")
        exchange_order_id = order.get("orderId") or order.get("i") or tracked_order.exchange_order_id
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(timestamp) / 1000 if timestamp else self._time_synchronizer.time(),
            new_state=CONSTANTS.ORDER_STATE.get(status, tracked_order.current_state),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(exchange_order_id) if exchange_order_id else None,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        trades = await self._signed_pages(CONSTANTS.USER_TRADES_PATH_URL,
                                          {"symbol": symbol, "orderId": order.exchange_order_id})
        return [self._trade_update_from_trade(trade, order) for trade in trades
                if str(trade.get("orderId")) == str(order.exchange_order_id)]

    def _fee(self, order: InFlightOrder, amount: Any, asset: Optional[str]) -> TradeFeeBase:
        fee_amount = Decimal(str(amount or 0))
        token = asset or CONSTANTS.FEE_CURRENCY
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=order.position,
            percent_token=token,
            flat_fees=[TokenAmount(amount=fee_amount, token=token)] if fee_amount else [],
        )

    def _trade_update_from_trade(self, trade: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        """From a ``GET /fapi/v1/userTrades`` row."""
        price = Decimal(str(trade["price"]))
        amount = Decimal(str(trade["qty"]))
        return TradeUpdate(
            trade_id=str(trade["id"]),
            client_order_id=order.client_order_id,
            exchange_order_id=str(trade.get("orderId", order.exchange_order_id)),
            trading_pair=order.trading_pair,
            fee=self._fee(order, trade.get("commission"), trade.get("commissionAsset")),
            fill_base_amount=amount,
            fill_quote_amount=Decimal(str(trade.get("quoteQty") or amount * price)),
            fill_price=price,
            fill_timestamp=float(trade.get("time") or 0) / 1000 or self._time_synchronizer.time(),
        )

    def _trade_update_from_stream(self, update: Dict[str, Any], order: InFlightOrder) -> Optional[TradeUpdate]:
        """
        A fill from an ORDER_TRADE_UPDATE ``o`` block. The guide documents only ``z``
        (cumulative quantity); the per-fill fields are Binance's (``l`` quantity, ``L``
        price, ``t`` trade id, ``n``/``N`` commission). None when they are absent.
        """
        if update.get("x") != "TRADE":
            return None
        amount, price, trade_id = update.get("l"), update.get("L"), update.get("t")
        if amount is None or price is None or trade_id is None:
            return None
        amount, price = Decimal(str(amount)), Decimal(str(price))
        if amount <= s_decimal_0:
            return None
        timestamp = update.get("T")
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=str(update.get("i", order.exchange_order_id)),
            trading_pair=order.trading_pair,
            fee=self._fee(order, update.get("n"), update.get("N")),
            fill_base_amount=amount,
            fill_quote_amount=amount * price,
            fill_price=price,
            fill_timestamp=float(timestamp) / 1000 if timestamp else self._time_synchronizer.time(),
        )

    async def get_open_orders(self) -> List[OpenOrder]:
        """Orders resting on the venue now, for every symbol, including ones placed elsewhere."""
        orders: List[OpenOrder] = []
        for order in await self._signed_pages(CONSTANTS.OPEN_ORDERS_PATH_URL, {}):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=order.get("symbol", ""))
            except KeyError:
                continue
            orders.append(OpenOrder(
                client_order_id=str(order.get("clientOrderId") or ""),
                trading_pair=trading_pair,
                price=Decimal(str(order.get("price") or 0)),
                amount=Decimal(str(order.get("origQty") or 0)),
                executed_amount=Decimal(str(order.get("executedQty") or 0)),
                status=str(order.get("status", "")),
                order_type=OrderType.MARKET if order.get("type") == CONSTANTS.ORDER_TYPE_MARKET else OrderType.LIMIT,
                is_buy=order.get("side") == CONSTANTS.SIDE_BUY,
                time=int(order.get("time") or 0),
                exchange_order_id=str(order.get("orderId") or ""),
            ))
        return orders

    # ---- balances ------------------------------------------------------------

    async def _update_balances(self):
        rows = await self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, data={}, is_auth_required=True)
        local_assets = set(self._account_balances.keys())
        remote_assets = set()
        for row in rows if isinstance(rows, list) else []:
            asset = row.get("asset")
            if not asset or row.get("balance") is None:
                continue
            total = Decimal(str(row["balance"]))
            self._account_balances[asset] = total
            self._account_available_balances[asset] = Decimal(str(row.get("availableBalance", total)))
            remote_assets.add(asset)
        for asset in local_assets - remote_assets:
            self._account_balances.pop(asset, None)
            self._account_available_balances.pop(asset, None)

    def _process_balance_rows(self, rows: List[Dict[str, Any]]):
        """ACCOUNT_UPDATE ``a.B``: ``wb`` wallet balance and ``lb`` locked, per asset."""
        for row in rows or []:
            asset, wallet = row.get("a"), row.get("wb")
            if not asset or wallet is None:
                continue
            total = Decimal(str(wallet))
            self._account_balances[asset] = total
            self._account_available_balances[asset] = total - Decimal(str(row.get("lb") or 0))

    # ---- positions -----------------------------------------------------------

    async def _apply_position(self, symbol: str, amount: Any, entry_price: Any, unrealized_pnl: Any,
                              leverage: Any = None) -> Optional[str]:
        """Record one position (one-way, signed amount). Returns its key, or None if flat or unknown."""
        try:
            trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        except KeyError:
            return None
        amount = Decimal(str(amount or 0))
        if amount == s_decimal_0:
            for side in (PositionSide.LONG, PositionSide.SHORT):
                self._perpetual_trading.remove_position(self._perpetual_trading.position_key(trading_pair, side))
            return None
        side = PositionSide.LONG if amount > 0 else PositionSide.SHORT
        position_key = self._perpetual_trading.position_key(trading_pair, side)
        self._perpetual_trading.set_position(position_key, Position(
            trading_pair=trading_pair,
            position_side=side,
            unrealized_pnl=Decimal(str(unrealized_pnl or 0)),
            entry_price=Decimal(str(entry_price or 0)),
            amount=amount,
            leverage=Decimal(str(leverage or self.get_leverage(trading_pair))),
        ))
        return position_key

    async def _update_positions(self):
        rows = await self._api_get(path_url=CONSTANTS.POSITION_RISK_PATH_URL, data={}, is_auth_required=True)
        reported = set()
        for row in rows if isinstance(rows, list) else []:
            key = await self._apply_position(row.get("symbol", ""), row.get("positionAmt"), row.get("entryPrice"),
                                             row.get("unRealizedProfit"), row.get("leverage"))
            if key is not None:
                reported.add(key)
        # Anything the venue no longer reports is flat, however it was closed.
        for key in list(self._perpetual_trading.account_positions.keys()):
            if key not in reported:
                self._perpetual_trading.remove_position(key)

    async def _process_position_rows(self, rows: List[Dict[str, Any]]):
        """POSITION_UPDATE ``P``: ``s`` symbol, ``pa`` amount, ``ep`` entry, ``up`` unrealized PnL."""
        for row in rows or []:
            await self._apply_position(row.get("s", ""), row.get("pa"), row.get("ep"), row.get("up"))

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is PositionMode.ONEWAY:
            return True, ""
        return False, "CoinDCX futures supports ONEWAY position mode only."

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            response = await self._api_post(path_url=CONSTANTS.LEVERAGE_PATH_URL,
                                            data={"symbol": symbol, "leverage": int(leverage)},
                                            is_auth_required=True)
        except Exception as exception:
            return False, str(exception)
        if isinstance(response, dict) and str(response.get("leverage")) == str(int(leverage)):
            return True, ""
        return False, f"Unexpected leverage response: {response}"

    async def exit_position(self, trading_pair: str) -> str:
        """
        Close the whole position on ``trading_pair`` with the venue's reduce-only market
        order (POST /fapi/v1/exitPosition). Returns the ``groupId`` that tags the
        resulting order updates; those orders are not tracked as ours.
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_post(path_url=CONSTANTS.EXIT_POSITION_PATH_URL,
                                        data={"symbol": symbol, "marginCurrency": self._margin_currency},
                                        is_auth_required=True)
        return str((response or {}).get("groupId", ""))

    async def close_all_positions(self) -> str:
        """Close every position in the margin currency (POST /fapi/v1/closeAllPositions)."""
        response = await self._api_post(path_url=CONSTANTS.CLOSE_ALL_POSITIONS_PATH_URL,
                                        data={"marginCurrency": self._margin_currency}, is_auth_required=True)
        return str((response or {}).get("groupId", ""))

    # ---- funding -------------------------------------------------------------

    async def build_funding_info(self, trading_pair: str) -> FundingInfo:
        """Mark price, index price, funding rate and next funding time from ``premiumIndex``."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        info = await self._api_get(path_url=CONSTANTS.PREMIUM_INDEX_PATH_URL, params={"symbol": symbol})
        if isinstance(info, list):
            info = next((entry for entry in info if isinstance(entry, dict) and entry.get("symbol") == symbol), None)
        mark_price = Decimal(str((info or {}).get("markPrice") or 0))
        if mark_price <= s_decimal_0:
            # A zero mark price would silently corrupt PnL and funding maths.
            raise ValueError(f"CoinDCX reported no usable mark price for {trading_pair}: {info}")
        index_price = Decimal(str(info.get("indexPrice") or 0))
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price if index_price > s_decimal_0 else mark_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=int(float(info["nextFundingTime"]) * 1e-3),
            rate=Decimal(str(info.get("lastFundingRate") or 0)),
        )

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        """
        Most recent funding payment for the pair, as (timestamp, rate, amount), from the
        FUNDING_FEE income records. The records carry no rate, so - as Binance's
        connector does - the current rate is read from ``premiumIndex``.

        Returns ``(0, -1, -1)`` when the account has no funding payment on this pair,
        which is what the base class expects.
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        records = await self._signed_pages(CONSTANTS.INCOME_PATH_URL,
                                           {"symbol": symbol, "incomeType": CONSTANTS.INCOME_TYPE_FUNDING_FEE})
        records = [record for record in records if record.get("symbol", symbol) == symbol and record.get("time")]
        if not records:
            return 0, Decimal("-1"), Decimal("-1")
        latest = max(records, key=lambda record: float(record["time"]))
        rate = Decimal("-1")
        try:
            rate = (await self.build_funding_info(trading_pair)).rate
        except Exception as exception:
            self.logger().debug(f"Could not read the funding rate for {trading_pair}: {exception}")
        return float(latest["time"]) / 1000, rate, Decimal(str(latest.get("income") or 0))

    # ---- prices --------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(path_url=CONSTANTS.TICKER_PRICE_PATH_URL, params={"symbol": symbol})
        price = response.get("price") if isinstance(response, dict) else None
        if price is None or Decimal(str(price)) <= s_decimal_0:
            # Returning 0 would feed a zero price straight into strategy maths.
            raise ValueError(f"CoinDCX reported no last price for {trading_pair}: {response}")
        return float(price)

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """Best bid and ask for every symbol, from ``ticker/bookTicker`` (rate oracle)."""
        tickers = await self._api_get(path_url=CONSTANTS.BOOK_TICKER_PATH_URL)
        return tickers if isinstance(tickers, list) else []

    async def get_all_24h_volume_tickers(self, trading_pairs: Optional[List[str]] = None) -> List[Dict[str, str]]:
        """
        24h statistics for every symbol, from ``ticker/24hr`` (volume oracle). One
        all-symbols call is cheaper than one call per pair once there are more than
        a handful, so callers filter.
        """
        tickers = await self._api_get(path_url=CONSTANTS.TICKER_24HR_PATH_URL)
        return tickers if isinstance(tickers, list) else []

    # ---- user stream ---------------------------------------------------------

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                event_type = event.get("e")
                if event_type == CONSTANTS.ORDER_TRADE_UPDATE_EVENT:
                    await self._process_order_trade_update(event.get("o") or {})
                elif event_type == CONSTANTS.ACCOUNT_UPDATE_EVENT:
                    account = event.get("a") or {}
                    self._process_balance_rows(account.get("B"))
                    await self._process_position_rows(account.get("P"))
                elif event_type == CONSTANTS.POSITION_UPDATE_EVENT:
                    await self._process_position_rows(event.get("P"))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the CoinDCX private stream listener.")
                await self._sleep(5.0)

    async def _process_order_trade_update(self, update: Dict[str, Any]):
        """
        Apply one ORDER_TRADE_UPDATE. Fills go first: the tracker stops tracking an
        order once it is terminal, so a FILLED applied before its fills would settle
        the order with nothing executed. When the frame carries no per-fill detail but
        ``z`` (cumulative filled) has moved, the fills are read over REST first.
        """
        tracked_order = self._order_tracker.all_updatable_orders.get(update.get("c"))
        if tracked_order is None:
            return  # placed by someone else, or from an earlier session we no longer track
        if tracked_order.exchange_order_id is None and update.get("i"):
            tracked_order.update_exchange_order_id(str(update["i"]))

        trade_update = self._trade_update_from_stream(update, tracked_order)
        if trade_update is not None:
            self._order_tracker.process_trade_update(trade_update)
        elif Decimal(str(update.get("z") or 0)) > tracked_order.executed_amount_base:
            await self._update_orders_fills([tracked_order])

        self._order_tracker.process_order_update(self._order_update_from_order(update, tracked_order))
