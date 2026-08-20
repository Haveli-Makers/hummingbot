import asyncio
import json
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS, ajaib_utils, ajaib_web_utils as web_utils
from hummingbot.connector.exchange.ajaib.ajaib_api_order_book_data_source import AjaibAPIOrderBookDataSource
from hummingbot.connector.exchange.ajaib.ajaib_api_user_stream_data_source import AjaibAPIUserStreamDataSource
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class AjaibExchange(ExchangePyBase):
    """
    Ajaib (Coin Exchange) Open API connector implementation.

    The Ajaib Open API (https://ajaib.gitbook.io/ajaib-exchange-open-api) is a
    Binance-style REST + WebSocket API with Ed25519 request signing.

    API access is restricted by IP ALLOWLIST, so traffic normally has to be
    routed through a cleared egress via ``ajaib_proxy_url`` (see
    ``docs/PROXY_SERVER_GUIDE.md``). Mainnet and testnet are allowlisted
    separately. Every REST endpoint is signed -- there are no anonymous public
    endpoints -- and ``recvWindow`` is capped at 5000ms by the server.
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 ajaib_api_key: str,
                 ajaib_api_secret: str,
                 ajaib_proxy_url: str = "",
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = ajaib_api_key
        self.secret_key = ajaib_api_secret
        self._proxy_url = ajaib_proxy_url or ""
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
        # Ajaib MARKET orders are sized by ``quoteOrderQty`` which does not map
        # cleanly onto Hummingbot's base-amount market orders, so only limit
        # order types are advertised.
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        order_id = ajaib_utils.generate_client_order_id()
        safe_ensure_future(self._create_order(
            trade_type=TradeType.BUY,
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.LIMIT,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        order_id = ajaib_utils.generate_client_order_id()
        safe_ensure_future(self._create_order(
            trade_type=TradeType.SELL,
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return order_id

    async def start_network(self):
        await super().start_network()
        if self.is_trading_required:
            try:
                await self._update_balances()
            except Exception as e:
                self.logger().warning(f"Failed to fetch initial balances: {e}")

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Returns a list of ``{symbol, bidPrice, askPrice, lastPrice}`` dicts.

        Sourced from ``/v1/ticker/book-ticker``, which returns a real bid and
        ask and accepts up to 50 symbols per request. The previous
        implementation issued one ``/v1/klines`` call PER SYMBOL (~53 sequential
        requests on mainnet) and reported the close as bid, ask and last alike,
        so the oracle always saw a zero spread. Klines remains the per-symbol
        fallback for anything book-ticker does not return.
        """
        if not self._keys_configured:
            self.logger().warning("Ajaib API keys not configured. Cannot fetch prices.")
            return []

        exchange_info = await self._make_trading_pairs_request()
        symbols = [s["symbol"] for s in exchange_info.get("symbols", [])
                   if ajaib_utils.is_exchange_information_valid(s)]

        results: List[Dict[str, str]] = []
        covered = set()

        batch_size = CONSTANTS.BOOK_TICKER_MAX_SYMBOLS
        for start in range(0, len(symbols), batch_size):
            batch = symbols[start:start + batch_size]
            try:
                tickers = await self._api_get(
                    path_url=CONSTANTS.BOOK_TICKER_PATH_URL,
                    params={"symbols": json.dumps(batch)},
                    is_auth_required=True)
            except Exception as exception:
                self.logger().debug(
                    f"book-ticker batch {start // batch_size} failed: {exception}")
                continue

            for ticker in tickers if isinstance(tickers, list) else []:
                if not isinstance(ticker, dict):
                    continue
                symbol = ticker.get("symbol")
                bid, ask = ticker.get("bidPrice"), ticker.get("askPrice")
                mid = self._mid_or_side(bid, ask)
                if not symbol or mid <= 0:
                    continue
                covered.add(symbol)
                results.append({
                    "symbol": symbol,
                    # A one-sided book quotes only one side; report the side that
                    # exists rather than a zero that reads as a real price.
                    "bidPrice": str(bid if self._decimal_or_zero(bid) > 0 else mid),
                    "askPrice": str(ask if self._decimal_or_zero(ask) > 0 else mid),
                    "lastPrice": str(mid),
                })

        for symbol in (s for s in symbols if s not in covered):
            try:
                price = await self._price_from_klines(symbol)
            except Exception as exception:
                self.logger().debug(f"Failed to fetch kline price for {symbol}: {exception}")
                continue
            if price > 0:
                results.append({
                    "symbol": symbol,
                    "bidPrice": str(price),
                    "askPrice": str(price),
                    "lastPrice": str(price),
                })

        return results

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
        # Match the API's own code (-2013 NO_SUCH_ORDER) or message, never the
        # bare HTTP status: the gateway also answers 404 for an unknown route
        # ("no Route matched"), and reading that as "order gone" would drop a
        # live order from tracking.
        exc = str(status_update_exception).lower()
        return (str(CONSTANTS.ORDER_NOT_EXIST_API_CODE) in exc
                or CONSTANTS.ORDER_NOT_EXIST_MESSAGE.lower() in exc)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        exc_str = str(cancelation_exception).lower()
        # Match on the API's own code (-2013) or message, not on the bare HTTP
        # status: "404" appears in unrelated gateway errors ("no Route matched"),
        # and mistaking one for "already gone" drops a live order from tracking.
        return (
            str(CONSTANTS.ORDER_NOT_EXIST_API_CODE) in exc_str
            or CONSTANTS.ORDER_NOT_EXIST_MESSAGE.lower() in exc_str
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
            proxy_url=self._proxy_url or None)

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
        is_maker = is_maker if is_maker is not None else (order_type is OrderType.LIMIT_MAKER)
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    @staticmethod
    def _is_server_side_error(exception: Exception) -> bool:
        """
        True for HTTP 5XX, which Ajaib documents as "the issue is on our server
        side ... the execution status is UNKNOWN and could have been a success".
        """
        text = str(exception)
        return any(f"HTTP status is {code}" in text for code in range(500, 512))

    async def _find_order_by_client_id(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        """Look an order up by the client id we minted, or None if absent."""
        try:
            return await self._api_get(
                path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
                params={"origClientOrderId": client_order_id},
                is_auth_required=True)
        except Exception as exception:
            if self._is_order_not_found_during_status_update_error(exception):
                return None
            self.logger().warning(
                f"Could not confirm whether {client_order_id} exists: {exception}")
            return None

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
            "quantity": f"{amount:f}",
            "price": f"{price:f}",
            "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
            "newClientOrderId": order_id,
        }

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
        except Exception as exception:
            # Ajaib's docs are explicit: "HTTP 5XX return codes are used for
            # internal errors; ... It is important to NOT treat this as a failure
            # operation; the execution status is UNKNOWN and could have been a
            # success." Marking the order FAILED here would leave a live order on
            # the exchange that we no longer track, and a retry would double up.
            if not self._is_server_side_error(exception):
                raise
            self.logger().warning(
                f"Ajaib returned a server-side error placing {order_id}; the order MAY have "
                f"been accepted. Checking by client order id before deciding. Error: {exception}")
            existing = await self._find_order_by_client_id(order_id)
            if existing is None:
                raise
            self.logger().warning(
                f"Order {order_id} WAS created despite the server error "
                f"(exchange id {existing.get('orderId')}); adopting it.")
            order_result = existing

        o_id = str(order_result.get("orderId", ""))
        transact_time = order_result.get("time", self._time_synchronizer.time() * 1e3) * 1e-3

        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        # ``order_id`` is the UUIDv4 we sent as newClientOrderId == clientOrderId.
        # ``newClientOrderId`` is MANDATORY here and identifies the CANCEL itself
        # (not the order being cancelled), so it must be a fresh UUIDv4.
        api_params = {
            "symbol": symbol,
            "origClientOrderId": order_id,
            "newClientOrderId": ajaib_utils.generate_client_order_id(),
        }

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True)

        # Only an explicit cancelled/terminal status counts. Accepting "any
        # non-null payload" would report an error body as a successful cancel and
        # drop tracking of an order still resting on the exchange.
        if isinstance(cancel_result, dict):
            status = str(cancel_result.get("status", "")).upper()
            if status in CONSTANTS.CANCEL_ACCEPTED_STATUSES:
                return True
            raise IOError(
                f"Unexpected status cancelling order {order_id}: {cancel_result}")
        raise IOError(f"Unexpected response cancelling order {order_id}: {cancel_result}")

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
                if event_type != CONSTANTS.WS_EXECUTION_REPORT_EVENT_TYPE:
                    continue

                client_order_id = self.execution_report_client_order_id(event_message)
                exchange_order_id = str(event_message.get("i", ""))
                tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                if tracked_order is None:
                    # Fall back to the exchange id: a fast fill can arrive before
                    # _place_order has recorded the client id against the order.
                    tracked_order = next(
                        (o for o in self._order_tracker.all_updatable_orders.values()
                         if o.exchange_order_id == exchange_order_id and exchange_order_id),
                        None)
                if tracked_order is None:
                    self.logger().debug(
                        f"Ignoring executionReport for untracked order "
                        f"(c={event_message.get('c')!r} C={event_message.get('C')!r} "
                        f"i={exchange_order_id!r} X={event_message.get('X')!r}).")
                    continue
                client_order_id = tracked_order.client_order_id

                event_ts = event_message.get("T") or event_message.get("E")
                update_ts = (event_ts * 1e-3) if event_ts else self._time_synchronizer.time()

                status = event_message.get("X", "")
                new_state = self.resolve_order_state(
                    status, event_message.get("W"), tracked_order.current_state)
                order_update = OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=update_ts,
                    new_state=new_state,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                )
                self._order_tracker.process_order_update(order_update=order_update)

                # The execution report carries no commission, so emit the fill
                # amount in real time with an estimated fee; the authoritative
                # commission/tax is reconciled from ``/v1/trades`` polling.
                # l/L/Y are blank ("") on non-trade events and are being migrated
                # to "0", so both shapes must parse to zero rather than raise.
                fill_qty = self._decimal_or_zero(event_message.get("l"))
                trade_id = str(event_message.get("t", ""))
                # "t" is -1 when the event is not a trade (Open API changes).
                if fill_qty > 0 and trade_id not in ("", "-1"):
                    fill_price = self._decimal_or_zero(event_message.get("L"))
                    fill_quote_amount = self._decimal_or_zero(event_message.get("Y")) or fill_qty * fill_price
                    is_maker = bool(event_message.get("m", False))

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        percent=self.estimate_fee_pct(is_maker),
                        percent_token=tracked_order.quote_asset,
                    )

                    trade_update = TradeUpdate(
                        trade_id=trade_id,
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_qty,
                        fill_quote_amount=fill_quote_amount,
                        fill_price=fill_price,
                        fill_timestamp=update_ts,
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
                    flat_fees = []
                    commission = Decimal(str(trade.get("commission", "0")))
                    if commission > 0:
                        flat_fees.append(TokenAmount(amount=commission, token=trade.get("commissionAsset", "")))
                    # Indonesian VAT is reported separately on each fill.
                    tax = Decimal(str(trade.get("tax", "0")))
                    if tax > 0:
                        flat_fees.append(TokenAmount(amount=tax, token=trade.get("taxAsset", "")))

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        flat_fees=flat_fees,
                    )

                    trade_update = TradeUpdate(
                        trade_id=str(trade.get("id", "")),
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(str(trade.get("qty", "0"))),
                        fill_quote_amount=Decimal(str(trade.get("quoteQty", "0"))),
                        fill_price=Decimal(str(trade.get("price", "0"))),
                        fill_timestamp=trade.get("time", self._time_synchronizer.time() * 1e3) * 1e-3,
                    )
                    trade_updates.append(trade_update)
            except Exception as e:
                self.logger().error(f"Error fetching trades for order {order.client_order_id}: {e}")

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        # Get Order is keyed by the client order id (our UUIDv4) per the docs.
        params = {"symbol": symbol, "origClientOrderId": tracked_order.client_order_id}

        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            params=params,
            is_auth_required=True)

        status = updated_order_data.get("status", "")
        new_state = self.resolve_order_state(
            status, updated_order_data.get("workingTime"), tracked_order.current_state)

        update_ts = updated_order_data.get("updateTime") or updated_order_data.get("time")
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data.get("orderId", "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=(update_ts * 1e-3) if update_ts else self._time_synchronizer.time(),
            new_state=new_state,
        )
        return order_update

    @staticmethod
    def _decimal_or_zero(value: Any) -> Decimal:
        """Execution-report numeric fields arrive blank on non-trade events."""
        try:
            text = str(value).strip()
            return Decimal(text) if text else Decimal("0")
        except (TypeError, ValueError, InvalidOperation):
            return Decimal("0")

    @staticmethod
    def execution_report_client_order_id(event: Dict[str, Any]) -> str:
        """
        Pull OUR order's client id out of an executionReport.

        The field that carries it moves depending on the event (Open API
        changes > Execution Report WS):

          * any event except CANCELLED -- ``c`` is the ClientOrderId and ``C``
            is an empty string.
          * CANCELLED -- ``c`` is the *newClientOrderId* that identified the
            CANCEL request, and ``C`` is the original ClientOrderId.

        So reading ``c`` unconditionally silently drops every cancellation:
        it holds the cancel's own UUID, which matches no tracked order.
        ``C`` when populated, ``c`` otherwise, covers both shapes.
        """
        original = str(event.get("C") or "").strip()
        return original or str(event.get("c") or "")

    @staticmethod
    def resolve_order_state(status: str, working_time: Any, current_state):
        """
        Map an Ajaib order status onto a Hummingbot order state.

        ``NEW`` is two different things depending on ``workingTime`` (docs >
        Definitions): zero means the exchange has received the order but it is
        "not valid yet", non-zero means it reached the matching engine. Treating
        the first case as OPEN advertises an order as live before it can trade.
        """
        state = CONSTANTS.ORDER_STATE.get(status, current_state)
        if status == "NEW":
            try:
                is_working = working_time is not None and float(working_time) != 0
            except (TypeError, ValueError):
                is_working = False
            if not is_working:
                return OrderState.PENDING_CREATE
        return state

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_PATH_URL,
            is_auth_required=True)

        balances = account_info.get("balances", []) if isinstance(account_info, dict) else account_info

        for balance_entry in balances:
            asset_name = balance_entry.get("asset", "")
            free_balance = Decimal(str(balance_entry.get("free", "0")))
            locked_balance = Decimal(str(balance_entry.get("locked", "0")))
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
                {"symbol": "BTC_USDT", "baseAsset": "BTC", "quoteAsset": "USDT", ...}
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
        """
        Last traded price, falling back across the three sources Ajaib offers.

        ``/v1/klines`` is the natural source but is not always available (it
        returns 503 on testnet), and a single failing endpoint used to make the
        connector report 0.0 -- which silently disables anything that sizes or
        prices from it. ``/v1/ticker/book-ticker`` gives a live bid/ask and
        ``/v1/depth`` the raw book, so either can stand in.
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        for source in (self._price_from_klines,
                       self._price_from_book_ticker,
                       self._price_from_depth):
            try:
                price = await source(symbol)
            except Exception as exception:
                self.logger().debug(
                    f"{source.__name__} failed for {trading_pair}: {exception}")
                continue
            if price > 0:
                return price

        self.logger().error(
            f"No price available for {trading_pair}: klines, book-ticker and depth all failed.")
        return 0.0

    async def _price_from_klines(self, symbol: str) -> float:
        klines = await self._api_get(
            path_url=CONSTANTS.KLINES_PATH_URL,
            params={"symbol": symbol, "interval": "1m", "limit": 1},
            is_auth_required=True)
        # [openTime, open, high, low, CLOSE, volume, closeTime]
        return float(klines[0][4]) if klines else 0.0

    async def _price_from_book_ticker(self, symbol: str) -> float:
        # Note the PLURAL "symbols" parameter -- "symbol" is rejected.
        tickers = await self._api_get(
            path_url=CONSTANTS.BOOK_TICKER_PATH_URL,
            params={"symbols": symbol},
            is_auth_required=True)
        for ticker in tickers if isinstance(tickers, list) else [tickers]:
            if not isinstance(ticker, dict) or ticker.get("symbol") != symbol:
                continue
            return self._mid_or_side(ticker.get("bidPrice"), ticker.get("askPrice"))
        return 0.0

    async def _price_from_depth(self, symbol: str) -> float:
        book = await self._api_get(
            path_url=CONSTANTS.DEPTH_PATH_URL,
            params={"symbol": symbol, "limit": 5},
            is_auth_required=True)
        if not isinstance(book, dict):
            return 0.0
        bids, asks = book.get("bids") or [], book.get("asks") or []
        return self._mid_or_side(bids[0][0] if bids else None,
                                 asks[0][0] if asks else None)

    @staticmethod
    def _mid_or_side(bid: Any, ask: Any) -> float:
        """
        Mid price when both sides quote, otherwise whichever side exists.

        A one-sided book is normal on a thin market (testnet BTC_IDR currently
        has bids and no asks); averaging in a zero would halve the price.
        """
        bid_price = float(AjaibExchange._decimal_or_zero(bid))
        ask_price = float(AjaibExchange._decimal_or_zero(ask))
        if bid_price > 0 and ask_price > 0:
            return (bid_price + ask_price) / 2
        return bid_price or ask_price or 0.0
