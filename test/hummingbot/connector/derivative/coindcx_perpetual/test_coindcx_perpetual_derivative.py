import asyncio
import os
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.coindcx_perpetual import (
    coindcx_perpetual_constants as CONSTANTS,
    coindcx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
    CoindcxPerpetualDerivative,
    error_code,
)
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent


def _symbol(symbol, base, quote, status="TRADING", contract_type="PERPETUAL"):
    """An exchangeInfo symbol in the shape of the FAPI guide's example."""
    return {
        "symbol": symbol, "pair": symbol, "contractType": contract_type, "status": status,
        "baseAsset": base, "quoteAsset": quote, "marginAsset": quote,
        "pricePrecision": 1, "quantityPrecision": 3,
        "filters": [
            {"filterType": "PRICE_FILTER", "maxPrice": "794445.0", "minPrice": "261.1", "tickSize": "0.1"},
            {"filterType": "LOT_SIZE", "maxQty": "10000.0", "minQty": "0.001", "stepSize": "0.001"},
            {"filterType": "MARKET_LOT_SIZE", "maxQty": "120.0", "minQty": "0.001", "stepSize": "0.001"},
            {"filterType": "MAX_NUM_ORDERS", "limit": 200},
            {"filterType": "MIN_NOTIONAL", "notional": "50.0"},
            {"filterType": "PERCENT_PRICE", "multiplierUp": "5.0", "multiplierDown": "5.0",
             "multiplierDecimal": "4"},
        ],
        "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"],
        "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
    }


EXCHANGE_INFO = {
    "timezone": "UTC",
    "serverTime": 1715692200000,
    "rateLimits": [],
    "assets": [{"asset": "USDT", "marginAvailable": True}, {"asset": "INR", "marginAvailable": True}],
    "symbols": [
        _symbol("BTCUSDT", "BTC", "USDT"),
        _symbol("ETHUSDT", "ETH", "USDT"),
        _symbol("OLDUSDT", "OLD", "USDT", status="SETTLING"),
        _symbol("BTCUSDT_261225", "BTC", "USDT", contract_type="CURRENT_QUARTER"),
    ],
}


class CoindcxPerpetualDerivativeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.trading_pair = "BTC-USDT"
        self.exchange = CoindcxPerpetualDerivative(
            coindcx_perpetual_api_key="key",
            coindcx_perpetual_api_secret="secret",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self._event_loggers = []

    # ---- CLI wiring and market data (phase 1) -------------------------------------

    def test_connector_class_resolves_the_way_the_cli_loads_it(self):
        """
        The CLI does not import the class by name — it derives it from the module
        name by capitalising each underscore-separated segment
        (coindcx_perpetual_derivative -> CoindcxPerpetualDerivative) and getattrs
        it off the module. A class named e.g. CoinDCXPerpetualDerivative imports
        fine everywhere else but breaks `connect coindcx_perpetual` at runtime.
        """
        import importlib

        from hummingbot.client.settings import AllConnectorSettings

        setting = AllConnectorSettings.get_connector_settings()["coindcx_perpetual"]
        self.assertEqual("CoindcxPerpetualDerivative", setting.class_name())

        module = importlib.import_module(setting.module_path())
        self.assertTrue(hasattr(module, setting.class_name()),
                        f"{setting.module_path()} has no attribute {setting.class_name()}")
        self.assertIs(CoindcxPerpetualDerivative, getattr(module, setting.class_name()))

    def test_cli_can_build_a_default_connector_instance(self):
        # Exercises the full `connect` path: config keys -> constructor kwargs.
        from hummingbot.client.settings import AllConnectorSettings

        setting = AllConnectorSettings.get_connector_settings()["coindcx_perpetual"]
        connector = setting.non_trading_connector_instance_with_default_configuration(
            trading_pairs=[self.trading_pair])
        self.assertIsInstance(connector, CoindcxPerpetualDerivative)
        self.assertEqual("coindcx_perpetual", connector.name)

    async def test_symbol_map_comes_from_tradable_perpetuals_in_exchange_info(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.assertEqual("BTCUSDT", await self.exchange.exchange_symbol_associated_to_pair(self.trading_pair))
        self.assertEqual("ETH-USDT", await self.exchange.trading_pair_associated_to_exchange_symbol("ETHUSDT"))
        symbol_map = await self.exchange.trading_pair_symbol_map()
        # A settling contract and a dated future are not tradable perpetuals.
        self.assertNotIn("OLDUSDT", symbol_map)
        self.assertNotIn("BTCUSDT_261225", symbol_map)

    async def test_trading_rules_come_from_the_symbol_filters(self):
        rules = await self.exchange._format_trading_rules(EXCHANGE_INFO)
        self.assertEqual(["BTC-USDT", "ETH-USDT"], [rule.trading_pair for rule in rules])
        rule = rules[0]
        self.assertEqual(Decimal("0.1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.001"), rule.min_order_size)
        self.assertEqual(Decimal("10000.0"), rule.max_order_size)
        self.assertEqual(Decimal("50.0"), rule.min_notional_size)
        self.assertEqual("USDT", rule.buy_order_collateral_token)

    async def test_a_symbol_with_broken_filters_is_skipped_not_fatal(self):
        broken = _symbol("XRPUSDT", "XRP", "USDT")
        broken["filters"] = [{"filterType": "PRICE_FILTER", "tickSize": "0.0001"}]
        rules = await self.exchange._format_trading_rules(
            {"symbols": [broken, _symbol("BTCUSDT", "BTC", "USDT")]})
        self.assertEqual(["BTC-USDT"], [rule.trading_pair for rule in rules])

    def test_exchange_info_and_ping_are_the_request_paths(self):
        self.assertEqual("/fapi/v1/exchangeInfo", self.exchange.trading_rules_request_path)
        self.assertEqual("/fapi/v1/exchangeInfo", self.exchange.trading_pairs_request_path)
        self.assertEqual("/fapi/v1/ping", self.exchange.check_network_request_path)

    async def test_symbol_map_costs_a_single_exchange_info_call(self):
        self.exchange._api_get = AsyncMock(return_value=EXCHANGE_INFO)
        await self.exchange._initialize_trading_pair_symbol_map()
        self.exchange._api_get.assert_awaited_once_with(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL)
        self.assertEqual("BTC-USDT", await self.exchange.trading_pair_associated_to_exchange_symbol("BTCUSDT"))

    def test_fee_uses_the_default_schema(self):
        # The FAPI publishes no per-instrument fees.
        maker = self.exchange._get_fee(
            base_currency="BTC", quote_currency="USDT", order_type=OrderType.LIMIT,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN,
            amount=Decimal("1"), price=Decimal("60000"))
        taker = self.exchange._get_fee(
            base_currency="BTC", quote_currency="USDT", order_type=OrderType.MARKET,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN,
            amount=Decimal("1"), price=Decimal("60000"))
        self.assertEqual(Decimal("0.000236"), maker.percent)
        self.assertEqual(Decimal("0.00059"), taker.percent)

    async def test_build_funding_info_reads_premium_index(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value={
            "symbol": "BTCUSDT", "markPrice": "63175.30000000", "indexPrice": "63120.95434782",
            "lastFundingRate": "0.00124107", "interestRate": "0.00010000",
            "nextFundingTime": 1780934400000, "time": 1780914420078})

        info = await self.exchange.build_funding_info(self.trading_pair)

        self.exchange._api_get.assert_awaited_once_with(
            path_url=CONSTANTS.PREMIUM_INDEX_PATH_URL, params={"symbol": "BTCUSDT"})
        self.assertEqual(Decimal("63175.30000000"), info.mark_price)
        self.assertEqual(Decimal("63120.95434782"), info.index_price)
        self.assertEqual(Decimal("0.00124107"), info.rate)
        self.assertEqual(1780934400, info.next_funding_utc_timestamp)

    async def test_build_funding_info_picks_the_symbol_from_a_list_response(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value=[
            {"symbol": "ETHUSDT", "markPrice": "3000", "lastFundingRate": "0.1", "nextFundingTime": 1},
            {"symbol": "BTCUSDT", "markPrice": "63175.3", "indexPrice": "0",
             "lastFundingRate": "0.0001", "nextFundingTime": 1780934400000},
        ])
        info = await self.exchange.build_funding_info(self.trading_pair)
        self.assertEqual(Decimal("63175.3"), info.mark_price)
        # A missing index price falls back to the mark price rather than zero.
        self.assertEqual(Decimal("63175.3"), info.index_price)

    async def test_last_traded_price_reads_ticker_price(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value={
            "symbol": "BTCUSDT", "price": "63175.30000000", "time": 1780914420078})
        self.assertEqual(63175.3, await self.exchange._get_last_traded_price(self.trading_pair))
        self.exchange._api_get.assert_awaited_once_with(
            path_url=CONSTANTS.TICKER_PRICE_PATH_URL, params={"symbol": "BTCUSDT"})

    async def test_get_all_pairs_prices_returns_book_tickers(self):
        tickers = [{"symbol": "BTCUSDT", "bidPrice": "67070.0", "bidQty": "9.866",
                    "askPrice": "67070.2", "askQty": "9.866", "time": 1780915240114}]
        self.exchange._api_get = AsyncMock(return_value=tickers)
        self.assertEqual(tickers, await self.exchange.get_all_pairs_prices())
        self.exchange._api_get.assert_awaited_once_with(path_url=CONSTANTS.BOOK_TICKER_PATH_URL)

    async def test_get_all_24h_volume_tickers_is_one_all_symbols_call(self):
        tickers = [{"symbol": "BTCUSDT", "volume": "45230.500", "quoteVolume": "3068420750.115"}]
        self.exchange._api_get = AsyncMock(return_value=tickers)
        self.assertEqual(tickers, await self.exchange.get_all_24h_volume_tickers(["BTC-USDT"]))
        self.exchange._api_get.assert_awaited_once_with(path_url=CONSTANTS.TICKER_24HR_PATH_URL)

    async def test_non_list_ticker_responses_become_empty(self):
        self.exchange._api_get = AsyncMock(return_value={"code": -1000})
        self.assertEqual([], await self.exchange.get_all_pairs_prices())
        self.assertEqual([], await self.exchange.get_all_24h_volume_tickers())

    async def test_missing_price_raises_instead_of_returning_zero(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value={})
        with self.assertRaises(ValueError):
            await self.exchange._get_last_traded_price(self.trading_pair)
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

    async def test_zero_last_price_raises(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value={"symbol": "BTCUSDT", "price": "0"})
        with self.assertRaises(ValueError):
            await self.exchange._get_last_traded_price(self.trading_pair)

    async def test_funding_info_raises_when_prices_are_zero(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        self.exchange._api_get = AsyncMock(return_value={
            "symbol": "BTCUSDT", "markPrice": "0", "indexPrice": "0",
            "lastFundingRate": "0.0001", "nextFundingTime": 1780934400000})
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

    def test_unconfigured_hosts_fail_with_a_clear_message(self):
        with patch.dict(os.environ, {CONSTANTS.REST_URL_ENV_VAR: "", CONSTANTS.WSS_URL_ENV_VAR: " "}):
            with self.assertRaisesRegex(ValueError, "Set the COINDCX_FAPI_REST_URL environment variable"):
                web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
            with self.assertRaisesRegex(ValueError, "Set the COINDCX_FAPI_WS_URL environment variable"):
                web_utils.wss_url(CONSTANTS.PUBLIC_WS_PATH)

    def test_urls_are_built_on_the_configured_hosts(self):
        with patch.dict(os.environ, {CONSTANTS.REST_URL_ENV_VAR: "https://fapi.example/",
                                     CONSTANTS.WSS_URL_ENV_VAR: "wss://stream.example"}):
            self.assertEqual("https://fapi.example/fapi/v1/depth", web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL))
            self.assertEqual("https://fapi.example/fapi/v2/balance", web_utils.private_rest_url("/fapi/v2/balance"))
            self.assertEqual("wss://stream.example/ws", web_utils.wss_url(CONSTANTS.PUBLIC_WS_PATH))

    def test_hosts_are_read_when_a_url_is_built_not_at_import(self):
        with patch.dict(os.environ, {CONSTANTS.REST_URL_ENV_VAR: "https://first.example"}):
            self.assertEqual("https://first.example/fapi/v1/ping", web_utils.rest_url(CONSTANTS.PING_PATH_URL))
        with patch.dict(os.environ, {CONSTANTS.REST_URL_ENV_VAR: "https://second.example"}):
            self.assertEqual("https://second.example/fapi/v1/ping", web_utils.rest_url(CONSTANTS.PING_PATH_URL))

    async def test_server_time_comes_from_the_time_endpoint(self):
        assistant = AsyncMock()
        assistant.execute_request = AsyncMock(return_value={"serverTime": 1780915597863})
        with patch.dict(os.environ, {CONSTANTS.REST_URL_ENV_VAR: "https://fapi.example"}), \
                patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_rest_assistant",
                      AsyncMock(return_value=assistant)):
            self.assertEqual(1780915597863.0, await web_utils.get_current_server_time())
        self.assertEqual("https://fapi.example/fapi/v1/time", assistant.execute_request.call_args.kwargs["url"])

    # ---- setup helpers ------------------------------------------------------------

    async def _ready(self):
        """Symbol map and trading rules from the guide-shaped exchangeInfo."""
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        rules = await self.exchange._format_trading_rules(EXCHANGE_INFO)
        self.exchange._trading_rules = {rule.trading_pair: rule for rule in rules}

    def _track(self, client_order_id="haveli-1", exchange_order_id=None, order_type=OrderType.LIMIT,
               trade_type=TradeType.BUY, amount="0.01", price="60000", position_action=PositionAction.OPEN):
        self.exchange.start_tracking_order(
            order_id=client_order_id, exchange_order_id=exchange_order_id, trading_pair=self.trading_pair,
            trade_type=trade_type, price=Decimal(price), amount=Decimal(amount), order_type=order_type,
            position_action=position_action)
        return self.exchange._order_tracker.fetch_order(client_order_id)

    @staticmethod
    async def _settle():
        """The order tracker applies order updates in a scheduled task; let it run."""
        for _ in range(5):
            await asyncio.sleep(0)

    async def _apply(self, update):
        await self.exchange._process_order_trade_update(update)
        await self._settle()

    def _listen(self, *events):
        logger = EventLogger()
        self._event_loggers.append(logger)  # listeners are held weakly
        for event in events:
            self.exchange.add_listener(event, logger)
        return logger

    @staticmethod
    def _capture(response):
        """An AsyncMock that records (path_url, kwargs) and answers ``response``."""
        calls = []

        async def call(path_url, **kwargs):
            calls.append((path_url, kwargs))
            return response(path_url, kwargs) if callable(response) else response

        return AsyncMock(side_effect=call), calls

    @staticmethod
    def _update(status, client_order_id="haveli-1", exchange_order_id="ex-1", **fields):
        """An ORDER_TRADE_UPDATE ``o`` block in the guide's shape."""
        return {"s": "BTCUSDT", "c": client_order_id, "S": "BUY", "o": "LIMIT", "X": status,
                "i": exchange_order_id, "q": "0.01", "p": "60000.0", "z": "0", "V": "EXPIRE_MAKER",
                "T": 1714003200000, **fields}

    # ---- static -------------------------------------------------------------------

    def test_order_types_include_post_only(self):
        self.assertEqual([OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET],
                         self.exchange.supported_order_types())

    def test_oneway_only(self):
        self.assertEqual([PositionMode.ONEWAY], self.exchange.supported_position_modes())

    def test_cancels_are_asynchronous_and_edits_native(self):
        self.assertFalse(self.exchange.is_cancel_request_in_exchange_synchronous)
        self.assertTrue(self.exchange.is_edit_order_supported_by_exchange)

    async def test_collateral_is_the_contracts_margin_asset(self):
        await self._ready()
        self.assertEqual("USDT", self.exchange.get_buy_collateral_token(self.trading_pair))
        self.assertEqual("USDT", self.exchange.get_sell_collateral_token(self.trading_pair))

    def test_margin_currency_is_validated(self):
        self.assertEqual("USDT", self.exchange.margin_currency)
        inr = CoindcxPerpetualDerivative(coindcx_perpetual_api_key="k", coindcx_perpetual_api_secret="s",
                                         coindcx_perpetual_margin_currency="inr", trading_required=False)
        self.assertEqual("INR", inr.margin_currency)
        with self.assertRaises(ValueError):
            CoindcxPerpetualDerivative(coindcx_perpetual_api_key="k", coindcx_perpetual_api_secret="s",
                                       coindcx_perpetual_margin_currency="BTC", trading_required=False)

    def test_decimals_are_sent_at_the_instruments_precision(self):
        self.assertEqual("0.010", CoindcxPerpetualDerivative._decimal_string(Decimal("0.01"), Decimal("0.001")))
        self.assertEqual("60000.0", CoindcxPerpetualDerivative._decimal_string(Decimal("60000"), Decimal("0.1")))
        self.assertEqual("60000", CoindcxPerpetualDerivative._decimal_string(Decimal("60000"), Decimal("10")))
        self.assertEqual("0.5", CoindcxPerpetualDerivative._decimal_string(Decimal("0.500"), None))

    # ---- error classification ----------------------------------------------------------

    def test_errors_are_classified_by_their_code(self):
        def failure(code):
            return IOError(f'Error executing request. HTTP status is 400. Error: {{"code": {code}, "message": "x"}}')

        self.assertEqual(-2011, error_code(failure(-2011)))
        self.assertIsNone(error_code(IOError("no body")))
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(failure(-1021)))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(failure(-1022)))
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(failure(-2013)))
        self.assertFalse(self.exchange._is_order_not_found_during_status_update_error(failure(-2011)))
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(failure(-2011)))
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(failure(-2013)))
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(failure(-2010)))

    # ---- placing orders ------------------------------------------------------------------

    async def test_limit_order_body(self):
        await self._ready()
        self.exchange._api_post, calls = self._capture({"orderId": "ex-1", "status": "CREATE_IN_PROGRESS"})

        exchange_order_id, _ = await self.exchange._place_order(
            order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"), trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, price=Decimal("60000"), position_action=PositionAction.OPEN)

        self.assertEqual("ex-1", exchange_order_id)
        path_url, kwargs = calls[0]
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, path_url)
        self.assertEqual(CONSTANTS.CREATE_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertTrue(kwargs["is_auth_required"])
        self.assertEqual({"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "quantity": "0.010",
                          "newClientOrderId": "haveli-1", "price": "60000.0", "timeInForce": "GTC"},
                         kwargs["data"])

    async def test_limit_maker_is_sent_post_only(self):
        await self._ready()
        self.exchange._api_post, calls = self._capture({"orderId": "ex-1", "status": "CREATE_IN_PROGRESS"})
        await self.exchange._place_order(
            order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"), trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT_MAKER, price=Decimal("61000"))
        body = calls[0][1]["data"]
        self.assertEqual(("SELL", "LIMIT", "GTX"), (body["side"], body["type"], body["timeInForce"]))

    async def test_market_order_has_no_price_or_time_in_force(self):
        await self._ready()
        self.exchange._api_post, calls = self._capture({"orderId": "ex-1", "status": "CREATE_IN_PROGRESS"})
        await self.exchange._place_order(
            order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"), trade_type=TradeType.SELL,
            order_type=OrderType.MARKET, price=Decimal("NaN"), position_action=PositionAction.CLOSE)
        body = calls[0][1]["data"]
        self.assertEqual("MARKET", body["type"])
        self.assertNotIn("price", body)
        self.assertNotIn("timeInForce", body)
        # The guide documents no reduceOnly / positionSide, so a close is a plain opposite order.
        self.assertNotIn("reduceOnly", body)

    async def test_a_create_without_an_order_id_fails(self):
        await self._ready()
        self.exchange._api_post = AsyncMock(return_value={"code": -2010, "message": "rejected"})
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"))

    async def test_a_placed_order_stays_pending_until_the_venue_confirms_it(self):
        await self._ready()
        created = self._listen(MarketEvent.BuyOrderCreated)
        self.exchange._api_post = AsyncMock(return_value={"orderId": "ex-1", "status": "CREATE_IN_PROGRESS"})

        await self.exchange._create_order(trade_type=TradeType.BUY, order_id="haveli-1",
                                          trading_pair=self.trading_pair, amount=Decimal("0.01"),
                                          order_type=OrderType.LIMIT, price=Decimal("60000"),
                                          position_action=PositionAction.OPEN)
        await self._settle()

        order = self.exchange._order_tracker.fetch_order("haveli-1")
        self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
        self.assertEqual("ex-1", order.exchange_order_id)
        self.assertEqual([], created.event_log, "not created until the venue says NEW")

        await self._apply(self._update("NEW"))
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(1, len(created.event_log))

    async def test_a_rejected_create_fails_the_order(self):
        await self._ready()
        failures = self._listen(MarketEvent.OrderFailure)
        self.exchange._api_post = AsyncMock(side_effect=IOError('HTTP status is 400. Error: {"code": -2010}'))
        await self.exchange._create_order(trade_type=TradeType.BUY, order_id="haveli-1",
                                          trading_pair=self.trading_pair, amount=Decimal("0.01"),
                                          order_type=OrderType.LIMIT, price=Decimal("60000"),
                                          position_action=PositionAction.OPEN)
        await self._settle()
        self.assertEqual(1, len(failures.event_log))

    # ---- cancelling -------------------------------------------------------------------------

    async def test_cancel_is_by_client_id_and_only_starts_the_cancel(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        self.exchange._api_delete, calls = self._capture({"orderId": "ex-1", "status": "CANCEL_IN_PROGRESS"})

        self.assertTrue(await self.exchange._execute_order_cancel_and_process_update(order))
        await self._settle()

        path_url, kwargs = calls[0]
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, path_url)
        self.assertEqual(CONSTANTS.CANCEL_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertEqual({"symbol": "BTCUSDT", "origClientOrderId": "haveli-1"}, kwargs["data"])
        self.assertEqual(OrderState.PENDING_CANCEL, order.current_state, "a cancel is not trusted until confirmed")

    async def test_an_unexpected_cancel_response_raises(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        self.exchange._api_delete = AsyncMock(return_value={"orderId": "ex-1", "status": "FILLED"})
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("haveli-1", order)

    async def test_the_cancel_completes_when_the_stream_says_so(self):
        await self._ready()
        cancelled = self._listen(MarketEvent.OrderCancelled)
        order = self._track(exchange_order_id="ex-1")
        await self._apply(self._update("NEW"))
        await self._apply(self._update("CANCEL_IN_PROGRESS"))
        self.assertEqual(OrderState.PENDING_CANCEL, order.current_state)
        await self._apply(self._update("CANCELED"))
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(1, len(cancelled.event_log))

    # ---- editing ------------------------------------------------------------------------------

    async def test_native_edit_sends_quantity_and_price_by_client_id(self):
        await self._ready()
        self.exchange._api_put, calls = self._capture({"orderId": "ex-1", "status": "EDIT_IN_PROGRESS"})
        result = await self.exchange._place_edit(client_order_id="haveli-1", exchange_order_id="ex-1",
                                                 trading_pair=self.trading_pair, new_price=Decimal("60100"),
                                                 new_amount=Decimal("0.02"))
        self.assertEqual(("haveli-1", "ex-1"), result[:2])
        path_url, kwargs = calls[0]
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, path_url)
        self.assertEqual(CONSTANTS.MODIFY_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertEqual({"symbol": "BTCUSDT", "origClientOrderId": "haveli-1", "quantity": "0.020",
                          "price": "60100.0"}, kwargs["data"])

    async def test_edit_through_the_framework_uses_the_native_path(self):
        await self._ready()
        edited = self._listen(MarketEvent.OrderEdited)
        order = self._track(exchange_order_id="ex-1")
        await self._apply(self._update("NEW"))
        self.exchange._api_put = AsyncMock(return_value={"orderId": "ex-1", "status": "EDIT_IN_PROGRESS"})

        self.assertEqual("haveli-1", await self.exchange._execute_edit("haveli-1", self.trading_pair,
                                                                       Decimal("60100"), Decimal("0.02")))
        await self._settle()
        self.assertEqual(Decimal("60100"), order.price)
        self.assertEqual(1, len(edited.event_log))

    async def test_a_failed_edit_is_reported(self):
        await self._ready()
        self.exchange._api_put = AsyncMock(return_value={"code": -1003, "msg": "Too many requests"})
        with self.assertRaises(IOError):
            await self.exchange._place_edit(client_order_id="haveli-1", exchange_order_id="ex-1",
                                            trading_pair=self.trading_pair, new_price=Decimal("60100"),
                                            new_amount=Decimal("0.02"))

    # ---- order status -------------------------------------------------------------------------

    async def test_order_status_is_queried_by_client_id(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        self.exchange._api_get, calls = self._capture({
            "orderId": "ex-1", "clientOrderId": "haveli-1", "symbol": "BTCUSDT", "status": "PARTIALLY_FILLED",
            "updateTime": 1566818724722})

        update = await self.exchange._request_order_status(order)

        path_url, kwargs = calls[0]
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, path_url)
        self.assertEqual(CONSTANTS.QUERY_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertEqual({"symbol": "BTCUSDT", "origClientOrderId": "haveli-1"}, kwargs["data"])
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)
        self.assertEqual("ex-1", update.exchange_order_id)
        self.assertEqual(1566818724.722, update.update_timestamp)

    async def test_an_order_whose_create_reply_was_lost_can_still_be_found(self):
        # The client id is ours, so the query needs no exchange id.
        await self._ready()
        order = self._track(exchange_order_id=None)
        self.exchange._api_get = AsyncMock(return_value={"orderId": "ex-9", "status": "NEW"})
        update = await self.exchange._request_order_status(order)
        self.assertEqual("ex-9", update.exchange_order_id)
        self.assertEqual(OrderState.OPEN, update.new_state)

    def test_every_documented_status_maps_to_a_state(self):
        order = self._track(exchange_order_id="ex-1")
        expected = {
            "CREATE_IN_PROGRESS": OrderState.PENDING_CREATE, "PENDING_TRIGGER": OrderState.OPEN,
            "TRIGGERED": OrderState.OPEN, "NEW": OrderState.OPEN, "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
            "FILLED": OrderState.FILLED, "EDIT_IN_PROGRESS": OrderState.PENDING_EDIT,
            "CANCEL_IN_PROGRESS": OrderState.PENDING_CANCEL, "CANCELED": OrderState.CANCELED,
            "EXPIRED": OrderState.CANCELED, "REJECTED": OrderState.FAILED,
        }
        for status, state in expected.items():
            with self.subTest(status=status):
                self.assertEqual(state, self.exchange._order_update_from_order({"status": status}, order).new_state)
        # An unknown status leaves the state alone rather than guessing.
        self.assertEqual(order.current_state,
                         self.exchange._order_update_from_order({"status": "SOMETHING_NEW"}, order).new_state)

    # ---- fills --------------------------------------------------------------------------------

    async def test_fills_come_from_user_trades_for_the_order(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        self.exchange._api_get, calls = self._capture([
            {"id": "698759", "orderId": "ex-1", "symbol": "BTCUSDT", "side": "BUY", "price": "60000.0",
             "qty": "0.004", "quoteQty": "240.0", "commission": "0.05", "commissionAsset": "USDT",
             "time": 1714003200000, "maker": True, "buyer": True},
            {"id": "1", "orderId": "someone-else", "price": "1", "qty": "1"},
        ])

        fills = await self.exchange._all_trade_updates_for_order(order)

        path_url, kwargs = calls[0]
        self.assertEqual(CONSTANTS.USER_TRADES_PATH_URL, path_url)
        self.assertEqual({"symbol": "BTCUSDT", "orderId": "ex-1", "page": 1, "limit": CONSTANTS.PAGE_LIMIT},
                         kwargs["data"])
        self.assertEqual(1, len(fills))
        fill = fills[0]
        self.assertEqual("698759", fill.trade_id)
        self.assertEqual(Decimal("0.004"), fill.fill_base_amount)
        self.assertEqual(Decimal("240.0"), fill.fill_quote_amount)
        self.assertEqual(Decimal("0.05"), fill.fee.flat_fees[0].amount)
        self.assertEqual("USDT", fill.fee.flat_fees[0].token)
        self.assertEqual(1714003200.0, fill.fill_timestamp)

    async def test_no_fills_are_fetched_without_an_exchange_id(self):
        self.exchange._api_get = AsyncMock(side_effect=AssertionError("must not be called"))
        self.assertEqual([], await self.exchange._all_trade_updates_for_order(self._track(exchange_order_id=None)))

    async def test_private_lists_are_read_page_by_page(self):
        full = [{"id": str(i)} for i in range(CONSTANTS.PAGE_LIMIT)]
        self.exchange._api_get, calls = self._capture(
            lambda path_url, kwargs: full if kwargs["data"]["page"] == 1 else [{"id": "last"}])
        records = await self.exchange._signed_pages(CONSTANTS.INCOME_PATH_URL, {"symbol": "BTCUSDT"})
        self.assertEqual(CONSTANTS.PAGE_LIMIT + 1, len(records))
        self.assertEqual([1, 2], [kwargs["data"]["page"] for _, kwargs in calls])

    async def test_paging_is_capped(self):
        full = [{"id": "x"}] * CONSTANTS.PAGE_LIMIT
        self.exchange._api_get, calls = self._capture(full)
        await self.exchange._signed_pages(CONSTANTS.INCOME_PATH_URL, {})
        self.assertEqual(CONSTANTS.MAX_PAGES, len(calls))

    # ---- private stream: orders --------------------------------------------------------------

    async def test_stream_fills_are_applied_before_the_terminal_status(self):
        await self._ready()
        fills = self._listen(MarketEvent.OrderFilled)
        completed = self._listen(MarketEvent.BuyOrderCompleted)
        order = self._track(exchange_order_id="ex-1")
        await self._apply(self._update("NEW"))

        await self._apply(self._update(
            "PARTIALLY_FILLED", x="TRADE", l="0.004", L="60000.0", t=11, n="0.02", N="USDT", z="0.004"))
        self.assertEqual(Decimal("0.004"), order.executed_amount_base)
        await self._apply(self._update(
            "FILLED", x="TRADE", l="0.006", L="60010.0", t=12, n="0.03", N="USDT", z="0.01"))

        self.assertEqual(2, len(fills.event_log))
        self.assertEqual(1, len(completed.event_log))
        self.assertEqual(Decimal("0.01"), completed.event_log[0].base_asset_amount)

    async def test_a_fill_without_detail_is_read_over_rest_before_settling(self):
        await self._ready()
        completed = self._listen(MarketEvent.BuyOrderCompleted)
        order = self._track(exchange_order_id="ex-1")
        await self._apply(self._update("NEW"))
        self.exchange._api_get = AsyncMock(return_value=[
            {"id": "698759", "orderId": "ex-1", "price": "60000.0", "qty": "0.01", "commission": "0",
             "commissionAsset": "USDT", "time": 1714003200000}])

        # The guide's documented fields only: status and cumulative quantity.
        await self._apply(self._update("FILLED", x="TRADE", z="0.01"))

        self.assertEqual(Decimal("0.01"), order.executed_amount_base)
        self.assertEqual(1, len(completed.event_log))
        self.assertEqual(Decimal("0.01"), completed.event_log[0].base_asset_amount)

    async def test_repeated_stream_fills_count_once(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        frame = self._update("PARTIALLY_FILLED", x="TRADE", l="0.004", L="60000.0", t=11, z="0.004")
        await self._apply(frame)
        await self._apply(frame)
        self.assertEqual(Decimal("0.004"), order.executed_amount_base)

    async def test_the_stream_can_beat_the_create_response(self):
        await self._ready()
        order = self._track(exchange_order_id=None)
        await self._apply(self._update("NEW", exchange_order_id="ex-7"))
        self.assertEqual("ex-7", order.exchange_order_id)
        self.assertEqual(OrderState.OPEN, order.current_state)

    async def test_updates_for_orders_we_do_not_track_are_ignored(self):
        await self._ready()
        self.exchange._api_get = AsyncMock(side_effect=AssertionError("must not fetch fills"))
        await self._apply(self._update("FILLED", client_order_id="manual-1", z="1"))

    async def test_rejected_and_expired_orders(self):
        await self._ready()
        failures = self._listen(MarketEvent.OrderFailure)
        cancelled = self._listen(MarketEvent.OrderCancelled)
        self._track(client_order_id="haveli-1", exchange_order_id="ex-1")
        self._track(client_order_id="haveli-2", exchange_order_id="ex-2")
        # A post-only order that would have crossed is rejected after the create was accepted.
        await self._apply(self._update("REJECTED", "haveli-1", "ex-1"))
        await self._apply(self._update("NEW", "haveli-2", "ex-2"))
        # Self-trade prevention expires the resting maker order.
        await self._apply(self._update("EXPIRED", "haveli-2", "ex-2"))
        self.assertEqual(1, len(failures.event_log))
        self.assertEqual(1, len(cancelled.event_log))

    async def test_the_listener_dispatches_each_event_type(self):
        await self._ready()
        order = self._track(exchange_order_id="ex-1")
        events = [
            {"e": "ORDER_TRADE_UPDATE", "E": 1714003200000, "o": self._update("NEW")},
            {"e": "ACCOUNT_UPDATE", "E": 1714003200000, "a": {"B": [{"a": "USDT", "wb": "1000.50", "lb": "50.00"}]}},
            {"e": "POSITION_UPDATE", "E": 1714003200000,
             "P": [{"s": "BTCUSDT", "pa": "0.001", "ep": "68000.0", "up": "0.45"}]},
            {"e": "somethingElse"},
        ]

        async def queue():
            for event in events:
                yield event

        self.exchange._iter_user_event_queue = queue
        await self.exchange._user_stream_event_listener()
        await self._settle()

        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(Decimal("1000.50"), self.exchange.get_balance("USDT"))
        self.assertEqual(Decimal("950.50"), self.exchange.get_available_balance("USDT"))
        self.assertEqual(Decimal("0.001"), self.exchange.account_positions[self.trading_pair].amount)

    async def test_a_bad_event_does_not_stop_the_listener(self):
        self.exchange._sleep = AsyncMock()

        async def queue():
            yield {"e": "ACCOUNT_UPDATE", "a": {"B": [{"a": "USDT", "wb": "not-a-number"}]}}
            yield {"e": "ACCOUNT_UPDATE", "a": {"B": [{"a": "USDT", "wb": "7"}]}}

        self.exchange._iter_user_event_queue = queue
        await self.exchange._user_stream_event_listener()
        self.assertEqual(Decimal("7"), self.exchange.get_balance("USDT"))

    # ---- balances --------------------------------------------------------------------------------

    async def test_balances_come_from_the_balance_endpoint(self):
        self.exchange._account_balances["OLD"] = Decimal("1")
        self.exchange._account_available_balances["OLD"] = Decimal("1")
        self.exchange._api_get, calls = self._capture([
            {"accountAlias": "FAPI", "asset": "USDT", "balance": "10000.0", "crossWalletBalance": "10000.0",
             "availableBalance": "9500.0", "maxWithdrawAmount": "9500.0"}])

        await self.exchange._update_balances()

        self.assertEqual(CONSTANTS.BALANCE_PATH_URL, calls[0][0])
        self.assertTrue(calls[0][1]["is_auth_required"])
        self.assertEqual(Decimal("10000.0"), self.exchange.get_balance("USDT"))
        self.assertEqual(Decimal("9500.0"), self.exchange.get_available_balance("USDT"))
        self.assertNotIn("OLD", self.exchange._account_balances, "assets no longer reported are dropped")

    # ---- positions -------------------------------------------------------------------------------

    async def test_positions_come_from_position_risk(self):
        await self._ready()
        self.exchange._api_get, calls = self._capture([
            {"symbol": "BTCUSDT", "positionAmt": "0.001", "entryPrice": "64000.0", "markPrice": "65000.0",
             "unRealizedProfit": "1.0", "leverage": "10", "marginType": "isolated"},
            {"symbol": "ETHUSDT", "positionAmt": "-0.5", "entryPrice": "3000.0", "unRealizedProfit": "-2.0",
             "leverage": "5"},
        ])

        await self.exchange._update_positions()

        self.assertEqual(CONSTANTS.POSITION_RISK_PATH_URL, calls[0][0])
        btc = self.exchange.account_positions["BTC-USDT"]
        self.assertEqual((PositionSide.LONG, Decimal("0.001"), Decimal("64000.0"), Decimal("1.0"), Decimal("10")),
                         (btc.position_side, btc.amount, btc.entry_price, btc.unrealized_pnl, btc.leverage))
        eth = self.exchange.account_positions["ETH-USDT"]
        self.assertEqual((PositionSide.SHORT, Decimal("-0.5")), (eth.position_side, eth.amount))

    async def test_flat_and_vanished_positions_are_removed(self):
        await self._ready()
        self.exchange._api_get = AsyncMock(return_value=[
            {"symbol": "BTCUSDT", "positionAmt": "0.001", "entryPrice": "64000.0", "unRealizedProfit": "0"},
            {"symbol": "ETHUSDT", "positionAmt": "0.2", "entryPrice": "3000.0", "unRealizedProfit": "0"}])
        await self.exchange._update_positions()

        # BTC goes flat; ETH disappears from the report altogether (closed elsewhere).
        self.exchange._api_get = AsyncMock(return_value=[
            {"symbol": "BTCUSDT", "positionAmt": "0", "entryPrice": "0", "unRealizedProfit": "0"}])
        await self.exchange._update_positions()
        self.assertEqual({}, self.exchange.account_positions)

    async def test_a_position_update_frame_does_not_drop_other_positions(self):
        await self._ready()
        await self.exchange._process_position_rows([{"s": "ETHUSDT", "pa": "0.2", "ep": "3000", "up": "0"}])
        await self.exchange._process_position_rows([{"s": "BTCUSDT", "pa": "-0.001", "ep": "68000", "up": "0.4"}])
        self.assertEqual({"ETH-USDT", "BTC-USDT"}, set(self.exchange.account_positions))
        await self.exchange._process_position_rows([{"s": "XRPUSDT", "pa": "1"}])  # unknown symbol: ignored
        await self.exchange._process_position_rows([{"s": "ETHUSDT", "pa": "0"}])
        self.assertEqual({"BTC-USDT"}, set(self.exchange.account_positions))

    async def test_leverage(self):
        await self._ready()
        self.exchange._api_post, calls = self._capture(
            {"symbol": "BTCUSDT", "leverage": 20, "maxNotionalValue": "100000"})
        self.assertEqual((True, ""), await self.exchange._set_trading_pair_leverage(self.trading_pair, 20))
        self.assertEqual((CONSTANTS.LEVERAGE_PATH_URL, {"symbol": "BTCUSDT", "leverage": 20}),
                         (calls[0][0], calls[0][1]["data"]))

        self.exchange._api_post = AsyncMock(return_value={"symbol": "BTCUSDT", "leverage": 10})
        ok, message = await self.exchange._set_trading_pair_leverage(self.trading_pair, 20)
        self.assertFalse(ok)
        self.assertIn("Unexpected leverage response", message)

        self.exchange._api_post = AsyncMock(side_effect=IOError("HTTP status is 400"))
        self.assertEqual((False, "HTTP status is 400"),
                         await self.exchange._set_trading_pair_leverage(self.trading_pair, 20))

    async def test_only_oneway_position_mode(self):
        self.assertEqual((True, ""), await self.exchange._trading_pair_position_mode_set(
            PositionMode.ONEWAY, self.trading_pair))
        ok, _ = await self.exchange._trading_pair_position_mode_set(PositionMode.HEDGE, self.trading_pair)
        self.assertFalse(ok)

    async def test_exit_position_and_close_all_use_the_margin_currency(self):
        await self._ready()
        self.exchange._api_post, calls = self._capture({"symbol": "BTCUSDT", "groupId": "a1b2c3"})
        self.assertEqual("a1b2c3", await self.exchange.exit_position(self.trading_pair))
        self.assertEqual((CONSTANTS.EXIT_POSITION_PATH_URL, {"symbol": "BTCUSDT", "marginCurrency": "USDT"}),
                         (calls[0][0], calls[0][1]["data"]))

        self.exchange._api_post, calls = self._capture({"groupId": "a1b2c3USDT"})
        self.assertEqual("a1b2c3USDT", await self.exchange.close_all_positions())
        self.assertEqual((CONSTANTS.CLOSE_ALL_POSITIONS_PATH_URL, {"marginCurrency": "USDT"}),
                         (calls[0][0], calls[0][1]["data"]))

    # ---- funding payments --------------------------------------------------------------------------

    async def test_the_latest_funding_payment_is_reported(self):
        await self._ready()
        self.exchange._api_get, calls = self._capture([
            {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "-0.42", "asset": "USDT",
             "time": 1714003200000, "tranId": 1},
            {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "0.10", "asset": "USDT",
             "time": 1714032000000, "tranId": 2},
        ])
        self.exchange.build_funding_info = AsyncMock(return_value=FundingInfo(
            trading_pair=self.trading_pair, index_price=Decimal("1"), mark_price=Decimal("1"),
            next_funding_utc_timestamp=0, rate=Decimal("0.0001")))

        self.assertEqual((1714032000.0, Decimal("0.0001"), Decimal("0.10")),
                         await self.exchange._fetch_last_fee_payment(self.trading_pair))
        self.assertEqual({"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "page": 1,
                          "limit": CONSTANTS.PAGE_LIMIT}, calls[0][1]["data"])

    async def test_no_funding_history_returns_the_sentinel(self):
        await self._ready()
        self.exchange._api_get = AsyncMock(return_value=[])
        self.assertEqual((0, Decimal("-1"), Decimal("-1")),
                         await self.exchange._fetch_last_fee_payment(self.trading_pair))

    async def test_a_funding_payment_survives_a_missing_rate(self):
        await self._ready()
        self.exchange._api_get = AsyncMock(return_value=[{"symbol": "BTCUSDT", "income": "-0.5", "time": 1}])
        self.exchange.build_funding_info = AsyncMock(side_effect=ValueError("no mark price"))
        timestamp, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual((Decimal("-1"), Decimal("-0.5")), (rate, payment))

    # ---- open orders ---------------------------------------------------------------------------------

    async def test_open_orders_come_from_the_venue(self):
        await self._ready()
        self.exchange._api_get = AsyncMock(return_value=[
            {"orderId": "ex-1", "clientOrderId": "manual-1", "symbol": "BTCUSDT", "status": "NEW", "type": "LIMIT",
             "side": "BUY", "price": "60000", "origQty": "10", "executedQty": "2", "time": 1566818724722},
            {"orderId": "ex-2", "symbol": "XRPUSDT", "status": "NEW"},
        ])
        orders = await self.exchange.get_open_orders()
        self.assertEqual(1, len(orders), "symbols we don't map are skipped")
        order = orders[0]
        self.assertEqual(("manual-1", "BTC-USDT", Decimal("60000"), Decimal("10"), Decimal("2"), True, "ex-1"),
                         (order.client_order_id, order.trading_pair, order.price, order.amount,
                          order.executed_amount, order.is_buy, order.exchange_order_id))
