import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.wazirx_perpetual import wazirx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import OrderState

BTCINR = {
    "symbol": "BTCINR", "contractType": "PERPETUAL", "contractName": "Bitcoin", "baseAsset": "BTC",
    "quoteAsset": "INR", "marginAsset": "INR", "orderTypes": ["MARKET", "LIMIT"], "maxLeverage": "150",
    "pricePrecision": "0", "quantityPrecision": "3",
    "filters": [
        {"maxQty": "50", "minQty": "0.001", "filterType": "limit_qty_size"},
        {"maxQty": "25", "minQty": "0.001", "filterType": "market_qty_size"},
        {"filterType": "max_num_orders", "limit": "200"},
        {"filterType": "min_notional", "notional": "10999.75"},
    ],
}
# USDT-quoted, but margined from the INR wallet like every live contract.
BTCUSDT = {
    **BTCINR, "symbol": "BTCUSDT", "quoteAsset": "USDT", "pricePrecision": "1",
    "filters": [
        {"maxQty": "50", "minQty": "0.001", "filterType": "limit_qty_size"},
        {"maxQty": "25", "minQty": "0.001", "filterType": "market_qty_size"},
        {"filterType": "max_num_orders", "limit": "200"},
        {"filterType": "min_notional", "notional": "115"},
    ],
}
DOGEINR = {
    **BTCINR, "symbol": "DOGEINR", "baseAsset": "DOGE", "maxLeverage": "75", "pricePrecision": "3",
    "quantityPrecision": "0",
    "filters": [
        {"maxQty": "1000000", "minQty": "1", "filterType": "limit_qty_size"},
        {"filterType": "min_notional", "notional": "546.25"},
    ],
}
EXCHANGE_INFO = {
    "timeZone": "UTC",
    "serverTime": 1791362744805,
    "assets": [{"asset": "INR", "precision": 2}, {"asset": "USDT", "precision": 4}],
    "conversionRates": {"INR_MARGIN_INR": 1, "INR_MARGIN_USDT": 102, "USDT_MARGIN_USDT": 1},
    "symbols": [BTCINR, BTCUSDT, DOGEINR, {**BTCINR, "symbol": "OLDINR", "baseAsset": "OLD",
                                           "contractType": "QUARTERLY"}],
}


def _order_payload(**overrides):
    payload = {
        "id": 1234567, "symbol": "BTCINR", "status": "INIT", "requestId": "haveliBBIR1",
        "price": "5700000.00", "avgPrice": "0.00000000", "origQty": "0.010", "executedQty": "0.000",
        "type": "LIMIT", "side": "BUY", "leverage": 10, "linkId": "", "linkType": "ORDER", "subType": "PRIMARY",
        "createdTime": 1781891824866, "updatedTime": 1781891824866,
    }
    payload.update(overrides)
    return payload


class WazirxPerpetualDerivativeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.trading_pair = "BTC-INR"
        self.exchange = self._make_exchange([self.trading_pair])

    def _make_exchange(self, trading_pairs):
        exchange = WazirxPerpetualDerivative(
            wazirx_perpetual_api_key="key",
            wazirx_perpetual_api_secret="secret",
            trading_pairs=trading_pairs,
            trading_required=False,
        )
        exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)
        return exchange

    async def _load_rules(self, exchange=None):
        exchange = exchange or self.exchange
        rules = await exchange._format_trading_rules(EXCHANGE_INFO)
        exchange._trading_rules = {rule.trading_pair: rule for rule in rules}
        return rules

    def _track(self, order_id="haveliBBIR1", exchange_order_id=None, trade_type=TradeType.BUY,
               order_type=OrderType.LIMIT, amount=Decimal("0.010"), position_action=PositionAction.OPEN,
               trading_pair=None):
        self.exchange.start_tracking_order(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair or self.trading_pair,
            trade_type=trade_type,
            price=Decimal("5700000"),
            amount=amount,
            order_type=order_type,
            position_action=position_action,
        )
        return self.exchange._order_tracker.fetch_order(order_id)

    def _open_position(self, side=PositionSide.LONG, amount=Decimal("0.015"), leverage=Decimal("10"),
                       position_id=12345):
        self.exchange._perpetual_trading.set_position(self.trading_pair, Position(
            trading_pair=self.trading_pair, position_side=side, unrealized_pnl=Decimal("0"),
            entry_price=Decimal("5600000"), amount=amount if side is PositionSide.LONG else -amount,
            leverage=leverage))
        self.exchange._position_ids[self.trading_pair] = position_id

    @staticmethod
    async def _settle():
        for _ in range(5):
            await asyncio.sleep(0)

    # ---- CLI wiring -----------------------------------------------------------

    def test_connector_class_resolves_the_way_the_cli_loads_it(self):
        import importlib

        from hummingbot.client.settings import AllConnectorSettings

        setting = AllConnectorSettings.get_connector_settings()["wazirx_perpetual"]
        self.assertEqual("WazirxPerpetualDerivative", setting.class_name())
        self.assertEqual("Derivative", setting.type.name)
        module = importlib.import_module(setting.module_path())
        self.assertIs(WazirxPerpetualDerivative, getattr(module, setting.class_name()))

    def test_cli_can_build_a_default_connector_instance(self):
        from hummingbot.client.settings import AllConnectorSettings

        setting = AllConnectorSettings.get_connector_settings()["wazirx_perpetual"]
        connector = setting.non_trading_connector_instance_with_default_configuration(
            trading_pairs=[self.trading_pair])
        self.assertIsInstance(connector, WazirxPerpetualDerivative)
        self.assertEqual("wazirx_perpetual", connector.name)

    def test_registered_with_both_oracles(self):
        from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
        from hummingbot.core.volume_oracle.volume_oracle import VOLUME_ORACLE_SOURCES

        self.assertIn("wazirx_perpetual", RATE_ORACLE_SOURCES)
        self.assertIn("wazirx_perpetual", VOLUME_ORACLE_SOURCES)

    # ---- static ---------------------------------------------------------------

    def test_supported_order_types_and_modes(self):
        self.assertEqual([OrderType.LIMIT, OrderType.MARKET], self.exchange.supported_order_types())
        self.assertNotIn(OrderType.LIMIT_MAKER, self.exchange.supported_order_types())
        self.assertEqual([PositionMode.ONEWAY], self.exchange.supported_position_modes())

    def test_cancel_ack_is_not_treated_as_terminal(self):
        self.assertFalse(self.exchange.is_cancel_request_in_exchange_synchronous)

    def test_client_order_ids_are_valid_request_ids(self):
        from hummingbot.connector.utils import get_new_client_order_id

        for pair in ("BTC-INR", "1000PEPE-USDT", "1MBABYDOGE-INR"):
            order_id = get_new_client_order_id(True, pair, self.exchange.client_order_id_prefix,
                                               self.exchange.client_order_id_max_length)
            self.assertTrue(order_id.isalnum(), order_id)
            self.assertLessEqual(len(order_id), 64)

    # ---- exchange info ----------------------------------------------------------

    async def test_symbol_map_skips_non_perpetuals(self):
        self.assertEqual("BTCINR", await self.exchange.exchange_symbol_associated_to_pair("BTC-INR"))
        self.assertEqual("BTCUSDT", await self.exchange.exchange_symbol_associated_to_pair("BTC-USDT"))
        self.assertEqual("DOGE-INR", await self.exchange.trading_pair_associated_to_exchange_symbol("DOGEINR"))
        symbol_map = await self.exchange.trading_pair_symbol_map()
        self.assertNotIn("OLDINR", symbol_map)

    async def test_trading_rules_from_exchange_info(self):
        rules = {rule.trading_pair: rule for rule in await self._load_rules()}
        self.assertEqual({"BTC-INR", "BTC-USDT", "DOGE-INR"}, set(rules))

        btc_inr = rules["BTC-INR"]
        self.assertEqual(Decimal("0.001"), btc_inr.min_order_size)
        self.assertEqual(Decimal("50"), btc_inr.max_order_size)
        self.assertEqual(Decimal("1"), btc_inr.min_price_increment)
        self.assertEqual(Decimal("0.001"), btc_inr.min_base_amount_increment)
        self.assertEqual(Decimal("10999.75"), btc_inr.min_notional_size)
        self.assertEqual("INR", btc_inr.buy_order_collateral_token)

        self.assertEqual(Decimal("0.1"), rules["BTC-USDT"].min_price_increment)
        self.assertEqual("USDT", rules["BTC-USDT"].sell_order_collateral_token)
        self.assertEqual(Decimal("0.001"), rules["DOGE-INR"].min_price_increment)
        self.assertEqual(Decimal("1"), rules["DOGE-INR"].min_base_amount_increment)

    async def test_collateral_is_the_contract_quote(self):
        await self._load_rules()
        self.assertEqual("INR", self.exchange.get_buy_collateral_token("BTC-INR"))
        self.assertEqual("USDT", self.exchange.get_buy_collateral_token("BTC-USDT"))
        # Before rules load it still falls back to the quote.
        fresh = self._make_exchange(["ETH-USDT"])
        self.assertEqual("USDT", fresh.get_sell_collateral_token("ETH-USDT"))

    def test_conversion_rate_and_max_leverage(self):
        self.assertEqual(Decimal("102"), self.exchange.margin_conversion_rate("USDT", "INR"))
        self.assertEqual(Decimal("1"), self.exchange.margin_conversion_rate("INR", "INR"))
        self.assertIsNone(self.exchange.margin_conversion_rate("EUR", "INR"))
        self.assertEqual(150, self.exchange.max_leverage("BTC-INR"))
        self.assertEqual(75, self.exchange.max_leverage("DOGE-INR"))
        self.assertIsNone(self.exchange.max_leverage("XYZ-INR"))

    # ---- placing orders ---------------------------------------------------------

    async def test_place_limit_open_order(self):
        self.exchange.set_leverage(self.trading_pair, 5)
        await self._settle()
        self.exchange._api_post = AsyncMock(return_value=_order_payload())

        exchange_order_id, ts = await self.exchange._place_order(
            order_id="haveliBBIR1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
            position_action=PositionAction.OPEN)

        self.assertEqual("1234567", exchange_order_id)
        self.assertAlmostEqual(1781891824.866, ts, places=3)
        kwargs = self.exchange._api_post.call_args.kwargs
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, kwargs["path_url"])
        self.assertEqual(CONSTANTS.CREATE_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertTrue(kwargs["is_auth_required"])
        self.assertEqual({
            "requestId": "haveliBBIR1", "symbol": "BTCINR", "side": "BUY", "type": "LIMIT",
            "quantity": "0.010", "price": "5700000", "leverage": 5,
        }, kwargs["data"])

    async def test_place_market_order_has_no_price_and_no_exponent(self):
        self.exchange._api_post = AsyncMock(return_value=_order_payload(type="MARKET", status="DONE"))
        await self.exchange._place_order(
            order_id="haveliSBIR2", trading_pair=self.trading_pair, amount=Decimal("1E+1"),
            trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("NaN"),
            position_action=PositionAction.OPEN)
        data = self.exchange._api_post.call_args.kwargs["data"]
        self.assertNotIn("price", data)
        self.assertEqual("10", data["quantity"])
        self.assertEqual("SELL", data["side"])
        self.assertEqual("MARKET", data["type"])
        self.assertEqual(1, data["leverage"])

    async def test_open_order_uses_the_open_positions_leverage(self):
        # WazirX rejects a different leverage while a position is open (3113).
        self.exchange.set_leverage(self.trading_pair, 20)
        await self._settle()
        self._open_position(leverage=Decimal("10"))
        self.exchange._api_post = AsyncMock(return_value=_order_payload())
        await self.exchange._place_order(
            order_id="haveliBBIR1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
            position_action=PositionAction.OPEN)
        self.assertEqual(10, self.exchange._api_post.call_args.kwargs["data"]["leverage"])

    async def test_close_order_names_the_position_and_sends_no_leverage(self):
        self._open_position(side=PositionSide.LONG)
        self.exchange._api_post = AsyncMock(return_value=_order_payload(side="SELL"))
        await self.exchange._place_order(
            order_id="haveliSBIR3", trading_pair=self.trading_pair, amount=Decimal("0.015"),
            trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("NaN"),
            position_action=PositionAction.CLOSE)
        data = self.exchange._api_post.call_args.kwargs["data"]
        self.assertEqual(12345, data["positionId"])
        self.assertNotIn("leverage", data)

    async def test_close_with_the_wrong_side_is_refused(self):
        # positionId makes WazirX ignore the side; a BUY "close" of a long would
        # actually sell, while Hummingbot booked a buy.
        self._open_position(side=PositionSide.LONG)
        self.exchange._api_post = AsyncMock()
        with self.assertRaises(ValueError):
            await self.exchange._place_order(
                order_id="haveliBBIR4", trading_pair=self.trading_pair, amount=Decimal("0.015"),
                trade_type=TradeType.BUY, order_type=OrderType.MARKET, price=Decimal("NaN"),
                position_action=PositionAction.CLOSE)
        self.exchange._api_post.assert_not_called()

    async def test_close_fetches_the_position_when_unknown_and_refuses_when_flat(self):
        self.exchange._api_get = AsyncMock(return_value=[])
        self.exchange._api_post = AsyncMock()
        with self.assertRaises(ValueError):
            await self.exchange._place_order(
                order_id="haveliSBIR5", trading_pair=self.trading_pair, amount=Decimal("0.015"),
                trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("NaN"),
                position_action=PositionAction.CLOSE)
        self.assertEqual({"symbol": "BTCINR"}, self.exchange._api_get.call_args.kwargs["params"])
        self.exchange._api_post.assert_not_called()

    async def test_close_after_fetching_the_position(self):
        self.exchange._api_get = AsyncMock(return_value=[{
            "id": 777, "symbol": "BTCINR", "positionAmt": "0.015", "positionType": "SHORT",
            "entryPrice": "5600000", "markPrice": "5700000", "leverage": "10"}])
        self.exchange._api_post = AsyncMock(return_value=_order_payload(side="BUY"))
        await self.exchange._place_order(
            order_id="haveliBBIR6", trading_pair=self.trading_pair, amount=Decimal("0.015"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5650000"),
            position_action=PositionAction.CLOSE)
        self.assertEqual(777, self.exchange._api_post.call_args.kwargs["data"]["positionId"])

    async def test_unknown_create_outcome_is_resolved_by_request_id(self):
        self.exchange._api_post = AsyncMock(side_effect=IOError(
            "Error executing request POST https://api.wazirx.com/fapi/v1/order. HTTP status is 502. Error: N/A"))
        self.exchange._api_get = AsyncMock(return_value=_order_payload(id=999, status="WAIT"))

        exchange_order_id, _ = await self.exchange._place_order(
            order_id="haveliBBIR7", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
            position_action=PositionAction.OPEN)

        self.assertEqual("999", exchange_order_id)
        self.assertEqual({"requestId": "haveliBBIR7"}, self.exchange._api_get.call_args.kwargs["params"])

    async def test_unknown_create_outcome_that_never_landed_still_fails(self):
        original = IOError("Error executing request POST x. HTTP status is 503. Error: N/A")
        self.exchange._api_post = AsyncMock(side_effect=original)
        self.exchange._api_get = AsyncMock(side_effect=IOError(
            'Error executing request GET x. HTTP status is 400. Error: {"code":3020,"message":"Unknown requestId."}'))
        with self.assertRaises(IOError) as context:
            await self.exchange._place_order(
                order_id="haveliBBIR8", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
                position_action=PositionAction.OPEN)
        self.assertIs(original, context.exception)

    async def test_client_errors_are_not_looked_up(self):
        self.exchange._api_post = AsyncMock(side_effect=IOError(
            'Error executing request POST x. HTTP status is 400. Error: {"code":3204,"message":"You don\'t have '
            'enough balance in your Futures wallet to place this order."}'))
        self.exchange._api_get = AsyncMock()
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="haveliBBIR9", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
                position_action=PositionAction.OPEN)
        self.exchange._api_get.assert_not_called()

    async def test_rejected_create_fails_the_order(self):
        self.exchange._api_post = AsyncMock(return_value=_order_payload(status="REJECT"))
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="haveliBBIR10", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("5700000"),
                position_action=PositionAction.OPEN)

    # ---- cancel / status -----------------------------------------------------------

    async def test_cancel_by_exchange_id_leaves_the_order_pending(self):
        order = self._track(exchange_order_id="1234567")
        self.exchange._api_delete = AsyncMock(return_value=_order_payload(status="CANCEL"))

        cancelled = await self.exchange._execute_order_cancel_and_process_update(order)
        await self._settle()

        self.assertTrue(cancelled)
        self.assertEqual({"symbol": "BTCINR", "orderId": "1234567"},
                         self.exchange._api_delete.call_args.kwargs["data"])
        self.assertEqual(CONSTANTS.CANCEL_ORDER_LIMIT_ID, self.exchange._api_delete.call_args.kwargs["limit_id"])
        # Settled by the order stream / poll, not by the acknowledgement.
        self.assertEqual(OrderState.PENDING_CANCEL, order.current_state)

    async def test_cancel_by_request_id_before_the_exchange_id_is_known(self):
        order = self._track()
        self.exchange._api_delete = AsyncMock(return_value=_order_payload(status="CANCEL"))
        self.assertTrue(await self.exchange._place_cancel(order.client_order_id, order))
        self.assertEqual({"symbol": "BTCINR", "requestId": "haveliBBIR1"},
                         self.exchange._api_delete.call_args.kwargs["data"])

    async def test_cancel_with_unexpected_body_raises(self):
        order = self._track(exchange_order_id="1234567")
        self.exchange._api_delete = AsyncMock(return_value={"code": 2003, "message": "Failed to cancel order."})
        with self.assertRaises(IOError):
            await self.exchange._place_cancel(order.client_order_id, order)

    async def test_order_status_mapping(self):
        order = self._track(exchange_order_id="1234567")
        cases = [
            ("INIT", "0.000", OrderState.OPEN),
            ("WAIT", "0.000", OrderState.OPEN),
            ("WAIT", "0.004", OrderState.PARTIALLY_FILLED),
            ("DONE", "0.010", OrderState.FILLED),
            ("CANCEL", "0.004", OrderState.CANCELED),
            ("EXPIRE", "0.000", OrderState.CANCELED),
            ("REJECT", "0.000", OrderState.FAILED),
        ]
        for status, executed, expected in cases:
            self.exchange._api_get = AsyncMock(return_value=_order_payload(status=status, executedQty=executed))
            update = await self.exchange._request_order_status(order)
            self.assertEqual(expected, update.new_state, status)
            self.assertEqual("1234567", update.exchange_order_id)
        self.assertEqual({"orderId": "1234567"}, self.exchange._api_get.call_args.kwargs["params"])
        self.assertEqual(CONSTANTS.QUERY_ORDER_LIMIT_ID, self.exchange._api_get.call_args.kwargs["limit_id"])

    async def test_order_status_by_request_id_when_create_response_was_lost(self):
        order = self._track()
        self.exchange._api_get = AsyncMock(return_value=_order_payload(status="WAIT"))
        update = await self.exchange._request_order_status(order)
        self.assertEqual({"requestId": "haveliBBIR1"}, self.exchange._api_get.call_args.kwargs["params"])
        self.assertEqual("1234567", update.exchange_order_id)

    def test_error_classification(self):
        def err(code):
            return IOError(f'Error executing request. HTTP status is 400. Error: {{"code":{code},"message":"x"}}')

        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(err(2004)))
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(err(3020)))
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(err(2004)))
        # "cannot be cancelled in the current state" is not proof the order is gone.
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(err(2194)))
        self.assertFalse(self.exchange._is_order_not_found_during_status_update_error(err(2000)))
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(err(2098)))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(err(2005)))

    # ---- fills -----------------------------------------------------------------------

    async def test_trade_updates_from_user_trades(self):
        order = self._track(exchange_order_id="1234567")
        self.exchange._api_get = AsyncMock(return_value=[
            {"symbol": "BTCINR", "id": 88001, "orderId": 1234567, "side": "BUY", "price": "5700000.00",
             "qty": "0.004", "realizedPnl": "0", "quoteQty": "22800.00", "commission": "9.12",
             "commissionAsset": "INR", "createdTime": 1781891824866, "buyer": True, "maker": True},
            {"symbol": "BTCINR", "id": 88002, "orderId": 7654321, "side": "BUY", "price": "1", "qty": "1",
             "commission": "0", "commissionAsset": "INR", "createdTime": 1781891824866},
        ])
        updates = await self.exchange._all_trade_updates_for_order(order)

        self.assertEqual(1, len(updates))
        update = updates[0]
        self.assertEqual("88001", update.trade_id)
        self.assertEqual(Decimal("0.004"), update.fill_base_amount)
        self.assertEqual(Decimal("22800.000"), update.fill_quote_amount)
        self.assertEqual("INR", update.fee.flat_fees[0].token)
        self.assertEqual(Decimal("9.12"), update.fee.flat_fees[0].amount)
        self.assertEqual({"symbol": "BTCINR", "orderId": "1234567", "limit": 1000},
                         self.exchange._api_get.call_args.kwargs["params"])

    async def test_no_fill_lookup_without_exchange_id(self):
        order = self._track()
        self.exchange._api_get = AsyncMock()
        self.assertEqual([], await self.exchange._all_trade_updates_for_order(order))
        self.exchange._api_get.assert_not_called()

    # ---- balances ---------------------------------------------------------------------

    async def test_balances_from_the_futures_wallet(self):
        self.exchange._account_balances["OLD"] = Decimal("1")
        self.exchange._account_available_balances["OLD"] = Decimal("1")
        self.exchange._api_get = AsyncMock(return_value={"futures": [
            {"asset": "inr", "free": "5000.00", "locked": "250.50"}]})

        await self.exchange._update_balances()

        self.assertEqual({"wallets": "futures"}, self.exchange._api_get.call_args.kwargs["params"])
        self.assertEqual(Decimal("5000.00"), self.exchange.available_balances["INR"])
        self.assertEqual(Decimal("5250.50"), self.exchange.get_all_balances()["INR"])
        self.assertNotIn("OLD", self.exchange.get_all_balances())
        # No USDT-quoted pair configured, so no derived row.
        self.assertNotIn("USDT", self.exchange.get_all_balances())

    async def test_usdt_quoted_pair_gets_the_inr_wallet_at_the_margin_rate(self):
        exchange = self._make_exchange(["BTC-USDT"])
        exchange._api_get = AsyncMock(return_value={"futures": [
            {"asset": "inr", "free": "10200", "locked": "1020"}]})

        await exchange._update_balances()

        self.assertEqual(Decimal("10200"), exchange.available_balances["INR"])
        self.assertEqual(Decimal("100"), exchange.available_balances["USDT"])
        self.assertEqual(Decimal("110"), exchange.get_all_balances()["USDT"])

    async def test_missing_futures_wallet_raises(self):
        self.exchange._api_get = AsyncMock(return_value={"spot": []})
        with self.assertRaises(IOError):
            await self.exchange._update_balances()

    async def test_balance_stream_frame_is_partial(self):
        exchange = self._make_exchange(["BTC-USDT"])
        exchange._api_get = AsyncMock(return_value={"futures": [
            {"asset": "inr", "free": "10200", "locked": "0"}, {"asset": "btc", "free": "1", "locked": "0"}]})
        await exchange._update_balances()

        exchange._apply_wallet_rows([{"a": "inr", "b": "5100", "l": "5100"}], full_snapshot=False)

        self.assertEqual(Decimal("5100"), exchange.available_balances["INR"])
        self.assertEqual(Decimal("10200"), exchange.get_all_balances()["INR"])
        self.assertIn("BTC", exchange.get_all_balances())
        self.assertEqual(Decimal("50"), exchange.available_balances["USDT"])

    # ---- positions --------------------------------------------------------------------

    async def test_update_positions(self):
        exchange = self._make_exchange(["BTC-INR", "DOGE-INR"])
        exchange._perpetual_trading.set_position("ETH-INR", Position(
            trading_pair="ETH-INR", position_side=PositionSide.LONG, unrealized_pnl=Decimal("0"),
            entry_price=Decimal("1"), amount=Decimal("1"), leverage=Decimal("1")))
        exchange._api_get = AsyncMock(return_value=[
            {"id": 12345, "symbol": "BTCINR", "positionAmt": "0.015", "positionType": "LONG",
             "entryPrice": "5600000.00", "markPrice": "5750000.50", "liquidationPrice": "5100000.00",
             "leverage": "10", "marginType": "ISOLATED", "margin": "8400.00", "marginAsset": "INR"},
            {"id": 12346, "symbol": "DOGEINR", "positionAmt": "100", "positionType": "SHORT",
             "entryPrice": "9.000", "markPrice": "8.500", "leverage": "5"},
        ])

        await exchange._update_positions()

        positions = exchange.account_positions
        self.assertEqual({"BTC-INR", "DOGE-INR"}, set(positions))
        long = positions["BTC-INR"]
        self.assertEqual(PositionSide.LONG, long.position_side)
        self.assertEqual(Decimal("0.015"), long.amount)
        self.assertEqual(Decimal("0.015") * Decimal("150000.50"), long.unrealized_pnl)
        self.assertEqual(Decimal("10"), long.leverage)
        short = positions["DOGE-INR"]
        self.assertEqual(Decimal("-100"), short.amount)
        self.assertEqual(Decimal("50.000"), short.unrealized_pnl)
        self.assertEqual({"BTC-INR": 12345, "DOGE-INR": 12346}, exchange._position_ids)

    async def test_position_stream_open_then_closed(self):
        self.exchange._perpetual_trading.initialize_funding_info(FundingInfo(
            trading_pair=self.trading_pair, index_price=Decimal("5700000"), mark_price=Decimal("5700000"),
            next_funding_utc_timestamp=0, rate=Decimal("0")))
        event = {"e": "positionUpdate", "E": 1568879465651, "s": "btcinr", "ep": "5600000.00", "l": 10,
                 "lp": "5100000.00", "m": "8400.00", "ma": "inr", "i": 12345, "X": "open", "ps": "long",
                 "rp": "0", "q": "0.015", "S": "0.015", "atu": False}

        await self.exchange._process_position_event(event)
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(Decimal("0.015"), position.amount)
        self.assertEqual(Decimal("1500.000"), position.unrealized_pnl)
        self.assertEqual(12345, self.exchange._position_ids[self.trading_pair])

        await self.exchange._process_position_event({**event, "X": "closed", "q": "0"})
        self.assertNotIn(self.trading_pair, self.exchange.account_positions)
        self.assertNotIn(self.trading_pair, self.exchange._position_ids)

    async def test_leverage_is_validated_locally(self):
        self.assertEqual((True, ""), await self.exchange._set_trading_pair_leverage("BTC-INR", 20))
        success, message = await self.exchange._set_trading_pair_leverage("DOGE-INR", 100)
        self.assertFalse(success)
        self.assertIn("75x", message)
        self.assertFalse((await self.exchange._set_trading_pair_leverage("BTC-INR", 0))[0])

    async def test_only_oneway_mode(self):
        self.assertEqual((True, ""), await self.exchange._trading_pair_position_mode_set(
            PositionMode.ONEWAY, self.trading_pair))
        self.assertFalse((await self.exchange._trading_pair_position_mode_set(
            PositionMode.HEDGE, self.trading_pair))[0])

    # ---- user stream ------------------------------------------------------------------

    async def test_order_stream_matches_by_client_id_and_records_the_exchange_id(self):
        order = self._track()
        frame = {"e": "orderUpdate", "E": 1568879465651, "s": "btcinr", "S": "buy", "o": "limit", "q": "0.010",
                 "p": "5700000", "ap": "5700000", "X": "wait", "i": 8886774, "c": "haveliBBIR1", "z": "0.004",
                 "rq": "0.006", "pi": 12345, "lv": 10, "st": "primary"}

        self.exchange._process_order_event(frame)
        # Recorded synchronously so a fill keyed by exchange id can match at once.
        self.assertEqual("8886774", order.exchange_order_id)
        await self._settle()
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)
        self.assertEqual(12345, self.exchange._position_ids[self.trading_pair])

    async def test_order_stream_ignores_foreign_orders(self):
        self._track()
        self.exchange._process_order_event({"X": "wait", "i": 1, "c": "someoneElse"})
        await self._settle()
        self.assertEqual(OrderState.PENDING_CREATE,
                         self.exchange._order_tracker.fetch_order("haveliBBIR1").current_state)

    async def test_own_trade_stream_books_the_fill(self):
        order = self._track(exchange_order_id="8886774")
        self.exchange._process_trade_event({
            "e": "ownTrade", "E": 1568879465651, "i": 778, "t": 1568879465650, "s": "btcinr", "S": "buy",
            "p": "5700000", "q": "0.004", "f": "9.12", "rp": "0", "o": 8886774, "m": True, "pi": "12345",
            "T": "limit", "fc": "inr", "r": "maker", "dt": "Limit"})
        self.assertEqual(Decimal("0.004"), order.executed_amount_base)
        fill = order.order_fills["778"]
        self.assertEqual("INR", fill.fee.flat_fees[0].token)
        self.assertEqual(Decimal("9.12"), fill.fee.flat_fees[0].amount)

    async def test_fill_before_its_order_id_is_known_is_replayed(self):
        order = self._track()
        self.exchange._process_trade_event({
            "i": 779, "t": 1568879465650, "p": "5700000", "q": "0.010", "f": "22.8", "fc": "inr", "o": 8886775})
        self.assertEqual(1, len(self.exchange._pending_trade_events))
        self.assertEqual(Decimal("0"), order.executed_amount_base)

        self.exchange._process_order_event({"X": "wait", "i": 8886775, "c": "haveliBBIR1", "z": "0"})
        for _ in range(20):
            await asyncio.sleep(CONSTANTS.PENDING_TRADE_EVENT_RETRY_INTERVAL / 4)
            if not self.exchange._pending_trade_events:
                break

        self.assertEqual(Decimal("0.010"), order.executed_amount_base)
        self.assertEqual([], self.exchange._pending_trade_events)

    async def test_fill_for_an_unrelated_order_is_not_buffered(self):
        self._track(exchange_order_id="1")
        self.exchange._process_trade_event({"i": 1, "p": "1", "q": "1", "o": 999})
        self.assertEqual([], self.exchange._pending_trade_events)

    async def test_user_stream_dispatch(self):
        exchange = self._make_exchange(["BTC-USDT"])
        exchange._account_balances["INR"] = Decimal("0")
        events = [
            {"stream": "outboundAccountPosition",
             "data": {"e": "outboundAccountPosition", "E": 1, "B": [{"a": "inr", "b": "1020", "l": "0"}]}},
            {"stream": "positionUpdate",
             "data": {"s": "btcusdt", "ep": "84000", "l": 5, "i": 5, "X": "open", "ps": "short", "q": "0.002"}},
        ]

        async def _iter():
            for event in events:
                yield event

        exchange._iter_user_event_queue = _iter
        await exchange._user_stream_event_listener()

        self.assertEqual(Decimal("1020"), exchange.available_balances["INR"])
        self.assertEqual(Decimal("10"), exchange.available_balances["USDT"])
        self.assertEqual(Decimal("-0.002"), exchange.account_positions["BTC-USDT"].amount)

    # ---- funding / prices -------------------------------------------------------------

    async def test_build_funding_info(self):
        self.exchange._api_get = AsyncMock(return_value={
            "E": 1, "T": 1, "symbol": "BTCINR", "markPrice": "8031148", "indexPrice": "8035595",
            "estimatedSettlePrice": "8038642", "lastFundingRate": "-0.000042003", "nextFundingTime": 1791388800000,
            "time": 1})
        info = await self.exchange.build_funding_info(self.trading_pair)
        self.assertEqual(Decimal("8031148"), info.mark_price)
        self.assertEqual(Decimal("8035595"), info.index_price)
        self.assertEqual(Decimal("-0.000042003"), info.rate)
        self.assertEqual(1791388800, info.next_funding_utc_timestamp)
        self.assertEqual({"symbol": "BTCINR"}, self.exchange._api_get.call_args.kwargs["params"])

    async def test_funding_info_without_mark_price_raises(self):
        self.exchange._api_get = AsyncMock(return_value={"symbol": "BTCINR", "markPrice": "0"})
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

    async def test_last_funding_payment(self):
        self.exchange._api_get = AsyncMock(return_value=[])
        self.assertEqual((0, Decimal("-1"), Decimal("-1")),
                         await self.exchange._fetch_last_fee_payment(self.trading_pair))
        self.assertEqual({"symbol": "BTCINR", "incomeType": "FUNDING_FEE", "limit": 1},
                         self.exchange._api_get.call_args.kwargs["params"])

        self.exchange._perpetual_trading.initialize_funding_info(FundingInfo(
            trading_pair=self.trading_pair, index_price=Decimal("1"), mark_price=Decimal("1"),
            next_funding_utc_timestamp=0, rate=Decimal("0.0001")))
        self.exchange._api_get = AsyncMock(return_value=[
            {"id": 5012, "time": 1781891824866, "amount": "-0.52", "asset": "inr", "symbol": "BTCINR",
             "type": "FUNDING_FEE", "contractType": "PERPETUAL", "positionId": "pos-123"}])
        timestamp, rate, amount = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertAlmostEqual(1781891824.866, timestamp, places=3)
        self.assertEqual(Decimal("0.0001"), rate)
        self.assertEqual(Decimal("-0.52"), amount)

    async def test_last_traded_prices_use_one_call(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {"symbol": "BTCINR", "lastPrice": "8030248", "volume": "1176846891522"},
            {"symbol": "DOGEINR", "lastPrice": "8.605", "volume": "51244703646.894"},
            {"symbol": "UNLISTEDINR", "lastPrice": "1"},
        ])
        prices = await self.exchange.get_last_traded_prices(["BTC-INR", "DOGE-INR"])
        self.assertEqual({"BTC-INR": 8030248.0, "DOGE-INR": 8.605}, prices)
        self.assertEqual(1, self.exchange._api_get.call_count)

    async def test_last_traded_price_for_one_pair(self):
        self.exchange._api_get = AsyncMock(return_value={"symbol": "BTCINR", "lastPrice": "8030248"})
        self.assertEqual(8030248.0, await self.exchange._get_last_traded_price(self.trading_pair))
        self.exchange._api_get = AsyncMock(return_value={"symbol": "BTCINR"})
        with self.assertRaises(ValueError):
            await self.exchange._get_last_traded_price(self.trading_pair)
