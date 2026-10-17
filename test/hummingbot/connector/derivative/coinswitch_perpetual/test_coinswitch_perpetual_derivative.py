import asyncio
import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.coinswitch_perpetual import (
    coinswitch_perpetual_constants as CONSTANTS,
    coinswitch_perpetual_utils as utils,
)
from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
    CoinswitchPerpetualDerivative,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Shapes from the Futures reference pages.
BTCUSDT = {
    "symbol": "btc", "base_asset": "btc", "quote_asset": "usdt", "status": "TRADING", "type": "PERPETUAL_FUTURES",
    "min_leverage": "1", "max_leverage": "25", "leverage_step": 1, "min_base_quantity": "0.001",
    "base_quantity_step_size": "0.001", "lot_size": "0.001", "quantity_precision": 3, "price_precision": 1,
    "tick_size": 1, "max_market_base_quantity": "119", "max_base_quantity": "952", "risk_limit": "1000000",
    "maint_margin_rate": "0.56", "taker_fee_rate": "0.00065", "maker_fee_rate": "0.00024", "liq_fee_rate": "0.0075",
    "quote_asset_precision": 1,
}
DOGEUSDT = {**BTCUSDT, "symbol": "doge", "base_asset": "doge", "max_leverage": "50", "min_base_quantity": "1",
            "base_quantity_step_size": "1", "lot_size": "1", "quantity_precision": 0, "price_precision": 5,
            "maker_fee_rate": "0.0002", "taker_fee_rate": "0.0005"}
INSTRUMENT_INFO = {"data": {
    "BTCUSDT": BTCUSDT,
    "DOGEUSDT": DOGEUSDT,
    "1000PEPEUSDT": {**DOGEUSDT, "symbol": "pepe", "base_asset": "pepe", "quantity_precision": -2,
                     "base_quantity_step_size": "100"},
    "OLDUSDT": {**BTCUSDT, "symbol": "old", "base_asset": "old", "status": "DELISTED"},
}}


def _order(**overrides):
    payload = {
        "order_id": "01936474-a76f-767e-a056-ca1d41915778", "exchange": "EXCHANGE_2", "symbol": "BTCUSDT",
        "side": "BUY", "status": "RAISED", "order_type": "LIMIT", "quantity": "0.010", "exec_quantity": "0",
        "price": "95000.0", "avg_execution_price": "0", "execution_fee": "0", "realised_pnl": "0",
        "reduce_only": False, "trigger_price": "0", "created_at": 1732557186934, "updated_at": 1732557186934,
    }
    payload.update(overrides)
    return payload


def _position(**overrides):
    payload = {
        "exchange": "EXCHANGE_2", "position_id": "8b81b763-df36-4c93-9bc8-9a93d65b8546", "symbol": "BTCUSDT",
        "position_side": "LONG", "leverage": "10", "position_size": "0.010", "position_value": "951.36",
        "position_margin": "95.1", "maint_margin": "5.0", "avg_entry_price": "95000", "mark_price": "95136.6",
        "last_price": "95136.6", "unrealised_pnl": "1.366", "liquidation_price": "86000",
        "client_txn_id": "00000000-0000-0000-0000-000000000000", "margin_type": "ISOLATED", "status": "OPEN",
        "created_at": 1732617093684, "updated_at": 1732636761825,
    }
    payload.update(overrides)
    return {"data": [payload]}


TICKER = {"data": {"EXCHANGE_2": {
    "low_price_24h": "94707.00", "high_price_24h": "97276.00", "last_price": "95136.60", "symbol": "BTCUSDT",
    "exchange": "EXCHANGE_2", "timestamp": 1732821806680, "best_ask_price": "95136.70",
    "best_bid_price": "95136.60", "price_24h_pcnt": "-1.297300", "base_asset_volume_24h": "93707.6800",
    "quote_asset_volume_24h": "8970676685.2608", "index_price": 95046.53, "mark_price": 95136.7,
    "open_interest": "67529.878", "open_interest_value": "6424569744.32", "funding_rate": 0.00039681,
    "next_funding_timestamp": 1732838400000, "best_bid_size": "3.189", "best_ask_size": "3.963",
}}}


class CoinswitchPerpetualDerivativeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.trading_pair = "BTC-USDT"
        self.exchange = self._make_exchange([self.trading_pair])

    def _make_exchange(self, trading_pairs):
        exchange = CoinswitchPerpetualDerivative(
            coinswitch_perpetual_api_key="",
            coinswitch_perpetual_api_secret="",
            trading_pairs=trading_pairs,
            trading_required=False,
        )
        exchange._initialize_trading_pair_symbols_from_exchange_info(INSTRUMENT_INFO)
        return exchange

    async def _load_rules(self):
        rules = await self.exchange._format_trading_rules(INSTRUMENT_INFO)
        self.exchange._trading_rules = {rule.trading_pair: rule for rule in rules}
        return rules

    def _track(self, order_id="x-CSPBBTUT1", exchange_order_id=None, trade_type=TradeType.BUY,
               order_type=OrderType.LIMIT, amount=Decimal("0.010"), price=Decimal("95000"),
               position_action=PositionAction.OPEN):
        self.exchange.start_tracking_order(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            trade_type=trade_type,
            price=price,
            amount=amount,
            order_type=order_type,
            position_action=position_action,
        )
        return self.exchange._order_tracker.fetch_order(order_id)

    @staticmethod
    async def _settle():
        for _ in range(5):
            await asyncio.sleep(0)

    @staticmethod
    def _error(status, text="boom"):
        return IOError(f"Error executing request POST https://coinswitch.co/x. HTTP status is {status}. Error: {text}")

    # ---- CLI wiring -----------------------------------------------------------

    def test_connector_class_resolves_the_way_the_cli_loads_it(self):
        import importlib

        from hummingbot.client.settings import AllConnectorSettings

        setting = AllConnectorSettings.get_connector_settings()["coinswitch_perpetual"]
        self.assertEqual("CoinswitchPerpetualDerivative", setting.class_name())
        self.assertEqual("Derivative", setting.type.name)
        module = importlib.import_module(setting.module_path())
        self.assertIs(CoinswitchPerpetualDerivative, getattr(module, setting.class_name()))
        connector = setting.non_trading_connector_instance_with_default_configuration(trading_pairs=["BTC-USDT"])
        self.assertIsInstance(connector, CoinswitchPerpetualDerivative)

    def test_registered_with_both_oracles_and_the_scripts(self):
        from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
        from hummingbot.core.volume_oracle.volume_oracle import VOLUME_ORACLE_SOURCES

        self.assertIn("coinswitch_perpetual", RATE_ORACLE_SOURCES)
        self.assertIn("coinswitch_perpetual", VOLUME_ORACLE_SOURCES)

    def test_static_properties(self):
        self.assertEqual("coinswitch_perpetual", self.exchange.name)
        self.assertEqual([OrderType.LIMIT, OrderType.MARKET], self.exchange.supported_order_types())
        self.assertEqual([PositionMode.ONEWAY], self.exchange.supported_position_modes())
        self.assertFalse(self.exchange.is_cancel_request_in_exchange_synchronous)
        self.assertEqual(CONSTANTS.SERVER_TIME_PATH_URL, self.exchange.check_network_request_path)
        self.assertEqual(CONSTANTS.INSTRUMENT_INFO_PATH_URL, self.exchange.trading_rules_request_path)
        self.assertEqual(CONSTANTS.INSTRUMENT_INFO_PATH_URL, self.exchange.trading_pairs_request_path)
        self.assertEqual(CONSTANTS.FUNDING_FEE_POLL_INTERVAL, self.exchange.funding_fee_poll_interval)
        self.assertEqual("x-CSP", self.exchange.client_order_id_prefix)
        self.assertEqual(36, self.exchange.client_order_id_max_length)
        self.assertEqual("USDT", self.exchange.get_buy_collateral_token("ETH-USDT"))
        self.assertEqual("USDT", self.exchange.get_sell_collateral_token("ETH-USDT"))
        self.assertIs(CONSTANTS.RATE_LIMITS, self.exchange.rate_limits_rules)
        self.assertEqual(CONSTANTS.DEFAULT_DOMAIN, self.exchange.domain)
        self.assertFalse(self.exchange.authenticator.has_credentials)

    def test_proxy_url_gives_a_dedicated_session(self):
        exchange = CoinswitchPerpetualDerivative(
            coinswitch_perpetual_api_key="", coinswitch_perpetual_api_secret="",
            coinswitch_perpetual_proxy_url="http://127.0.0.1:3128", trading_pairs=[], trading_required=False)
        self.assertEqual("ProxyConnectionsFactory",
                         type(exchange._web_assistants_factory._connections_factory).__name__)

    async def test_stop_network_closes_the_proxy_session(self):
        exchange = CoinswitchPerpetualDerivative(
            coinswitch_perpetual_api_key="", coinswitch_perpetual_api_secret="",
            coinswitch_perpetual_proxy_url="http://127.0.0.1:3128", trading_pairs=[], trading_required=False)
        exchange._web_assistants_factory.close = AsyncMock(side_effect=RuntimeError("already closed"))
        await exchange.stop_network()
        exchange._web_assistants_factory.close.assert_awaited_once()

    # ---- instruments / rules ---------------------------------------------------

    async def test_symbol_map_skips_untradable_instruments(self):
        mapping = await self.exchange.trading_pair_symbol_map()
        self.assertEqual({"BTCUSDT": "BTC-USDT", "DOGEUSDT": "DOGE-USDT", "1000PEPEUSDT": "1000PEPE-USDT"},
                         dict(mapping))

    async def test_instrument_info_nested_under_the_exchange_id(self):
        exchange = self._make_exchange([])
        exchange._initialize_trading_pair_symbols_from_exchange_info({"data": {"EXCHANGE_2": INSTRUMENT_INFO["data"]}})
        self.assertEqual("BTC-USDT", await exchange.trading_pair_associated_to_exchange_symbol("BTCUSDT"))
        self.assertEqual({}, CoinswitchPerpetualDerivative._instruments_from({"data": []}))

    async def test_trading_rules(self):
        rules = {rule.trading_pair: rule for rule in await self._load_rules()}
        self.assertEqual({"BTC-USDT", "DOGE-USDT", "1000PEPE-USDT"}, set(rules))
        btc = rules["BTC-USDT"]
        self.assertEqual(Decimal("0.001"), btc.min_order_size)
        self.assertEqual(Decimal("952"), btc.max_order_size)
        self.assertEqual(Decimal("0.1"), btc.min_price_increment)
        self.assertEqual(Decimal("0.001"), btc.min_base_amount_increment)
        self.assertEqual("USDT", btc.buy_order_collateral_token)
        self.assertEqual(Decimal("0.00001"), rules["DOGE-USDT"].min_price_increment)
        self.assertEqual(Decimal("100"), rules["1000PEPE-USDT"].min_base_amount_increment)
        self.assertEqual(25, self.exchange.max_leverage("BTC-USDT"))
        self.assertEqual(1, self.exchange.min_leverage("BTC-USDT"))
        self.assertIsNone(self.exchange.max_leverage("ETH-USDT"))

    async def test_trading_rules_request_is_signed_with_the_exchange(self):
        self.exchange._api_get = AsyncMock(return_value=INSTRUMENT_INFO)
        self.assertIs(INSTRUMENT_INFO, await self.exchange._make_trading_pairs_request())
        kwargs = self.exchange._api_get.call_args.kwargs
        self.assertEqual({"exchange": "EXCHANGE_2"}, kwargs["params"])
        self.assertTrue(kwargs["is_auth_required"])

    async def test_fee_estimates_use_the_contract_rates(self):
        await self._load_rules()
        maker = self.exchange.get_fee("DOGE", "USDT", OrderType.LIMIT, TradeType.BUY, PositionAction.OPEN,
                                      Decimal("1"), Decimal("1"))
        taker = self.exchange.get_fee("DOGE", "USDT", OrderType.MARKET, TradeType.BUY, PositionAction.OPEN,
                                      Decimal("1"), Decimal("1"))
        self.assertEqual(Decimal("0.0002"), maker.percent)
        self.assertEqual(Decimal("0.0005"), taker.percent)
        unknown = self.exchange.get_fee("ETH", "USDT", OrderType.MARKET, TradeType.BUY, PositionAction.OPEN,
                                        Decimal("1"), Decimal("1"))
        self.assertEqual(self.exchange.estimate_fee_pct(False), unknown.percent)
        await self.exchange._update_trading_fees()

    # ---- placing -----------------------------------------------------------------

    async def test_place_limit_open_order(self):
        self.exchange._api_post = AsyncMock(return_value={"data": _order()})
        exchange_order_id, transact_time = await self.exchange._place_order(
            order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000.5"),
            position_action=PositionAction.OPEN)

        self.assertEqual("01936474-a76f-767e-a056-ca1d41915778", exchange_order_id)
        self.assertAlmostEqual(1732557186.934, transact_time)
        kwargs = self.exchange._api_post.call_args.kwargs
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, kwargs["path_url"])
        self.assertEqual(CONSTANTS.CREATE_ORDER_LIMIT_ID, kwargs["limit_id"])
        self.assertTrue(kwargs["is_auth_required"])
        self.assertEqual({
            "exchange": "EXCHANGE_2", "symbol": "BTCUSDT", "side": "BUY", "order_type": "LIMIT",
            "quantity": 0.01, "price": 95000.5, "client_order_id": utils.client_order_uuid("x-CSPBBTUT1"),
        }, kwargs["data"])
        json.dumps(kwargs["data"])

    async def test_place_market_close_is_reduce_only_without_price(self):
        self.exchange._api_post = AsyncMock(return_value={"data": _order(order_type="MARKET", created_at=None)})
        await self.exchange._place_order(
            order_id="x-CSPSBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
            trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("NaN"),
            position_action=PositionAction.CLOSE)
        data = self.exchange._api_post.call_args.kwargs["data"]
        self.assertEqual("SELL", data["side"])
        self.assertEqual("MARKET", data["order_type"])
        self.assertNotIn("price", data)
        self.assertTrue(data["reduce_only"])

    async def test_unknown_create_outcome_is_resolved_by_client_order_id(self):
        self.exchange._api_post = AsyncMock(side_effect=self._error(502))
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="found")}})
        exchange_order_id, _ = await self.exchange._place_order(
            order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000"))
        self.assertEqual("found", exchange_order_id)
        self.assertEqual(1, self.exchange._api_post.call_count)
        params = self.exchange._api_get.call_args.kwargs["params"]
        self.assertEqual(utils.client_order_uuid("x-CSPBBTUT1"), params["client_order_id"])

    async def test_unknown_create_outcome_that_never_landed_is_resent_with_the_same_id(self):
        self.exchange._api_post = AsyncMock(side_effect=[asyncio.TimeoutError(), {"data": _order(order_id="o2")}])
        self.exchange._api_get = AsyncMock(side_effect=self._error(400, '{"message": "Order not found"}'))
        exchange_order_id, _ = await self.exchange._place_order(
            order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000"))
        self.assertEqual("o2", exchange_order_id)
        first, second = [call.kwargs["data"] for call in self.exchange._api_post.call_args_list]
        self.assertEqual(first, second)

    async def test_client_errors_fail_without_a_lookup(self):
        self.exchange._api_post = AsyncMock(
            side_effect=self._error(400, '{"message": "Amount is more than available balance"}'))
        self.exchange._api_get = AsyncMock()
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000"))
        self.exchange._api_get.assert_not_called()

    async def test_lookup_errors_other_than_4xx_propagate(self):
        self.exchange._api_post = AsyncMock(side_effect=self._error(503))
        self.exchange._api_get = AsyncMock(side_effect=self._error(500))
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000"))

    async def test_rejected_or_malformed_creates_fail(self):
        for response in ({"data": _order(status="REJECTED")}, {"data": {}}, {"message": "?"}):
            self.exchange._api_post = AsyncMock(return_value=response)
            with self.assertRaises(IOError):
                await self.exchange._place_order(
                    order_id="x-CSPBBTUT1", trading_pair=self.trading_pair, amount=Decimal("0.010"),
                    trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("95000"))

    # ---- cancelling ----------------------------------------------------------------

    async def test_cancel_by_exchange_id_leaves_the_order_pending(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_delete = AsyncMock(return_value={"data": {**_order(order_id="o1"),
                                                                     "status": "CANCELLATION_RAISED"}})
        self.assertTrue(await self.exchange._place_cancel(order.client_order_id, order))
        kwargs = self.exchange._api_delete.call_args.kwargs
        self.assertEqual({"exchange": "EXCHANGE_2", "order_id": "o1"}, kwargs["data"])
        self.assertEqual(CONSTANTS.CANCEL_ORDER_LIMIT_ID, kwargs["limit_id"])

        await self.exchange._execute_order_cancel_and_process_update(order)
        await self._settle()
        self.assertEqual(OrderState.PENDING_CANCEL, order.current_state)

    async def test_cancel_before_the_exchange_id_is_known(self):
        order = self._track()
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="o9")}})
        self.exchange._api_delete = AsyncMock(return_value={"data": {"order_id": "o9", "status": "CANCELLED"}})
        self.assertTrue(await self.exchange._place_cancel(order.client_order_id, order))
        self.assertEqual("o9", order.exchange_order_id)
        self.assertEqual("o9", self.exchange._api_delete.call_args.kwargs["data"]["order_id"])

        lost = self._track(order_id="x-CSPBBTUT2")
        self.exchange._api_get = AsyncMock(side_effect=self._error(404, "Order not found"))
        with self.assertRaises(IOError):
            await self.exchange._place_cancel(lost.client_order_id, lost)

    async def test_cancel_with_unexpected_body_raises(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_delete = AsyncMock(return_value={"data": {}})
        with self.assertRaises(IOError):
            await self.exchange._place_cancel(order.client_order_id, order)

    # ---- status ---------------------------------------------------------------------

    def test_order_state_mapping(self):
        state = self.exchange._order_state
        current = OrderState.OPEN
        self.assertEqual(OrderState.OPEN, state(_order(status="RAISED"), None, current))
        self.assertEqual(OrderState.PARTIALLY_FILLED, state(_order(status="RAISED", exec_quantity="0.004"), True,
                                                            current))
        self.assertEqual(OrderState.FILLED, state(_order(status="EXECUTED", exec_quantity="0.010"), False, current))
        self.assertEqual(OrderState.PARTIALLY_FILLED,
                         state(_order(status="PARTIALLY_EXECUTED", exec_quantity="0.004"), True, current))
        self.assertEqual(OrderState.CANCELED,
                         state(_order(status="PARTIALLY_EXECUTED", exec_quantity="0.004"), False, current))
        self.assertEqual(OrderState.CANCELED,
                         state(_order(status="PARTIALLY_EXECUTED", exec_quantity="0.004"), None, current))
        self.assertEqual(OrderState.FILLED,
                         state(_order(status="PARTIALLY_EXECUTED", exec_quantity="0.010"), None, current))
        self.assertEqual(OrderState.PENDING_CANCEL, state(_order(status="CANCELLATION_RAISED"), None, current))
        self.assertEqual(OrderState.CANCELED, state(_order(status="CANCELLED"), False, current))
        self.assertEqual(OrderState.FAILED, state(_order(status="REJECTED"), None, current))
        self.assertEqual(OrderState.PENDING_CANCEL, state(_order(status="SOMETHING_NEW"), None,
                                                          OrderState.PENDING_CANCEL))

    async def test_order_status_by_exchange_id_and_by_client_id(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="o1", status="CANCELLED")}})
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.CANCELED, update.new_state)
        self.assertEqual({"exchange": "EXCHANGE_2", "order_id": "o1"},
                         self.exchange._api_get.call_args.kwargs["params"])
        self.assertEqual(CONSTANTS.ORDER_STATUS_LIMIT_ID, self.exchange._api_get.call_args.kwargs["limit_id"])

        unidentified = self._track(order_id="x-CSPBBTUT2")
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="o2")}})
        update = await self.exchange._request_order_status(unidentified)
        self.assertEqual("o2", update.exchange_order_id)
        self.assertEqual(utils.client_order_uuid("x-CSPBBTUT2"),
                         self.exchange._api_get.call_args.kwargs["params"]["client_order_id"])

        self.exchange._api_get = AsyncMock(return_value={"data": {}})
        with self.assertRaises(IOError):
            await self.exchange._request_order_status(order)

    async def test_fills_pass_and_state_pass_share_one_call(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(
            order_id="o1", status="EXECUTED", exec_quantity="0.010", avg_execution_price="95000",
            execution_fee="0.6175")}})
        fills = await self.exchange._all_trade_updates_for_order(order)
        self.assertEqual(1, len(fills))
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.FILLED, update.new_state)
        self.assertEqual(1, self.exchange._api_get.call_count)

    # ---- fills ------------------------------------------------------------------------

    def test_cumulative_fills_book_only_the_increment(self):
        order = self._track(exchange_order_id="o1", amount=Decimal("22"), price=Decimal("0.28"))
        first = self.exchange._trade_update_from_payload(order, _order(
            order_id="o1", quantity="22", exec_quantity="10", avg_execution_price="0.28", execution_fee="0.0018"))
        self.assertEqual(Decimal("10"), first.fill_base_amount)
        self.assertEqual(Decimal("2.80"), first.fill_quote_amount)
        self.assertEqual("o1-10", first.trade_id)
        self.assertEqual([Decimal("0.0018")], [fee.amount for fee in first.fee.flat_fees])
        self.assertEqual("USDT", first.fee.flat_fees[0].token)
        self.exchange._order_tracker.process_trade_update(first)

        second = self.exchange._trade_update_from_payload(order, _order(
            order_id="o1", quantity="22", exec_quantity="22", avg_execution_price="0.2805", execution_fee="0.0040"))
        self.assertEqual(Decimal("12"), second.fill_base_amount)
        self.assertEqual(Decimal("22") * Decimal("0.2805") - Decimal("2.80"), second.fill_quote_amount)
        self.assertEqual(second.fill_quote_amount / Decimal("12"), second.fill_price)
        self.assertEqual([Decimal("0.0022")], [fee.amount for fee in second.fee.flat_fees])
        self.exchange._order_tracker.process_trade_update(second)

        self.assertIsNone(self.exchange._trade_update_from_payload(order, _order(
            order_id="o1", quantity="22", exec_quantity="22.0", avg_execution_price="0.2805")))

    def test_cancel_response_field_names_and_missing_fee(self):
        order = self._track(exchange_order_id="o1")
        fill = self.exchange._trade_update_from_payload(order, {
            "order_id": "o1", "exec_quantity": "0.004", "avg_exec_price": "95010", "exec_fee": "0.25",
            "updated_at": 1732626020104})
        self.assertEqual(Decimal("95010"), fill.fill_price)
        self.assertEqual([Decimal("0.25")], [fee.amount for fee in fill.fee.flat_fees])
        self.assertAlmostEqual(1732626020.104, fill.fill_timestamp)

        estimated = self.exchange._trade_update_from_payload(order, {
            "order_id": "o1", "exec_quantity": "0.004", "avg_execution_price": "95010"})
        self.assertEqual([], estimated.fee.flat_fees)
        self.assertEqual("USDT", estimated.fee.percent_token)

    def test_fill_without_average_price(self):
        order = self._track(exchange_order_id="o1")
        fill = self.exchange._trade_update_from_payload(order, _order(order_id="o1", exec_quantity="0.004"))
        self.assertEqual(Decimal("95000"), fill.fill_price)

        market = self._track(order_id="x-CSPBBTUT2", exchange_order_id="o2", order_type=OrderType.MARKET,
                             price=Decimal("NaN"))
        self.assertIsNone(self.exchange._trade_update_from_payload(market, _order(order_id="o2",
                                                                                  exec_quantity="0.004")))

    # ---- batch tracking ---------------------------------------------------------------

    async def test_order_snapshots_use_the_lists_first(self):
        self.exchange._set_current_timestamp(1000.0)
        working = self._track(order_id="c1", exchange_order_id="o1")
        settled = self._track(order_id="c2", exchange_order_id="o2")
        missing = self._track(order_id="c3", exchange_order_id="o3")
        unidentified = self._track(order_id="c4")
        for order in (working, settled, missing, unidentified):
            order.creation_timestamp = 900.0
        young = self._track(order_id="c5", exchange_order_id="o5")
        young.creation_timestamp = 999.0

        def _list(path_url, **kwargs):
            if path_url == CONSTANTS.OPEN_ORDERS_PATH_URL:
                return {"data": {"orders": [_order(order_id="o1"), _order(order_id="foreign")], "cursor": 1}}
            return {"data": {"orders": [_order(order_id="o2", status="CANCELLED")], "cursor": 1}}

        self.exchange._api_post = AsyncMock(side_effect=_list)
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="o3", status="EXECUTED")}})
        snapshots = await self.exchange._fetch_order_snapshots(
            [working, settled, missing, unidentified, young])

        self.assertEqual([("o1", True), ("o2", False), ("o3", None)],
                         [(payload["order_id"], flag) for payload, flag in snapshots])
        self.assertEqual(2, self.exchange._api_post.call_count)
        for call in self.exchange._api_post.call_args_list:
            self.assertEqual({"exchange": "EXCHANGE_2", "limit": 50}, call.kwargs["data"])
        self.assertEqual(1, self.exchange._api_get.call_count)

    async def test_order_snapshots_skip_the_closed_list_when_all_are_open(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_post = AsyncMock(return_value={"data": {"orders": [_order(order_id="o1")]}})
        snapshots = await self.exchange._fetch_order_snapshots([order])
        self.assertEqual(1, len(snapshots))
        self.assertEqual(1, self.exchange._api_post.call_count)
        self.assertEqual([], await self.exchange._fetch_order_snapshots([self._track(order_id="c9")]))

    async def test_failed_single_lookups_are_skipped(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._api_post = AsyncMock(return_value={"data": {"orders": []}})
        self.exchange._api_get = AsyncMock(side_effect=self._error(429))
        self.assertEqual([], await self.exchange._fetch_order_snapshots([order]))

    async def test_snapshot_books_the_fill_before_the_terminal_state(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._process_order_snapshots([
            (_order(order_id="o1", status="EXECUTED", exec_quantity="0.010", avg_execution_price="95000",
                    execution_fee="0.6175"), False),
            (_order(order_id="foreign"), True),
            ("junk", None),
        ])
        await self._settle()
        self.assertTrue(order.is_filled)
        self.assertEqual(Decimal("0.010"), order.executed_amount_base)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

    async def test_update_order_status_is_batched(self):
        self.exchange._set_current_timestamp(1000.0)
        listed = self._track(order_id="c1", exchange_order_id="o1")
        listed.creation_timestamp = 900.0
        lost_id = self._track(order_id="c2")
        lost_id.current_state = OrderState.OPEN
        lost_id.creation_timestamp = 900.0
        self.exchange._api_post = AsyncMock(return_value={"data": {"orders": [
            _order(order_id="o1", exec_quantity="0.004", avg_execution_price="95000")]}})
        self.exchange._api_get = AsyncMock(return_value={"data": {"order": _order(order_id="o2")}})

        await self.exchange._update_order_status()
        await self._settle()

        self.assertEqual(OrderState.PARTIALLY_FILLED, listed.current_state)
        self.assertEqual(Decimal("0.004"), listed.executed_amount_base)
        self.assertEqual("o2", lost_id.exchange_order_id)
        self.assertEqual(1, self.exchange._api_get.call_count)

    # ---- balances / positions ---------------------------------------------------------

    def test_wallet_balance(self):
        self.exchange._account_balances["INR"] = Decimal("5")
        self.exchange._account_available_balances["INR"] = Decimal("5")
        self.exchange._apply_wallet_balance({"data": {
            "base_asset_balances": [{"base_asset": "USDT", "balances": {
                "total_balance": "60960.82599588", "total_available_balance": "60910.77418715",
                "total_blocked_balance": "50.05180873", "total_position_margin": "56.83032573",
                "total_open_order_margin": "-6.778517"}}, "junk", {"balances": {}}],
            "asset": [{"symbol": "DOGEUSDT", "base_asset": "USDT", "blocked_balance": "50.04968529"}],
        }})
        self.assertEqual(Decimal("60960.82599588"), self.exchange.get_balance("USDT"))
        self.assertEqual(Decimal("60910.77418715"), self.exchange.get_available_balance("USDT"))
        self.assertNotIn("INR", self.exchange._account_balances)

        with self.assertRaises(IOError):
            self.exchange._apply_wallet_balance({"message": "Invalid Access"})
        self.assertEqual(Decimal("60960.82599588"), self.exchange.get_balance("USDT"))

    async def test_update_balances(self):
        self.exchange._api_get = AsyncMock(return_value={"data": {"base_asset_balances": []}})
        await self.exchange._update_balances()
        self.assertEqual(CONSTANTS.WALLET_BALANCE_PATH_URL, self.exchange._api_get.call_args.kwargs["path_url"])

    def test_positions(self):
        self.exchange._apply_positions(self.trading_pair, _position())
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(PositionSide.LONG, position.position_side)
        self.assertEqual(Decimal("0.010"), position.amount)
        self.assertEqual(Decimal("95000"), position.entry_price)
        self.assertEqual(Decimal("1.366"), position.unrealized_pnl)
        self.assertEqual(Decimal("10"), position.leverage)

        # Mid-update snapshot: size and entry are kept, so is the last PnL.
        self.exchange._apply_positions(self.trading_pair, _position(mark_price="0", unrealised_pnl="0",
                                                                    position_size="0.020"))
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(Decimal("0.020"), position.amount)
        self.assertEqual(Decimal("1.366"), position.unrealized_pnl)

        self.exchange._apply_positions(self.trading_pair, _position(position_side="SHORT", unrealised_pnl="-2"))
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(PositionSide.SHORT, position.position_side)
        self.assertEqual(Decimal("-0.010"), position.amount)

        self.exchange._apply_positions(self.trading_pair, _position(symbol="ETHUSDT"))
        self.assertEqual({}, self.exchange.account_positions)

        self.exchange._apply_positions(self.trading_pair, {"data": _position()["data"][0]})
        self.assertIn(self.trading_pair, self.exchange.account_positions)
        self.exchange._apply_positions(self.trading_pair, {"data": []})
        self.assertEqual({}, self.exchange.account_positions)

        with self.assertRaises(IOError):
            self.exchange._apply_positions(self.trading_pair, {"message": "Input symbol is missing"})

    async def test_update_positions_is_one_call_per_pair(self):
        exchange = self._make_exchange(["BTC-USDT", "DOGE-USDT"])
        exchange._api_get = AsyncMock(side_effect=[_position(), {"data": []}])
        await exchange._update_positions()
        expected = [{"exchange": "EXCHANGE_2", "symbol": "BTCUSDT"}, {"exchange": "EXCHANGE_2", "symbol": "DOGEUSDT"}]
        self.assertEqual(expected, [call.kwargs["params"] for call in exchange._api_get.call_args_list])
        self.assertEqual(["BTC-USDT"], [p.trading_pair for p in exchange.account_positions.values()])

    async def test_only_oneway_mode(self):
        self.assertEqual((True, ""), await self.exchange._trading_pair_position_mode_set(PositionMode.ONEWAY, "X"))
        ok, message = await self.exchange._trading_pair_position_mode_set(PositionMode.HEDGE, "X")
        self.assertFalse(ok)
        self.assertIn("ONEWAY", message)

    # ---- leverage ------------------------------------------------------------------------

    async def test_leverage_outside_the_contract_range_is_refused_locally(self):
        await self._load_rules()
        self.exchange._api_get = AsyncMock()
        ok, message = await self.exchange._set_trading_pair_leverage(self.trading_pair, 50)
        self.assertFalse(ok)
        self.assertIn("25x", message)
        self.exchange._api_get.assert_not_called()

    async def test_same_leverage_is_not_reapplied(self):
        self.exchange._api_get = AsyncMock(return_value={"data": {"exchange": "EXCHANGE_2", "symbol": "BTCUSDT",
                                                                  "leverage": "10"}})
        self.exchange._api_post = AsyncMock()
        self.assertEqual((True, ""), await self.exchange._set_trading_pair_leverage(self.trading_pair, 10))
        self.exchange._api_post.assert_not_called()
        self.assertEqual(CONSTANTS.GET_LEVERAGE_LIMIT_ID, self.exchange._api_get.call_args.kwargs["limit_id"])

    async def test_set_leverage(self):
        self.exchange._api_get = AsyncMock(side_effect=self._error(500))
        self.exchange._api_post = AsyncMock(return_value={"data": {"symbol": "BTCUSDT", "leverage": "13"}})
        self.assertEqual((True, ""), await self.exchange._set_trading_pair_leverage(self.trading_pair, 13))
        kwargs = self.exchange._api_post.call_args.kwargs
        self.assertEqual({"exchange": "EXCHANGE_2", "symbol": "BTCUSDT", "leverage": 13}, kwargs["data"])
        self.assertEqual(CONSTANTS.SET_LEVERAGE_LIMIT_ID, kwargs["limit_id"])

        self.exchange._api_post = AsyncMock(return_value={"data": {"leverage": "5"}})
        ok, message = await self.exchange._set_trading_pair_leverage(self.trading_pair, 13)
        self.assertFalse(ok)
        self.assertIn("5", message)

        self.exchange._api_post = AsyncMock(side_effect=self._error(400, "open position"))
        ok, message = await self.exchange._set_trading_pair_leverage(self.trading_pair, 13)
        self.assertFalse(ok)
        self.assertIn("open position or open orders", message)

        self.exchange._api_get = AsyncMock(return_value={"data": []})
        self.exchange._api_post = AsyncMock(return_value={"data": {"leverage": "13"}})
        self.assertEqual((True, ""), await self.exchange._set_trading_pair_leverage(self.trading_pair, 13))

    # ---- funding ---------------------------------------------------------------------------

    async def test_build_funding_info(self):
        self.exchange._api_get = AsyncMock(return_value=TICKER)
        info = await self.exchange.build_funding_info(self.trading_pair)
        self.assertEqual(Decimal("95136.7"), info.mark_price)
        self.assertEqual(Decimal("95046.53"), info.index_price)
        self.assertEqual(Decimal("0.00039681"), info.rate)
        self.assertEqual(1732838400, info.next_funding_utc_timestamp)
        self.assertEqual({"exchange": "EXCHANGE_2", "symbol": "BTCUSDT"},
                         self.exchange._api_get.call_args.kwargs["params"])

    async def test_funding_info_edge_cases(self):
        ticker = dict(TICKER["data"]["EXCHANGE_2"])
        self.exchange._api_get = AsyncMock(return_value={"data": {**ticker, "next_funding_timestamp": None}})
        info = await self.exchange.build_funding_info(self.trading_pair)
        self.assertEqual(0, info.next_funding_utc_timestamp % (8 * 3600))

        self.exchange._api_get = AsyncMock(return_value={"data": {"BTCUSDT": {**ticker, "mark_price": 0}}})
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

        self.exchange._api_get = AsyncMock(return_value={"data": {"unexpected": True}})
        with self.assertRaises(IOError):
            await self.exchange.build_funding_info(self.trading_pair)

    async def test_funding_payments_without_timestamps_are_detected_by_id(self):
        self.exchange._perpetual_trading.initialize_funding_info(
            await self._funding_info())
        rows = [{"exchange": "EXCHANGE_2", "transaction_id": "t1", "symbol": "BTCUSDT", "type": "FUNDING_FEE",
                 "quote_asset": "USDT", "amount": "-0.05"},
                {"exchange": "EXCHANGE_2", "transaction_id": "c1", "symbol": "BTCUSDT", "type": "COMMISSION",
                 "quote_asset": "USDT", "amount": "-0.6"}]
        self.exchange._api_get = AsyncMock(return_value={"data": rows})
        self.assertEqual((0, Decimal("-1"), Decimal("-1")),
                         await self.exchange._fetch_last_fee_payment(self.trading_pair))
        params = self.exchange._api_get.call_args.kwargs["params"]
        self.assertEqual({"exchange": "EXCHANGE_2", "symbol": "BTCUSDT", "type": "funding fee", "limit": 20}, params)

        # Nothing new: no payment.
        self.assertEqual(0, (await self.exchange._fetch_last_fee_payment(self.trading_pair))[0])

        rows = [{**rows[0], "transaction_id": "t2", "amount": "0.02"},
                {**rows[0], "transaction_id": "t3", "amount": "0.01"}] + rows
        self.exchange._api_get = AsyncMock(return_value={"data": rows})
        with patch.object(self.exchange._time_synchronizer, "time", return_value=1732838401.0):
            timestamp, rate, amount = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(1732838401.0, timestamp)
        self.assertEqual(Decimal("0.00039681"), rate)
        self.assertEqual(Decimal("0.03"), amount)
        # The same rows again report the same payment, which fires no second event.
        self.assertEqual(1732838401.0, (await self.exchange._fetch_last_fee_payment(self.trading_pair))[0])

    async def _funding_info(self):
        self.exchange._api_get = AsyncMock(return_value=TICKER)
        return await self.exchange.build_funding_info(self.trading_pair)

    async def test_funding_payment_rows_with_a_time_field(self):
        rows = [{"transaction_id": "t1", "symbol": "BTCUSDT", "type": "FUNDING_FEE", "amount": "-0.05",
                 "created_at": 1732809600000},
                {"transaction_id": "t2", "symbol": "BTCUSDT", "type": "funding fee", "amount": "-0.07",
                 "created_at": 1732838400000}]
        self.exchange._api_get = AsyncMock(side_effect=[{"data": rows}, TICKER])
        timestamp, rate, amount = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(1732838400.0, timestamp)
        self.assertEqual(Decimal("-0.07"), amount)
        self.assertEqual(Decimal("0.00039681"), rate)

        self.exchange._api_get = AsyncMock(side_effect=[{"data": rows}, IOError("down")])
        self.assertEqual(Decimal("-1"), (await self.exchange._fetch_last_fee_payment(self.trading_pair))[1])

    # ---- prices ----------------------------------------------------------------------------

    async def test_prices(self):
        all_pairs = {"data": {"BTCUSDT": TICKER["data"]["EXCHANGE_2"],
                              "DOGEUSDT": {"last_price": ""}, "UNKNOWNUSDT": {"last_price": "1"}}}
        self.exchange._api_get = AsyncMock(return_value=all_pairs)
        self.assertEqual({"BTC-USDT": 95136.6},
                         await self.exchange.get_last_traded_prices(["BTC-USDT", "DOGE-USDT"]))
        self.assertEqual(1, self.exchange._api_get.call_count)
        tickers = await self.exchange.get_all_pairs_prices()
        self.assertEqual({"BTCUSDT", "DOGEUSDT", "UNKNOWNUSDT"}, {t["symbol"] for t in tickers})

        self.exchange._api_get = AsyncMock(return_value={"data": [{"symbol": "BTCUSDT"}, "junk"]})
        self.assertEqual([{"symbol": "BTCUSDT"}], await self.exchange.get_all_pairs_prices())
        self.exchange._api_get = AsyncMock(return_value={"message": "x"})
        self.assertEqual([], await self.exchange.get_all_pairs_prices())

        self.exchange._api_get = AsyncMock(return_value=TICKER)
        self.assertEqual(95136.6, await self.exchange._get_last_traded_price(self.trading_pair))
        self.exchange._api_get = AsyncMock(return_value={"data": {"EXCHANGE_2": {"mark_price": 1}}})
        with self.assertRaises(ValueError):
            await self.exchange._get_last_traded_price(self.trading_pair)

    # ---- user stream -----------------------------------------------------------------------

    async def test_user_stream_dispatch(self):
        order = self._track(exchange_order_id="o1")
        events = [
            {"channel": "balance", "data": {"data": {"base_asset_balances": [
                {"base_asset": "USDT", "balances": {"total_balance": "100", "total_available_balance": "90"}}]}}},
            {"channel": "positions", "trading_pair": self.trading_pair, "data": _position()},
            {"channel": "orders", "data": [
                {"order": _order(order_id="o1", status="RAISED", exec_quantity="0.004",
                                 avg_execution_price="95000"), "working": True}, "junk"]},
            {"channel": "balance", "data": {"message": "broken"}},
            {"channel": "unknown"},
        ]
        queue = asyncio.Queue()
        for event in events:
            queue.put_nowait(event)
        self.exchange._user_stream_tracker._user_stream = queue
        self.exchange._sleep = AsyncMock()

        task = asyncio.ensure_future(self.exchange._user_stream_event_listener())
        for _ in range(20):
            await asyncio.sleep(0)
        task.cancel()
        await self._settle()

        self.assertEqual(Decimal("100"), self.exchange.get_balance("USDT"))
        self.assertIn(self.trading_pair, self.exchange.account_positions)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)
        self.assertEqual(Decimal("0.004"), order.executed_amount_base)
        self.exchange._sleep.assert_awaited()

    # ---- errors ------------------------------------------------------------------------------

    def test_error_classification(self):
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(
            self._error(401, '{"message": "Invalid Access"}')))
        # At most once a minute, so bad keys do not double every call.
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(
            self._error(401, '{"message": "Invalid Access"}')))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(self._error(400)))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(IOError("no status")))

        not_found = self._error(400, '{"message": "Order not found"}')
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(not_found))
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(not_found))
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(self._error(500, "not found")))
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(self._error(400, "bad price")))

        self.assertTrue(self.exchange._is_outcome_unknown(asyncio.TimeoutError()))
        self.assertTrue(self.exchange._is_outcome_unknown(self._error(504)))
        self.assertFalse(self.exchange._is_outcome_unknown(self._error(422)))
        self.assertIsNone(self.exchange._http_status(IOError("HTTP status is abc")))

    def test_order_age_without_a_clock_is_unbounded(self):
        order = self._track(exchange_order_id="o1")
        self.exchange._set_current_timestamp(float("nan"))
        self.assertEqual(float("inf"), self.exchange._order_age(order))

    def test_position_helper(self):
        self.exchange._perpetual_trading.set_position(self.trading_pair, Position(
            trading_pair=self.trading_pair, position_side=PositionSide.LONG, unrealized_pnl=Decimal("0"),
            entry_price=Decimal("1"), amount=Decimal("1"), leverage=Decimal("1")))
        self.exchange._apply_positions(self.trading_pair, {"data": [{"symbol": "BTCUSDT", "position_size": "0"}]})
        self.assertEqual({}, self.exchange.account_positions)
