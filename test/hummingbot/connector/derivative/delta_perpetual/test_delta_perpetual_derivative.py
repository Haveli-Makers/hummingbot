from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative, _result
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _product(symbol="BTCUSD", pid=27, base="BTC", quote="USD", settling="USD",
             contract_value="0.001", tick_size="0.5", state="live"):
    return {
        "id": pid,
        "symbol": symbol,
        "contract_type": CONSTANTS.PERPETUAL_CONTRACT_TYPE,
        "state": state,
        "contract_value": contract_value,
        "tick_size": tick_size,
        "underlying_asset": {"symbol": base},
        "quoting_asset": {"symbol": quote},
        "settling_asset": {"symbol": settling},
    }


def _wrap(result):
    return {"success": True, "result": result}


class DeltaPerpetualDerivativeTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base = "BTC"
        cls.quote = "USD"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base, cls.quote)
        cls.symbol = "BTCUSD"

    def setUp(self):
        super().setUp()
        self.connector = DeltaPerpetualDerivative(
            delta_perpetual_api_key="key",
            delta_perpetual_api_secret="secret",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )

    def _bootstrap_symbol_map(self, contract_value="0.001"):
        self.connector._initialize_trading_pair_symbols_from_exchange_info(
            _wrap([_product(contract_value=contract_value)])
        )
        self.connector._trading_rules[self.trading_pair] = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(contract_value),
            min_price_increment=Decimal("0.5"),
            min_base_amount_increment=Decimal(contract_value),
            min_notional_size=Decimal("0"),
            buy_order_collateral_token="USD",
            sell_order_collateral_token="USD",
        )

    # ── Static / pure helpers ──────────────────────────────────────────────────

    def test_result_unwraps_success(self):
        self.assertEqual([1, 2], _result({"success": True, "result": [1, 2]}))

    def test_result_raises_on_failure(self):
        with self.assertRaises(IOError):
            _result({"success": False, "error": "bad"})

    def test_result_passthrough_when_no_envelope(self):
        self.assertEqual({"a": 1}, _result({"a": 1}))

    def test_parse_delta_timestamp_microseconds(self):
        self.assertAlmostEqual(1700000000.0, self.connector._parse_delta_timestamp(1700000000000000))

    def test_parse_delta_timestamp_milliseconds(self):
        self.assertAlmostEqual(1700000000.0, self.connector._parse_delta_timestamp(1700000000000))

    def test_parse_delta_timestamp_invalid(self):
        self.assertEqual(0.0, self.connector._parse_delta_timestamp("not-a-number"))

    def test_supported_order_types(self):
        types = self.connector.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.MARKET, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)

    def test_supported_position_modes_oneway_only(self):
        self.assertEqual([PositionMode.ONEWAY], self.connector.supported_position_modes())

    def test_authenticator_is_cached(self):
        # The authenticator (and its HMAC key) is built once and reused.
        self.assertIs(self.connector.authenticator, self.connector.authenticator)

    # ── Symbol map / trading rules ─────────────────────────────────────────────

    async def test_initialize_symbol_map(self):
        self._bootstrap_symbol_map()
        self.assertEqual(27, self.connector._product_id_by_symbol[self.symbol])
        self.assertEqual(Decimal("0.001"), self.connector._contract_value_by_symbol[self.symbol])
        mapped = await self.connector.trading_pair_associated_to_exchange_symbol(self.symbol)
        self.assertEqual(self.trading_pair, mapped)

    def test_initialize_symbol_map_skips_non_perpetual(self):
        spot = _product()
        spot["contract_type"] = "spot"
        self.connector._initialize_trading_pair_symbols_from_exchange_info(_wrap([spot]))
        self.assertEqual({}, self.connector._product_id_by_symbol)

    async def test_format_trading_rules(self):
        rules = await self.connector._format_trading_rules(_wrap([_product(contract_value="0.001", tick_size="0.5")]))
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(self.trading_pair, rule.trading_pair)
        self.assertEqual(Decimal("0.5"), rule.min_price_increment)
        self.assertEqual(Decimal("0.001"), rule.min_base_amount_increment)
        self.assertEqual("USD", rule.buy_order_collateral_token)
        self.assertEqual("USD", rule.sell_order_collateral_token)

    async def test_format_trading_rules_warns_on_missing_fields(self):
        # Missing contract_value/tick_size must fall back to defaults WITH a warning,
        # not silently quantize orders to a possibly-wrong increment.
        product = _product()
        product.pop("contract_value")
        product.pop("tick_size")
        with self.assertLogs(level="WARNING") as cm:
            rules = await self.connector._format_trading_rules(_wrap([product]))
        self.assertEqual(Decimal("1"), rules[0].min_base_amount_increment)
        self.assertEqual(Decimal("0.5"), rules[0].min_price_increment)
        self.assertTrue(any("contract_value" in line for line in cm.output))
        self.assertTrue(any("tick_size" in line for line in cm.output))

    def test_contract_size_round_trip(self):
        self._bootstrap_symbol_map(contract_value="0.001")
        size = self.connector._format_amount_to_size(self.trading_pair, Decimal("0.005"))
        self.assertEqual(Decimal("5"), size)
        amount = self.connector._format_size_to_amount(self.trading_pair, Decimal("5"))
        self.assertEqual(Decimal("0.005"), amount)

    def test_contract_conversion_works_before_trading_rules(self):
        # Order-book init converts size<->amount before trading rules load; this must
        # use the symbol-map contract value, not self._trading_rules (which KeyErrors).
        self.connector._initialize_trading_pair_symbols_from_exchange_info(
            _wrap([_product(contract_value="0.001")])
        )
        self.assertNotIn(self.trading_pair, self.connector._trading_rules)
        self.assertEqual(Decimal("0.005"),
                         self.connector._format_size_to_amount(self.trading_pair, Decimal("5")))
        self.assertEqual(Decimal("5"),
                         self.connector._format_amount_to_size(self.trading_pair, Decimal("0.005")))

    # ── Order placement / cancellation / status ────────────────────────────────

    async def test_place_order_builds_payload(self):
        self._bootstrap_symbol_map()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kwargs):
            captured["path"] = path_url
            captured["data"] = data
            return _wrap({"id": 9999, "created_at": 1700000000000000})

        self.connector._api_post = AsyncMock(side_effect=fake_post)
        ex_id, ts = await self.connector._place_order(
            order_id="DLTA-1",
            trading_pair=self.trading_pair,
            amount=Decimal("0.003"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            position_action=PositionAction.OPEN,
        )
        self.assertEqual("9999", ex_id)
        self.assertEqual(CONSTANTS.ORDERS_PATH_URL, captured["path"])
        self.assertEqual(27, captured["data"]["product_id"])
        self.assertEqual(3, captured["data"]["size"])
        self.assertEqual("buy", captured["data"]["side"])
        self.assertEqual("limit_order", captured["data"]["order_type"])
        self.assertEqual("50000", captured["data"]["limit_price"])
        self.assertFalse(captured["data"]["reduce_only"])

    async def test_place_order_limit_maker_sets_post_only(self):
        self._bootstrap_symbol_map()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kwargs):
            captured.update(data)
            return _wrap({"id": 1, "created_at": 0})

        self.connector._api_post = AsyncMock(side_effect=fake_post)
        await self.connector._place_order(
            order_id="DLTA-2", trading_pair=self.trading_pair, amount=Decimal("0.001"),
            trade_type=TradeType.SELL, order_type=OrderType.LIMIT_MAKER, price=Decimal("60000"),
            position_action=PositionAction.OPEN,
        )
        self.assertTrue(captured["post_only"])
        self.assertEqual("sell", captured["side"])

    async def test_place_order_close_sets_reduce_only(self):
        self._bootstrap_symbol_map()
        captured = {}
        self.connector._api_post = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or _wrap({"id": 2, "created_at": 0})
        )
        await self.connector._place_order(
            order_id="DLTA-3", trading_pair=self.trading_pair, amount=Decimal("0.001"),
            trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("0"),
            position_action=PositionAction.CLOSE,
        )
        self.assertTrue(captured["reduce_only"])
        self.assertEqual("market_order", captured["order_type"])
        self.assertNotIn("limit_price", captured)

    async def test_place_cancel(self):
        self._bootstrap_symbol_map()
        order = InFlightOrder(
            client_order_id="DLTA-4", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="555",
        )
        captured = {}
        self.connector._api_delete = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or _wrap({"id": 555, "state": "cancelled"})
        )
        ok = await self.connector._place_cancel("DLTA-4", order)
        self.assertTrue(ok)
        self.assertEqual(555, captured["id"])
        self.assertEqual(27, captured["product_id"])

    async def test_stop_network_closes_proxy_factory(self):
        # With a proxy configured, stop_network must close the dedicated session.
        c = DeltaPerpetualDerivative(
            delta_perpetual_api_key="k", delta_perpetual_api_secret="s",
            delta_perpetual_proxy_url="socks5://user:pass@host:1080",
            trading_pairs=[self.trading_pair], trading_required=False,
        )
        self.assertEqual("socks5://user:pass@host:1080", c._proxy_url)
        c._web_assistants_factory.close = AsyncMock()
        await c.stop_network()
        c._web_assistants_factory.close.assert_awaited_once()

    async def test_stop_network_without_proxy_keeps_shared_factory(self):
        # Without a proxy the connections factory is a shared singleton — never close it.
        self.connector._web_assistants_factory.close = AsyncMock()
        await self.connector.stop_network()
        self.connector._web_assistants_factory.close.assert_not_called()

    async def test_place_cancel_pending_order_without_exchange_id(self):
        # An order still pending creation has no exchange_order_id; cancelling it
        # must not crash on int(None) — it returns False so the framework retries.
        self._bootstrap_symbol_map()
        order = InFlightOrder(
            client_order_id="DLTA-4b", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"), price=Decimal("50000"),
            creation_timestamp=1700000000.0,
        )
        self.assertIsNone(order.exchange_order_id)
        self.connector._api_delete = AsyncMock(side_effect=AssertionError("should not call the API"))
        ok = await self.connector._place_cancel("DLTA-4b", order)
        self.assertFalse(ok)
        self.connector._api_delete.assert_not_called()

    def test_cancel_of_terminal_order_classifies_as_not_found(self):
        # Live-confirmed: Delta returns {"error":{"code":"open_order_not_found"}}
        # (HTTP 400) when cancelling an order that is no longer open (filled or
        # already cancelled). That must classify as not-found so the base cancel
        # flow settles the order rather than leaving it phantom-in-flight.
        err = IOError('HTTP status is 400. Error: '
                      '{"error":{"code":"open_order_not_found"},"success":false}')
        self.assertTrue(self.connector._is_order_not_found_during_cancelation_error(err))
        self.assertFalse(
            self.connector._is_order_not_found_during_cancelation_error(IOError("insufficient_margin")))

    async def test_request_order_status_maps_state(self):
        self._bootstrap_symbol_map()
        order = InFlightOrder(
            client_order_id="DLTA-5", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="777",
        )
        self.connector._api_get = AsyncMock(
            return_value=_wrap({"id": 777, "state": "closed", "size": 1, "unfilled_size": 0,
                                "updated_at": 1700000001000000})
        )
        update = await self.connector._request_order_status(order)
        self.assertEqual(OrderState.FILLED, update.new_state)
        self.assertEqual("777", update.exchange_order_id)

    async def test_request_order_status_partial_fill(self):
        self._bootstrap_symbol_map()
        order = InFlightOrder(
            client_order_id="DLTA-6", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.002"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="888",
        )
        self.connector._api_get = AsyncMock(
            return_value=_wrap({"id": 888, "state": "open", "size": 2, "unfilled_size": 1,
                                "updated_at": 1700000001000000})
        )
        update = await self.connector._request_order_status(order)
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)

    async def test_process_order_event_unmapped_state_keeps_current(self):
        # An unmapped WS order state must not be dropped; the order stays tracked at
        # its current state instead of being silently ignored.
        self._bootstrap_symbol_map()
        order = InFlightOrder(
            client_order_id="DLTA-7", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="999",
        )
        self.connector._order_tracker.start_tracking_order(order)
        captured = {}
        self.connector._order_tracker.process_order_update = lambda order_update: captured.update(
            state=order_update.new_state)
        self.connector._process_order_event(
            {"client_order_id": "DLTA-7", "id": "999", "state": "some_unmapped_state",
             "updated_at": 1700000001000000})
        self.assertIn("state", captured)  # not dropped
        self.assertEqual(order.current_state, captured["state"])

    # ── Balances / positions / leverage ────────────────────────────────────────

    async def test_update_balances(self):
        self.connector._api_get = AsyncMock(
            return_value=_wrap([{"asset_symbol": "USD", "balance": "1000", "available_balance": "800"}])
        )
        await self.connector._update_balances()
        self.assertEqual(Decimal("1000"), self.connector._account_balances["USD"])
        self.assertEqual(Decimal("800"), self.connector._account_available_balances["USD"])

    async def test_update_balances_empty_does_not_wipe(self):
        # A degenerate empty wallet payload (success:true, result:[]) must not wipe
        # tracked balances.
        self.connector._account_balances["USD"] = Decimal("1000")
        self.connector._account_available_balances["USD"] = Decimal("800")
        self.connector._api_get = AsyncMock(return_value=_wrap([]))
        await self.connector._update_balances()
        self.assertEqual(Decimal("1000"), self.connector._account_balances.get("USD"))

    async def test_update_positions_long(self):
        self._bootstrap_symbol_map()
        self.connector._api_get = AsyncMock(
            return_value=_wrap([{
                "product_symbol": self.symbol, "size": 5, "entry_price": "50000",
                "unrealized_pnl": "12.5", "leverage": "10",
            }])
        )
        await self.connector._update_positions()
        positions = self.connector.account_positions
        self.assertEqual(1, len(positions))
        pos = list(positions.values())[0]
        self.assertEqual(PositionSide.LONG, pos.position_side)
        self.assertEqual(Decimal("0.005"), pos.amount)  # 5 contracts * 0.001
        self.assertEqual(Decimal("10"), pos.leverage)

    async def test_update_positions_zero_removes(self):
        self._bootstrap_symbol_map()
        self.connector._api_get = AsyncMock(
            return_value=_wrap([{"product_symbol": self.symbol, "size": 0}])
        )
        await self.connector._update_positions()
        self.assertEqual(0, len(self.connector.account_positions))

    async def test_update_positions_resolves_by_product_id(self):
        # /v2/positions/margined may omit product_symbol — fall back to product_id.
        self._bootstrap_symbol_map()
        captured = {}
        self.connector._api_get = AsyncMock(
            side_effect=lambda path_url, **kw: captured.update(path=path_url) or _wrap([
                {"product_id": 27, "size": -3, "entry_price": "50000"}
            ])
        )
        await self.connector._update_positions()
        # Uses the all-positions endpoint, not the filtered /v2/positions.
        self.assertEqual(CONSTANTS.POSITIONS_MARGINED_PATH_URL, captured["path"])
        positions = self.connector.account_positions
        self.assertEqual(1, len(positions))
        pos = list(positions.values())[0]
        self.assertEqual(PositionSide.SHORT, pos.position_side)
        self.assertEqual(Decimal("-0.003"), pos.amount)  # -3 contracts * 0.001

    def test_symbol_for_product_id(self):
        self._bootstrap_symbol_map()
        self.assertEqual(self.symbol, self.connector._symbol_for_product_id(27))
        self.assertIsNone(self.connector._symbol_for_product_id(999999))
        self.assertIsNone(self.connector._symbol_for_product_id(None))

    async def test_set_leverage(self):
        self._bootstrap_symbol_map()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kwargs):
            captured["path"] = path_url
            captured["data"] = data
            return _wrap({"leverage": "10"})

        self.connector._api_post = AsyncMock(side_effect=fake_post)
        ok, msg = await self.connector._set_trading_pair_leverage(self.trading_pair, 10)
        self.assertTrue(ok)
        self.assertIn("27", captured["path"])
        self.assertEqual("10", captured["data"]["leverage"])

    async def test_position_mode_oneway_accepted_hedge_rejected(self):
        ok, _ = await self.connector._trading_pair_position_mode_set(PositionMode.ONEWAY, self.trading_pair)
        self.assertTrue(ok)
        bad, msg = await self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, self.trading_pair)
        self.assertFalse(bad)

    def test_get_fee_perpetual(self):
        fee = self.connector._get_fee(
            base_currency="BTC", quote_currency="USDT", order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN, amount=Decimal("1"),
            price=Decimal("50000"),
        )
        self.assertIsNotNone(fee)

    def test_get_fee_respects_explicit_taker(self):
        # An explicit is_maker=False must not be clobbered to maker just because the
        # order type is LIMIT_MAKER.
        taker = self.connector._get_fee(
            base_currency="BTC", quote_currency="USD", order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN, amount=Decimal("1"),
            price=Decimal("50000"), is_maker=False)
        self.assertEqual(self.connector.estimate_fee_pct(False), taker.percent)

        inferred = self.connector._get_fee(
            base_currency="BTC", quote_currency="USD", order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN, amount=Decimal("1"),
            price=Decimal("50000"))  # is_maker unspecified → infer maker
        self.assertEqual(self.connector.estimate_fee_pct(True), inferred.percent)

    @patch.object(DeltaPerpetualDerivative, "_api_get", new_callable=AsyncMock)
    async def test_get_last_traded_price(self, api_get_mock):
        self._bootstrap_symbol_map()
        api_get_mock.return_value = _wrap({"symbol": self.symbol, "close": "51000"})
        price = await self.connector._get_last_traded_price(self.trading_pair)
        self.assertEqual(51000.0, price)
