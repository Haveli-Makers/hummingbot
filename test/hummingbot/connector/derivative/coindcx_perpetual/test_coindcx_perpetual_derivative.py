import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import CoindcxPerpetualDerivative
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

INSTRUMENT = {
    "pair": "B-BTC_USDT",
    "status": "active",
    "kind": "perpetual",
    "is_inverse": False,
    "exit_only": False,
    "underlying_currency_short_name": "BTC",
    "position_currency_short_name": "BTC",
    "quote_currency_short_name": "USDT",
    "settle_currency_short_name": "USDT",
    "price_increment": 0.1,
    "quantity_increment": 0.001,
    "min_quantity": 0.001,
    "min_trade_size": 0.001,
    "max_quantity": 950.0,
    "min_notional": 60.0,
    "maker_fee": 0.0236,
    "taker_fee": 0.059,
    "funding_frequency": 8,
    "max_leverage_long": 20.0,
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
        self.exchange._instruments["B-BTC_USDT"] = INSTRUMENT

    def _bootstrap(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info([INSTRUMENT])

    def _order(self, exchange_order_id="ex-1", trade_type=TradeType.BUY):
        return InFlightOrder(
            client_order_id="haveli-1",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=trade_type,
            amount=Decimal("0.01"),
            price=Decimal("60000"),
            creation_timestamp=1700000000.0,
            exchange_order_id=exchange_order_id,
        )

    # ---- CLI wiring -----------------------------------------------------------

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

    # ---- static -------------------------------------------------------------

    def test_only_limit_and_market_supported(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.MARKET, types)
        # allow_post_only is false on every CoinDCX futures instrument.
        self.assertNotIn(OrderType.LIMIT_MAKER, types)

    def test_oneway_only_and_usdt_collateral(self):
        self.assertEqual([PositionMode.ONEWAY], self.exchange.supported_position_modes())
        self.assertEqual("USDT", self.exchange.get_buy_collateral_token(self.trading_pair))
        self.assertEqual("USDT", self.exchange.get_sell_collateral_token(self.trading_pair))

    def test_order_type_and_side_mapping(self):
        self.assertEqual("limit_order", CoindcxPerpetualDerivative.coindcx_order_type(OrderType.LIMIT))
        self.assertEqual("market_order", CoindcxPerpetualDerivative.coindcx_order_type(OrderType.MARKET))
        self.assertEqual("buy", CoindcxPerpetualDerivative.coindcx_side(TradeType.BUY))
        self.assertEqual("sell", CoindcxPerpetualDerivative.coindcx_side(TradeType.SELL))

    async def test_symbol_map_and_trading_rules(self):
        self._bootstrap()
        symbol = await self.exchange.exchange_symbol_associated_to_pair(trading_pair=self.trading_pair)
        self.assertEqual("B-BTC_USDT", symbol)

        rules = await self.exchange._format_trading_rules([INSTRUMENT])
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(self.trading_pair, rule.trading_pair)
        self.assertEqual(Decimal("0.1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.001"), rule.min_order_size)
        self.assertEqual(Decimal("60.0"), rule.min_notional_size)
        self.assertEqual("USDT", rule.buy_order_collateral_token)

    def test_fee_uses_instrument_percentages(self):
        fee = self.exchange._get_fee(
            base_currency="BTC", quote_currency="USDT", order_type=OrderType.LIMIT,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN,
            amount=Decimal("1"), price=Decimal("60000"), is_maker=True)
        # 0.0236 (percent) -> 0.000236 as a fraction
        self.assertEqual(Decimal("0.000236"), fee.percent)

    # ---- orders --------------------------------------------------------------

    async def test_place_order_nests_body_under_order_key(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["path"] = path_url
            captured["data"] = data
            return [{"id": "uuid-1", "created_at": 1700000000000}]

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        exchange_id, ts = await self.exchange._place_order(
            order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
            position_action=PositionAction.OPEN)

        self.assertEqual("uuid-1", exchange_id)
        self.assertEqual(1700000000.0, ts)
        self.assertEqual(CONSTANTS.CREATE_ORDER_PATH_URL, captured["path"])
        order = captured["data"]["order"]
        self.assertEqual("B-BTC_USDT", order["pair"])
        self.assertEqual("buy", order["side"])
        self.assertEqual("limit_order", order["order_type"])
        self.assertEqual("good_till_cancel", order["time_in_force"])
        self.assertEqual(60000.0, order["price"])
        self.assertEqual(0.01, order["total_quantity"])

    async def test_place_order_sends_the_connectors_current_leverage(self):
        """
        The order carries ex.get_leverage(pair), which reads PerpetualTrading's
        cache — a defaultdict returning 1. Setting leverage on the exchange alone
        (``_set_trading_pair_leverage``) does NOT update it, so the order would go
        out at 1x and be rejected for insufficient margin.
        """
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["data"] = data
            return [{"id": "uuid-lev", "created_at": 1700000000000}]

        self.exchange._api_post = AsyncMock(side_effect=fake_post)

        # Default, before any leverage is set.
        await self.exchange._place_order(
            order_id="haveli-lev-default", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
            position_action=PositionAction.OPEN)
        self.assertEqual(1, captured["data"]["order"]["leverage"])

        # After the connector's leverage is set, the order must carry it.
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, 20)
        await self.exchange._place_order(
            order_id="haveli-lev-20", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
            position_action=PositionAction.OPEN)
        self.assertEqual(20, captured["data"]["order"]["leverage"])

    async def test_market_order_omits_time_in_force_and_price(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["data"] = data
            return [{"id": "uuid-2", "created_at": 1700000000000}]

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        await self.exchange._place_order(
            order_id="haveli-2", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.SELL, order_type=OrderType.MARKET, price=Decimal("0"),
            position_action=PositionAction.OPEN)

        order = captured["data"]["order"]
        # CoinDCX rejects time_in_force on market orders.
        self.assertNotIn("time_in_force", order)
        self.assertNotIn("price", order)

    async def test_place_order_raises_when_no_id_returned(self):
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value={"message": "insufficient margin"})
        with self.assertRaises(IOError):
            await self.exchange._place_order(
                order_id="haveli-3", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
                position_action=PositionAction.OPEN)

    async def test_place_cancel_success(self):
        self.exchange._api_post = AsyncMock(return_value={"message": "success", "status": 200, "code": 200})
        self.assertTrue(await self.exchange._place_cancel("haveli-1", self._order()))

    async def test_place_cancel_without_exchange_id_returns_false(self):
        # Must not raise: the framework retries once the id is assigned.
        self.exchange._api_post = AsyncMock(side_effect=AssertionError("should not call the API"))
        self.assertFalse(await self.exchange._place_cancel("haveli-1", self._order(exchange_order_id=None)))

    async def test_request_order_status_finds_order_in_list(self):
        self.exchange._api_post = AsyncMock(return_value=[
            {"id": "other", "status": "open"},
            {"id": "ex-1", "status": "partially_filled", "updated_at": 1700000001000},
        ])
        update = await self.exchange._request_order_status(self._order())
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)
        self.assertEqual("ex-1", update.exchange_order_id)
        self.assertEqual(1700000001.0, update.update_timestamp)

    async def test_request_order_status_raises_when_missing(self):
        self.exchange._api_post = AsyncMock(return_value=[])
        with self.assertRaises(IOError):
            await self.exchange._request_order_status(self._order())

    async def test_trade_updates_parse_fills(self):
        self.exchange._api_post = AsyncMock(return_value=[
            {"id": "t1", "order_id": "ex-1", "price": "60000", "quantity": "0.004",
             "fee_amount": "0.05", "timestamp": 1700000002000},
            {"id": "t2", "order_id": "other-order", "price": "1", "quantity": "1"},
        ])
        updates = await self.exchange._all_trade_updates_for_order(self._order())
        self.assertEqual(1, len(updates))
        fill = updates[0]
        self.assertEqual("t1", fill.trade_id)
        self.assertEqual(Decimal("0.004"), fill.fill_base_amount)
        self.assertEqual(Decimal("240.000"), fill.fill_quote_amount)
        self.assertEqual(Decimal("0.05"), fill.fee.flat_fees[0].amount)
        self.assertEqual("USDT", fill.fee.flat_fees[0].token)

    # ---- balances / positions -------------------------------------------------

    async def test_partial_fills_are_not_collapsed_by_a_missing_trade_id(self):
        """
        CoinDCX's /futures/trades payload has NO trade id field. InFlightOrder
        drops any fill whose trade_id it has already recorded, so a constant id
        would collapse every partial fill into the first and under-count the
        executed amount. Fills must therefore get distinct synthesised ids.
        """
        order = self._order()
        # Two genuine partial fills of the same order — real payload shape.
        first = {"order_id": "ex-1", "timestamp": 1705645534425.8374, "price": "60000",
                 "quantity": "0.004", "fee_amount": "0.02", "is_maker": False}
        second = {"order_id": "ex-1", "timestamp": 1705645539871.1121, "price": "60010",
                  "quantity": "0.006", "fee_amount": "0.03", "is_maker": False}

        update_one = self.exchange._trade_update_from_fill(first, order)
        update_two = self.exchange._trade_update_from_fill(second, order)
        self.assertNotEqual(update_one.trade_id, update_two.trade_id)
        self.assertTrue(update_one.trade_id, "trade_id must not be empty")

        # Deterministic: re-polling the same fill yields the same id, so it dedupes.
        self.assertEqual(update_one.trade_id, self.exchange._trade_update_from_fill(first, order).trade_id)

        # Exercise the real accounting path.
        self.assertTrue(order.update_with_trade_update(update_one))
        self.assertTrue(order.update_with_trade_update(update_two),
                        "second partial fill was dropped as a duplicate")
        self.assertFalse(order.update_with_trade_update(update_one), "re-polled fill must dedupe")
        self.assertEqual(Decimal("0.01"), order.executed_amount_base)

    def test_fill_id_prefers_an_explicit_id_when_present(self):
        self.assertEqual("abc", self.exchange._fill_id({"id": "abc", "order_id": "x"}))
        self.assertEqual("t-1", self.exchange._fill_id({"trade_id": "t-1"}))

    async def test_update_balances(self):
        self.exchange._api_request = AsyncMock(return_value=[
            {"currency_short_name": "USDT", "balance": "6.1693226", "locked_balance": "0.5"},
        ])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("6.1693226"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("6.6693226"), self.exchange._account_balances["USDT"])

    def test_long_position_derives_unrealized_pnl(self):
        self.exchange._process_position_payload({
            "pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
            "mark_price": 61000.0, "leverage": 10.0,
        })
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(PositionSide.LONG, position.position_side)
        self.assertEqual(Decimal("0.5"), position.amount)
        # CoinDCX reports no unrealised PnL; it is derived from the mark price.
        self.assertEqual(Decimal("500.0"), position.unrealized_pnl)

    def test_short_position_has_negative_amount(self):
        self.exchange._process_position_payload({
            "pair": "B-BTC_USDT", "active_pos": -0.2, "avg_price": 60000.0,
            "mark_price": 59000.0, "leverage": 5.0,
        })
        position = self.exchange.account_positions[self.trading_pair]
        self.assertEqual(PositionSide.SHORT, position.position_side)
        self.assertEqual(Decimal("-0.2"), position.amount)
        self.assertEqual(Decimal("200.0"), position.unrealized_pnl)

    def test_flat_position_is_removed(self):
        self.exchange._process_position_payload({
            "pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0, "mark_price": 60000.0})
        self.assertIn(self.trading_pair, self.exchange.account_positions)
        self.exchange._process_position_payload({"pair": "B-BTC_USDT", "active_pos": 0})
        self.assertNotIn(self.trading_pair, self.exchange.account_positions)

    async def test_set_leverage_success(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured.update(data)
            return {"message": "success", "status": 200, "code": 200}

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 10)
        self.assertTrue(success)
        self.assertEqual("10", captured["leverage"])
        self.assertEqual("B-BTC_USDT", captured["pair"])

    async def test_set_leverage_failure_reports_message(self):
        self.exchange._api_post = AsyncMock(return_value={"message": "leverage too high", "code": 422})
        success, msg = await self.exchange._set_trading_pair_leverage(self.trading_pair, 500)
        self.assertFalse(success)
        self.assertIn("leverage too high", msg)

    async def test_hedge_mode_rejected(self):
        ok, _ = await self.exchange._trading_pair_position_mode_set(PositionMode.HEDGE, self.trading_pair)
        self.assertFalse(ok)
        ok, _ = await self.exchange._trading_pair_position_mode_set(PositionMode.ONEWAY, self.trading_pair)
        self.assertTrue(ok)

    # ---- funding / prices -----------------------------------------------------

    def test_next_funding_timestamp_aligns_to_interval(self):
        self._bootstrap()
        ts = self.exchange.next_funding_timestamp(self.trading_pair)
        self.assertEqual(0, ts % (8 * 3600))
        self.assertGreater(ts, self.exchange._time_synchronizer.time())

    async def test_build_funding_info(self):
        self._bootstrap()
        self.exchange._fetch_current_prices = AsyncMock(return_value={
            "B-BTC_USDT": {"mp": 65500.5, "ls": 65499.0, "fr": 0.0001},
        })
        info = await self.exchange.build_funding_info(self.trading_pair)
        self.assertEqual(Decimal("65500.5"), info.mark_price)
        self.assertEqual(Decimal("0.0001"), info.rate)

    async def test_get_all_pairs_prices(self):
        self.exchange._fetch_current_prices = AsyncMock(return_value={
            "B-BTC_USDT": {"mp": 65500.5, "ls": 65499.0, "fr": 0.0001, "v": 5260392600},
            "B-BAD_USDT": {},
        })
        prices = await self.exchange.get_all_pairs_prices()
        self.assertEqual(1, len(prices))
        self.assertEqual("B-BTC_USDT", prices[0]["symbol"])
        self.assertEqual("65499.0", prices[0]["lastPrice"])

    # ---- margin currency ------------------------------------------------------

    async def test_usdt_margin_sent_on_every_authenticated_call(self):
        self._bootstrap()
        captured = []

        async def fake_post(path_url, data, is_auth_required):
            captured.append(data)
            return []

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        await self.exchange._update_positions()
        await self.exchange._fetch_order_by_id(self._order())
        for data in captured:
            self.assertEqual(["USDT"], data["margin_currency_short_name"])

    async def test_trading_rules_use_usdt_collateral(self):
        rules = await self.exchange._format_trading_rules([INSTRUMENT])
        self.assertEqual("USDT", rules[0].buy_order_collateral_token)
        self.assertEqual("USDT", rules[0].sell_order_collateral_token)

    def _inr_exchange(self):
        ex = CoindcxPerpetualDerivative(
            coindcx_perpetual_api_key="key",
            coindcx_perpetual_api_secret="secret",
            coindcx_perpetual_margin_currency="INR",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        ex._instruments["B-BTC_USDT"] = INSTRUMENT
        ex._initialize_trading_pair_symbols_from_exchange_info([INSTRUMENT])
        return ex

    def test_margin_currency_is_configurable(self):
        ex = self._inr_exchange()
        self.assertEqual("INR", ex.margin_currency)
        # Default stays USDT.
        self.assertEqual("USDT", self.exchange.margin_currency)

    def test_margin_currency_lowercase_is_normalised(self):
        ex = CoindcxPerpetualDerivative(
            coindcx_perpetual_api_key="k", coindcx_perpetual_api_secret="s",
            coindcx_perpetual_margin_currency="inr",
            trading_pairs=[self.trading_pair], trading_required=False)
        self.assertEqual("INR", ex.margin_currency)

    def test_invalid_margin_currency_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            CoindcxPerpetualDerivative(
                coindcx_perpetual_api_key="key",
                coindcx_perpetual_api_secret="secret",
                coindcx_perpetual_margin_currency="BTC",
                trading_pairs=[self.trading_pair],
                trading_required=False,
            )

    def test_collateral_token_is_the_quote_currency_even_under_inr_margin(self):
        """
        Collateral must be reported in the contract's quote currency. Reporting
        INR makes OrderCandidate look for a "USDT-INR" market to convert with —
        which CoinDCX futures does not list — and every v2 executor dies with
        "No order book exists for 'USDT-INR'".
        """
        inr = self._inr_exchange()
        self.assertEqual("INR", inr.margin_currency, "the wallet/API currency stays INR")
        self.assertEqual("USDT", inr.get_buy_collateral_token(self.trading_pair))
        self.assertEqual("USDT", inr.get_sell_collateral_token(self.trading_pair))

    async def test_inr_wallet_is_reported_only_as_quote_denominated_buying_power(self):
        """
        The INR wallet must be CONVERTED, not listed alongside its equivalent:
        publishing both shows the same money twice in `balance` (and the INR row
        prices at $0 because Hummingbot has no INR rate).
        """
        inr = self._inr_exchange()
        inr._api_request = AsyncMock(return_value=[
            {"currency_short_name": "INR", "balance": "969.1044", "locked_balance": "0.0"},
        ])
        inr._margin_conversion_rate = AsyncMock(return_value=Decimal("100.25"))

        await inr._update_balances()

        self.assertNotIn("INR", inr._account_balances, "must not double-report the wallet")
        self.assertAlmostEqual(
            float(Decimal("969.1044") / Decimal("100.25")),
            float(inr._account_available_balances["USDT"]), places=6)
        self.assertEqual(1, len(inr._account_balances))

    async def test_locked_margin_counts_towards_total_but_not_available(self):
        inr = self._inr_exchange()
        inr._api_request = AsyncMock(return_value=[
            {"currency_short_name": "INR", "balance": "500.0", "locked_balance": "100.0"},
        ])
        inr._margin_conversion_rate = AsyncMock(return_value=Decimal("100.0"))
        await inr._update_balances()
        self.assertEqual(Decimal("5"), inr._account_available_balances["USDT"])
        self.assertEqual(Decimal("6"), inr._account_balances["USDT"])

    async def test_balances_untouched_when_the_rate_is_unavailable(self):
        inr = self._inr_exchange()
        inr._account_available_balances["USDT"] = Decimal("9.5")
        inr._api_request = AsyncMock(return_value=[
            {"currency_short_name": "INR", "balance": "969.0", "locked_balance": "0.0"}])
        inr._margin_conversion_rate = AsyncMock(return_value=None)
        await inr._update_balances()
        # Better a stale figure than a wrong one.
        self.assertEqual(Decimal("9.5"), inr._account_available_balances["USDT"])

    async def test_non_margin_wallets_are_ignored(self):
        inr = self._inr_exchange()
        inr._api_request = AsyncMock(return_value=[
            {"currency_short_name": "USDT", "balance": "5.0", "locked_balance": "0.0"},
            {"currency_short_name": "INR", "balance": "1000.0", "locked_balance": "0.0"},
        ])
        inr._margin_conversion_rate = AsyncMock(return_value=Decimal("100.0"))
        await inr._update_balances()
        # Orders carry margin_currency_short_name=INR, so only the INR wallet backs them.
        self.assertEqual(Decimal("10"), inr._account_available_balances["USDT"])

    async def test_usdt_margin_balances_are_untouched(self):
        self.exchange._api_request = AsyncMock(return_value=[
            {"currency_short_name": "USDT", "balance": "10.0", "locked_balance": "2.0"},
        ])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("10.0"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("12.0"), self.exchange._account_balances["USDT"])

    async def test_conversion_rate_reads_the_spot_ticker_and_caches(self):
        inr = self._inr_exchange()
        calls = []

        async def fake_execute(url, method, throttler_limit_id, **kwargs):
            calls.append(url)
            return [{"market": "BTCINR", "last_price": "6300000"},
                    {"market": "USDTINR", "last_price": "100.25"}]

        assistant = AsyncMock()
        assistant.execute_request = AsyncMock(side_effect=fake_execute)
        inr._web_assistants_factory.get_rest_assistant = AsyncMock(return_value=assistant)

        self.assertEqual(Decimal("100.25"), await inr._margin_conversion_rate())
        self.assertEqual(Decimal("100.25"), await inr._margin_conversion_rate())
        self.assertEqual(1, len(calls), "rate should be cached, not re-fetched every poll")

    async def test_conversion_rate_is_one_for_usdt_margin(self):
        self.assertEqual(Decimal("1"), await self.exchange._margin_conversion_rate())

    async def test_inr_margin_sent_when_placing_order(self):
        ex = self._inr_exchange()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["data"] = data
            return [{"id": "uuid-inr", "created_at": 1700000000000}]

        ex._api_post = AsyncMock(side_effect=fake_post)
        await ex._place_order(
            order_id="haveli-inr", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
            position_action=PositionAction.OPEN)
        self.assertEqual("INR", captured["data"]["order"]["margin_currency_short_name"])

    async def test_inr_margin_sent_when_fetching_positions_and_orders(self):
        ex = self._inr_exchange()
        captured = []

        async def fake_post(path_url, data, is_auth_required):
            captured.append((path_url, data))
            return []

        ex._api_post = AsyncMock(side_effect=fake_post)
        await ex._update_positions()
        await ex._fetch_order_by_id(self._order())
        for path_url, data in captured:
            self.assertEqual(["INR"], data["margin_currency_short_name"], f"for {path_url}")

    async def test_inr_margin_sent_when_setting_leverage(self):
        ex = self._inr_exchange()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured.update(data)
            return {"message": "success", "code": 200}

        ex._api_post = AsyncMock(side_effect=fake_post)
        await ex._set_trading_pair_leverage(self.trading_pair, 15)
        self.assertEqual("INR", captured["margin_currency_short_name"])

    async def test_trading_rule_collateral_matches_the_collateral_token(self):
        # Both must be the quote currency, whatever the margin wallet is.
        for exchange in (self.exchange, self._inr_exchange()):
            rules = await exchange._format_trading_rules([INSTRUMENT])
            self.assertEqual(exchange.get_buy_collateral_token(self.trading_pair),
                             rules[0].buy_order_collateral_token)
            self.assertEqual("USDT", rules[0].sell_order_collateral_token)

    async def test_fees_stay_in_usdt_under_inr_margin(self):
        # "fee_amount and ideal_margin values are in USDT for INR Futures".
        ex = self._inr_exchange()
        ex._api_post = AsyncMock(return_value=[
            {"id": "t1", "order_id": "ex-1", "price": "60000", "quantity": "0.004",
             "fee_amount": "0.05", "timestamp": 1700000002000},
        ])
        updates = await ex._all_trade_updates_for_order(self._order())
        self.assertEqual("USDT", updates[0].fee.flat_fees[0].token)

        fee = ex._get_fee(
            base_currency="BTC", quote_currency="USDT", order_type=OrderType.LIMIT,
            order_side=TradeType.BUY, position_action=PositionAction.OPEN,
            amount=Decimal("1"), price=Decimal("60000"), is_maker=True)
        self.assertEqual("USDT", fee.percent_token)

    # ---- user-stream order updates --------------------------------------------

    @staticmethod
    def _ws_order_frame(status, updated_at, fee=0):
        """Real df-order-update shape captured from the live private stream."""
        return {"id": "ws-1", "pair": "B-BTC_USDT", "side": "buy", "status": status,
                "order_type": "limit_order", "price": 64443.1, "avg_price": 64128.4,
                "total_quantity": 0.001, "remaining_quantity": 0.0,
                "fee_amount": fee, "trades": [], "created_at": 1785306177433,
                "updated_at": updated_at}

    async def test_ws_order_frames_drive_the_order_to_open(self):
        """
        Live sequence is initial -> initial -> open -> filled. process_order_update
        schedules via safe_ensure_future, so state settles only after the loop runs.
        """
        self._bootstrap()
        order = self._order(exchange_order_id="ws-1")
        self.exchange._order_tracker.start_tracking_order(order)

        for status, ts in (("initial", 1785306177433), ("open", 1785306177728)):
            self.exchange._process_order_event(self._ws_order_frame(status, ts))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        tracked = self.exchange._order_tracker.all_updatable_orders[order.client_order_id]
        self.assertEqual(OrderState.OPEN, tracked.current_state)

    async def test_ws_filled_frame_completes_only_once_the_fill_arrives(self):
        """
        A FILLED order update deliberately waits for the fills (ClientOrderTracker
        awaits wait_until_completely_filled). CoinDCX's WS order frames always
        carry an EMPTY 'trades' array — verified live — so the fill itself comes
        from the REST /futures/trades poll. This pins that interplay: the order
        only completes when both halves have landed.
        """
        self._bootstrap()
        order = self._order(exchange_order_id="ws-1")
        order.amount = Decimal("0.001")
        self.exchange._order_tracker.start_tracking_order(order)

        self.exchange._process_order_event(self._ws_order_frame("open", 1785306177728))
        await asyncio.sleep(0)

        # WS says filled, but no fill has been recorded yet.
        self.exchange._process_order_event(
            self._ws_order_frame("filled", 1785306178063, fee=0.037835756))
        await asyncio.sleep(0)
        tracked = self.exchange._order_tracker.all_updatable_orders[order.client_order_id]
        self.assertNotEqual(OrderState.FILLED, tracked.current_state,
                            "must not complete before the fill is known")

        # Now the REST fill poll supplies the trade, using the real fill_id.
        fill = {"fill_id": "98df7c2a-d2d4-43fe-a6c3-d001962c177b", "order_id": "ws-1",
                "price": 64128.4, "quantity": 0.001, "fee_amount": 0.037835756,
                "timestamp": 1785306177643.0, "is_maker": False}
        self.exchange._order_tracker.process_trade_update(
            self.exchange._trade_update_from_fill(fill, order))
        for _ in range(4):
            await asyncio.sleep(0)

        completed = (self.exchange._order_tracker.all_updatable_orders.get(order.client_order_id)
                     or self.exchange._order_tracker.all_orders.get(order.client_order_id))
        self.assertEqual(Decimal("0.001"), completed.executed_amount_base)
        self.assertTrue(completed.is_filled, "order should be filled once the trade landed")

    def test_fill_id_uses_the_exchanges_fill_id_field(self):
        # Confirmed live: /futures/trades returns fill_id (the docs omit it).
        self.assertEqual(
            "98df7c2a-d2d4-43fe-a6c3-d001962c177b",
            self.exchange._fill_id({"fill_id": "98df7c2a-d2d4-43fe-a6c3-d001962c177b",
                                    "order_id": "ws-1", "timestamp": 1785306177643.0}))

    def test_ws_order_frame_for_an_untracked_order_is_ignored(self):
        self._bootstrap()
        # Must not raise when a frame arrives for an order we do not track.
        self.exchange._process_order_event(
            {"id": "someone-elses-order", "pair": "B-BTC_USDT", "status": "filled"})

    # ---- funding payments -----------------------------------------------------

    async def test_fetch_last_fee_payment_returns_the_most_recent_for_the_pair(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["path"] = path_url
            captured["data"] = data
            # Real shape from POST /positions/transactions (stage=funding).
            return [
                {"pair": "B-BTC_USDT", "stage": "funding", "amount": -0.12,
                 "fee_amount": 0.0, "margin_currency_short_name": "USDT",
                 "position_id": "pos-1", "created_at": 1728459094499},
                {"pair": "B-BTC_USDT", "stage": "funding", "amount": 0.34,
                 "fee_amount": 0.0, "margin_currency_short_name": "USDT",
                 "position_id": "pos-1", "created_at": 1728462694499},
                {"pair": "B-ETH_USDT", "stage": "funding", "amount": 9.99,
                 "created_at": 1728462694999},
            ]

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        self.exchange._fetch_current_prices = AsyncMock(
            return_value={"B-BTC_USDT": {"fr": 0.0001}})

        timestamp, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)

        self.assertEqual(CONSTANTS.TRANSACTIONS_PATH_URL, captured["path"])
        self.assertEqual("funding", captured["data"]["stage"])
        self.assertEqual(["USDT"], captured["data"]["margin_currency_short_name"])
        # Newest BTC record wins; the ETH record must not leak in.
        self.assertEqual(Decimal("0.34"), payment)
        self.assertEqual(1728462694.499, timestamp)
        self.assertEqual(Decimal("0.0001"), rate)

    async def test_fetch_last_fee_payment_sentinel_when_no_history(self):
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value=[])
        timestamp, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        # Contract from the base class: "if no payment exists, return (0, -1, -1)".
        self.assertEqual(0, timestamp)
        self.assertEqual(Decimal("-1"), rate)
        self.assertEqual(Decimal("-1"), payment)

    async def test_fetch_last_fee_payment_survives_a_missing_rate(self):
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "amount": -0.5, "created_at": 1728462694499}])
        self.exchange._fetch_current_prices = AsyncMock(side_effect=IOError("prices down"))
        timestamp, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(Decimal("-0.5"), payment)
        self.assertEqual(Decimal("-1"), rate)

    async def test_fetch_last_fee_payment_uses_the_configured_margin_currency(self):
        ex = self._inr_exchange()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured.update(data)
            return []

        ex._api_post = AsyncMock(side_effect=fake_post)
        await ex._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(["INR"], captured["margin_currency_short_name"])

    # ---- user stream ----------------------------------------------------------

    def test_balance_event_updates_cache(self):
        self.exchange._process_balance_event(
            {"currency_short_name": "USDT", "balance": "10", "locked_balance": "2"})
        self.assertEqual(Decimal("10"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("12"), self.exchange._account_balances["USDT"])
