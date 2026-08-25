import asyncio
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import CoindcxPerpetualDerivative
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import BuyOrderCreatedEvent, MarketEvent, OrderFilledEvent

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

    async def _captured_order(self, position_action):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["data"] = data
            return [{"id": "uuid-1", "created_at": 1700000000000}]

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        await self.exchange._place_order(
            order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.SELL, order_type=OrderType.LIMIT, price=Decimal("60000"),
            position_action=position_action)
        return captured["data"]["order"]

    async def test_closing_orders_are_sent_reduce_only(self):
        """
        Without reduce_only the venue treats a close as a new opposite position and asks for
        margin the wallet does not have, so closing fails with "Insufficient funds" precisely
        when the position is large relative to the account.
        """
        order = await self._captured_order(PositionAction.CLOSE)
        self.assertIs(True, order[CONSTANTS.REDUCE_ONLY_FIELD])

    async def test_opening_orders_are_not_reduce_only(self):
        for action in (PositionAction.OPEN, PositionAction.NIL):
            order = await self._captured_order(action)
            self.assertNotIn(CONSTANTS.REDUCE_ONLY_FIELD, order)

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

    # ---- pagination (list endpoints are account-wide, no pair filter) ----------

    @staticmethod
    def _pages(*pages):
        """Serve successive pages to _api_post, keyed on the requested page."""
        async def _post(path_url, data, is_auth_required):
            index = int(data.get("page", 1)) - 1
            return pages[index] if index < len(pages) else []
        return _post

    async def test_order_lookup_pages_past_the_first_page(self):
        """A resting order beyond page 1 must not be reported as missing —
        the caller turns 'not found' into an order failure."""
        self._bootstrap()
        page1 = [{"id": f"other-{i}", "status": "filled"} for i in range(CONSTANTS.PAGE_SIZE)]
        page2 = [{"id": "ex-1", "status": "open", "updated_at": 1700000001000}]
        self.exchange._api_post = AsyncMock(side_effect=self._pages(page1, page2))

        found = await self.exchange._fetch_order_by_id(self._order())
        self.assertIsNotNone(found, "order on page 2 was not found")
        self.assertEqual("ex-1", found["id"])

    async def test_order_lookup_stops_on_a_short_page(self):
        self._bootstrap()
        calls = []

        async def _post(path_url, data, is_auth_required):
            calls.append(data["page"])
            return [{"id": "other", "status": "filled"}]  # short page -> last

        self.exchange._api_post = AsyncMock(side_effect=_post)
        self.assertIsNone(await self.exchange._fetch_order_by_id(self._order()))
        self.assertEqual(["1"], calls, "should not keep paging past a short page")

    async def test_positions_are_paged(self):
        self._bootstrap()
        page1 = [{"pair": f"B-X{i}_USDT", "active_pos": 0} for i in range(CONSTANTS.PAGE_SIZE)]
        page2 = [{"pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
                  "mark_price": 61000.0, "leverage": 10.0}]
        self.exchange._api_post = AsyncMock(side_effect=self._pages(page1, page2))

        await self.exchange._update_positions()
        self.assertIn(self.trading_pair, self.exchange.account_positions,
                      "position on page 2 was missed")

    async def test_funding_ledger_is_paged(self):
        self._bootstrap()
        page1 = [{"pair": "B-ETH_USDT", "amount": 1.0, "created_at": 1}
                 for _ in range(CONSTANTS.PAGE_SIZE)]
        page2 = [{"pair": "B-BTC_USDT", "amount": -0.25, "created_at": 1728462694499}]
        self.exchange._api_post = AsyncMock(side_effect=self._pages(page1, page2))
        self.exchange._fetch_current_prices = AsyncMock(return_value={"B-BTC_USDT": {"fr": 0.0001}})

        timestamp, rate, payment = await self.exchange._fetch_last_fee_payment(self.trading_pair)
        self.assertEqual(Decimal("-0.25"), payment, "funding entry on page 2 was missed")
        self.assertEqual(1728462694.499, timestamp)

    async def test_paging_is_capped(self):
        self._bootstrap()
        calls = []

        async def _post(path_url, data, is_auth_required):
            calls.append(data["page"])
            return [{"id": "x"} for _ in range(CONSTANTS.PAGE_SIZE)]  # always full

        self.exchange._api_post = AsyncMock(side_effect=_post)
        await self.exchange._fetch_order_by_id(self._order())
        self.assertEqual(CONSTANTS.MAX_PAGES, len(calls), "paging must be bounded")

    # ---- start-up fan-out -----------------------------------------------------

    async def test_symbol_map_request_makes_a_single_call(self):
        """TradingPairFetcher builds this connector with no trading pairs at every
        startup; the symbol map must not fan out to one request per instrument."""
        calls = []

        async def _execute(url, method, throttler_limit_id, **kwargs):
            calls.append(url)
            return ["B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT"]

        assistant = AsyncMock()
        assistant.execute_request = AsyncMock(side_effect=_execute)
        self.exchange._web_assistants_factory.get_rest_assistant = AsyncMock(return_value=assistant)

        instruments = await self.exchange._make_trading_pairs_request()
        self.assertEqual(1, len(calls), "symbol map must cost exactly one request")
        self.assertEqual(3, len(instruments))

        self.exchange._initialize_trading_pair_symbols_from_exchange_info(instruments)
        self.assertEqual(
            "BTC-USDT",
            await self.exchange.trading_pair_associated_to_exchange_symbol(symbol="B-BTC_USDT"))

    async def test_instrument_detail_fetches_are_concurrency_capped(self):
        in_flight = 0
        peak = 0

        async def _fetch(pair):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            in_flight -= 1
            return {**INSTRUMENT, "pair": pair}

        names = [f"B-X{i}_USDT" for i in range(60)]
        assistant = AsyncMock()
        assistant.execute_request = AsyncMock(return_value=names)
        self.exchange._web_assistants_factory.get_rest_assistant = AsyncMock(return_value=assistant)
        self.exchange._trading_pairs = []
        self.exchange._fetch_instrument = _fetch

        await self.exchange._fetch_instruments()
        self.assertLessEqual(peak, CONSTANTS.INSTRUMENT_FETCH_CONCURRENCY,
                             f"fan-out peaked at {peak} concurrent requests")

    # ---- error classification / cancel safety ---------------------------------

    def test_generic_422_is_not_treated_as_order_gone(self):
        """422 is a generic validation status on CoinDCX. Misreading it as
        'already cancelled' drops a still-live order from tracking."""
        for message in ("HTTP status is 422. Error: invalid margin_currency_short_name",
                        "HTTP status is 422. Error: permission denied",
                        "HTTP status is 422. Error: malformed parameter"):
            self.assertFalse(
                self.exchange._is_order_not_found_during_cancelation_error(IOError(message)),
                f"must not classify as order-gone: {message}")

    def test_422_with_an_order_gone_message_is_recognised(self):
        for message in ("HTTP status is 422. Error: Order not found",
                        "HTTP status is 422. Error: order already cancelled",
                        "HTTP status is 404. Error: whatever"):
            self.assertTrue(
                self.exchange._is_order_not_found_during_cancelation_error(IOError(message)),
                f"should classify as order-gone: {message}")

    async def test_cancel_rejects_a_non_dict_response(self):
        # A list-shaped error body must not read as a successful cancel.
        self.exchange._api_post = AsyncMock(return_value=[{"message": "invalid request"}])
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("haveli-1", self._order())

    async def test_cancel_rejects_a_dict_without_success(self):
        self.exchange._api_post = AsyncMock(return_value={"message": "something else", "code": 422})
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("haveli-1", self._order())

    # ---- missing price data ---------------------------------------------------

    async def test_missing_price_raises_instead_of_returning_zero(self):
        self._bootstrap()
        self.exchange._fetch_current_prices = AsyncMock(return_value={})
        with self.assertRaises(ValueError):
            await self.exchange._get_last_traded_price(self.trading_pair)
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

    async def test_funding_info_raises_when_prices_are_zero(self):
        self._bootstrap()
        self.exchange._fetch_current_prices = AsyncMock(
            return_value={"B-BTC_USDT": {"mp": 0, "ls": 0, "fr": 0.0001}})
        with self.assertRaises(ValueError):
            await self.exchange.build_funding_info(self.trading_pair)

    # ---- websocket balance frames ---------------------------------------------

    def test_ws_balance_frame_is_converted_like_the_rest_poll(self):
        """A raw write would add a bogus INR row and leave the USDT figure —
        the one the budget checker reads — stale."""
        inr = self._inr_exchange()
        inr._conversion_rate = Decimal("100.0")
        inr._process_balance_event(
            {"currency_short_name": "INR", "balance": "500.0", "locked_balance": "100.0"})

        self.assertNotIn("INR", inr._account_balances)
        self.assertEqual(Decimal("5"), inr._account_available_balances["USDT"])
        self.assertEqual(Decimal("6"), inr._account_balances["USDT"])

    def test_ws_balance_frame_for_another_wallet_is_ignored(self):
        inr = self._inr_exchange()
        inr._conversion_rate = Decimal("100.0")
        inr._process_balance_event({"currency_short_name": "USDT", "balance": "7.0"})
        self.assertNotIn("USDT", inr._account_balances)

    def test_ws_balance_frame_skipped_without_a_cached_rate(self):
        inr = self._inr_exchange()
        inr._account_available_balances["USDT"] = Decimal("9.5")
        inr._conversion_rate = None
        inr._process_balance_event({"currency_short_name": "INR", "balance": "500.0"})
        self.assertEqual(Decimal("9.5"), inr._account_available_balances["USDT"])

    def test_usdt_margin_ws_balance_frame_applies_directly(self):
        self.exchange._process_balance_event(
            {"currency_short_name": "USDT", "balance": "10.0", "locked_balance": "2.0"})
        self.assertEqual(Decimal("10"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("12"), self.exchange._account_balances["USDT"])

    # ---- wire format (shapes confirmed against the live API) --------------------
    #
    # CoinDCX rejects the wrong type outright: a bare string for
    # margin_currency_short_name 500s on every filter endpoint, and a list for
    # `status` 422s. These pin the shapes so a tidy-up refactor cannot undo them.

    async def _capture_post(self, coroutine, response=None):
        """Run a coroutine and return the payloads it sent to _api_post."""
        sent = []

        async def _post(path_url, data, is_auth_required=False, **kwargs):
            sent.append(data)
            return response if response is not None else []

        self.exchange._api_post = AsyncMock(side_effect=_post)
        await coroutine()
        return sent

    async def test_filter_endpoints_send_margin_currency_as_a_list(self):
        self._bootstrap()

        sent = await self._capture_post(lambda: self.exchange._update_positions())
        self.assertEqual([self.exchange.margin_currency],
                         sent[0]["margin_currency_short_name"],
                         "positions requires a list; a string returns HTTP 500")

        sent = await self._capture_post(
            lambda: self.exchange._fetch_order_by_id(self._order()))
        self.assertEqual([self.exchange.margin_currency],
                         sent[0]["margin_currency_short_name"],
                         "list orders requires a list; a string returns HTTP 500")

        self.exchange._fetch_current_prices = AsyncMock(return_value={})
        sent = await self._capture_post(
            lambda: self.exchange._fetch_last_fee_payment(self.trading_pair))
        self.assertEqual([self.exchange.margin_currency],
                         sent[0]["margin_currency_short_name"],
                         "transactions requires a list; a string returns HTTP 500")

    async def test_list_orders_sends_status_as_a_comma_string(self):
        self._bootstrap()
        sent = await self._capture_post(
            lambda: self.exchange._fetch_order_by_id(self._order()))
        status = sent[0]["status"]
        self.assertIsInstance(status, str, "a list of statuses returns HTTP 422")
        self.assertIn(",", status)

    async def test_action_endpoints_send_margin_currency_as_a_string(self):
        self._bootstrap()

        sent = await self._capture_post(
            lambda: self.exchange._set_trading_pair_leverage(self.trading_pair, 10),
            response={"message": "success"})
        self.assertEqual(self.exchange.margin_currency, sent[0]["margin_currency_short_name"])

        sent = await self._capture_post(
            lambda: self.exchange._place_order(
                order_id="haveli-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("60000"),
                position_action=PositionAction.OPEN),
            response=[{"id": "ex-1", "created_at": 1700000000000}])
        self.assertEqual(self.exchange.margin_currency,
                         sent[0]["order"]["margin_currency_short_name"])

    # ---- order events racing _place_order --------------------------------------

    def _track(self, client_order_id="haveli-1", exchange_order_id=None):
        """Track an order the way _create_order does: exchange id not yet known."""
        self.exchange.start_tracking_order(
            order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("60000"),
            amount=Decimal("0.01"),
            order_type=OrderType.LIMIT,
            leverage=10,
            position=PositionAction.OPEN,
        )
        return self.exchange._order_tracker.active_orders[client_order_id]

    @staticmethod
    def _fill_event(exchange_order_id="ex-1", status="filled"):
        return {
            "id": exchange_order_id,
            "status": status,
            "updated_at": 1700000001000,
            "trades": [{
                "fill_id": "f-1", "id": exchange_order_id, "pair": "B-BTC_USDT",
                "price": "60000", "quantity": "0.01", "fee_amount": "0.35",
                "timestamp": 1700000001000,
            }],
        }

    async def test_fill_arriving_before_the_exchange_id_is_replayed(self):
        """A market order can fill before the REST create response lands. The
        event must be held and replayed, not dropped to the slow REST poll."""
        self._bootstrap()
        order = self._track()  # exchange_order_id is still None
        self.exchange._process_order_event(self._fill_event())

        self.assertEqual(1, len(self.exchange._pending_order_events), "event was not deferred")
        self.assertEqual(Decimal("0"), order.executed_amount_base)

        # _place_order returns and the tracker records the id.
        order.update_exchange_order_id("ex-1")
        await self.exchange._replay_pending_order_events()

        self.assertEqual(0, len(self.exchange._pending_order_events))
        self.assertEqual(Decimal("0.01"), order.executed_amount_base,
                         "replayed fill was not applied")

    async def test_deferred_events_replay_in_arrival_order(self):
        self._bootstrap()
        order = self._track()
        self.exchange._process_order_event(
            {"id": "ex-1", "status": "open", "updated_at": 1700000000000})
        self.exchange._process_order_event(self._fill_event())
        self.assertEqual(2, len(self.exchange._pending_order_events))

        order.update_exchange_order_id("ex-1")
        await self.exchange._replay_pending_order_events()
        self.assertEqual(Decimal("0.01"), order.executed_amount_base)

    async def test_replayed_event_cannot_resurrect_a_settled_order(self):
        """update_with_order_update assigns current_state unconditionally, so a
        stale 'open' replayed over a finished order would reopen it."""
        self._bootstrap()
        order = self._track()
        self.exchange._process_order_event(
            {"id": "ex-1", "status": "open", "updated_at": 1700000000000})

        order.update_exchange_order_id("ex-1")
        order.current_state = OrderState.CANCELED
        await self.exchange._replay_pending_order_events()

        self.assertEqual(OrderState.CANCELED, order.current_state,
                         "a stale replayed event reopened a settled order")

    def test_event_is_not_buffered_when_no_order_awaits_an_exchange_id(self):
        """Someone else's order (or a manual trade) must not accumulate."""
        self._bootstrap()
        self._track(exchange_order_id="ex-known")
        self.exchange._process_order_event(self._fill_event(exchange_order_id="ex-other"))
        self.assertEqual(0, len(self.exchange._pending_order_events))

    async def test_filled_frame_without_trades_fetches_the_fills_before_settling(self):
        """
        A FILLED status with no ``trades`` must not settle the order empty.

        executed_amount_base is only written by trade updates, and the tracker stops
        tracking an order the moment it settles — so settling first loses the size and any
        strategy reading it believes it holds nothing while a real position is open.
        """
        self._bootstrap()
        order = self._track(exchange_order_id="ex-1")
        frame = self._fill_event()
        frame["trades"] = []

        with patch.object(self.exchange, "_all_trade_updates_for_order",
                          new=AsyncMock(return_value=[self.exchange._trade_update_from_fill(
                              {"fill_id": "f-1", "id": "ex-1", "pair": "B-BTC_USDT",
                               "price": "60000", "quantity": "0.01", "fee_amount": "0.35",
                               "timestamp": 1700000001000}, order)])) as fetch:
            self.exchange._process_order_event(frame)
            for _ in range(4):
                await asyncio.sleep(0)

        fetch.assert_awaited_once()
        self.assertEqual(Decimal("0.01"), order.executed_amount_base)

    async def test_repeated_order_frames_are_applied_once(self):
        """
        CoinDCX repeats frames. Applying one twice re-fires the created event, and
        MarketsRecorder then fails on `UNIQUE constraint failed: Order.id`, which aborts
        whatever was queued behind it on that frame.
        """
        self._bootstrap()
        order = self._track(exchange_order_id="ex-1")

        for _ in range(3):
            self.exchange._process_order_event(self._fill_event())
        await asyncio.sleep(0)

        # One fill's worth of size, not three.
        self.assertEqual(Decimal("0.01"), order.executed_amount_base)
        self.assertEqual(1, len(self.exchange._applied_order_events))

    def test_an_order_is_only_announced_as_created_once(self):
        """
        The REST placement response and the websocket frame can both drive an order out of
        PENDING_CREATE and both fire the created event. MarketsRecorder writes a row per
        event, so the second fails on `UNIQUE constraint failed: Order.id` and aborts
        whatever was queued behind it.
        """
        self._bootstrap()
        created = BuyOrderCreatedEvent(
            timestamp=1700000000.0, type=OrderType.MARKET, trading_pair="BTC-USDT",
            amount=Decimal("1"), price=Decimal("60000"), order_id="oid-1",
            creation_timestamp=1700000000.0)

        with patch.object(PerpetualDerivativePyBase, "trigger_event") as forwarded:
            for _ in range(3):
                self.exchange.trigger_event(MarketEvent.BuyOrderCreated, created)

        self.assertEqual(1, forwarded.call_count, "created event escaped more than once")

    def test_other_events_are_never_suppressed(self):
        self._bootstrap()
        filled = OrderFilledEvent(
            timestamp=1700000000.0, order_id="oid-1", trading_pair="BTC-USDT",
            trade_type=TradeType.BUY, order_type=OrderType.MARKET,
            price=Decimal("60000"), amount=Decimal("1"),
            trade_fee=AddedToCostTradeFee(flat_fees=[]))

        with patch.object(PerpetualDerivativePyBase, "trigger_event") as forwarded:
            for _ in range(3):
                self.exchange.trigger_event(MarketEvent.OrderFilled, filled)

        self.assertEqual(3, forwarded.call_count)

    def test_a_frame_that_changes_nothing_is_not_pushed(self):
        """
        The REST placement response and the websocket both report a new order as open.
        Pushing the second one lets both fire the created event, and MarketsRecorder then
        fails its insert on `UNIQUE constraint failed: Order.id`.
        """
        self._bootstrap()
        order = self._track(exchange_order_id="ex-1")
        order.current_state = OrderState.OPEN

        with patch.object(self.exchange._order_tracker, "process_order_update") as push:
            self.exchange._apply_order_event(self._fill_event(status="open"), order)

        push.assert_not_called()

    def test_a_genuine_status_change_is_not_suppressed(self):
        self._bootstrap()
        self._track(exchange_order_id="ex-1")
        self.exchange._process_order_event(self._fill_event(status="open"))
        self.exchange._process_order_event(self._fill_event(status="filled"))
        self.assertEqual(2, len(self.exchange._applied_order_events),
                         "a real transition must still get through")

    def test_matched_events_still_apply_directly(self):
        self._bootstrap()
        order = self._track(exchange_order_id="ex-1")
        self.exchange._process_order_event(self._fill_event())
        self.assertEqual(0, len(self.exchange._pending_order_events),
                         "a matchable event must not be deferred")
        self.assertEqual(Decimal("0.01"), order.executed_amount_base)

    async def test_deferred_events_expire_and_do_not_leak(self):
        self._bootstrap()
        self._track()
        self.exchange._process_order_event(self._fill_event())
        self.assertEqual(1, len(self.exchange._pending_order_events))

        # Backdate past the TTL; the order never gets its id.
        exchange_order_id, payload, _ = self.exchange._pending_order_events[0]
        self.exchange._pending_order_events = [
            (exchange_order_id, payload, time.time() - CONSTANTS.PENDING_ORDER_EVENT_TTL - 1)]
        await self.exchange._replay_pending_order_events()
        self.assertEqual(0, len(self.exchange._pending_order_events), "buffer leaked")

    def test_pending_buffer_is_bounded(self):
        self._bootstrap()
        self._track()
        for index in range(CONSTANTS.MAX_PENDING_ORDER_EVENTS + 25):
            self.exchange._process_order_event(self._fill_event(exchange_order_id=f"ex-{index}"))
        self.assertEqual(CONSTANTS.MAX_PENDING_ORDER_EVENTS,
                         len(self.exchange._pending_order_events))

    async def test_stop_network_clears_the_pending_buffer(self):
        self._bootstrap()
        self._track()
        self.exchange._process_order_event(self._fill_event())
        self.assertEqual(1, len(self.exchange._pending_order_events))

        await asyncio.sleep(0)  # let the replay task actually start
        await self.exchange.stop_network()
        self.assertEqual(0, len(self.exchange._pending_order_events))
        self.assertIsNone(self.exchange._pending_order_events_task)

    # ---- positions closed outside the poll -------------------------------------

    async def test_position_absent_from_the_response_is_closed(self):
        """CoinDCX may omit a closed position rather than return active_pos=0
        (e.g. closed from the web UI), which would strand a ghost position."""
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
             "mark_price": 61000.0, "leverage": 10.0}])
        await self.exchange._update_positions()
        self.assertEqual(1, len(self.exchange.account_positions))

        # Position closed externally; CoinDCX now simply omits it.
        self.exchange._api_post = AsyncMock(return_value=[])
        await self.exchange._update_positions()
        self.assertEqual(0, len(self.exchange.account_positions),
                         "ghost position survived the poll")

    async def test_explicit_zero_size_still_closes_a_position(self):
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
             "mark_price": 61000.0, "leverage": 10.0}])
        await self.exchange._update_positions()

        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "active_pos": 0}])
        await self.exchange._update_positions()
        self.assertEqual(0, len(self.exchange.account_positions))

    async def test_other_pairs_are_not_swept_away(self):
        self._bootstrap()
        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
             "mark_price": 61000.0, "leverage": 10.0},
            {"pair": "B-ETH_USDT", "active_pos": 2.0, "avg_price": 3000.0,
             "mark_price": 3100.0, "leverage": 5.0}])
        await self.exchange._update_positions()
        self.assertEqual(2, len(self.exchange.account_positions))

        # Only ETH closes; BTC must survive.
        self.exchange._api_post = AsyncMock(return_value=[
            {"pair": "B-BTC_USDT", "active_pos": 0.5, "avg_price": 60000.0,
             "mark_price": 61000.0, "leverage": 10.0}])
        await self.exchange._update_positions()
        self.assertEqual(1, len(self.exchange.account_positions))
        self.assertIn(self.trading_pair, self.exchange.account_positions)

    # ---- user stream ----------------------------------------------------------

    def test_balance_event_updates_cache(self):
        self.exchange._process_balance_event(
            {"currency_short_name": "USDT", "balance": "10", "locked_balance": "2"})
        self.assertEqual(Decimal("10"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("12"), self.exchange._account_balances["USDT"])

    # ---- persisted tracking state ---------------------------------------------

    def _tracked(self, client_order_id, exchange_order_id, state):
        order = InFlightOrder(
            client_order_id=client_order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("0.01"),
            price=Decimal("60000"),
            creation_timestamp=1700000000.0,
            exchange_order_id=exchange_order_id,
            initial_state=state,
        )
        return order

    def test_an_order_the_venue_never_accepted_is_not_persisted(self):
        """
        A rejected order gets no exchange order id, and CoinDCX has no client-order-id to look
        it up by instead — so there is nothing to reconcile against, ever. Saving it means the
        next run restores it, polls it until the not-found counter trips, and warns that it
        cannot be cancelled, for as many restarts as it takes someone to notice.
        """
        rejected = self._tracked("haveli-rejected", None, OrderState.FAILED)
        self.exchange._order_tracker._lost_orders[rejected.client_order_id] = rejected

        self.assertNotIn("haveli-rejected", self.exchange.tracking_states)

    def test_a_failure_that_does_have_an_exchange_id_is_kept(self):
        """That one can still be checked against the venue, so it is worth carrying over."""
        failed = self._tracked("haveli-failed", "ex-9", OrderState.FAILED)
        self.exchange._order_tracker._lost_orders[failed.client_order_id] = failed

        self.assertIn("haveli-failed", self.exchange.tracking_states)

    def test_an_order_still_awaiting_its_exchange_id_is_kept(self):
        """
        Dropping these would risk hiding a position: the venue may have accepted the order
        while we missed the reply. Only outright failures are safe to forget.
        """
        pending = self._tracked("haveli-pending", None, OrderState.PENDING_CREATE)
        self.exchange._order_tracker.start_tracking_order(pending)

        self.assertIn("haveli-pending", self.exchange.tracking_states)

    def test_a_live_order_is_still_persisted(self):
        live = self._tracked("haveli-open", "ex-2", OrderState.OPEN)
        self.exchange._order_tracker.start_tracking_order(live)

        self.assertIn("haveli-open", self.exchange.tracking_states)
