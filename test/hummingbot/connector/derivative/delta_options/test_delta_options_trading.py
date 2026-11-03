import asyncio
from decimal import Decimal
from test.hummingbot.connector.derivative.delta_options.test_delta_options_utils import option_product
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.option_derivative_py_base import OptionDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, PerpetualDerivativeInFlightOrder
from hummingbot.core.data_type.option_info import OptionInfo
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

CALL = "C_BTC_100000_251226-USD"
PUT = "P_BTC_90000_251226-USD"


def ok(result):
    return {"success": True, "result": result}


class DeltaOptionsTradingTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = self._connector()
        self.failures = EventLogger()  # held here: event listeners are weakly referenced
        self.connector.add_listener(MarketEvent.OrderFailure, self.failures)

    def _connector(self, **settings) -> DeltaOptionsDerivative:
        connector = DeltaOptionsDerivative(
            delta_options_api_key="key", delta_options_api_secret="secret",
            trading_pairs=[CALL, PUT], trading_required=False, **settings)
        connector._initialize_trading_pair_symbols_from_exchange_info([
            option_product("C-BTC-100000-251226", id=11),
            option_product("P-BTC-90000-251226", "put_options", id=12),
        ])
        for pair in (CALL, PUT):
            connector._trading_rules[pair] = TradingRule(
                trading_pair=pair, min_order_size=Decimal("0.001"), min_price_increment=Decimal("0.1"),
                min_base_amount_increment=Decimal("0.001"), min_notional_size=Decimal("0"),
                buy_order_collateral_token="USD", sell_order_collateral_token="USD")
            connector._options_trading.initialize_option_info(OptionInfo(
                trading_pair=pair, mark_price=Decimal("500"), underlying_price=Decimal("98000"),
                price_band_lower=Decimal("100"), price_band_upper=Decimal("2000")))
        connector._api_post = AsyncMock(return_value=ok({"id": 9001, "created_at": 1766664000000000}))
        return connector

    def _track(self, connector, order_id="DLTO-1", side=TradeType.BUY, amount="0.002", exchange_order_id="9001"):
        order = PerpetualDerivativeInFlightOrder(
            client_order_id=order_id, exchange_order_id=exchange_order_id, trading_pair=CALL,
            order_type=OrderType.LIMIT, trade_type=side, amount=Decimal(amount), price=Decimal("500"),
            creation_timestamp=1766664000.0, leverage=1, position=PositionAction.OPEN)
        connector._order_tracker.start_tracking_order(order)
        return order

    # ── Order placement, cancel, status ───────────────────────────────────────

    async def test_limit_order_payload(self):
        exchange_order_id, timestamp = await self.connector._place_order(
            "DLTO-1", CALL, Decimal("0.003"), TradeType.BUY, OrderType.LIMIT, Decimal("500.5"), PositionAction.OPEN)

        payload = self.connector._api_post.call_args.kwargs["data"]
        self.assertEqual({"product_id": 11, "size": 3, "side": "buy", "order_type": "limit_order",
                          "limit_price": "500.5", "time_in_force": "gtc", "client_order_id": "DLTO-1",
                          "reduce_only": False}, payload)
        self.assertEqual(("9001", 1766664000.0), (exchange_order_id, timestamp))

    async def test_post_only_and_reduce_only(self):
        await self.connector._place_order(
            "DLTO-2", PUT, Decimal("0.001"), TradeType.SELL, OrderType.LIMIT_MAKER, Decimal("300"), PositionAction.CLOSE)

        payload = self.connector._api_post.call_args.kwargs["data"]
        self.assertTrue(payload["post_only"])
        self.assertTrue(payload["reduce_only"])
        self.assertEqual((12, "sell"), (payload["product_id"], payload["side"]))

    def test_market_orders_are_not_offered(self):
        self.assertEqual([OrderType.LIMIT, OrderType.LIMIT_MAKER], self.connector.supported_order_types())

    async def test_cancel_counts_only_when_delta_says_cancelled(self):
        order = self._track(self.connector)
        self.connector._api_delete = AsyncMock(return_value=ok({"id": 9001, "state": "cancelled"}))
        self.assertTrue(await self.connector._place_cancel(order.client_order_id, order))
        self.assertEqual({"id": 9001, "product_id": 11}, self.connector._api_delete.call_args.kwargs["data"])

        self.connector._api_delete = AsyncMock(return_value=ok({"id": 9001, "state": "closed"}))
        self.assertFalse(await self.connector._place_cancel(order.client_order_id, order))

    async def test_cancel_waits_for_an_order_still_being_placed(self):
        order = self._track(self.connector, exchange_order_id=None)
        self.connector._api_delete = AsyncMock(return_value=ok({"id": 9002, "state": "cancelled"}))

        cancel = asyncio.ensure_future(self.connector._place_cancel(order.client_order_id, order))
        await asyncio.sleep(0)
        self.connector._api_delete.assert_not_awaited()
        order.update_exchange_order_id("9002")

        self.assertTrue(await cancel)
        self.assertEqual(9002, self.connector._api_delete.call_args.kwargs["data"]["id"])

    async def test_cancel_of_an_order_delta_already_cancelled_is_confirmed(self):
        # The dead-man switch cancelled it while the bot wasn't listening; the bot's own cancel then finds nothing.
        order = self._track(self.connector)
        self.connector._api_delete = AsyncMock(side_effect=IOError(
            'HTTP status is 400. Error: {"error":{"code":"open_order_not_found"},"success":false}'))
        self.connector._api_get = AsyncMock(return_value=ok({"id": 9001, "state": "cancelled", "size": 2,
                                                             "unfilled_size": 2}))

        self.assertTrue(await self.connector._place_cancel(order.client_order_id, order))

    async def test_cancel_of_an_order_that_filled_records_the_fill_instead(self):
        order = self._track(self.connector)
        self.connector._api_delete = AsyncMock(side_effect=IOError("open_order_not_found"))
        fill = {"id": "f1", "order_id": "9001", "size": "2", "price": "505", "commission": "0.0092"}

        async def api_get(path_url, **kwargs):
            if path_url == CONSTANTS.FILLS_PATH_URL:
                return {"success": True, "meta": {"after": None}, "result": [fill]}
            return ok({"id": 9001, "state": "closed", "size": 2, "unfilled_size": 0})

        self.connector._api_get = api_get

        self.assertFalse(await self.connector._place_cancel(order.client_order_id, order))
        await self._settle()

        self.assertIn("f1", order.order_fills)
        self.assertEqual(OrderState.FILLED, order.current_state)

    async def test_other_cancel_errors_are_raised(self):
        order = self._track(self.connector)
        self.connector._api_delete = AsyncMock(side_effect=IOError("HTTP status is 401. Error: unauthorized"))

        with self.assertRaises(IOError):
            await self.connector._place_cancel(order.client_order_id, order)

    def test_resync_after_a_stream_gap_polls_at_once(self):
        self.connector.resync_after_stream_gap()

        self.assertTrue(self.connector._poll_notifier.is_set())

    async def test_order_status_maps_partial_fills(self):
        order = self._track(self.connector)
        self.connector._api_get = AsyncMock(return_value=ok({"id": 9001, "state": "open", "size": 2, "unfilled_size": 1}))
        self.assertEqual(OrderState.PARTIALLY_FILLED, (await self.connector._request_order_status(order)).new_state)

        self.connector._api_get = AsyncMock(return_value=ok({"id": 9001, "state": "closed", "size": 2, "unfilled_size": 0}))
        self.assertEqual(OrderState.FILLED, (await self.connector._request_order_status(order)).new_state)

    def test_terminal_cancel_error_is_not_found(self):
        error = IOError('HTTP status is 400. Error: {"error":{"code":"open_order_not_found"},"success":false}')
        self.assertTrue(self.connector._is_order_not_found_during_cancelation_error(error))
        self.assertFalse(self.connector._is_order_not_found_during_cancelation_error(IOError("insufficient_margin")))

    # ── Fills ─────────────────────────────────────────────────────────────────

    async def test_fills_come_from_rest_with_their_commission(self):
        order = self._track(self.connector)
        self.connector._api_get = AsyncMock(return_value={"success": True, "meta": {"after": None}, "result": [
            {"id": "f1", "order_id": "9001", "size": "2", "price": "505", "commission": "0.0092",
             "settling_asset_symbol": "USD", "created_at": "2026-12-25T00:00:01Z"},
            {"id": "f2", "order_id": "7777", "size": "5", "price": "505", "commission": "1"},
        ]})

        updates = await self.connector._all_trade_updates_for_order(order)

        self.assertEqual(["f1"], [u.trade_id for u in updates])
        self.assertEqual((Decimal("0.002"), Decimal("1.010")), (updates[0].fill_base_amount, updates[0].fill_quote_amount))
        self.assertEqual([("USD", Decimal("0.0092"))], [(f.token, f.amount) for f in updates[0].fee.flat_fees])
        params = self.connector._api_get.call_args.kwargs["params"]
        self.assertEqual(("11", int((1766664000.0 - 60) * 1e6)), (params["product_ids"], params["start_time"]))

    async def test_stream_fill_is_read_from_rest_until_it_is_recorded(self):
        order = self._track(self.connector)
        fill = {"id": "f1", "order_id": "9001", "size": "2", "price": "505", "commission": "0.0092"}
        self.connector._api_get = AsyncMock(side_effect=[ok([]), ok([fill])])
        self.connector._sleep = AsyncMock()

        self.connector._process_trade_event({"type": "v2/user_trades", "c": "DLTO-1", "o": 9001, "s": 2, "p": "505"})
        for _ in range(20):
            await asyncio.sleep(0)

        self.assertEqual(2, self.connector._api_get.await_count)
        self.assertIn("f1", order.order_fills)

    def test_order_stream_update_uses_order_id(self):
        self._track(self.connector, order_id="DLTO-7", exchange_order_id="9007")
        updates = []
        self.connector._order_tracker.process_order_update = updates.append

        self.connector._process_order_event({"type": "orders", "order_id": 9007, "client_order_id": "",
                                             "state": "cancelled", "size": 2, "unfilled_size": 2})

        self.assertEqual((OrderState.CANCELED, "DLTO-7", "9007"),
                         (updates[0].new_state, updates[0].client_order_id, updates[0].exchange_order_id))

    # ── Selling guard and price band ──────────────────────────────────────────

    async def _sell(self, connector, amount="0.001", action=PositionAction.OPEN, order_id="DLTO-S"):
        await connector._create_order(TradeType.SELL, order_id, CALL, Decimal(amount), OrderType.LIMIT,
                                      Decimal("500"), position_action=action)
        await self._settle()

    @staticmethod
    async def _settle():
        # The order tracker emits failure events from a task of its own.
        for _ in range(5):
            await asyncio.sleep(0)

    async def test_selling_to_open_is_refused_unless_enabled(self):
        await self._sell(self.connector)

        self.connector._api_post.assert_not_awaited()
        self.assertEqual(1, len(self.failures.event_log))

    async def test_selling_a_held_long_needs_no_permission(self):
        self.connector._options_trading.set_position(CALL, self.connector._position(CALL, Decimal("2"), Decimal("500"),
                                                                                    Decimal("0")))
        await self._sell(self.connector)

        self.connector._api_post.assert_awaited_once()

    async def test_closing_is_always_allowed(self):
        await self._sell(self.connector, action=PositionAction.CLOSE)

        self.connector._api_post.assert_awaited_once()

    async def test_short_limit_counts_open_sell_orders(self):
        connector = self._connector(delta_options_allow_selling="yes", delta_options_max_short_contracts=2)
        self._track(connector, order_id="DLTO-OPEN-SELL", side=TradeType.SELL, amount="0.002")
        failures = EventLogger()
        connector.add_listener(MarketEvent.OrderFailure, failures)

        await self._sell(connector, amount="0.001")

        connector._api_post.assert_not_awaited()
        self.assertEqual(1, len(failures.event_log))

    async def test_selling_within_the_limit_is_sent(self):
        connector = self._connector(delta_options_allow_selling=True, delta_options_max_short_contracts=2)

        await self._sell(connector, amount="0.002")

        connector._api_post.assert_awaited_once()

    def test_a_no_answer_keeps_selling_off(self):
        connector = self._connector(delta_options_allow_selling="no")

        self.assertFalse(connector._allow_selling)

    async def test_price_outside_the_band_is_refused(self):
        await self.connector._create_order(TradeType.BUY, "DLTO-B", CALL, Decimal("0.001"), OrderType.LIMIT,
                                           Decimal("2500"), position_action=PositionAction.OPEN)
        await self._settle()

        self.connector._api_post.assert_not_awaited()
        self.assertEqual(1, len(self.failures.event_log))

    async def test_band_is_checked_on_the_price_that_would_be_sent(self):
        self._set_band(CALL, "100.05", "2000")

        # 100.06 rounds down to the 0.1 tick, 100.0, which is below the band.
        await self.connector._create_order(TradeType.SELL, "DLTO-T", CALL, Decimal("0.001"), OrderType.LIMIT,
                                           Decimal("100.06"), position_action=PositionAction.CLOSE)
        await self._settle()

        self.connector._api_post.assert_not_awaited()
        self.assertIn("price band", self.failures.event_log[0].error_message)

    async def test_market_order_is_refused_without_an_error(self):
        await self.connector._create_order(TradeType.BUY, "DLTO-M", CALL, Decimal("0.001"), OrderType.MARKET,
                                           Decimal("NaN"), position_action=PositionAction.OPEN)
        await self._settle()

        self.connector._api_post.assert_not_awaited()
        self.assertIn("MARKET orders are not supported", self.failures.event_log[0].error_message)

    def _set_band(self, trading_pair, lower, upper):
        self.connector._options_trading.initialize_option_info(OptionInfo(
            trading_pair=trading_pair, mark_price=Decimal("500"), underlying_price=Decimal("98000"),
            price_band_lower=Decimal(lower), price_band_upper=Decimal(upper)))

    # ── Margin estimate and budget checks ─────────────────────────────────────

    def test_short_margin_estimate(self):
        # Call 100,000 with the underlying at 98,000: 2,000 out of the money.
        rate = max(Decimal("0.15") - Decimal("2000") / Decimal("98000"), Decimal("0.10"))

        margin = self.connector.estimate_short_margin(CALL, Decimal("0.001"), Decimal("500"))

        self.assertEqual((rate * Decimal("98000") + Decimal("500")) * Decimal("0.001"), margin)

    def test_far_out_of_the_money_margin_has_a_floor(self):
        margin = self.connector.estimate_short_margin(PUT, Decimal("0.001"), Decimal("10"))

        self.assertEqual((Decimal("0.10") * Decimal("98000") + Decimal("10")) * Decimal("0.001"), margin)

    def test_budget_checker_locks_margin_for_sells_and_nothing_for_closes(self):
        self.connector._account_balances["USD"] = self.connector._account_available_balances["USD"] = Decimal("20")
        checker = self.connector.budget_checker

        def candidate(side, amount, close=False):
            return PerpetualOrderCandidate(trading_pair=CALL, is_maker=True, order_type=OrderType.LIMIT,
                                           order_side=side, amount=Decimal(amount), price=Decimal("500"),
                                           leverage=Decimal("1"), position_close=close)

        sell = checker.adjust_candidate(candidate(TradeType.SELL, "0.001"))
        too_big = checker.adjust_candidate(candidate(TradeType.SELL, "0.002"))
        buy = checker.adjust_candidate(candidate(TradeType.BUY, "0.001"))
        close = checker.adjust_candidate(candidate(TradeType.SELL, "0.001", close=True))

        self.assertEqual(self.connector.estimate_short_margin(CALL, Decimal("0.001"), Decimal("500")),
                         sell.order_collateral.amount)
        self.assertEqual(Decimal("0"), too_big.amount)
        self.assertEqual(Decimal("0.5"), buy.order_collateral.amount)
        self.assertIsNone(close.order_collateral)

    # ── Dead-man switch ───────────────────────────────────────────────────────

    async def test_heartbeat_is_registered_for_the_bots_options(self):
        heartbeat_id = await self.connector._register_heartbeat()

        create = self.connector._api_post.call_args.kwargs
        self.assertEqual(CONSTANTS.HEARTBEAT_CREATE_PATH_URL, create["path_url"])
        self.assertEqual({"heartbeat_id": heartbeat_id, "impact": "products",
                          "product_symbols": ["C-BTC-100000-251226", "P-BTC-90000-251226"],
                          "default_ttl": CONSTANTS.HEARTBEAT_TTL_MS,
                          "config": [{"action": "cancel_orders", "unhealthy_count": 1}]}, create["data"])

    async def test_existing_heartbeat_is_reused_after_a_restart(self):
        restarted = self._connector()
        restarted._api_post = AsyncMock(side_effect=[IOError("HTTP status is 400. Error: heartbeat exists"),
                                                     ok({"process_enabled": "true"})])

        first, second = await self.connector._register_heartbeat(), await restarted._register_heartbeat()

        self.assertEqual(first, second)
        ack = restarted._api_post.call_args.kwargs
        self.assertEqual((CONSTANTS.HEARTBEAT_PATH_URL, {"heartbeat_id": second, "ttl": CONSTANTS.HEARTBEAT_TTL_MS}),
                         (ack["path_url"], ack["data"]))

    async def test_heartbeat_that_can_be_neither_created_nor_acknowledged_is_retried(self):
        self.connector._api_post = AsyncMock(side_effect=[
            IOError("invalid product symbols"), IOError("heartbeat not found"),  # first attempt
            ok({}), ok({}),  # second attempt: created, then acknowledged
        ])
        self.connector._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with self.assertLogs(self.connector.logger(), level=1) as logs, self.assertRaises(asyncio.CancelledError):
            await self.connector._heartbeat_loop()

        self.assertIn("invalid product symbols", "".join(logs.output))
        self.assertIsNotNone(self.connector._heartbeat_id)
        self.assertEqual(4, self.connector._api_post.await_count)

    async def test_heartbeat_loop_acks_and_survives_errors(self):
        self.connector._heartbeat_id = "hb1"
        self.connector._api_post = AsyncMock(side_effect=[IOError("timeout"), ok({})])
        self.connector._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with self.assertRaises(asyncio.CancelledError):
            await self.connector._heartbeat_loop()

        self.assertEqual(2, self.connector._api_post.await_count)
        self.assertEqual({"heartbeat_id": "hb1", "ttl": CONSTANTS.HEARTBEAT_TTL_MS},
                         self.connector._api_post.call_args.kwargs["data"])

    async def test_stop_switches_the_heartbeat_off(self):
        self.connector._heartbeat_task = asyncio.ensure_future(asyncio.sleep(10))
        self.connector._heartbeat_id = "hb1"

        await self.connector._stop_heartbeat()

        self.assertEqual({"heartbeat_id": "hb1", "ttl": 0}, self.connector._api_post.call_args.kwargs["data"])
        self.assertIsNone(self.connector._heartbeat_task)

    async def test_a_second_start_does_not_add_a_heartbeat_loop(self):
        connector = self._connector(delta_options_dead_man_switch=True)
        connector._trading_required = True
        running = asyncio.ensure_future(asyncio.sleep(10))
        connector._heartbeat_task = running

        with patch.object(OptionDerivativePyBase, "start_network", new=AsyncMock()):
            await connector.start_network()

        self.assertIs(running, connector._heartbeat_task)
        running.cancel()

    async def test_overlapping_stops_let_the_heartbeat_switch_off_finish(self):
        # Stopping the clock runs stop_network in the background; a second stop must not close the proxy session
        # while the first one is still switching the heartbeat off.
        connector = self.connector
        connector._proxy_url = "http://proxy:3128"
        connector._heartbeat_task = asyncio.ensure_future(asyncio.sleep(10))
        connector._heartbeat_id = "hb1"
        steps = []

        async def switch_off(**kwargs):
            await asyncio.sleep(0.01)
            steps.append("switched off")
            return ok({})

        async def close():
            steps.append("session closed")

        connector._api_post = switch_off
        connector._web_assistants_factory.close = close

        await asyncio.gather(connector.stop_network(), connector.stop_network())

        self.assertEqual(["switched off", "session closed", "session closed"], steps)

    async def test_failed_switch_off_is_logged_with_its_reason(self):
        self.connector._heartbeat_task = asyncio.ensure_future(asyncio.sleep(10))
        self.connector._heartbeat_id = "hb1"
        self.connector._api_post = AsyncMock(side_effect=IOError("proxy refused the connection"))

        with self.assertLogs(self.connector.logger(), level="WARNING") as logs:
            await self.connector._stop_heartbeat()

        self.assertIn("proxy refused the connection", logs.output[0])

    # ── Flatten ───────────────────────────────────────────────────────────────

    async def test_flatten_cancels_then_closes_every_position(self):
        connector = self.connector
        connector.cancel_all = AsyncMock(return_value=[])
        connector.buy, connector.sell = MagicMock(return_value="B"), MagicMock(return_value="S")
        connector._options_trading.set_position(CALL, connector._position(CALL, Decimal("2"), Decimal("500"), Decimal("0")))
        connector._options_trading.set_position(PUT, connector._position(PUT, Decimal("-1"), Decimal("500"), Decimal("0")))

        order_ids = await connector.flatten(slippage=Decimal("0.05"))

        connector.cancel_all.assert_awaited_once()
        self.assertEqual(["S", "B"], order_ids)
        # Empty books: the mark price is the reference, crossed by 5% and kept inside the band.
        connector.sell.assert_called_once_with(CALL, Decimal("0.002"), OrderType.LIMIT, Decimal("475.00"),
                                               position_action=PositionAction.CLOSE)
        connector.buy.assert_called_once_with(PUT, Decimal("0.001"), OrderType.LIMIT, Decimal("525.00"),
                                              position_action=PositionAction.CLOSE)

    async def test_flatten_leaves_other_and_expired_options_alone(self):
        connector = self.connector
        connector.cancel_all = AsyncMock(return_value=[])
        connector.buy, connector.sell = MagicMock(return_value="B"), MagicMock(return_value="S")
        connector.is_expired = lambda trading_pair: trading_pair == PUT
        # A long on an expired option settles by itself; one on an option this connector doesn't trade isn't its own.
        for pair, amount in ((CALL, "-0.001"), (PUT, "0.002"), ("C_BTC_95000_261226-USD", "0.003")):
            connector._options_trading.set_position(pair, Position(
                trading_pair=pair, position_side=PositionSide.LONG if Decimal(amount) > 0 else PositionSide.SHORT,
                unrealized_pnl=Decimal("0"), entry_price=Decimal("500"), amount=Decimal(amount), leverage=Decimal("1")))

        order_ids = await connector.flatten()

        self.assertEqual(["B"], order_ids)
        connector.sell.assert_not_called()
        connector.buy.assert_called_once_with(CALL, Decimal("0.001"), OrderType.LIMIT, Decimal("525.0"),
                                              position_action=PositionAction.CLOSE)

    def test_crossing_price_stays_inside_the_band(self):
        self.assertEqual(Decimal("2000"), self.connector._crossing_price(CALL, is_buy=True, slippage=Decimal("10")))
        self.assertEqual(Decimal("100"), self.connector._crossing_price(CALL, is_buy=False, slippage=Decimal("0.9")))

    def test_crossing_price_is_on_the_tick_grid_inside_the_band(self):
        self._set_band(CALL, "100.05", "1999.95")

        self.assertEqual(Decimal("1999.9"), self.connector._crossing_price(CALL, is_buy=True, slippage=Decimal("10")))
        self.assertEqual(Decimal("100.1"), self.connector._crossing_price(CALL, is_buy=False, slippage=Decimal("0.9")))
        # Away from the band, a buy rounds up and a sell rounds down, so they still cross the book.
        self.assertEqual(Decimal("525.1"), self.connector._crossing_price(CALL, is_buy=True, slippage=Decimal("0.0501")))
        self.assertEqual(Decimal("474.9"), self.connector._crossing_price(CALL, is_buy=False, slippage=Decimal("0.0501")))
