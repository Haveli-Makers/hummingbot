import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from test.hummingbot.connector.derivative.delta_options.test_delta_options_utils import option_product
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import PerpetualDerivativeInFlightOrder
from hummingbot.core.data_type.option_info import OptionInfo, OptionType
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

CALL = "C_BTC_100000_251226-USD"
PUT = "P_BTC_90000_251226-USD"
LATER = "C_BTC_95000_261226-USD"
EXPIRY = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()
PRODUCTS = [
    option_product("C-BTC-100000-251226", id=11),
    option_product("P-BTC-90000-251226", "put_options", id=12),
    option_product("C-BTC-95000-261226", id=13, settlement_time="2026-12-26T12:00:00Z"),
    option_product("C-BTC-80000-251226", id=14),
]


class DeltaOptionsExpiryTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = self._connector()
        # Held here: connectors keep their listeners weakly.
        self.expiries = EventLogger()
        self.failures = EventLogger()
        self.connector.add_listener(MarketEvent.OptionExpired, self.expiries)
        self.connector.add_listener(MarketEvent.OrderFailure, self.failures)

    @staticmethod
    def _connector(**settings) -> DeltaOptionsDerivative:
        connector = DeltaOptionsDerivative(
            delta_options_api_key="key", delta_options_api_secret="secret",
            trading_pairs=[CALL, PUT, LATER], trading_required=False, **settings)
        connector._initialize_trading_pair_symbols_from_exchange_info(PRODUCTS)
        for pair in (CALL, PUT, LATER):
            connector._trading_rules[pair] = TradingRule(
                trading_pair=pair, min_order_size=Decimal("0.001"), min_price_increment=Decimal("0.1"),
                min_base_amount_increment=Decimal("0.001"), min_notional_size=Decimal("0"),
                buy_order_collateral_token="USD", sell_order_collateral_token="USD")
            connector._options_trading.initialize_option_info(OptionInfo(
                trading_pair=pair, mark_price=Decimal("500"), underlying_price=Decimal("98000"),
                price_band_lower=Decimal("100"), price_band_upper=Decimal("2000")))
        placed = {"success": True, "result": {"id": 9001, "created_at": 1766664000000000}}
        connector._api_post = AsyncMock(return_value=placed)
        return connector

    def _at(self, timestamp: float):
        return patch.object(self.connector, "_time", return_value=timestamp)

    def _track(self, order_id: str, position: PositionAction) -> PerpetualDerivativeInFlightOrder:
        order = PerpetualDerivativeInFlightOrder(
            client_order_id=order_id, exchange_order_id="9001", trading_pair=CALL, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"), price=Decimal("500"), creation_timestamp=EXPIRY - 3600,
            leverage=1, position=position)
        self.connector._order_tracker.start_tracking_order(order)
        return order

    async def _order(self, order_id: str, side: TradeType, action: PositionAction, trading_pair: str = CALL):
        await self.connector._create_order(side, order_id, trading_pair, Decimal("0.001"), OrderType.LIMIT,
                                           Decimal("500"), position_action=action)
        # The order tracker emits failure events from a task of its own.
        for _ in range(5):
            await asyncio.sleep(0)

    # ── Expired options stay known ────────────────────────────────────────────

    async def test_own_options_stay_known_after_delta_stops_listing_them(self):
        still_listed = [p for p in PRODUCTS if p["symbol"] not in ("C-BTC-100000-251226", "C-BTC-80000-251226")]

        self.connector._initialize_trading_pair_symbols_from_exchange_info(still_listed)

        self.assertEqual((11, Decimal("0.001")), (self.connector._product_ids[CALL], self.connector.contract_size(CALL)))
        self.assertEqual("C-BTC-100000-251226", await self.connector.exchange_symbol_associated_to_pair(CALL))
        with self._at(EXPIRY):
            self.assertTrue(self.connector.is_expired(CALL))
        # An option the bot doesn't trade is simply dropped.
        with self.assertRaises(KeyError):
            await self.connector.exchange_symbol_associated_to_pair("C_BTC_80000_251226-USD")

    async def test_trading_rules_of_own_options_are_kept(self):
        still_listed = [p for p in PRODUCTS if p["symbol"] != "C-BTC-100000-251226"]

        rules = await self.connector._format_trading_rules(still_listed)

        self.assertIn(CALL, {rule.trading_pair for rule in rules})

    def test_an_option_never_seen_listed_is_known_from_its_name(self):
        expired = "C_BTC_70000_011126-USD"
        connector = DeltaOptionsDerivative("key", "secret", trading_pairs=[expired], trading_required=False)

        connector._initialize_trading_pair_symbols_from_exchange_info(PRODUCTS)

        instrument = connector.get_instrument(expired)
        self.assertEqual((OptionType.CALL, Decimal("70000"), datetime(2026, 11, 1, 12, tzinfo=timezone.utc).timestamp()),
                         (instrument.option_type, instrument.strike, instrument.expiry_timestamp))
        with patch.object(connector, "_time", return_value=instrument.expiry_timestamp + 1):
            self.assertTrue(connector.is_expired(expired))
        self.assertNotIn(expired, connector._product_ids)

    def test_option_info_readiness_waits_only_for_live_options(self):
        self.connector._options_trading._option_info.clear()
        self.connector._options_trading.initialize_option_info(OptionInfo(
            trading_pair=LATER, mark_price=Decimal("500"), underlying_price=Decimal("98000")))

        with self._at(EXPIRY - 1):
            self.assertFalse(self.connector.status_dict["option_info"])
        with self._at(EXPIRY):
            self.assertTrue(self.connector.status_dict["option_info"])

    async def test_option_info_is_only_fetched_for_live_options(self):
        self.connector._orderbook_ds = MagicMock()
        self.connector._orderbook_ds.get_option_info = AsyncMock(side_effect=lambda pair: OptionInfo(
            trading_pair=pair, mark_price=Decimal("500"), underlying_price=Decimal("98000")))

        with self._at(EXPIRY):
            await self.connector._init_option_info()

        self.assertEqual([LATER], [c.args[0] for c in self.connector._orderbook_ds.get_option_info.call_args_list])

    async def test_orders_on_an_expired_option_are_refused(self):
        with self._at(EXPIRY):
            await self._order("DLTO-X", TradeType.SELL, PositionAction.CLOSE)

        self.connector._api_post.assert_not_awaited()
        self.assertIn("expired at 25 Dec 12:00 UTC", self.failures.event_log[0].error_message)

    # ── Stop before expiry ────────────────────────────────────────────────────

    async def test_opening_orders_stop_30_minutes_before_expiry_and_closing_ones_do_not(self):
        with self._at(EXPIRY - 31 * 60):
            await self._order("DLTO-1", TradeType.BUY, PositionAction.OPEN)
        self.assertEqual(1, self.connector._api_post.await_count)

        with self._at(EXPIRY - 29 * 60):
            await self._order("DLTO-2", TradeType.BUY, PositionAction.OPEN)
            await self._order("DLTO-3", TradeType.SELL, PositionAction.CLOSE)

        self.assertEqual(2, self.connector._api_post.await_count)
        self.assertEqual(["DLTO-2"], [event.order_id for event in self.failures.event_log])
        self.assertIn("last 30 minutes", self.failures.event_log[0].error_message)

    async def test_a_zero_window_lets_positions_open_until_expiry(self):
        self.connector = self._connector(delta_options_stop_before_expiry_minutes=0)

        with self._at(EXPIRY - 1):
            await self._order("DLTO-1", TradeType.BUY, PositionAction.OPEN)

        self.connector._api_post.assert_awaited_once()

    def test_resting_opening_orders_are_cancelled_once_when_the_window_starts(self):
        self._track("DLTO-OPEN", PositionAction.OPEN)
        self._track("DLTO-CLOSE", PositionAction.CLOSE)
        self.connector.cancel = MagicMock()

        with self._at(EXPIRY - 31 * 60):
            self.connector._check_expiries()
        self.connector.cancel.assert_not_called()

        with self._at(EXPIRY - 29 * 60):
            self.connector._check_expiries()
            self.connector._check_expiries()
        self.connector.cancel.assert_called_once_with(CALL, "DLTO-OPEN")

    def test_expiry_is_announced_once_with_the_position_held(self):
        position = Position(trading_pair=CALL, position_side=PositionSide.LONG, unrealized_pnl=Decimal("0"),
                            entry_price=Decimal("500"), amount=Decimal("0.002"), leverage=Decimal("1"))
        self.connector._options_trading.set_position(CALL, position)
        self.connector._on_option_expired = MagicMock()

        with self._at(EXPIRY - 1):
            self.connector._check_expiries()
        self.assertEqual([], self.expiries.event_log)

        with self._at(EXPIRY):
            self.connector._check_expiries()
            self.connector._check_expiries()

        self.assertEqual([CALL, PUT], [event.trading_pair for event in self.expiries.event_log])
        self.assertEqual({(CALL, position), (PUT, None)},
                         {c.args for c in self.connector._on_option_expired.call_args_list})

    async def test_expiry_watch_survives_errors(self):
        self.connector._check_expiries = MagicMock(side_effect=[ValueError("boom"), None])
        self.connector._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with self.assertRaises(asyncio.CancelledError):
            await self.connector._expiry_watch_loop()

        self.assertEqual(2, self.connector._check_expiries.call_count)

    def test_the_window_is_set_in_minutes(self):
        self.assertEqual(30 * 60, self.connector._stop_before_expiry)
        self.assertEqual(5 * 60, self._connector(delta_options_stop_before_expiry_minutes="5")._stop_before_expiry)
