import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from test.hummingbot.connector.derivative.delta_options.test_delta_options_utils import option_product
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.derivative.delta_options.delta_options_derivative import DeltaOptionsDerivative
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.option_info import OptionInfo, OptionType

CALL = "C_BTC_100000_251226-USD"
PUT = "P_BTC_90000_251226-USD"
EXPIRY = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()


class DeltaOptionsDerivativeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.connector = DeltaOptionsDerivative(
            delta_options_api_key="key",
            delta_options_api_secret="secret",
            trading_pairs=[CALL, PUT],
            trading_required=False,
        )
        self.products = [
            option_product("C-BTC-100000-251226", maker_commission_rate="0.0001", taker_commission_rate="0.0002",
                           product_specs={"premium_commission_rate": 0.035}),
            option_product("P-BTC-90000-251226", "put_options"),
            option_product("C-BTC-95000-261226", settlement_time="2026-12-26T12:00:00Z"),
            option_product("C-ETH-4000-251226", underlying_asset={"symbol": "ETH"}),
            option_product("C-BTC-80000-241226", settlement_time="2026-12-24T12:00:00Z"),
            option_product("C-BTC-70000-241226", state="expired"),
            {"symbol": "BTCUSD", "contract_type": "perpetual_futures", "state": "live"},
        ]
        self.connector._initialize_trading_pair_symbols_from_exchange_info(self.products)

    def _set_option_info(self, trading_pair: str, underlying_price: str):
        self.connector._options_trading.initialize_option_info(
            OptionInfo(trading_pair=trading_pair, mark_price=Decimal("500"), underlying_price=Decimal(underlying_price)))

    def test_too_many_instruments_are_rejected(self):
        pairs = [f"C_BTC_{strike}_251226-USD" for strike in range(CONSTANTS.WS_MAX_SYMBOLS + 1)]

        with self.assertRaises(ValueError):
            DeltaOptionsDerivative("key", "secret", trading_pairs=pairs, trading_required=False)

    async def test_symbol_map_holds_only_live_vanilla_options(self):
        symbol_map = await self.connector.trading_pair_symbol_map()

        self.assertEqual(CALL, symbol_map["C-BTC-100000-251226"])
        self.assertEqual(PUT, symbol_map["P-BTC-90000-251226"])
        self.assertNotIn("BTCUSD", symbol_map)
        self.assertNotIn("C-BTC-70000-241226", symbol_map)
        self.assertEqual(5, len(symbol_map))
        instrument = self.connector.get_instrument(PUT)
        self.assertEqual((OptionType.PUT, Decimal("90000"), "BTC", EXPIRY),
                         (instrument.option_type, instrument.strike, instrument.underlying, instrument.expiry_timestamp))
        self.assertEqual(Decimal("0.005"), self.connector.size_to_amount(CALL, Decimal("5")))

    async def test_products_are_read_page_by_page(self):
        responses = [
            {"success": True, "result": self.products[:2], "meta": {"after": "cursor-1"}},
            {"success": True, "result": self.products[2:], "meta": {"after": None}},
        ]
        with patch.object(self.connector, "_api_get", new=AsyncMock(side_effect=responses)) as api_get:
            products = await self.connector._make_trading_pairs_request()

        self.assertEqual(self.products, products)
        first, second = (call.kwargs["params"] for call in api_get.call_args_list)
        self.assertEqual({"contract_types": "call_options,put_options", "states": "live", "page_size": 100}, first)
        self.assertEqual("cursor-1", second["after"])

    async def test_trading_rules_use_contract_value_and_tick_size(self):
        rules = {rule.trading_pair: rule for rule in await self.connector._format_trading_rules(self.products)}

        self.assertEqual(5, len(rules))
        rule = rules[CALL]
        self.assertEqual(Decimal("0.001"), rule.min_order_size)
        self.assertEqual(Decimal("0.001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.1"), rule.min_price_increment)
        self.assertEqual("USD", rule.buy_order_collateral_token)

    def test_option_chain_lists_live_options_of_one_underlying_in_order(self):
        with patch.object(self.connector, "_time", return_value=EXPIRY - 100):
            chain = self.connector.get_option_chain("BTC")
            one_expiry = self.connector.get_option_chain("BTC", expiry_timestamp=EXPIRY)

        self.assertEqual([CALL, PUT, "C_BTC_95000_261226-USD"], [i.trading_pair for i in chain])
        self.assertEqual([CALL, PUT], [i.trading_pair for i in one_expiry])

    async def test_expired_and_delisted_pairs_are_left_out_of_subscriptions(self):
        with patch.object(self.connector, "_time", return_value=EXPIRY - 100):
            symbols = await self.connector.exchange_symbols_for_live_pairs(
                [CALL, "C_BTC_80000_241226-USD", "C_BTC_1_010127-USD"])

        self.assertEqual(["C-BTC-100000-251226"], symbols)

    def test_fee_is_charged_on_the_underlying_value(self):
        self._set_option_info(CALL, "98000")

        fee = self.connector.get_fee("C_BTC_100000_251226", "USD", OrderType.LIMIT, TradeType.BUY,
                                     PositionAction.OPEN, Decimal("0.01"), Decimal("1000"), is_maker=False)

        # 0.02% of 98,000 x 0.01 = 0.196, below the cap of 3.5% x 10 = 0.35; plus 18% GST.
        self.assertEqual(Decimal("0.196") * Decimal("1.18"), fee.percent * Decimal("1000") * Decimal("0.01"))

    def test_fee_is_capped_at_a_share_of_the_premium(self):
        self._set_option_info(CALL, "98000")

        fee = self.connector.get_fee("C_BTC_100000_251226", "USD", OrderType.LIMIT_MAKER, TradeType.SELL,
                                     PositionAction.OPEN, Decimal("0.01"), Decimal("50"))

        self.assertEqual(Decimal("0.035") * Decimal("1.18"), fee.percent)

    def test_fee_falls_back_to_the_cap_without_prices(self):
        fee = self.connector.get_fee("P_BTC_90000_251226", "USD", OrderType.LIMIT, TradeType.BUY,
                                     PositionAction.OPEN, Decimal("0.01"))

        self.assertEqual(CONSTANTS.DEFAULT_PREMIUM_CAP_RATE * Decimal("1.18"), fee.percent)

    async def test_positions_follow_delta_and_drop_closed_ones(self):
        self.connector._options_trading.set_position(PUT, self.connector._position(PUT, Decimal("2"), Decimal("1"),
                                                                                   Decimal("0")))
        response = {"success": True, "result": [
            {"product_symbol": "C-BTC-100000-251226", "size": -3, "entry_price": "510", "unrealized_pnl": "-1.2"},
            {"product_symbol": "BTCUSD", "size": 10, "entry_price": "98000"},
        ]}
        with patch.object(self.connector, "_api_get", new=AsyncMock(return_value=response)) as api_get:
            await self.connector._update_positions()

        self.assertEqual({"contract_types": "call_options,put_options"}, api_get.call_args.kwargs["params"])
        self.assertEqual([CALL], list(self.connector.account_positions))
        position = self.connector.get_position(CALL)
        self.assertEqual(PositionSide.SHORT, position.position_side)
        self.assertEqual(Decimal("-0.003"), position.amount)
        self.assertEqual(Decimal("510"), position.entry_price)
        self.assertEqual(Decimal("-1.2"), position.unrealized_pnl)

    async def test_position_stream_updates_and_deletes(self):
        await self.connector._process_position_event({
            "type": "positions", "action": "snapshot",
            "result": [{"product_symbol": "C-BTC-100000-251226", "size": 4, "entry_price": "500"}]})
        await self.connector._process_position_event({
            "type": "positions", "action": "update", "symbol": "C-BTC-100000-251226", "size": 1, "entry_price": "505"})
        self.assertEqual(Decimal("0.001"), self.connector.get_position(CALL).amount)

        await self.connector._process_position_event({
            "type": "positions", "action": "delete", "symbol": "C-BTC-100000-251226", "size": 0})

        self.assertIsNone(self.connector.get_position(CALL))

    async def test_user_stream_routes_wallet_and_position_messages(self):
        async def events():
            yield {"type": "key-auth", "success": True}
            yield {"type": "margins", "asset_symbol": "USD", "balance": "100", "available_balance": "60"}
            yield {"type": "positions", "action": "create", "symbol": "P-BTC-90000-251226", "size": -2,
                   "entry_price": "300"}

        with patch.object(self.connector, "_iter_user_event_queue", new=events):
            await self.connector._user_stream_event_listener()

        self.assertEqual(Decimal("60"), self.connector.get_available_balance("USD"))
        self.assertEqual(Decimal("-0.002"), self.connector.get_position(PUT).amount)

    async def test_network_check_reads_a_single_product(self):
        with patch.object(self.connector, "_api_get", new=AsyncMock(return_value={"success": True})) as api_get:
            await self.connector._make_network_check_request()

        self.assertEqual({"page_size": 1}, api_get.call_args.kwargs["params"])

    def test_balance_stream_updates_wallet(self):
        self.connector._process_balance_event({"type": "margins", "asset_symbol": "USD", "balance": "100",
                                               "available_balance": "60"})

        self.assertEqual(Decimal("100"), self.connector.get_balance("USD"))
        self.assertEqual(Decimal("60"), self.connector.get_available_balance("USD"))

    async def test_empty_wallet_response_keeps_known_balances(self):
        self.connector._account_balances["USD"] = Decimal("100")
        self.connector._account_available_balances["USD"] = Decimal("60")

        with patch.object(self.connector, "_api_get", new=AsyncMock(return_value={"success": True, "result": []})):
            await self.connector._update_balances()
        self.assertEqual(Decimal("100"), self.connector.get_balance("USD"))

        wallet = {"success": True, "result": [{"asset_symbol": "INR", "balance": "5000", "available_balance": "4000"}]}
        with patch.object(self.connector, "_api_get", new=AsyncMock(return_value=wallet)):
            await self.connector._update_balances()
        self.assertEqual({"INR": Decimal("5000")}, self.connector.get_all_balances())

    async def test_order_placement_is_disabled(self):
        with self.assertRaises(NotImplementedError):
            await self.connector._place_order("id", CALL, Decimal("0.001"), TradeType.BUY, OrderType.LIMIT,
                                              Decimal("500"), PositionAction.OPEN)

    def test_options_are_net_with_no_leverage(self):
        self.connector.set_leverage(CALL, 10)
        self.connector.set_position_mode(PositionMode.HEDGE)

        self.assertEqual(1, self.connector.get_leverage(CALL))
        self.assertEqual(PositionMode.ONEWAY, self.connector.position_mode)
        self.assertEqual([PositionMode.ONEWAY], self.connector.supported_position_modes())

    def test_readiness_waits_for_option_info(self):
        self.assertFalse(self.connector.status_dict["option_info"])

        self._set_option_info(CALL, "98000")
        self._set_option_info(PUT, "98000")

        self.assertTrue(self.connector.status_dict["option_info"])

    async def test_option_info_is_retried_until_it_loads(self):
        call_info, put_info = (OptionInfo(trading_pair=pair, mark_price=Decimal("500"), underlying_price=Decimal("98000"))
                               for pair in (CALL, PUT))
        self.connector._orderbook_ds = MagicMock()
        self.connector._orderbook_ds.get_option_info = AsyncMock(side_effect=[IOError("timeout"), call_info, put_info])
        with patch.object(self.connector, "_sleep", new=AsyncMock()) as sleep:
            await asyncio.wait_for(self.connector._init_option_info(), timeout=1)

        sleep.assert_awaited_once_with(5.0)
        self.assertTrue(self.connector._options_trading.is_option_info_initialized())
