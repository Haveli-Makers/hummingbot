from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange, _ensure_ok
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _pair(symbol="BTCZAR", base="BTC", quote="ZAR", min_base="0.000008", min_quote="10",
          tick="1", base_dp="8", ptype="SPOT", active=True):
    return {
        "symbol": symbol, "baseCurrency": base, "quoteCurrency": quote, "active": active,
        "minBaseAmount": min_base, "minQuoteAmount": min_quote, "tickSize": tick,
        "baseDecimalPlaces": base_dp, "currencyPairType": ptype,
    }


class ValrExchangeTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = combine_to_hb_trading_pair("BTC", "ZAR")  # BTC-ZAR
        cls.symbol = "BTCZAR"

    def setUp(self):
        super().setUp()
        self.exchange = ValrExchange(
            valr_api_key="k", valr_api_secret="s",
            trading_pairs=[self.trading_pair], trading_required=False)

    def _bootstrap(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info([_pair()])
        self.exchange._trading_rules[self.trading_pair] = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.000008"),
            min_price_increment=Decimal("1"),
            min_base_amount_increment=Decimal("0.00000001"),
            min_notional_size=Decimal("10"),
        )

    # ── Envelope / static ─────────────────────────────────────────────────────

    def test_ensure_ok_passthrough_and_raise(self):
        self.assertEqual([1, 2], _ensure_ok([1, 2]))
        self.assertEqual({"id": "x"}, _ensure_ok({"id": "x"}))
        with self.assertRaises(IOError):
            _ensure_ok({"code": -12, "message": "Insufficient balance"})

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)
        self.assertIn(OrderType.MARKET, types)

    # ── Symbol map + trading rules ────────────────────────────────────────────

    async def test_symbol_map_spot_only(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            [_pair(), _pair(symbol="BTCUSDTPERP", base="BTC", quote="USDT", ptype="FUTURE")])
        self.assertEqual(self.symbol, await self.exchange.exchange_symbol_associated_to_pair(self.trading_pair))
        self.assertEqual(self.trading_pair, await self.exchange.trading_pair_associated_to_exchange_symbol(self.symbol))
        # the FUTURE pair is excluded
        with self.assertRaises(KeyError):
            await self.exchange.trading_pair_associated_to_exchange_symbol("BTCUSDTPERP")

    async def test_format_trading_rules(self):
        rules = await self.exchange._format_trading_rules([_pair(tick="1", base_dp="8")])
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(self.trading_pair, rule.trading_pair)
        self.assertEqual(Decimal("1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.00000001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.000008"), rule.min_order_size)
        self.assertEqual(Decimal("10"), rule.min_notional_size)

    # ── Order placement / cancel / status ─────────────────────────────────────

    async def test_place_limit_order_payload(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kw):
            captured["path"] = path_url
            captured["data"] = data
            return {"id": "ord-123"}

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        ex_id, _ = await self.exchange._place_order(
            order_id="HBOT-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("1000000"))
        self.assertEqual("ord-123", ex_id)
        self.assertEqual(CONSTANTS.PLACE_LIMIT_ORDER_PATH_URL, captured["path"])
        self.assertEqual(self.symbol, captured["data"]["pair"])
        self.assertEqual("BUY", captured["data"]["side"])
        self.assertEqual("0.01", captured["data"]["quantity"])
        self.assertEqual("1000000", captured["data"]["price"])
        self.assertFalse(captured["data"]["postOnly"])
        self.assertEqual("HBOT-1", captured["data"]["customerOrderId"])

    async def test_place_limit_maker_sets_post_only(self):
        self._bootstrap()
        captured = {}
        self.exchange._api_post = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or {"id": "x"})
        await self.exchange._place_order(
            order_id="HBOT-2", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.SELL, order_type=OrderType.LIMIT_MAKER, price=Decimal("1200000"))
        self.assertTrue(captured["postOnly"])
        self.assertEqual("SELL", captured["side"])

    async def test_place_market_order_uses_base_amount(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kw):
            captured["path"] = path_url
            captured["data"] = data
            return {"id": "m1"}

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        await self.exchange._place_order(
            order_id="HBOT-3", trading_pair=self.trading_pair, amount=Decimal("0.02"),
            trade_type=TradeType.BUY, order_type=OrderType.MARKET, price=Decimal("0"))
        self.assertEqual(CONSTANTS.PLACE_MARKET_ORDER_PATH_URL, captured["path"])
        self.assertEqual("0.02", captured["data"]["baseAmount"])
        self.assertNotIn("price", captured["data"])

    async def test_place_cancel(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-4", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("1000000"),
            creation_timestamp=1700000000.0, exchange_order_id="ord-555")
        captured = {}
        self.exchange._api_delete = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or {})
        ok = await self.exchange._place_cancel("HBOT-4", order)
        self.assertTrue(ok)
        self.assertEqual("ord-555", captured["orderId"])
        self.assertEqual(self.symbol, captured["pair"])

    async def test_place_cancel_pending_returns_false(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-4b", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("1000000"),
            creation_timestamp=1700000000.0)
        self.assertIsNone(order.exchange_order_id)
        self.exchange._api_delete = AsyncMock(side_effect=AssertionError("should not call API"))
        self.assertFalse(await self.exchange._place_cancel("HBOT-4b", order))

    async def test_request_order_status_maps_state(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-5", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("1000000"),
            creation_timestamp=1700000000.0, exchange_order_id="ord-777")
        self.exchange._api_get = AsyncMock(return_value={"orderId": "ord-777", "orderStatusType": "Partially Filled"})
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)
        self.assertEqual("ord-777", update.exchange_order_id)

    async def test_update_balances(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {"currency": "ZAR", "available": "100.5", "reserved": "9.5", "total": "110"},
            {"currency": "BTC", "available": "0.2", "reserved": "0", "total": "0.2"},
        ])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("110"), self.exchange._account_balances["ZAR"])
        self.assertEqual(Decimal("100.5"), self.exchange._account_available_balances["ZAR"])
        self.assertEqual(Decimal("0.2"), self.exchange._account_balances["BTC"])

    async def test_update_balances_currency_as_object(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {"currency": {"symbol": "BTC"}, "available": "1", "reserved": "0.5"},
        ])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("1.5"), self.exchange._account_balances["BTC"])
        self.assertEqual(Decimal("1"), self.exchange._account_available_balances["BTC"])
