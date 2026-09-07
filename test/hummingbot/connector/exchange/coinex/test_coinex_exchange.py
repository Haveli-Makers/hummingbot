from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS
from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange, _result
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _market(symbol="BTCUSDT", base="BTC", quote="USDT", base_prec=8, quote_prec=2,
            min_amount="0.0001", status="online"):
    return {
        "market": symbol, "base_ccy": base, "quote_ccy": quote,
        "base_ccy_precision": base_prec, "quote_ccy_precision": quote_prec,
        "min_amount": min_amount, "maker_fee_rate": "0.002", "taker_fee_rate": "0.002",
        "status": status,
    }


def _wrap(data):
    return {"code": 0, "data": data, "message": "OK"}


class CoinexExchangeTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = combine_to_hb_trading_pair("BTC", "USDT")  # BTC-USDT
        cls.symbol = "BTCUSDT"

    def setUp(self):
        super().setUp()
        self.exchange = CoinexExchange(
            coinex_api_key="k", coinex_api_secret="s",
            trading_pairs=[self.trading_pair], trading_required=False,
        )

    def _bootstrap(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(_wrap([_market()]))
        self.exchange._trading_rules[self.trading_pair] = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.00000001"),
        )

    # ── Envelope + static ─────────────────────────────────────────────────────

    def test_result_unwraps_and_raises(self):
        self.assertEqual([1, 2], _result({"code": 0, "data": [1, 2], "message": "OK"}))
        with self.assertRaises(IOError):
            _result({"code": 3008, "message": "service busy"})

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)
        self.assertIn(OrderType.MARKET, types)

    def test_coinex_order_type_mapping(self):
        self.assertEqual("limit", self.exchange.coinex_order_type(OrderType.LIMIT))
        self.assertEqual("maker_only", self.exchange.coinex_order_type(OrderType.LIMIT_MAKER))
        self.assertEqual("market", self.exchange.coinex_order_type(OrderType.MARKET))

    def test_normalize_ts_ms_to_seconds(self):
        self.assertAlmostEqual(1700000000.0, self.exchange._normalize_ts(1700000000000))
        self.assertEqual(0.0, self.exchange._normalize_ts("not-a-number"))
        self.assertEqual(0.0, self.exchange._normalize_ts(None))

    # ── Symbol map + trading rules ────────────────────────────────────────────

    async def test_symbol_map(self):
        self._bootstrap()
        self.assertEqual(self.symbol, await self.exchange.exchange_symbol_associated_to_pair(self.trading_pair))
        self.assertEqual(self.trading_pair, await self.exchange.trading_pair_associated_to_exchange_symbol(self.symbol))
        self.assertEqual("BTC", self.exchange._base_ccy_by_symbol[self.symbol])

    async def test_symbol_map_skips_offline(self):
        offline = _market(status="offline")
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(_wrap([offline]))
        self.assertEqual({}, self.exchange._base_ccy_by_symbol)

    async def test_format_trading_rules(self):
        rules = await self.exchange._format_trading_rules(_wrap([_market(base_prec=8, quote_prec=2)]))
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(self.trading_pair, rule.trading_pair)
        self.assertEqual(Decimal("0.01"), rule.min_price_increment)
        self.assertEqual(Decimal("0.00000001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.0001"), rule.min_order_size)

    # ── Order placement / cancel / status ─────────────────────────────────────

    async def test_place_limit_order_payload(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required, **kw):
            captured["path"] = path_url
            captured["data"] = data
            return _wrap({"order_id": 9999, "created_at": 1700000000000})

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        ex_id, ts = await self.exchange._place_order(
            order_id="HBOT-1", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("50000"))
        self.assertEqual("9999", ex_id)
        self.assertEqual(CONSTANTS.ORDER_PATH_URL, captured["path"])
        self.assertEqual(self.symbol, captured["data"]["market"])
        self.assertEqual("SPOT", captured["data"]["market_type"])
        self.assertEqual("buy", captured["data"]["side"])
        self.assertEqual("limit", captured["data"]["type"])
        self.assertEqual("50000", captured["data"]["price"])
        self.assertEqual("HBOT-1", captured["data"]["client_id"])

    async def test_place_limit_maker_uses_maker_only(self):
        self._bootstrap()
        captured = {}
        self.exchange._api_post = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or _wrap({"order_id": 1, "created_at": 0}))
        await self.exchange._place_order(
            order_id="HBOT-2", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.SELL, order_type=OrderType.LIMIT_MAKER, price=Decimal("70000"))
        self.assertEqual("maker_only", captured["type"])
        self.assertEqual("sell", captured["side"])

    async def test_place_market_order_sets_base_ccy(self):
        self._bootstrap()
        captured = {}
        self.exchange._api_post = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or _wrap({"order_id": 2, "created_at": 0}))
        await self.exchange._place_order(
            order_id="HBOT-3", trading_pair=self.trading_pair, amount=Decimal("0.01"),
            trade_type=TradeType.BUY, order_type=OrderType.MARKET, price=Decimal("0"))
        self.assertEqual("market", captured["type"])
        self.assertEqual("BTC", captured["ccy"])
        self.assertNotIn("price", captured)

    async def test_place_cancel(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-4", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="555")
        captured = {}
        self.exchange._api_post = AsyncMock(
            side_effect=lambda path_url, data, is_auth_required, **kw: captured.update(data) or _wrap({"order_id": 555, "status": "canceled"}))
        ok = await self.exchange._place_cancel("HBOT-4", order)
        self.assertTrue(ok)
        self.assertEqual(555, captured["order_id"])
        self.assertEqual("SPOT", captured["market_type"])

    async def test_place_cancel_pending_returns_false(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-4b", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("50000"),
            creation_timestamp=1700000000.0)
        self.assertIsNone(order.exchange_order_id)
        self.exchange._api_post = AsyncMock(side_effect=AssertionError("should not call API"))
        self.assertFalse(await self.exchange._place_cancel("HBOT-4b", order))

    async def test_request_order_status_maps_state(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="HBOT-5", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.01"), price=Decimal("50000"),
            creation_timestamp=1700000000.0, exchange_order_id="777")
        self.exchange._api_get = AsyncMock(return_value=_wrap(
            {"order_id": 777, "status": "part_filled", "filled_amount": "0.004",
             "unfilled_amount": "0.006", "updated_at": 1700000001000}))
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)
        self.assertEqual("777", update.exchange_order_id)

    async def test_update_balances(self):
        self.exchange._api_get = AsyncMock(return_value=_wrap([
            {"ccy": "USDT", "available": "100.5", "frozen": "9.5"},
            {"ccy": "BTC", "available": "0.2", "frozen": "0"},
        ]))
        await self.exchange._update_balances()
        self.assertEqual(Decimal("110.0"), self.exchange._account_balances["USDT"])
        self.assertEqual(Decimal("100.5"), self.exchange._account_available_balances["USDT"])
        self.assertEqual(Decimal("0.2"), self.exchange._account_balances["BTC"])

    async def test_get_last_traded_price(self):
        self._bootstrap()
        self.exchange._api_get = AsyncMock(return_value=_wrap([{"market": self.symbol, "last": "61234.5"}]))
        price = await self.exchange._get_last_traded_price(self.trading_pair)
        self.assertEqual(61234.5, price)
