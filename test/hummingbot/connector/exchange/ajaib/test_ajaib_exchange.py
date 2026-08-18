import uuid
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest import TestCase
from unittest.mock import AsyncMock

from bidict import bidict

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _symbol_info(symbol="BTC_IDR", base="BTC", quote="IDR"):
    return {
        "symbol": symbol,
        "baseAsset": base,
        "quoteAsset": quote,
        "isSpotTradingAllowed": True,
        "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "1"},
            {"filterType": "LOT_SIZE", "minQty": "0.0001", "maxQty": "100", "stepSize": "0.0001"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "10000"},
        ],
    }


class AjaibExchangeTests(IsolatedAsyncioWrapperTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.trading_pair = combine_to_hb_trading_pair("BTC", "IDR")  # BTC-IDR
        cls.symbol = "BTC_IDR"

    def setUp(self):
        super().setUp()
        self.exchange = AjaibExchange(
            ajaib_api_key="k", ajaib_api_secret="s",
            trading_pairs=[self.trading_pair], trading_required=False,
        )

    def _bootstrap(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info({"symbols": [_symbol_info()]})

    # ── static mappings ───────────────────────────────────────────────────────

    def test_supported_order_types_excludes_market(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)
        self.assertNotIn(OrderType.MARKET, types)

    def test_order_type_and_side_mapping(self):
        self.assertEqual("LIMIT", AjaibExchange.ajaib_order_type(OrderType.LIMIT))
        self.assertEqual("LIMIT_MAKER", AjaibExchange.ajaib_order_type(OrderType.LIMIT_MAKER))
        self.assertEqual("BUY", AjaibExchange.ajaib_side(TradeType.BUY))
        self.assertEqual("SELL", AjaibExchange.ajaib_side(TradeType.SELL))
        self.assertEqual(OrderType.LIMIT_MAKER, AjaibExchange.to_hb_order_type("LIMIT_MAKER"))

    def test_buy_and_sell_generate_uuid_client_ids(self):
        self.exchange._create_order = AsyncMock()
        buy_id = self.exchange.buy(self.trading_pair, Decimal("1"), OrderType.LIMIT, Decimal("100"))
        sell_id = self.exchange.sell(self.trading_pair, Decimal("1"), OrderType.LIMIT, Decimal("100"))
        self.assertEqual(4, uuid.UUID(buy_id).version)
        self.assertEqual(4, uuid.UUID(sell_id).version)

    # ── exchange info parsing ─────────────────────────────────────────────────

    async def test_symbol_map_initialization(self):
        self._bootstrap()
        trading_pair = await self.exchange.trading_pair_associated_to_exchange_symbol(symbol=self.symbol)
        self.assertEqual(self.trading_pair, trading_pair)
        symbol = await self.exchange.exchange_symbol_associated_to_pair(trading_pair=self.trading_pair)
        self.assertEqual(self.symbol, symbol)

    async def test_format_trading_rules(self):
        rules = await self.exchange._format_trading_rules({"symbols": [_symbol_info()]})
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(self.trading_pair, rule.trading_pair)
        self.assertEqual(Decimal("0.0001"), rule.min_order_size)
        self.assertEqual(Decimal("1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.0001"), rule.min_base_amount_increment)
        self.assertEqual(Decimal("10000"), rule.min_notional_size)

    # ── orders ────────────────────────────────────────────────────────────────

    async def test_place_order_sends_uuid_and_limit_params(self):
        self._bootstrap()
        captured = {}

        async def fake_post(path_url, data, is_auth_required):
            captured["path"] = path_url
            captured["data"] = data
            return {"orderId": "de7fad3e-d13e-41fb-973e-967065b42a54", "time": 1499827319559}

        self.exchange._api_post = AsyncMock(side_effect=fake_post)
        order_id = str(uuid.uuid4())
        exch_id, ts = await self.exchange._place_order(
            order_id=order_id, trading_pair=self.trading_pair, amount=Decimal("0.5"),
            trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("123"))

        self.assertEqual("de7fad3e-d13e-41fb-973e-967065b42a54", exch_id)
        self.assertEqual(1499827319.559, ts)
        self.assertEqual(CONSTANTS.CREATE_ORDER_PATH_URL, captured["path"])
        self.assertEqual(self.symbol, captured["data"]["symbol"])
        self.assertEqual("BUY", captured["data"]["side"])
        self.assertEqual("LIMIT", captured["data"]["type"])
        self.assertEqual("GTC", captured["data"]["timeInForce"])
        self.assertEqual(order_id, captured["data"]["newClientOrderId"])

    async def test_place_cancel_uses_orig_client_order_id(self):
        self._bootstrap()
        captured = {}

        async def fake_delete(path_url, params, is_auth_required):
            captured.update(params)
            return {"status": "CANCELED"}

        self.exchange._api_delete = AsyncMock(side_effect=fake_delete)
        order = InFlightOrder(
            client_order_id="abc-uuid", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("1"), price=Decimal("100"),
            creation_timestamp=1700000000.0, exchange_order_id="exch-1")
        result = await self.exchange._place_cancel("abc-uuid", order)
        self.assertTrue(result)
        self.assertEqual("abc-uuid", captured["origClientOrderId"])
        self.assertEqual(self.symbol, captured["symbol"])

    async def test_request_order_status_maps_state(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="abc-uuid", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("1"), price=Decimal("100"),
            creation_timestamp=1700000000.0, exchange_order_id="exch-1")
        self.exchange._api_get = AsyncMock(return_value={
            "orderId": "exch-1", "status": "PARTIALLY_FILLED", "updateTime": 1700000001000})
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)
        self.assertEqual("exch-1", update.exchange_order_id)
        self.assertEqual(1700000001.0, update.update_timestamp)

    async def test_all_trade_updates_parses_commission_and_tax(self):
        self._bootstrap()
        order = InFlightOrder(
            client_order_id="abc-uuid", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL, amount=Decimal("1"), price=Decimal("100"),
            creation_timestamp=1700000000.0, exchange_order_id="exch-1")
        self.exchange._api_get = AsyncMock(return_value=[{
            "id": "trade-1", "orderId": "exch-1", "price": "100", "qty": "1", "quoteQty": "100",
            "time": 1700000002000, "isMaker": True,
            "commission": "0.1", "commissionAsset": "IDR", "tax": "0.01", "taxAsset": "IDR",
        }])
        updates = await self.exchange._all_trade_updates_for_order(order)
        self.assertEqual(1, len(updates))
        tu = updates[0]
        self.assertEqual("trade-1", tu.trade_id)
        self.assertEqual(Decimal("1"), tu.fill_base_amount)
        self.assertEqual(Decimal("100"), tu.fill_quote_amount)
        self.assertEqual(2, len(tu.fee.flat_fees))
        self.assertTrue(all(f.token == "IDR" for f in tu.fee.flat_fees))
        self.assertEqual(Decimal("0.1") + Decimal("0.01"), sum(f.amount for f in tu.fee.flat_fees))

    async def test_update_balances_from_portfolio(self):
        self.exchange._api_get = AsyncMock(return_value={"balances": [
            {"asset": "IDR", "free": "100.5", "locked": "9.5"},
            {"asset": "BTC", "free": "0.2", "locked": "0"},
        ]})
        await self.exchange._update_balances()
        self.assertEqual(Decimal("110.0"), self.exchange._account_balances["IDR"])
        self.assertEqual(Decimal("100.5"), self.exchange._account_available_balances["IDR"])
        self.assertEqual(Decimal("0.2"), self.exchange._account_balances["BTC"])

    async def test_get_all_pairs_prices_uses_klines(self):
        self.exchange._make_trading_pairs_request = AsyncMock(return_value={"symbols": [_symbol_info()]})
        self.exchange._api_get = AsyncMock(return_value=[
            [1700000000000, "100", "110", "90", "105.5", "1000", 1700000059999]])
        prices = await self.exchange.get_all_pairs_prices()
        self.assertEqual(1, len(prices))
        self.assertEqual(self.symbol, prices[0]["symbol"])
        self.assertEqual("105.5", prices[0]["bidPrice"])
        self.assertEqual("105.5", prices[0]["askPrice"])

    async def test_get_last_traded_price(self):
        self._bootstrap()
        self.exchange._api_get = AsyncMock(return_value=[
            [1700000000000, "100", "110", "90", "61234.5", "1000", 1700000059999]])
        price = await self.exchange._get_last_traded_price(self.trading_pair)
        self.assertEqual(61234.5, price)


class AjaibOrderStateResolutionTests(TestCase):
    """NEW means two different things depending on workingTime (docs > Definitions)."""

    def _resolve(self, status, working_time, current=OrderState.PENDING_CREATE):
        return AjaibExchange.resolve_order_state(status, working_time, current)

    def test_new_with_zero_working_time_is_not_yet_open(self):
        # "received by exchange but it is not valid yet" -- advertising this as
        # OPEN tells a strategy the order can trade when it cannot.
        self.assertEqual(OrderState.PENDING_CREATE, self._resolve('NEW', 0))
        self.assertEqual(OrderState.PENDING_CREATE, self._resolve('NEW', '0'))
        self.assertEqual(OrderState.PENDING_CREATE, self._resolve('NEW', None))

    def test_new_with_working_time_is_open(self):
        self.assertEqual(OrderState.OPEN, self._resolve('NEW', 1700000000000))
        self.assertEqual(OrderState.OPEN, self._resolve('NEW', '1700000000000'))

    def test_terminal_states_ignore_working_time(self):
        self.assertEqual(OrderState.FILLED, self._resolve('FILLED', 0))
        self.assertEqual(OrderState.CANCELED, self._resolve('PARTIALLY_EXPIRED', 0))
        self.assertEqual(OrderState.FAILED, self._resolve('REJECTED', 0))

    def test_unknown_status_keeps_the_current_state(self):
        self.assertEqual(OrderState.OPEN, self._resolve('SOMETHING_NEW', 1, OrderState.OPEN))


class AjaibCancelContractTests(IsolatedAsyncioWrapperTestCase):
    """DELETE /v1/order requires newClientOrderId and returns the cancelled order."""

    def setUp(self):
        super().setUp()
        self.trading_pair = "BTC-IDR"
        self.exchange = AjaibExchange(
            ajaib_api_key="k", ajaib_api_secret="", ajaib_proxy_url="",
            trading_pairs=[self.trading_pair], trading_required=False)
        self.exchange._set_trading_pair_symbol_map(bidict({"BTC_IDR": self.trading_pair}))

    def _order(self):
        return InFlightOrder(
            client_order_id="11111111-1111-4111-8111-111111111111",
            trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("1000000000"), creation_timestamp=1700000000.0,
            exchange_order_id="ex-1")

    async def test_cancel_sends_the_mandatory_new_client_order_id(self):
        sent = {}

        async def _delete(path_url, params, is_auth_required=False, **kw):
            sent.update(params)
            return {"status": "CANCELLED"}

        self.exchange._api_delete = AsyncMock(side_effect=_delete)
        order = self._order()
        self.assertTrue(await self.exchange._place_cancel(order.client_order_id, order))

        self.assertEqual("BTC_IDR", sent["symbol"])
        self.assertEqual(order.client_order_id, sent["origClientOrderId"])
        self.assertIn("newClientOrderId", sent, "newClientOrderId is MANDATORY per the docs")
        # It identifies the CANCEL, so it must differ from the order's own id.
        self.assertNotEqual(order.client_order_id, sent["newClientOrderId"])
        uuid.UUID(sent["newClientOrderId"], version=4)

    async def test_cancel_rejects_a_non_dict_response(self):
        self.exchange._api_delete = AsyncMock(return_value=[{"msg": "nope"}])
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("x", self._order())

    async def test_cancel_rejects_a_non_cancelled_status(self):
        self.exchange._api_delete = AsyncMock(return_value={"status": "NEW"})
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("x", self._order())

    async def test_partially_cancelled_counts_as_cancelled(self):
        self.exchange._api_delete = AsyncMock(return_value={"status": "PARTIALLY_CANCELLED"})
        self.assertTrue(await self.exchange._place_cancel("x", self._order()))

    def test_order_not_found_matches_the_api_code_not_the_http_status(self):
        ex = self.exchange
        self.assertTrue(ex._is_order_not_found_during_cancelation_error(
            IOError('HTTP status is 404. Error: {"code":-2013,"msg":"Order not found"}')))
        # A gateway 404 for a wrong path must NOT read as "order already gone".
        self.assertFalse(ex._is_order_not_found_during_cancelation_error(
            IOError('HTTP 404. {"message":"no Route matched with those values"}')))
