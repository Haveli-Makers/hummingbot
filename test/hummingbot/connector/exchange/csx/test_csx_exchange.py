import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.connector.exchange.csx import csx_constants as CONSTANTS
from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

_VALID_SECRET = "aa" * 32


def _make_exchange(**kwargs) -> CsxExchange:
    """
    Factory helper used by all unit tests.

    PROXY USAGE IN TESTS
    --------------------
    Unit tests mock all HTTP calls (aioresponses / AsyncMock), so a real proxy
    is never needed here.  The proxy_url kwarg still flows through the full
    construction path so proxy-related code paths are exercised.

    To test WITH a real proxy (integration-style), pass:
        _make_exchange(csx_proxy_url="socks5://user:pass@host:1080")

    To test WITHOUT proxy (normal unit test — default):
        _make_exchange()   # csx_proxy_url defaults to "", meaning no proxy
    """
    defaults = dict(
        csx_api_key="test_key",
        csx_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-INR"],
        trading_required=False,
        # csx_proxy_url is intentionally omitted here so tests run without a proxy.
        # Pass csx_proxy_url="socks5://..." to override for proxy-path testing.
    )
    defaults.update(kwargs)
    return CsxExchange(**defaults)


class CsxExchangePropertiesTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    def test_name(self):
        self.assertEqual("csx", self.exchange.name)

    def test_client_order_id_prefix(self):
        self.assertEqual(CONSTANTS.HBOT_ORDER_ID_PREFIX, self.exchange.client_order_id_prefix)

    def test_client_order_id_max_length(self):
        self.assertEqual(CONSTANTS.MAX_ORDER_ID_LEN, self.exchange.client_order_id_max_length)

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)

    def test_is_cancel_request_synchronous(self):
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)

    def test_trading_rules_request_path(self):
        self.assertEqual(CONSTANTS.INSTRUMENTS_PATH_URL, self.exchange.trading_rules_request_path)

    def test_trading_pairs_request_path(self):
        self.assertEqual(CONSTANTS.INSTRUMENTS_PATH_URL, self.exchange.trading_pairs_request_path)

    def test_check_network_request_path(self):
        self.assertEqual(CONSTANTS.HEALTH_PATH_URL, self.exchange.check_network_request_path)

    def test_static_csx_order_type(self):
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, CsxExchange.csx_order_type(OrderType.LIMIT))
        self.assertEqual(CONSTANTS.ORDER_TYPE_LIMIT, CsxExchange.csx_order_type(OrderType.LIMIT_MAKER))

    def test_static_to_hb_order_type(self):
        self.assertEqual(OrderType.LIMIT, CsxExchange.to_hb_order_type("LIMIT"))


class CsxExchangeTradingPairTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    def test_initialize_trading_pair_symbols_from_list_of_strings(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(["BTC/INR", "ETH/INR"])
        loop = asyncio.get_event_loop()
        pair = loop.run_until_complete(
            self.exchange.trading_pair_associated_to_exchange_symbol("BTC/INR")
        )
        self.assertEqual("BTC-INR", pair)

    def test_initialize_trading_pair_symbols_from_list_of_dicts(self):
        instruments = [{"symbol": "ETH/USDT", "minQuantity": "0.01"}]
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(instruments)
        loop = asyncio.get_event_loop()
        pair = loop.run_until_complete(
            self.exchange.trading_pair_associated_to_exchange_symbol("ETH/USDT")
        )
        self.assertEqual("ETH-USDT", pair)

    def test_initialize_trading_pair_symbols_from_data_wrapper(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            {"data": ["BTC/INR"]}
        )
        loop = asyncio.get_event_loop()
        pair = loop.run_until_complete(
            self.exchange.trading_pair_associated_to_exchange_symbol("BTC/INR")
        )
        self.assertEqual("BTC-INR", pair)


class CsxExchangeTradingRulesTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    async def test_format_trading_rules_from_strings(self):
        rules = await self.exchange._format_trading_rules(["BTC/INR", "ETH/INR"])
        self.assertGreater(len(rules), 0)
        pairs = {r.trading_pair for r in rules}
        self.assertIn("BTC-INR", pairs)

    async def test_format_trading_rules_from_dicts(self):
        info = [
            {
                "symbol": "BTC/INR",
                "minQuantity": "0.0001",
                "maxQuantity": "10",
                "tickSize": "1",
                "stepSize": "0.0001",
            }
        ]
        rules = await self.exchange._format_trading_rules(info)
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BTC-INR", rule.trading_pair)
        self.assertEqual(Decimal("0.0001"), rule.min_order_size)
        self.assertEqual(Decimal("1"), rule.min_price_increment)

    async def test_format_trading_rules_empty_input(self):
        rules = await self.exchange._format_trading_rules([])
        self.assertEqual([], rules)


class CsxExchangeBalanceTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange()

    async def test_update_balances(self):
        balance_response = {
            "Available": {"BTC": "1.0", "INR": "80000"},
            "Locked": {"BTC": "0.2"},
        }
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = balance_response
            await self.exchange._update_balances()

        self.assertEqual(Decimal("1.2"), self.exchange._account_balances.get("BTC"))
        self.assertEqual(Decimal("1.0"), self.exchange._account_available_balances.get("BTC"))
        self.assertEqual(Decimal("80000"), self.exchange._account_balances.get("INR"))

    async def test_update_balances_removes_stale_assets(self):
        self.exchange._account_balances["STALE"] = Decimal("99")
        self.exchange._account_available_balances["STALE"] = Decimal("99")

        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"Available": {"BTC": "1"}, "Locked": {}}
            await self.exchange._update_balances()

        self.assertNotIn("STALE", self.exchange._account_balances)


class CsxExchangeOrderTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange(trading_pairs=["BTC-INR"])
        cls.exchange._set_trading_pair_symbol_map(bidict({"BTC/INR": "BTC-INR"}))

    async def test_place_order(self):
        # CSX wraps the create-order response under "data".
        order_resp = {
            "data": {
                "orderId": "order-uuid-123",
                "status": "OPEN",
                "createdAt": 1_725_010_288,
            },
            "message": "Order created",
        }
        # Pre-seed the cached username so _place_order does not make a real
        # GET /api/v1/me/ call to resolve it.
        self.exchange._username = "test_user"
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = order_resp
            oid, ts = await self.exchange._place_order(
                order_id="xCSXtest",
                trading_pair="BTC-INR",
                amount=Decimal("0.001"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("3000000"),
            )
        self.assertEqual("order-uuid-123", oid)
        self.assertEqual(1_725_010_288.0, ts)

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("data") or call_kwargs.args[1]
        self.assertEqual("BTC/INR", body["instrument"])
        self.assertEqual("BUY", body["side"])
        self.assertEqual("LIMIT", body["type"])
        self.assertEqual("test_user", body["username"])
        # clientOrderId must NOT be sent — CSX requires a UUID and rejects others.
        self.assertNotIn("clientOrderId", body)

    async def test_place_order_handles_top_level_response(self):
        # Also accept an un-wrapped response shape for robustness.
        order_resp = {"orderId": "oid-top", "createdAt": 1_725_010_288}
        self.exchange._username = "test_user"
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = order_resp
            oid, ts = await self.exchange._place_order(
                order_id="xCSXtest",
                trading_pair="BTC-INR",
                amount=Decimal("0.001"),
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT,
                price=Decimal("3000000"),
            )
        self.assertEqual("oid-top", oid)

    async def test_get_username_caches_profile_lookup(self):
        self.exchange._username = None  # reset shared class-level cache
        profile_resp = {"data": {"userName": "HaveliMakers4"}, "message": "ok"}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = profile_resp
            first = await self.exchange._get_username()
            second = await self.exchange._get_username()
        self.assertEqual("HaveliMakers4", first)
        self.assertEqual("HaveliMakers4", second)
        # Cached: only one network call despite two invocations
        self.assertEqual(1, mock_get.call_count)

    async def test_place_cancel_success(self):
        tracked = InFlightOrder(
            client_order_id="x-CSX-test",
            exchange_order_id="order-uuid-123",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("3000000"),
            creation_timestamp=1_725_010_288.0,
        )
        # CSX cancel response is wrapped: {"data": {"cancelled": true}, "message": ...}
        with patch.object(self.exchange, "_api_delete", new_callable=AsyncMock) as mock_del:
            mock_del.return_value = {"data": {"cancelled": True, "info": {"message": "Order Cancelled"}},
                                     "message": "Order cancelled"}
            result = await self.exchange._place_cancel("x-CSX-test", tracked)
        self.assertTrue(result)

    async def test_place_cancel_handles_status_shape(self):
        tracked = InFlightOrder(
            client_order_id="xCSXtest",
            exchange_order_id="order-uuid-123",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("3000000"),
            creation_timestamp=1_725_010_288.0,
        )
        with patch.object(self.exchange, "_api_delete", new_callable=AsyncMock) as mock_del:
            mock_del.return_value = {"status": "CANCELLED"}
            result = await self.exchange._place_cancel("xCSXtest", tracked)
        self.assertTrue(result)

    async def test_request_order_status_filled(self):
        tracked = InFlightOrder(
            client_order_id="x-CSX-test",
            exchange_order_id="order-uuid-123",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("3000000"),
            creation_timestamp=1_725_010_288.0,
        )
        order_data = {
            "orderId": "order-uuid-123",
            "status": "FILLED",
            "updatedAt": 1_725_010_300,
        }
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = order_data
            update = await self.exchange._request_order_status(tracked)

        self.assertEqual(OrderState.FILLED, update.new_state)
        self.assertEqual("order-uuid-123", update.exchange_order_id)

    async def test_request_order_status_unknown_raises(self):
        tracked = InFlightOrder(
            client_order_id="x-CSX-test",
            exchange_order_id="order-uuid-123",
            trading_pair="BTC-INR",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("0.001"),
            price=Decimal("3000000"),
            creation_timestamp=1_725_010_288.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"orderId": "x", "status": "UNKNOWN_STATUS", "updatedAt": 0}
            with self.assertRaises(ValueError):
                await self.exchange._request_order_status(tracked)


class CsxExchangeUserStreamListenerTests(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.exchange = _make_exchange(trading_pairs=["BTC-INR"])
        cls.exchange._set_trading_pair_symbol_map(bidict({"BTC/INR": "BTC-INR"}))

    async def test_user_stream_listener_balance_update(self):
        queue = asyncio.Queue()
        balance_event = {
            "event": "balance_update",
            "data": {
                "Available": {"BTC": "2.0"},
                "Locked": {},
            },
        }
        await queue.put(balance_event)
        await queue.put(asyncio.CancelledError())  # sentinel to stop loop

        async def _consume():
            try:
                async for _ in self.exchange._iter_user_event_queue():
                    pass
            except asyncio.CancelledError:
                pass

        self.exchange._user_stream_tracker._user_stream = queue
        with patch.object(
            self.exchange, "_iter_user_event_queue", return_value=_async_gen([balance_event])
        ):
            await self.exchange._user_stream_event_listener()

        self.assertEqual(Decimal("2.0"), self.exchange._account_balances.get("BTC"))


async def _async_gen(items):
    for item in items:
        yield item


if __name__ == "__main__":
    import unittest
    unittest.main()
