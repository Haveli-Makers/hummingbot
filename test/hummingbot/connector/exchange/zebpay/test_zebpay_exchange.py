import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.connector.exchange.zebpay import zebpay_constants as CONSTANTS
from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


def _make_exchange(**kwargs) -> ZebpayExchange:
    defaults = dict(
        zebpay_api_key="test_key",
        zebpay_api_secret="test_secret",
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    defaults.update(kwargs)
    return ZebpayExchange(**defaults)


class ZebpayExchangePropertiesTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # IsolatedAsyncioTestCase provides a running event loop during setUp,
        # which ExchangePyBase needs when constructing the connector.
        self.exchange = _make_exchange()

    def test_name(self):
        self.assertEqual("zebpay", self.exchange.name)

    def test_prefix_alphanumeric(self):
        self.assertTrue(self.exchange.client_order_id_prefix.isalnum())

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)

    def test_request_paths(self):
        self.assertEqual(CONSTANTS.EXCHANGE_INFO_PATH_URL, self.exchange.trading_rules_request_path)
        self.assertEqual(CONSTANTS.EXCHANGE_INFO_PATH_URL, self.exchange.trading_pairs_request_path)
        self.assertEqual(CONSTANTS.PING_PATH_URL, self.exchange.check_network_request_path)

    def test_cancel_synchronous(self):
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)


class ZebpayExchangeTradingPairTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange()

    async def test_symbol_map_from_exchange_info_dict_list(self):
        info = {"data": [
            {"symbol": "BTC-INR", "baseAsset": "BTC", "quoteAsset": "INR"},
            {"symbol": "ETH-INR", "baseAsset": "ETH", "quoteAsset": "INR"},
        ]}
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(info)
        self.assertEqual("BTC-INR", await self.exchange.trading_pair_associated_to_exchange_symbol("BTC-INR"))
        self.assertEqual("BTC-INR", await self.exchange.exchange_symbol_associated_to_pair("BTC-INR"))

    async def test_format_trading_rules(self):
        info = {"data": [{
            "symbol": "BTC-INR", "baseAsset": "BTC", "quoteAsset": "INR",
            "pricePrecision": 0, "quantityPrecision": 6, "tickSz": "1", "lotSz": "0.000001",
        }]}
        rules = await self.exchange._format_trading_rules(info)
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BTC-INR", rule.trading_pair)
        self.assertEqual(Decimal("1"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000001"), rule.min_base_amount_increment)


class ZebpayExchangeBalanceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange()

    async def test_update_balances(self):
        resp = {"data": [
            {"currency": "BTC", "total": "1.2", "free": "1.0", "used": "0.2"},
            {"currency": "INR", "total": "80000", "free": "80000", "used": "0"},
        ]}
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = resp
            await self.exchange._update_balances()
        self.assertEqual(Decimal("1.2"), self.exchange._account_balances["BTC"])
        self.assertEqual(Decimal("1.0"), self.exchange._account_available_balances["BTC"])
        self.assertEqual(Decimal("80000"), self.exchange._account_balances["INR"])

    async def test_update_balances_removes_stale(self):
        self.exchange._account_balances["STALE"] = Decimal("9")
        self.exchange._account_available_balances["STALE"] = Decimal("9")
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": [{"currency": "BTC", "total": "1", "free": "1", "used": "0"}]}
            await self.exchange._update_balances()
        self.assertNotIn("STALE", self.exchange._account_balances)


class ZebpayExchangeOrderTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _make_exchange(trading_pairs=["BTC-INR"])
        self.exchange._set_trading_pair_symbol_map(bidict({"BTC-INR": "BTC-INR"}))

    async def test_place_order_builds_payload(self):
        resp = {"data": {"orderId": "ord-1", "status": "OPEN", "timestamp": 1_700_000_000_000}}
        with patch.object(self.exchange, "_api_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = resp
            oid, ts = await self.exchange._place_order(
                order_id="ZEBtest", trading_pair="BTC-INR", amount=Decimal("0.001"),
                trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=Decimal("3000000"),
            )
        self.assertEqual("ord-1", oid)
        body = mock_post.call_args.kwargs.get("data")
        self.assertEqual("BTC-INR", body["symbol"])
        self.assertEqual("BUY", body["side"])
        self.assertEqual("LIMIT", body["type"])
        self.assertEqual("3000000", body["price"])
        self.assertEqual("0.001", body["amount"])

    async def test_place_cancel_success(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_delete", new_callable=AsyncMock) as mock_del:
            mock_del.return_value = {"data": {"orderId": "ord-1", "symbol": "BTC-INR", "status": "CANCELLED"}}
            self.assertTrue(await self.exchange._place_cancel("ZEBtest", tracked))

    async def test_request_order_status_filled(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "FILLED",
                                              "filled": "0.001", "updatedAt": 1_700_000_001_000}}
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.FILLED, upd.new_state)

    async def test_request_order_status_partial(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "OPEN", "filled": "0.0005"}}
            upd = await self.exchange._request_order_status(tracked)
        self.assertEqual(OrderState.PARTIALLY_FILLED, upd.new_state)

    async def test_request_order_status_unknown_raises(self):
        tracked = InFlightOrder(
            client_order_id="ZEBtest", exchange_order_id="ord-1", trading_pair="BTC-INR",
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("0.001"),
            price=Decimal("3000000"), creation_timestamp=1_700_000_000.0,
        )
        with patch.object(self.exchange, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": {"orderId": "ord-1", "status": "WAT"}}
            with self.assertRaises(ValueError):
                await self.exchange._request_order_status(tracked)


if __name__ == "__main__":
    unittest.main()
