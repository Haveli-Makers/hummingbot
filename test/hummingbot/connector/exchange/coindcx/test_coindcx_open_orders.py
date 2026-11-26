"""CoindcxExchange.get_open_orders: every open order on the account, read from CoinDCX's active orders endpoint."""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from bidict import bidict

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
from hummingbot.core.data_type.common import OrderType


class CoindcxOpenOrdersTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.exchange = CoindcxExchange(coindcx_api_key="key", coindcx_api_secret="secret",
                                        trading_pairs=["ETH-INR", "BTC-INR"], trading_required=False)
        self.exchange._set_trading_pair_symbol_map(bidict({"ETHINR": "ETH-INR", "BTCINR": "BTC-INR"}))

    async def test_reads_each_pair_and_maps_every_field(self):
        responses = {
            "ETHINR": {"orders": [{
                "id": "4075000764", "client_order_id": "haveli-BEHIR1", "market": "ETHINR", "order_type": "limit_order",
                "side": "buy", "status": "open", "total_quantity": 0.0007, "remaining_quantity": 0.0005,
                "price_per_unit": 173845.8, "created_at": "2026-11-26T06:47:00.118Z"}]},
            "BTCINR": {"orders": [{
                "id": "4075000999", "client_order_id": "", "market": "BTCINR", "order_type": "limit_order",
                "side": "sell", "status": "partially_filled", "total_quantity": "0.001", "remaining_quantity": "0.001",
                "price_per_unit": "9000000", "created_at": 1795675620118}]},
        }
        self.exchange._api_post = AsyncMock(side_effect=lambda path_url, data, is_auth_required: responses[data["market"]])

        orders = await self.exchange.get_open_orders()

        calls = [call.kwargs for call in self.exchange._api_post.call_args_list]
        self.assertEqual([{"path_url": CONSTANTS.ACTIVE_ORDERS_PATH_URL, "data": {"market": "ETHINR"},
                           "is_auth_required": True},
                          {"path_url": CONSTANTS.ACTIVE_ORDERS_PATH_URL, "data": {"market": "BTCINR"},
                           "is_auth_required": True}], calls)
        eth, btc = orders
        self.assertEqual(("haveli-BEHIR1", "4075000764", "ETH-INR", "open"),
                         (eth.client_order_id, eth.exchange_order_id, eth.trading_pair, eth.status))
        self.assertTrue(eth.is_buy)
        self.assertEqual(OrderType.LIMIT, eth.order_type)
        self.assertEqual(Decimal("173845.8"), eth.price)
        self.assertEqual(Decimal("0.0007"), eth.amount)
        self.assertEqual(Decimal("0.0002"), eth.executed_amount)
        self.assertEqual(1795675620118, eth.time)  # the ISO string, as epoch ms
        self.assertFalse(btc.is_buy)
        self.assertEqual("", btc.client_order_id)  # an order placed outside hummingbot
        self.assertEqual(Decimal("0"), btc.executed_amount)
        self.assertEqual(1795675620118, btc.time)

    async def test_a_bare_list_and_an_empty_answer_both_work(self):
        bare_list = [{"id": "1", "side": "buy", "total_quantity": 1, "remaining_quantity": 1, "price_per_unit": 5}]
        self.exchange._api_post = AsyncMock(side_effect=[bare_list, {}])
        orders = await self.exchange.get_open_orders()
        self.assertEqual(["1"], [order.exchange_order_id for order in orders])
        self.assertEqual(0, orders[0].time)  # no created_at

    async def test_errors_reach_the_caller(self):
        self.exchange._api_post = AsyncMock(side_effect=IOError("HTTP status is 401"))
        with self.assertRaises(IOError):
            await self.exchange.get_open_orders()

    def test_epoch_ms_reads_every_time_format(self):
        self.assertEqual(1795675620118, CoindcxExchange._epoch_ms(1795675620118))
        self.assertEqual(1795675620000, CoindcxExchange._epoch_ms(1795675620))  # seconds
        self.assertEqual(1795675620118, CoindcxExchange._epoch_ms("1795675620118"))
        self.assertEqual(1795675620118, CoindcxExchange._epoch_ms("2026-11-26T06:47:00.118Z"))
        self.assertEqual(0, CoindcxExchange._epoch_ms(None))
        self.assertEqual(0, CoindcxExchange._epoch_ms("yesterday"))
