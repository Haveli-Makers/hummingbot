from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.coinswitch_perpetual import (
    coinswitch_perpetual_constants as CONSTANTS,
    coinswitch_perpetual_web_utils as web_utils,
)
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory


class CoinswitchPerpetualWebUtilsTests(IsolatedAsyncioWrapperTestCase):
    def test_urls(self):
        self.assertEqual("https://coinswitch.co/trade/api/v2/futures/order",
                         web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL))
        self.assertEqual("https://coinswitch.co/trade/api/v2/time",
                         web_utils.public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL))
        self.assertEqual("https://ws.coinswitch.co", web_utils.wss_url())

    def test_urls_follow_patched_hosts(self):
        with patch.object(CONSTANTS, "REST_URL", "http://127.0.0.1:9000"):
            self.assertEqual("http://127.0.0.1:9000/trade/api/v2/futures/order",
                             web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL))

    def test_factory_without_and_with_proxy(self):
        factory = web_utils.build_api_factory()
        self.assertIsInstance(factory._connections_factory, ConnectionsFactory)
        proxied = web_utils.build_api_factory(proxy_url="http://user:pass@127.0.0.1:3128")
        self.assertEqual("ProxyConnectionsFactory", type(proxied._connections_factory).__name__)

    def test_every_limit_id_is_declared(self):
        declared = {limit.limit_id for limit in CONSTANTS.RATE_LIMITS}
        for limit_id in (CONSTANTS.CREATE_ORDER_LIMIT_ID, CONSTANTS.CANCEL_ORDER_LIMIT_ID,
                         CONSTANTS.ORDER_STATUS_LIMIT_ID, CONSTANTS.GET_LEVERAGE_LIMIT_ID,
                         CONSTANTS.SET_LEVERAGE_LIMIT_ID, CONSTANTS.OPEN_ORDERS_PATH_URL,
                         CONSTANTS.CLOSED_ORDERS_PATH_URL, CONSTANTS.POSITIONS_PATH_URL,
                         CONSTANTS.WALLET_BALANCE_PATH_URL, CONSTANTS.TRANSACTIONS_PATH_URL,
                         CONSTANTS.INSTRUMENT_INFO_PATH_URL, CONSTANTS.ORDER_BOOK_PATH_URL,
                         CONSTANTS.TICKER_PATH_URL, CONSTANTS.ALL_PAIRS_TICKER_PATH_URL,
                         CONSTANTS.SERVER_TIME_PATH_URL):
            self.assertIn(limit_id, declared)
        limits = {limit.limit_id: limit for limit in CONSTANTS.RATE_LIMITS}
        self.assertEqual(20, limits[CONSTANTS.CREATE_ORDER_LIMIT_ID].limit)
        self.assertEqual(10, limits[CONSTANTS.CANCEL_ORDER_LIMIT_ID].limit)
        self.assertEqual(60, limits[CONSTANTS.CANCEL_ORDER_LIMIT_ID].time_interval)

    def test_polling_stays_inside_the_budgets(self):
        limits = {limit.limit_id: limit for limit in CONSTANTS.RATE_LIMITS}
        per_minute = 60 / CONSTANTS.BALANCE_POLL_INTERVAL
        self.assertLess(per_minute, limits[CONSTANTS.WALLET_BALANCE_PATH_URL].limit)
        per_minute = 60 / CONSTANTS.OPEN_ORDERS_POLL_INTERVAL
        self.assertLess(per_minute, limits[CONSTANTS.OPEN_ORDERS_PATH_URL].limit)
        self.assertLess(CONSTANTS.POSITIONS_CALLS_PER_MINUTE, limits[CONSTANTS.POSITIONS_PATH_URL].limit)

    async def test_server_time(self):
        rest = MagicMock()
        rest.execute_request = AsyncMock(return_value={"serverTime": 1719905777483})
        factory = MagicMock()
        factory.get_rest_assistant = AsyncMock(return_value=rest)
        with patch.object(web_utils, "build_api_factory_without_time_synchronizer_pre_processor",
                          return_value=factory):
            self.assertEqual(1719905777483.0, await web_utils.get_current_server_time())
            kwargs = rest.execute_request.call_args.kwargs
            self.assertTrue(kwargs["url"].endswith("/trade/api/v2/time"))
            self.assertNotIn("is_auth_required", kwargs)

            rest.execute_request = AsyncMock(return_value={"data": {"serverTime": 5}})
            self.assertEqual(5.0, await web_utils.get_current_server_time())

            rest.execute_request = AsyncMock(return_value={"message": "nope"})
            with self.assertRaises(IOError):
                await web_utils.get_current_server_time()
