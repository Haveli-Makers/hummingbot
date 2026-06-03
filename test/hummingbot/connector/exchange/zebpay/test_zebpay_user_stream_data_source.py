import asyncio
import unittest
from unittest.mock import MagicMock

from bidict import bidict

from hummingbot.connector.exchange.zebpay.zebpay_api_user_stream_data_source import ZebpayAPIUserStreamDataSource
from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange


def _make_connector() -> ZebpayExchange:
    ex = ZebpayExchange(
        zebpay_api_key="key", zebpay_api_secret="secret",
        trading_pairs=["BTC-INR"], trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC-INR": "BTC-INR"}))
    return ex


class ZebpayUserStreamDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = ZebpayAPIUserStreamDataSource(
            auth=MagicMock(), trading_pairs=["BTC-INR"], connector=self.connector, api_factory=MagicMock(),
        )

    def test_extract_orders_paginated(self):
        resp = {"data": {"items": [{"orderId": "1"}, {"orderId": "2"}], "totalNum": 2}}
        self.assertEqual(2, len(ZebpayAPIUserStreamDataSource._extract_orders(resp)))

    def test_extract_orders_plain_list(self):
        self.assertEqual(1, len(ZebpayAPIUserStreamDataSource._extract_orders({"data": [{"orderId": "1"}]})))

    def test_extract_orders_empty(self):
        self.assertEqual([], ZebpayAPIUserStreamDataSource._extract_orders({"data": {"items": []}}))

    def test_last_recv_time_initial(self):
        self.assertEqual(0.0, self.source.last_recv_time)

    async def test_listen_emits_balance_event(self):
        output = asyncio.Queue()
        count = 0

        async def fake_api_get(path_url, **kwargs):
            nonlocal count
            count += 1
            if count == 1:
                return {"data": [{"currency": "BTC", "total": "1", "free": "1", "used": "0"}]}
            raise asyncio.CancelledError()

        self.connector._api_get = fake_api_get
        try:
            await asyncio.wait_for(self.source.listen_for_user_stream(output), timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        found = False
        while not output.empty():
            if output.get_nowait().get("event") == "balance_update":
                found = True
        self.assertTrue(found)

    async def test_subscribe_noops(self):
        await self.source._subscribe_to_user_stream()
        await self.source._unsubscribe_from_user_stream()


if __name__ == "__main__":
    unittest.main()
