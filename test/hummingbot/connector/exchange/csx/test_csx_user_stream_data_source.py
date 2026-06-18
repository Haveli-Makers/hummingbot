import asyncio
import unittest
from unittest.mock import MagicMock

from bidict import bidict

from hummingbot.connector.exchange.csx.csx_api_user_stream_data_source import CsxAPIUserStreamDataSource
from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange

_VALID_SECRET = "aa" * 32


def _make_connector() -> CsxExchange:
    ex = CsxExchange(
        csx_api_key="key",
        csx_api_secret=_VALID_SECRET,
        trading_pairs=["BTC-INR"],
        trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC/INR": "BTC-INR"}))
    return ex


class CsxUserStreamDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = CsxAPIUserStreamDataSource(
            auth=MagicMock(),
            trading_pairs=["BTC-INR"],
            connector=self.connector,
            api_factory=MagicMock(),
        )

    async def test_listen_for_user_stream_emits_balance_event(self):
        output = asyncio.Queue()
        balance_resp = {"Available": {"BTC": "1.0"}, "Locked": {}}

        call_count = 0

        async def fake_api_get(path_url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return balance_resp
            if call_count == 2:
                return {"orders": []}
            raise asyncio.CancelledError()

        self.connector._api_get = fake_api_get

        try:
            await asyncio.wait_for(self.source.listen_for_user_stream(output), timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        # At least the balance event should have been put
        found_balance = False
        while not output.empty():
            event = output.get_nowait()
            if event.get("event") == "balance_update":
                found_balance = True
        self.assertTrue(found_balance)

    async def test_subscribe_and_unsubscribe_are_noops(self):
        # Should complete without error
        await self.source._subscribe_to_user_stream()
        await self.source._unsubscribe_from_user_stream()

    def test_last_recv_time_initial_value(self):
        self.assertEqual(0.0, self.source.last_recv_time)


if __name__ == "__main__":
    unittest.main()
