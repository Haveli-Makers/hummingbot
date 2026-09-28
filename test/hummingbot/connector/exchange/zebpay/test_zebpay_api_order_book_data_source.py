import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.zebpay.zebpay_api_order_book_data_source import ZebpayAPIOrderBookDataSource
from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


def _make_connector() -> ZebpayExchange:
    ex = ZebpayExchange(
        zebpay_api_key="key", zebpay_api_secret="secret",
        trading_pairs=["BTC-INR"], trading_required=False,
    )
    ex._set_trading_pair_symbol_map(bidict({"BTC-INR": "BTC-INR"}))
    return ex


class ZebpayOrderBookDataSourceTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.connector = _make_connector()
        self.source = ZebpayAPIOrderBookDataSource(
            trading_pairs=["BTC-INR"], connector=self.connector, api_factory=MagicMock(),
        )

    def test_extract_depth_data_list_form(self):
        resp = {"data": {"bids": [["3000000", "0.5"]], "asks": [["3001000", "0.2"]]}}
        depth = ZebpayAPIOrderBookDataSource._extract_depth_data(resp)
        self.assertEqual([["3000000", "0.5"]], depth["bids"])
        self.assertEqual([["3001000", "0.2"]], depth["asks"])

    def test_extract_depth_data_dict_levels(self):
        resp = {"bids": [{"price": "100", "volume": "1"}], "asks": [{"price": "101", "quantity": "2"}]}
        depth = ZebpayAPIOrderBookDataSource._extract_depth_data(resp)
        self.assertEqual([["100", "1"]], depth["bids"])
        self.assertEqual([["101", "2"]], depth["asks"])

    async def test_order_book_snapshot(self):
        resp = {"bids": [["3000000", "0.5"]], "asks": [["3001000", "0.2"]]}
        with patch.object(self.connector, "_api_get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = resp
            msg = await self.source._order_book_snapshot("BTC-INR")
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-INR", msg.content["trading_pair"])
        self.assertEqual([["3000000", "0.5"]], msg.content["bids"])

    async def test_parse_snapshot_message(self):
        q = asyncio.Queue()
        await self.source._parse_order_book_snapshot_message(
            {"trading_pair": "BTC-INR", "bids": [["1", "1"]], "asks": [["2", "1"]],
             "timestamp": 1_700_000_000_000}, q)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, q.get_nowait().type)

    async def test_parse_snapshot_missing_pair_ignored(self):
        q = asyncio.Queue()
        await self.source._parse_order_book_snapshot_message({"bids": [], "asks": []}, q)
        self.assertTrue(q.empty())

    async def test_parse_trade_message(self):
        q = asyncio.Queue()
        await self.source._parse_trade_message(
            {"_trading_pair": "BTC-INR", "price": "3000000", "amount": "0.001",
             "id": "t1", "isBuyerMaker": False, "timestamp": 1_700_000_000_000}, q)
        self.assertEqual(OrderBookMessageType.TRADE, q.get_nowait().type)

    async def test_connected_websocket_raises(self):
        with self.assertRaises(NotImplementedError):
            await self.source._connected_websocket_assistant()

    async def test_get_last_traded_prices(self):
        with patch.object(self.connector, "_get_last_traded_price", new_callable=AsyncMock) as mock_p:
            mock_p.return_value = 3_000_000.0
            prices = await self.source.get_last_traded_prices(["BTC-INR"])
        self.assertEqual({"BTC-INR": 3_000_000.0}, prices)


class ZebpayOrderBookPollConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    """
    The depth poll runs at a 2s cadence. Fetching pairs one after another costs
    pairs x latency per cycle, so a serial loop self-throttles further behind its
    own interval as pairs are added.
    """

    PAIRS = ["BTC-INR", "ETH-INR", "XRP-INR"]

    def setUp(self):
        self.connector = _make_connector()
        self.connector._set_trading_pair_symbol_map(bidict({p: p for p in self.PAIRS}))
        self.source = ZebpayAPIOrderBookDataSource(
            trading_pairs=list(self.PAIRS), connector=self.connector, api_factory=MagicMock(),
        )

    @staticmethod
    def _concurrency_tracker(result):
        state = {"in_flight": 0, "peak": 0}

        async def tracked(*args, **kwargs):
            state["in_flight"] += 1
            state["peak"] = max(state["peak"], state["in_flight"])
            await asyncio.sleep(0.05)
            state["in_flight"] -= 1
            return result

        return tracked, state

    async def test_order_book_snapshots_fetched_concurrently(self):
        tracked, state = self._concurrency_tracker(
            {"bids": [["3000000", "0.5"]], "asks": [["3001000", "0.2"]]})
        self.source._request_order_book_snapshot = tracked
        await self.source._fetch_order_book_snapshots()
        self.assertEqual(len(self.PAIRS), state["peak"], "depth snapshots were fetched sequentially")
        self.assertEqual(len(self.PAIRS),
                         self.source._message_queue[self.source._snapshot_messages_queue_key].qsize())

    async def test_order_book_snapshot_failure_is_isolated(self):
        async def flaky(trading_pair):
            if trading_pair == "ETH-INR":
                raise IOError("boom")
            return {"bids": [["1", "1"]], "asks": [["2", "1"]]}

        self.source._request_order_book_snapshot = flaky
        await self.source._fetch_order_book_snapshots()
        self.assertEqual(len(self.PAIRS) - 1,
                         self.source._message_queue[self.source._snapshot_messages_queue_key].qsize())

    async def test_public_trades_fetched_concurrently(self):
        tracked, state = self._concurrency_tracker({"data": [{"id": "t1", "price": "1", "amount": "1"}]})
        self.connector._api_get = tracked
        await self.source._fetch_public_trades()
        self.assertEqual(len(self.PAIRS), state["peak"], "public trades were fetched sequentially")
        self.assertEqual(len(self.PAIRS),
                         self.source._message_queue[self.source._trade_messages_queue_key].qsize())


if __name__ == "__main__":
    unittest.main()
