import asyncio
import os
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_api_order_book_data_source import (
    RESYNC_KEY,
    CoinDCXPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

SYMBOLS = {"BTC-USDT": "BTCUSDT", "ETH-USDT": "ETHUSDT"}


def _snapshot(last_update_id):
    return {"lastUpdateId": last_update_id, "E": 1, "T": 1,
            "bids": [["67070.0", "9.866"]], "asks": [["67070.2", "9.866"]]}


def _diff(first, last, previous, symbol="BTCUSDT"):
    return {"e": "depthUpdate", "E": 1714003200000, "s": symbol, "U": first, "u": last, "pu": previous,
            "b": [["68450.00", "1.250"]], "a": [["68450.20", "0.800"]]}


class CoinDCXPerpetualAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    def setUp(self):
        super().setUp()
        self.log_records = []
        self.connector = MagicMock()

        async def to_symbol(trading_pair):
            return SYMBOLS[trading_pair]

        async def to_trading_pair(symbol):
            for trading_pair, exchange_symbol in SYMBOLS.items():
                if exchange_symbol == symbol:
                    return trading_pair
            raise KeyError(symbol)

        self.connector.exchange_symbol_associated_to_pair = AsyncMock(side_effect=to_symbol)
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(side_effect=to_trading_pair)
        self.connector._api_get = AsyncMock(return_value=_snapshot(100))
        self.api_factory = MagicMock()
        self.ds = CoinDCXPerpetualAPIOrderBookDataSource(
            trading_pairs=list(SYMBOLS), connector=self.connector, api_factory=self.api_factory)
        self.ds.logger().setLevel(1)
        self.ds.logger().addHandler(self)
        self.diffs = asyncio.Queue()
        self.ds._diff_output = self.diffs

    def handle(self, record):
        self.log_records.append(record)

    def _logged(self, level, fragment):
        return any(record.levelname == level and fragment in record.getMessage() for record in self.log_records)

    def _released(self):
        released = []
        while not self.diffs.empty():
            released.append(self.diffs.get_nowait())
        return released

    def _resync_requests(self):
        queue = self.ds._message_queue[self.ds._snapshot_messages_queue_key]
        requests = []
        while not queue.empty():
            requests.append(queue.get_nowait())
        return requests

    async def _parse_diff(self, diff):
        await self.ds._parse_order_book_diff_message(diff, self.diffs)

    async def _synced_at(self, snapshot_id, *diffs):
        """Snapshot at ``snapshot_id`` and release ``diffs`` after it."""
        self.connector._api_get.return_value = _snapshot(snapshot_id)
        await self.ds._order_book_snapshot("BTC-USDT")
        for diff in diffs:
            await self._parse_diff(diff)
        self._released()

    # ---- routing / subscription -------------------------------------------------

    def test_frames_are_routed_by_event_type(self):
        self.assertEqual(self.ds._diff_messages_queue_key, self.ds._channel_originating_message(_diff(1, 2, 0)))
        self.assertEqual(self.ds._trade_messages_queue_key,
                         self.ds._channel_originating_message({"e": "aggTrade", "s": "BTCUSDT"}))
        self.assertEqual(self.ds._funding_info_messages_queue_key,
                         self.ds._channel_originating_message({"e": "markPriceUpdate", "s": "BTCUSDT"}))
        # A combined-stream wrapper routes on the event inside it.
        self.assertEqual(self.ds._diff_messages_queue_key, self.ds._channel_originating_message(
            {"stream": "btcusdt@depth", "data": _diff(1, 2, 0)}))
        # The subscription acknowledgement is not a market data frame.
        self.assertEqual("", self.ds._channel_originating_message({"result": None, "id": 1}))

    async def test_one_subscribe_request_covers_every_stream_of_every_pair(self):
        ws = AsyncMock()
        await self.ds._subscribe_channels(ws)

        ws.send.assert_awaited_once()
        self.assertEqual({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth", "btcusdt@aggTrade", "btcusdt@markPrice",
                       "ethusdt@depth", "ethusdt@aggTrade", "ethusdt@markPrice"],
            "id": 1,
        }, ws.send.call_args.args[0].payload)

    async def test_connects_to_the_multiplexed_stream(self):
        ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=ws)
        with patch.dict(os.environ, {CONSTANTS.WSS_URL_ENV_VAR: "wss://stream.example"}):
            self.assertIs(ws, await self.ds._connected_websocket_assistant())
        ws.connect.assert_awaited_once_with(ws_url="wss://stream.example/ws",
                                            ping_timeout=CONSTANTS.PUBLIC_WS_HEARTBEAT_INTERVAL)

    async def test_rejected_requests_are_logged_as_errors(self):
        await self.ds._process_message_for_unknown_channel(
            {"error": {"code": 2, "msg": "Invalid request"}, "id": 1}, websocket_assistant=None)
        self.assertTrue(self._logged("ERROR", "rejected a request"))

    async def test_acknowledgements_and_unknown_frames_are_only_debug_logged(self):
        await self.ds._process_message_for_unknown_channel({"result": None, "id": 1}, websocket_assistant=None)
        await self.ds._process_message_for_unknown_channel({"e": "somethingNew"}, websocket_assistant=None)
        self.assertTrue(self._logged("DEBUG", "acknowledged a request"))
        self.assertTrue(self._logged("DEBUG", "unrecognised"))

    async def test_a_failed_subscription_is_logged_and_raised(self):
        ws = AsyncMock()
        ws.send.side_effect = ConnectionError("closed")
        with self.assertRaises(ConnectionError):
            await self.ds._subscribe_channels(ws)
        self.assertTrue(self._logged("ERROR", "Unexpected error subscribing"))

    async def test_frames_without_a_symbol_are_dropped(self):
        queue = asyncio.Queue()
        await self.ds._parse_trade_message({"e": "aggTrade", "a": 1, "p": "1", "q": "1"}, queue)
        self.assertTrue(queue.empty())

    # ---- snapshot -----------------------------------------------------------------

    async def test_snapshot_is_requested_for_the_exchange_symbol(self):
        message = await self.ds._order_book_snapshot("BTC-USDT")

        self.connector._api_get.assert_awaited_once_with(
            path_url=CONSTANTS.DEPTH_PATH_URL, params={"symbol": "BTCUSDT", "limit": 500})
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(100, message.update_id)
        self.assertEqual("BTC-USDT", message.trading_pair)

    # ---- local order book protocol -----------------------------------------------

    async def test_diffs_wait_for_the_snapshot_and_resume_right_after_it(self):
        for diff in (_diff(95, 98, 94), _diff(99, 101, 98), _diff(102, 104, 101)):
            await self._parse_diff(diff)
        self.assertEqual([], self._released(), "nothing can be applied before the snapshot")

        await self.ds._order_book_snapshot("BTC-USDT")

        # u=98 is already in the snapshot at 100; the rest follow on from it.
        self.assertEqual([101, 104], [message.update_id for message in self._released()])
        self.assertTrue(self.ds._book_syncs["BTC-USDT"].synced)
        self.assertEqual([], self._resync_requests())

    async def test_diffs_after_sync_are_released_while_the_chain_holds(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self._parse_diff(_diff(102, 104, 101))
        await self._parse_diff(_diff(105, 107, 104))
        self.assertEqual([104, 107], [message.update_id for message in self._released()])

    async def test_a_stream_resuming_past_the_snapshot_forces_a_new_one(self):
        await self.ds._order_book_snapshot("BTC-USDT")
        await self._parse_diff(_diff(105, 107, 104))

        self.assertEqual([], self._released())
        self.assertEqual([{RESYNC_KEY: "BTC-USDT"}], self._resync_requests())
        self.assertTrue(self._logged("WARNING", "Rebuilding the BTC-USDT order book"))

    async def test_a_broken_pu_chain_holds_the_diff_and_forces_a_new_snapshot(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self._parse_diff(_diff(103, 105, 103))  # pu=103, but the last u was 101

        self.assertEqual([], self._released())
        self.assertFalse(self.ds._book_syncs["BTC-USDT"].synced)
        self.assertEqual([{RESYNC_KEY: "BTC-USDT"}], self._resync_requests())

    async def test_a_resync_rebuilds_the_book_then_releases_the_held_diffs(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self._parse_diff(_diff(103, 105, 103))
        await self._parse_diff(_diff(106, 108, 105))
        self._resync_requests()

        self.connector._api_get.return_value = _snapshot(104)
        snapshots = asyncio.Queue()
        await self.ds._parse_order_book_snapshot_message({RESYNC_KEY: "BTC-USDT"}, snapshots)

        self.assertEqual(104, snapshots.get_nowait().update_id)
        self.assertEqual([105, 108], [message.update_id for message in self._released()])
        book_sync = self.ds._book_syncs["BTC-USDT"]
        self.assertTrue(book_sync.synced)
        self.assertFalse(book_sync.resync_pending)

    async def test_only_one_resync_is_requested_while_one_is_pending(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self._parse_diff(_diff(103, 105, 103))
        await self._parse_diff(_diff(109, 110, 108))
        self.assertEqual(1, len(self._resync_requests()))

    async def test_a_break_inside_the_held_diffs_releases_up_to_it_then_resyncs(self):
        for diff in (_diff(99, 101, 98), _diff(103, 105, 103), _diff(106, 108, 105)):
            await self._parse_diff(diff)

        await self.ds._order_book_snapshot("BTC-USDT")

        self.assertEqual([101], [message.update_id for message in self._released()])
        self.assertEqual([{RESYNC_KEY: "BTC-USDT"}], self._resync_requests())
        self.assertEqual([105, 108], [int(diff["u"]) for diff in self.ds._book_syncs["BTC-USDT"].buffer])

    async def test_a_periodic_snapshot_during_a_resync_does_not_queue_another(self):
        await self.ds._order_book_snapshot("BTC-USDT")
        await self._parse_diff(_diff(105, 107, 104))
        self.assertEqual(1, len(self._resync_requests()))

        # The hourly snapshot lands before the requested one and still leaves a gap.
        await self.ds._order_book_snapshot("BTC-USDT")
        self.assertEqual([], self._resync_requests())

    async def test_resync_requests_for_untracked_pairs_are_ignored(self):
        output = asyncio.Queue()
        await self.ds._parse_order_book_snapshot_message({RESYNC_KEY: "XRP-USDT"}, output)
        await self.ds._parse_order_book_snapshot_message({}, output)
        self.assertTrue(output.empty())
        self.connector._api_get.assert_not_awaited()

    async def test_resyncs_of_one_pair_are_spaced_out(self):
        self.ds._sleep = AsyncMock()
        self.ds._time = MagicMock(return_value=1000.0)
        self.ds._book_syncs["BTC-USDT"].last_resync_request = 998.0

        await self.ds._parse_order_book_snapshot_message({RESYNC_KEY: "BTC-USDT"}, asyncio.Queue())

        self.ds._sleep.assert_awaited_once_with(CONSTANTS.MIN_RESYNC_INTERVAL - 2.0)

    async def test_a_reconnect_rechecks_every_book_against_a_snapshot(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self.ds._on_order_stream_interruption(websocket_assistant=None)
        self.assertFalse(self.ds._book_syncs["BTC-USDT"].synced)

        # Updates were missed while disconnected, so the stream no longer meets the snapshot.
        await self._parse_diff(_diff(150, 152, 149))
        self.assertEqual([], self._released())
        self.assertEqual([{RESYNC_KEY: "BTC-USDT"}], self._resync_requests())

    async def test_diffs_without_pu_are_not_chain_checked(self):
        await self._synced_at(100, _diff(99, 101, 98))
        unchained = _diff(110, 112, 0)
        del unchained["pu"]
        await self._parse_diff(unchained)
        self.assertEqual([112], [message.update_id for message in self._released()])

    async def test_held_diffs_are_bounded(self):
        for index in range(CONSTANTS.MAX_BUFFERED_DIFFS + 10):
            await self._parse_diff(_diff(index * 2, index * 2 + 1, index * 2 - 1))
        self.assertEqual(CONSTANTS.MAX_BUFFERED_DIFFS, len(self.ds._book_syncs["BTC-USDT"].buffer))

    async def test_pairs_are_synchronised_independently(self):
        await self._synced_at(100, _diff(99, 101, 98))
        await self._parse_diff(_diff(5, 7, 4, symbol="ETHUSDT"))
        self.assertEqual([], self._released(), "ETH-USDT has no snapshot yet")
        self.assertTrue(self.ds._book_syncs["BTC-USDT"].synced)

    # ---- trades and funding -------------------------------------------------------

    async def test_trade_frame_becomes_a_trade_message(self):
        trades = asyncio.Queue()
        await self.ds._parse_trade_message(
            {"e": "aggTrade", "E": 1714003200000, "s": "BTCUSDT", "a": 5933014,
             "p": "68450.23", "q": "1.500", "T": 1714003199500, "m": True}, trades)

        message = trades.get_nowait()
        self.assertEqual("BTC-USDT", message.trading_pair)
        self.assertEqual(float(TradeType.SELL.value), message.content["trade_type"])
        self.assertEqual(5933014, message.trade_id)

    async def test_frames_for_unknown_symbols_are_dropped(self):
        queue = asyncio.Queue()
        await self.ds._parse_trade_message({"e": "aggTrade", "s": "XRPUSDT", "a": 1, "p": "1", "q": "1"}, queue)
        await self.ds._parse_funding_info_message({"e": "markPriceUpdate", "s": "XRPUSDT"}, queue)
        await self.ds._parse_order_book_diff_message(_diff(1, 2, 0, symbol="XRPUSDT"), queue)
        self.assertTrue(queue.empty())

    async def test_mark_price_frame_becomes_a_funding_update(self):
        updates = asyncio.Queue()
        await self.ds._parse_funding_info_message(
            {"e": "markPriceUpdate", "E": 1714003200000, "s": "BTCUSDT", "p": "68452.15000000",
             "i": "68440.62659091", "r": "0.00038167", "T": 1714032000000}, updates)

        self.assertEqual(FundingInfoUpdate(
            trading_pair="BTC-USDT",
            index_price=Decimal("68440.62659091"),
            mark_price=Decimal("68452.15000000"),
            next_funding_utc_timestamp=1714032000,
            rate=Decimal("0.00038167"),
        ), updates.get_nowait())

    async def test_funding_info_and_last_prices_come_from_the_connector(self):
        self.connector.build_funding_info = AsyncMock(return_value="info")
        self.connector.get_last_traded_prices = AsyncMock(return_value={"BTC-USDT": 1.0})
        self.assertEqual("info", await self.ds.get_funding_info("BTC-USDT"))
        self.assertEqual({"BTC-USDT": 1.0}, await self.ds.get_last_traded_prices(["BTC-USDT"]))

    # ---- through the base-class consumer loops ------------------------------------

    async def _consume(self, listener, queue_key, raw, output):
        self.ds._message_queue[queue_key].put_nowait(raw)
        task = asyncio.create_task(listener(output))
        try:
            return await asyncio.wait_for(output.get(), timeout=1)
        finally:
            task.cancel()

    async def test_raw_frames_survive_the_base_class_consumer_loops(self):
        loop = asyncio.get_running_loop()
        trade = await self._consume(
            lambda out: self.ds.listen_for_trades(loop, out), self.ds._trade_messages_queue_key,
            {"e": "aggTrade", "s": "BTCUSDT", "a": 1, "p": "1", "q": "2", "T": 1, "m": False}, asyncio.Queue())
        self.assertEqual("BTC-USDT", trade.trading_pair)

        funding = await self._consume(
            self.ds.listen_for_funding_info, self.ds._funding_info_messages_queue_key,
            {"e": "markPriceUpdate", "s": "ETHUSDT", "p": "1", "i": "1", "r": "0", "T": 1000}, asyncio.Queue())
        self.assertEqual("ETH-USDT", funding.trading_pair)

        await self.ds._order_book_snapshot("BTC-USDT")
        diff = await self._consume(
            lambda out: self.ds.listen_for_order_book_diffs(loop, out), self.ds._diff_messages_queue_key,
            _diff(99, 101, 98), asyncio.Queue())
        self.assertEqual(101, diff.update_id)
