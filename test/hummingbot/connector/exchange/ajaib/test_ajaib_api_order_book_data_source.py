from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_api_order_book_data_source import AjaibAPIOrderBookDataSource
from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange

DEPTH = {"lastUpdateId": 186627707, "e": "depth", "s": "BTC_IDR",
         "bids": [["1383456000", "0.35"]], "asks": [["1384000000", "0.20"]]}
EXCHANGE_INFO = {"symbols": [{
    "symbol": "BTC_IDR", "baseAsset": "BTC", "quoteAsset": "IDR", "isSpotTradingAllowed": True,
    "filters": [{"filterType": "LOT_SIZE", "minQty": "0.000001", "maxQty": "100", "stepSize": "0.000001"}],
}]}


class AjaibOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = AjaibExchange(ajaib_api_key="k", ajaib_api_secret="s",
                                      trading_pairs=["BTC-IDR"], trading_required=False)
        self.data_source = AjaibAPIOrderBookDataSource(
            trading_pairs=["BTC-IDR"], connector=self.exchange,
            api_factory=self.exchange._web_assistants_factory)
        self.data_source._sleep = AsyncMock()

    def _load_symbol_map(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(EXCHANGE_INFO)

    # ── seeding the book ──────────────────────────────────────────────────────

    async def test_snapshot_seeds_the_book_from_rest_depth(self):
        self._load_symbol_map()
        self.exchange._api_get = AsyncMock(return_value=DEPTH)

        message = await self.data_source._order_book_snapshot("BTC-IDR")

        self.assertEqual(DEPTH["bids"], message.content["bids"])
        self.assertEqual(DEPTH["lastUpdateId"], message.content["update_id"])
        params = self.exchange._api_get.call_args.kwargs["params"]
        self.assertEqual({"symbol": "BTC_IDR", "limit": CONSTANTS.DEPTH_SNAPSHOT_LIMIT}, params)

    async def test_symbol_map_failing_to_load_at_startup_is_retried(self):
        """
        The failure seen live on 2026-10-01: exchange-info did not load, the map
        came back empty, the lookup raised KeyError -- and because the tracker
        never retries, the connector hung forever.
        """
        self.exchange._make_trading_pairs_request = AsyncMock(
            side_effect=[IOError("proxy dropped the connection"), EXCHANGE_INFO])
        self.exchange._api_get = AsyncMock(return_value=DEPTH)

        message = await self.data_source._order_book_snapshot("BTC-IDR")

        self.assertEqual(DEPTH["bids"], message.content["bids"])
        self.assertEqual(1, self.data_source._sleep.await_count)

    async def test_failed_depth_request_is_retried(self):
        self._load_symbol_map()
        self.exchange._api_get = AsyncMock(side_effect=[IOError("HTTP status is 503"), DEPTH])

        message = await self.data_source._order_book_snapshot("BTC-IDR")

        self.assertEqual(DEPTH["asks"], message.content["asks"])

    async def test_unlisted_pair_fails_fast_with_a_clear_message(self):
        self._load_symbol_map()
        self.exchange._api_get = AsyncMock(return_value=DEPTH)

        with self.assertRaisesRegex(ValueError, "ELIZAOS-IDR is not listed on Ajaib"):
            await self.data_source._order_book_snapshot("ELIZAOS-IDR")

        self.exchange._api_get.assert_not_called()
        self.data_source._sleep.assert_not_called()

    async def test_persistent_failure_starts_an_empty_book_instead_of_killing_the_connector(self):
        self._load_symbol_map()
        self.exchange._api_get = AsyncMock(side_effect=IOError("HTTP status is 503"))

        with self.assertLogs(self.data_source.logger(), level="WARNING") as logs:
            message = await self.data_source._order_book_snapshot("BTC-IDR")

        self.assertEqual(([], []), (message.content["bids"], message.content["asks"]))
        self.assertEqual(CONSTANTS.DEPTH_SNAPSHOT_MAX_ATTEMPTS, self.exchange._api_get.await_count)
        self.assertTrue(any("Starting it empty" in line for line in logs.output))

    # ── streams ───────────────────────────────────────────────────────────────

    async def test_connects_at_ws_listen_key_not_bare_ws(self):
        """A bare /ws is rejected 401; every stream lives at /ws/<listenKey>."""
        ws = MagicMock(connect=AsyncMock())
        self.data_source._get_listen_key = AsyncMock(return_value="LISTENKEY")
        self.data_source._api_factory = MagicMock(get_ws_assistant=AsyncMock(return_value=ws))

        await self.data_source._connected_websocket_assistant()

        self.assertTrue(ws.connect.call_args.kwargs["ws_url"].endswith("/ws/LISTENKEY"))

    async def test_subscribes_with_uppercase_symbol_and_the_partial_depth_stream(self):
        """
        Ajaib matches symbols case-sensitively, and bare @depth is a top-of-book
        feed the snapshot parser cannot read -- the book stream is @depth20.
        """
        ws = MagicMock(send=AsyncMock())

        await self.data_source._subscribe_channels(ws)

        self.assertEqual(["BTC_IDR@depth20", "BTC_IDR@trade"], ws.send.call_args.args[0].payload["params"])

    def test_frames_route_on_the_payload_event_not_the_stream_name(self):
        """Subscribed as @depth20, but every frame is tagged e="depth"."""
        route = self.data_source._channel_originating_message
        self.assertEqual(self.data_source._snapshot_messages_queue_key, route({"e": "depth"}))
        self.assertEqual(self.data_source._trade_messages_queue_key, route({"e": "trade"}))
        self.assertEqual("", route({"u": 1, "s": "BTC_IDR", "b": "1", "a": "2"}))  # bookTicker shape
