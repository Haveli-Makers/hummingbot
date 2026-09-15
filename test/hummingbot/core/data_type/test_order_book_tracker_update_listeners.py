import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker


class OrderBookTrackerUpdateListenerTests(IsolatedAsyncioWrapperTestCase):
    """The tracker's order book update hook, which the MQTT market data publisher relies on."""

    trading_pair = "COINALPHA-HBOT"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.tracker = OrderBookTracker(data_source=MagicMock(), trading_pairs=[self.trading_pair])
        self.tracker._order_books[self.trading_pair] = OrderBook()
        self.queue = asyncio.Queue()
        self.tracker._tracking_message_queues[self.trading_pair] = self.queue
        self.tracking_task = asyncio.ensure_future(self.tracker._track_single_book(self.trading_pair))

    async def asyncTearDown(self):
        self.tracking_task.cancel()
        await super().asyncTearDown()

    @property
    def order_book(self) -> OrderBook:
        return self.tracker.order_books[self.trading_pair]

    def _message(self, message_type: OrderBookMessageType, update_id: int) -> OrderBookMessage:
        return OrderBookMessage(message_type, {
            "trading_pair": self.trading_pair,
            "update_id": update_id,
            "bids": [[10.0, 1.0]],
            "asks": [[11.0, 1.0]],
        }, 1.0)

    async def _process(self, *messages: OrderBookMessage):
        for message in messages:
            self.queue.put_nowait(message)
        for _ in range(20):
            await asyncio.sleep(0)

    async def test_listener_runs_after_each_diff_is_applied(self):
        seen = []
        self.tracker.add_order_book_update_listener(lambda pair: seen.append((pair, self.order_book.last_diff_uid)))

        await self._process(self._message(OrderBookMessageType.DIFF, 1), self._message(OrderBookMessageType.DIFF, 2))

        # The book already holds each diff by the time the listener hears about it
        self.assertEqual([(self.trading_pair, 1), (self.trading_pair, 2)], seen)

    async def test_listener_runs_after_a_snapshot_is_restored(self):
        seen = []
        self.tracker.add_order_book_update_listener(lambda pair: seen.append((pair, self.order_book.snapshot_uid)))

        await self._process(self._message(OrderBookMessageType.SNAPSHOT, 5))

        self.assertEqual([(self.trading_pair, 5)], seen)

    async def test_failing_listener_neither_stops_tracking_nor_other_listeners(self):
        healthy = MagicMock()
        self.tracker.add_order_book_update_listener(MagicMock(side_effect=Exception("boom")))
        self.tracker.add_order_book_update_listener(healthy)

        await self._process(self._message(OrderBookMessageType.DIFF, 1), self._message(OrderBookMessageType.DIFF, 2))

        self.assertEqual(2, healthy.call_count)
        # Had the error escaped into the tracking loop, it would have paused the pair for 5 seconds
        # and the second diff would still be waiting.
        self.assertEqual(2, self.order_book.last_diff_uid)

    async def test_removed_listener_is_not_called(self):
        listener = MagicMock()
        self.tracker.add_order_book_update_listener(listener)
        self.tracker.remove_order_book_update_listener(listener)

        await self._process(self._message(OrderBookMessageType.DIFF, 1))

        listener.assert_not_called()

    async def test_listener_added_twice_is_called_once(self):
        listener = MagicMock()
        self.tracker.add_order_book_update_listener(listener)
        self.tracker.add_order_book_update_listener(listener)

        await self._process(self._message(OrderBookMessageType.DIFF, 1))

        listener.assert_called_once_with(self.trading_pair)

    async def test_tracking_works_with_no_listeners(self):
        await self._process(self._message(OrderBookMessageType.DIFF, 3))

        self.assertEqual(3, self.order_book.last_diff_uid)
