import asyncio
import logging
import os
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from types import SimpleNamespace
from typing import List
from unittest import TestCase
from unittest.mock import patch

import aiohttp

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.pubsub import PubSub
from hummingbot.remote_iface.influxdb import ORDER_EVENTS, InfluxDBConfig, InfluxDBSink, InfluxDBWriter, number, to_line
from hummingbot.strategy_v2.models.executors_info import PerformanceReport

CONFIG = InfluxDBConfig(url="http://influx.test:8181", token="apiv3_test", database="hbot")


class FakeWriter:
    def __init__(self):
        self.lines: List[str] = []
        self.started = self.closed = False
        self.flushes = 0

    def add(self, line):
        if line:
            self.lines.append(line)

    def start(self):
        self.started = True

    async def flush(self):
        self.flushes += 1

    async def close(self, timeout: float = 3.0):
        self.closed = True

    def table(self, name: str) -> List[str]:
        return [line for line in self.lines if line.startswith(name + ",")]


class FakeConnector(PubSub):
    def __init__(self, name: str = "binance", pairs=("BTC-USDT",)):
        super().__init__()
        self.name = name
        self.trading_pairs = list(pairs)
        self.in_flight_orders = {}
        self.prices = {pair: (Decimal("99.9"), Decimal("100.1")) for pair in pairs}

    def get_price(self, pair: str, is_buy: bool) -> Decimal:
        bid, ask = self.prices[pair]
        return ask if is_buy else bid


class PaperConnector(FakeConnector):
    """Like PaperTradeExchange: no in-flight orders (reading them raises), open orders in limit_orders."""

    @property
    def in_flight_orders(self):
        raise NotImplementedError

    @in_flight_orders.setter
    def in_flight_orders(self, value):
        pass


class FakeResponse:
    def __init__(self, status: int, text: str = ""):
        self.status, self._text = status, text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Stands in for aiohttp.ClientSession: each post takes the next outcome (a status or an exception)."""

    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.posts = []
        self.closed = False

    def post(self, url, data):
        self.posts.append((url, data.decode()))
        outcome = self.outcomes.pop(0) if self.outcomes else 204
        if isinstance(outcome, Exception):
            raise outcome
        return FakeResponse(outcome, "line 1: invalid field")

    async def close(self):
        self.closed = True


def in_flight(order_id: str, side: TradeType, price: str, amount: str, state=OrderState.OPEN) -> InFlightOrder:
    return InFlightOrder(client_order_id=order_id, trading_pair="BTC-USDT", order_type=OrderType.LIMIT,
                         trade_type=side, amount=Decimal(amount), creation_timestamp=time.time() - 30,
                         price=Decimal(price), initial_state=state)


def fill(order_id: str, side: TradeType, price: str, amount: str) -> OrderFilledEvent:
    return OrderFilledEvent(timestamp=time.time(), order_id=order_id, trading_pair="BTC-USDT", trade_type=side,
                            order_type=OrderType.LIMIT, price=Decimal(price), amount=Decimal(amount),
                            trade_fee=AddedToCostTradeFee())


class HelpersTest(TestCase):
    def test_to_line(self):
        line = to_line("bot_price", {"bot": "grid one", "exchange": "binance", "empty": ""},
                       {"best_bid": 99.5, "seen": 1, "note": 'say "hi"', "ok": True, "missing": float("nan")}, 7)
        self.assertEqual('bot_price,bot=grid\\ one,exchange=binance best_bid=99.5,seen=1i,note="say \\"hi\\"",ok=true 7',
                         line)
        self.assertIsNone(to_line("t", {"bot": "b"}, {"x": None, "y": float("inf")}, 1))

    def test_number(self):
        self.assertEqual(1.5, number(Decimal("1.5")))
        self.assertEqual(2.0, number(2))
        self.assertIsNone(number(Decimal("NaN")))
        self.assertIsNone(number(float("inf")))
        self.assertIsNone(number(True))
        self.assertIsNone(number("1.5"))
        self.assertIsNone(number(None))

    def test_config_from_env(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(InfluxDBConfig.from_env())
        with patch.dict(os.environ, {"INFLUXDB_URL": "http://localhost:8181"}, clear=True):
            self.assertIsNone(InfluxDBConfig.from_env(), "a URL without a token is not enough")
        with patch.dict(os.environ, {"INFLUXDB_URL": "http://localhost:8181/", "INFLUXDB_TOKEN": "apiv3_x"}, clear=True):
            config = InfluxDBConfig.from_env()
            self.assertEqual("http://localhost:8181", config.url)
            self.assertEqual("hbot", config.database)
            self.assertEqual("http://localhost:8181/api/v3/write_lp?db=hbot&precision=nanosecond", config.write_url)
        with patch.dict(os.environ, {"INFLUXDB_URL": "http://h:1", "INFLUXDB_TOKEN": "t", "INFLUXDB_DATABASE": "other"},
                        clear=True):
            self.assertEqual("other", InfluxDBConfig.from_env().database)


class InfluxDBWriterTest(IsolatedAsyncioWrapperTestCase):
    level = 0  # this test case is also the log handler

    def setUp(self):
        super().setUp()
        self.log_records = []
        InfluxDBWriter.logger().setLevel(logging.DEBUG)
        InfluxDBWriter.logger().addHandler(self)

    def tearDown(self):
        InfluxDBWriter.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def warnings(self) -> List[str]:
        return [record.getMessage() for record in self.log_records if record.levelname == "WARNING"]

    def writer(self, outcomes=(), **kwargs) -> InfluxDBWriter:
        writer = InfluxDBWriter(CONFIG, **kwargs)
        writer._session = FakeSession(outcomes)
        return writer

    async def test_flush_writes_everything_in_batches(self):
        writer = self.writer(batch_size=2)
        for i in range(5):
            writer.add(f"t,bot=b x=1i {i}")
        writer.add(None)
        await writer.flush()
        self.assertEqual(0, writer.pending)
        self.assertEqual(3, len(writer._session.posts))
        self.assertEqual(CONFIG.write_url, writer._session.posts[0][0])
        self.assertEqual("t,bot=b x=1i 0\nt,bot=b x=1i 1", writer._session.posts[0][1])

    async def test_unreachable_keeps_the_points_and_retries(self):
        writer = self.writer(outcomes=[aiohttp.ClientConnectionError("refused"), 204])
        for i in range(3):
            writer.add(f"t,bot=b x=1i {i}")
        await writer.flush()
        self.assertEqual(3, writer.pending)
        self.assertTrue(any("unreachable" in message for message in self.warnings()))
        await writer.flush()
        self.assertEqual(0, writer.pending)
        self.assertEqual("t,bot=b x=1i 0\nt,bot=b x=1i 1\nt,bot=b x=1i 2", writer._session.posts[-1][1])

    async def test_a_full_buffer_keeps_the_newest_points(self):
        writer = self.writer(max_buffer=3)
        for i in range(5):
            writer.add(f"t,bot=b x=1i {i}")
        self.assertEqual(["t,bot=b x=1i 2", "t,bot=b x=1i 3", "t,bot=b x=1i 4"], list(writer._buffer))

    async def test_rejected_points_are_dropped(self):
        writer = self.writer(outcomes=[400])
        writer.add("t,bot=b x=1i 1")
        await writer.flush()
        self.assertEqual(0, writer.pending)
        self.assertTrue(any("rejected 1 points (HTTP 400)" in message for message in self.warnings()))

    async def test_refused_token_keeps_the_points_and_says_so(self):
        writer = self.writer(outcomes=[401])
        writer.add("t,bot=b x=1i 1")
        await writer.flush()
        self.assertEqual(1, writer.pending)
        self.assertTrue(any("INFLUXDB_TOKEN" in message for message in self.warnings()))

    async def test_warnings_are_rate_limited(self):
        writer = self.writer(outcomes=[aiohttp.ClientConnectionError(), aiohttp.ClientConnectionError()],
                             warn_every=60)
        writer.add("t,bot=b x=1i 1")
        await writer.flush()
        await writer.flush()
        self.assertEqual(1, len(self.warnings()))

    async def test_start_flushes_in_the_background_and_close_writes_the_rest(self):
        writer = self.writer(flush_interval=0.01)
        writer.start()
        writer.add("t,bot=b x=1i 1")
        await asyncio.sleep(0.05)
        self.assertEqual(1, len(writer._session.posts))
        writer.add("t,bot=b x=1i 2")
        session = writer._session
        await writer.close()
        self.assertEqual(0, writer.pending)
        self.assertTrue(session.closed)


class InfluxDBSinkTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.writer = FakeWriter()
        self.connector = FakeConnector("binance", ("BTC-USDT",))
        self.urlopen = patch("hummingbot.remote_iface.influxdb.urllib.request.urlopen").start()
        self.addCleanup(patch.stopall)

    def make_sink(self, strategy=None, connectors=None) -> InfluxDBSink:
        sink = InfluxDBSink(CONFIG, bot_name="grid_btc", strategy=strategy or SimpleNamespace(),
                            connectors=connectors or {"binance": self.connector}, writer=self.writer)
        sink.LINGER_AFTER_STOP = 0.01
        return sink

    def events(self, event: str) -> List[str]:
        return [line for line in self.writer.table("order_event") if f"event={event}" in line]

    def test_tick_writes_the_heartbeat_and_prices(self):
        other = FakeConnector("kucoin", ("ETH-USDT",))
        sink = self.make_sink(connectors={"binance": self.connector, "kucoin": other})
        sink.tick(1000.0)
        self.assertEqual(1, len(self.writer.table("bot_heartbeat")))
        self.assertIn("bot_heartbeat,bot=grid_btc,level=strategy seen=1i", self.writer.lines[0])
        prices = self.writer.table("bot_price")
        self.assertEqual(2, len(prices))
        self.assertIn("bot_price,bot=grid_btc,exchange=binance,pair=BTC-USDT best_bid=99.9,best_ask=100.1,mid_price=100.0",
                      prices[0])
        self.assertIn("exchange=kucoin,pair=ETH-USDT", prices[1])

    def test_no_price_from_an_empty_book_or_a_failing_connector(self):
        self.connector.prices["BTC-USDT"] = (Decimal("NaN"), Decimal("NaN"))
        broken = FakeConnector("broken", ("X-Y",))
        broken.get_price = lambda pair, is_buy: (_ for _ in ()).throw(ValueError("no book"))
        sink = self.make_sink(connectors={"binance": self.connector, "broken": broken})
        sink.tick(1000.0)
        self.assertEqual([], self.writer.table("bot_price"))
        self.assertEqual(1, len(self.writer.table("bot_heartbeat")))

    def test_open_orders_every_five_seconds(self):
        self.connector.in_flight_orders = {
            "buy-1": in_flight("buy-1", TradeType.BUY, "99", "0.01"),
            "buy-2": in_flight("buy-2", TradeType.BUY, "98", "0.01"),
            "done": in_flight("done", TradeType.SELL, "101", "0.01", state=OrderState.FILLED),
        }
        self.connector.in_flight_orders["buy-2"].executed_amount_base = Decimal("0.004")
        sink = self.make_sink()
        sink.tick(1000.0)
        rows = self.writer.table("open_order")
        self.assertEqual(2, len(rows))
        self.assertTrue(all("exchange=binance,pair=BTC-USDT,side=BUY" in row for row in rows))
        self.assertIn('order_id="buy-2",price=98.0,amount=0.01,filled=0.004', rows[1])
        self.assertNotEqual(rows[0].rsplit(" ", 1)[1], rows[1].rsplit(" ", 1)[1], "each row needs its own time")
        sink.tick(1001.0)
        self.assertEqual(2, len(self.writer.table("open_order")))
        sink.tick(1005.0)
        self.assertEqual(4, len(self.writer.table("open_order")))

    def test_paper_trade_limit_orders(self):
        paper = PaperConnector("binance_paper_trade", ("BTC-USDT",))
        paper.limit_orders = [SimpleNamespace(client_order_id="p-1", trading_pair="BTC-USDT", is_buy=False,
                                              price=Decimal("101"), quantity=Decimal("0.02"),
                                              filled_quantity=Decimal("0"))]
        sink = self.make_sink(connectors={"binance_paper_trade": paper})
        sink.tick(1000.0)
        self.assertIn('side=SELL order_id="p-1",price=101.0,amount=0.02,filled=0.0', self.writer.table("open_order")[0])

    async def test_paper_trade_fills_are_recorded(self):
        paper = PaperConnector("binance_paper_trade", ("BTC-USDT",))
        sink = self.make_sink(connectors={"binance_paper_trade": paper})
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        paper.trigger_event(MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
            timestamp=time.time(), type=OrderType.LIMIT, trading_pair="BTC-USDT", amount=Decimal("0.01"),
            price=Decimal("99"), order_id="paper-1", creation_timestamp=time.time()))
        paper.trigger_event(MarketEvent.OrderFilled, fill("paper-1", TradeType.BUY, "99", "0.01"))
        filled = self.events("filled")
        self.assertEqual(1, len(filled))
        self.assertIn("exchange=binance_paper_trade,pair=BTC-USDT,side=BUY", filled[0])
        paper.trigger_event(MarketEvent.OrderFilled, fill("paper-2", TradeType.SELL, "101", "0.01"))
        self.assertEqual(1, len(self.events("partially_filled")), "an unknown order's fill is still recorded")

    async def test_a_market_order_has_no_price_until_it_fills(self):
        sink = self.make_sink()
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        self.connector.trigger_event(MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
            timestamp=time.time(), type=OrderType.MARKET, trading_pair="BTC-USDT", amount=Decimal("0.01"),
            price=Decimal("0"), order_id="market-1", creation_timestamp=time.time()))
        created = self.events("created")[0]
        self.assertNotIn("price=", created)
        self.connector.trigger_event(MarketEvent.OrderFilled, fill("market-1", TradeType.BUY, "100.2", "0.01"))
        self.assertIn("price=100.2,amount=0.01", self.events("filled")[0])

    def test_v2_controllers_pnl_and_grid(self):
        controller = SimpleNamespace(
            config=SimpleNamespace(connector_name="binance", trading_pair="BTC-USDT"),
            get_custom_info=lambda: {"reference_price": 100.0, "levels": [
                {"id": "B1", "side": "BUY", "price": 99.0, "amount": 0.01, "state": "PLACED", "order_id": "buy-1"},
                {"id": "S1", "side": "SELL", "price": 101.0, "amount": 0.01, "state": "EMPTY", "order_id": None}]})
        strategy = SimpleNamespace(
            controllers={"grid_1": controller},
            get_performance_report=lambda controller_id: PerformanceReport(
                realized_pnl_quote=Decimal("1.5"), unrealized_pnl_quote=Decimal("-0.25"),
                global_pnl_quote=Decimal("1.25"), volume_traded=Decimal("300")))
        self.connector.in_flight_orders = {"buy-1": in_flight("buy-1", TradeType.BUY, "99", "0.01")}
        sink = self.make_sink(strategy=strategy)
        sink.tick(1000.0)
        performance = self.writer.table("controller_performance")
        self.assertEqual(1, len(performance))
        self.assertIn("controller_performance,bot=grid_btc,controller=grid_1 ", performance[0])
        self.assertIn("realized_pnl_quote=1.5,unrealized_pnl_quote=-0.25", performance[0])
        self.assertIn("global_pnl_quote=1.25", performance[0])
        self.assertNotIn("positions_summary", performance[0])
        self.assertIn("grid_state,bot=grid_btc,controller=grid_1,exchange=binance,pair=BTC-USDT "
                      "reference_price=100.0,levels_total=2i,levels_placed=1i", self.writer.table("grid_state")[0])
        self.assertEqual(2, len(self.writer.table("grid_level")))
        self.assertIn('level="B1"', self.writer.table("open_order")[0])

    def test_a_failing_controller_does_not_stop_the_others(self):
        def report(controller_id):
            if controller_id == "bad":
                raise RuntimeError("boom")
            return PerformanceReport(global_pnl_quote=Decimal("2"))

        strategy = SimpleNamespace(controllers={"bad": SimpleNamespace(), "good": SimpleNamespace()},
                                   get_performance_report=report)
        self.make_sink(strategy=strategy).tick(1000.0)
        self.assertEqual(1, len(self.writer.table("controller_performance")))
        self.assertIn("controller=good", self.writer.table("controller_performance")[0])

    def test_tick_never_raises(self):
        class Exploding(FakeConnector):
            @property
            def trading_pairs(self):
                raise RuntimeError("boom")

            @trading_pairs.setter
            def trading_pairs(self, value):
                pass

        self.make_sink(connectors={"x": Exploding("x")}).tick(1000.0)  # no exception

    async def test_start_and_clean_stop(self):
        sink = self.make_sink()
        sink.PROCESS_HEARTBEAT_INTERVAL = 0.01
        sink.start_sink()
        self.assertTrue(self.writer.started)
        for event in ORDER_EVENTS:
            self.assertEqual(1, len(self.connector.get_listeners(event)))
        self.assertIn("bot_lifecycle,bot=grid_btc event=\"started\"", self.writer.table("bot_lifecycle")[0])
        await asyncio.sleep(0.05)
        self.assertTrue(self.urlopen.called, "the process heartbeat writes from its own thread")
        request = self.urlopen.call_args[0][0]
        self.assertEqual(CONFIG.write_url, request.full_url)
        self.assertIn(b"bot_heartbeat,bot=grid_btc,level=process seen=1i", request.data)
        self.assertEqual("Bearer apiv3_test", request.headers["Authorization"])

        await sink.stop_sink()
        sink._heartbeat_thread.join(timeout=1)
        self.assertFalse(sink._heartbeat_thread.is_alive())
        self.assertIn('event="stopped"', self.writer.table("bot_lifecycle")[-1])
        self.assertEqual(1, self.writer.flushes, "stopped is written right away")
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(time.time(), "late-cancel"))
        self.assertEqual(1, len(self.events("cancelled")), "cancels during shutdown are still recorded")
        await asyncio.sleep(0.05)
        self.assertTrue(self.writer.closed)
        for event in ORDER_EVENTS:
            self.assertEqual(0, len(self.connector.get_listeners(event)))
        calls = self.urlopen.call_count
        await asyncio.sleep(0.05)
        self.assertEqual(calls, self.urlopen.call_count, "no process heartbeat after a clean stop")

    async def test_order_lifecycle_with_partial_fills(self):
        sink = self.make_sink()
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        tracked = in_flight("buy-1", TradeType.BUY, "99", "0.01")
        self.connector.in_flight_orders["buy-1"] = tracked
        self.connector.trigger_event(MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
            timestamp=time.time(), type=OrderType.LIMIT, trading_pair="BTC-USDT", amount=Decimal("0.01"),
            price=Decimal("99"), order_id="buy-1", creation_timestamp=time.time()))
        self.assertIn("order_event,bot=grid_btc,event=created,exchange=binance,pair=BTC-USDT,side=BUY "
                      'order_id="buy-1",price=99.0,amount=0.01', self.events("created")[0])

        tracked.executed_amount_base = Decimal("0.004")  # the tracker counts a fill before announcing it
        self.connector.trigger_event(MarketEvent.OrderFilled, fill("buy-1", TradeType.BUY, "99", "0.004"))
        self.assertEqual(1, len(self.events("partially_filled")))
        self.assertIn("amount=0.004", self.events("partially_filled")[0])
        self.assertEqual([], self.events("filled"))

        tracked.executed_amount_base = Decimal("0.01")
        self.connector.trigger_event(MarketEvent.OrderFilled, fill("buy-1", TradeType.BUY, "98.9", "0.006"))
        self.assertEqual(1, len(self.events("filled")))
        self.assertIn("price=98.9,amount=0.006", self.events("filled")[0])

        self.connector.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
            timestamp=time.time(), order_id="buy-1", base_asset="BTC", quote_asset="USDT",
            base_asset_amount=Decimal("0.01"), quote_asset_amount=Decimal("0.9894"), order_type=OrderType.LIMIT))
        self.assertEqual(1, len(self.events("filled")), "completion after the last fill adds nothing")

    async def test_completion_ends_the_order_when_its_last_fill_was_missed(self):
        sink = self.make_sink()
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        self.connector.trigger_event(MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
            timestamp=time.time(), type=OrderType.LIMIT, trading_pair="BTC-USDT", amount=Decimal("0.01"),
            price=Decimal("99"), order_id="buy-2", creation_timestamp=time.time()))
        self.connector.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
            timestamp=time.time(), order_id="buy-2", base_asset="BTC", quote_asset="USDT",
            base_asset_amount=Decimal("0.01"), quote_asset_amount=Decimal("0.99"), order_type=OrderType.LIMIT))
        filled = self.events("filled")
        self.assertEqual(1, len(filled))
        self.assertIn("price=99.0,amount=0.01", filled[0])

    async def test_cancel_failure_and_expiry_use_what_the_order_was(self):
        sink = self.make_sink()
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        for order_id in ("sell-1", "sell-2", "sell-3"):
            self.connector.trigger_event(MarketEvent.SellOrderCreated, SellOrderCreatedEvent(
                timestamp=time.time(), type=OrderType.LIMIT, trading_pair="BTC-USDT", amount=Decimal("0.02"),
                price=Decimal("101"), order_id=order_id, creation_timestamp=time.time()))
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(time.time(), "sell-1"))
        self.connector.trigger_event(MarketEvent.OrderFailure,
                                     MarketOrderFailureEvent(time.time(), "sell-2", OrderType.LIMIT, "rejected"))
        self.connector.trigger_event(MarketEvent.OrderExpired, OrderExpiredEvent(time.time(), "sell-3"))
        for event, order_id in (("cancelled", "sell-1"), ("failed", "sell-2"), ("expired", "sell-3")):
            rows = self.events(event)
            self.assertEqual(1, len(rows), event)
            self.assertIn(f'side=SELL order_id="{order_id}",price=101.0,amount=0.02', rows[0])
        self.assertEqual({}, sink._orders, "finished orders are forgotten")

    async def test_fill_of_an_order_placed_before_the_sink_started(self):
        tracked = in_flight("old-1", TradeType.SELL, "105", "0.02")
        tracked.executed_amount_base = Decimal("0.01")
        self.connector.in_flight_orders["old-1"] = tracked
        sink = self.make_sink()
        sink.start_sink()
        self.addAsyncCleanup(sink.stop_sink)
        self.connector.trigger_event(MarketEvent.OrderFilled, fill("old-1", TradeType.SELL, "105", "0.01"))
        self.assertIn("pair=BTC-USDT,side=SELL", self.events("partially_filled")[0])
