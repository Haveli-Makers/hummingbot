"""AccountMonitor: what it writes for one account, and the account_monitor script around it. Nothing here connects."""
import asyncio
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import OpenOrder, OrderType
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.remote_iface.account_monitor import AccountMonitor

Row = Tuple[Dict[str, str], Dict[str, Any], int]
T0 = 1_795_675_620_000  # epoch ms


def parse(line: str) -> Tuple[str, Row]:
    """Line protocol back into (table, (tags, fields, time)), for the simple values these tests write."""
    head, rest = re.match(r"((?:[^ \\]|\\.)+) (.*)", line).groups()
    fields_text, stamp = rest.rsplit(" ", 1)
    table, *tag_parts = re.split(r"(?<!\\),", head)
    tags = dict(part.replace("\\", "").split("=", 1) for part in tag_parts)
    fields = {}
    for key, value in re.findall(r'((?:[^=,\\]|\\.)+)=("(?:[^"\\]|\\.)*"|[^,]+)', fields_text):
        if value.startswith('"'):
            fields[key] = value[1:-1].replace('\\"', '"')
        elif value.endswith("i"):
            fields[key] = int(value[:-1])
        elif value in ("true", "false"):
            fields[key] = value == "true"
        else:
            fields[key] = float(value)
    return table, (tags, fields, int(stamp))


class FakeWriter:
    def __init__(self):
        self.lines: List[str] = []

    def add(self, line: Optional[str]):
        if line:
            self.lines.append(line)

    def rows(self, table: str) -> List[Row]:
        return [row for name, row in map(parse, self.lines) if name == table]

    def clear(self):
        self.lines.clear()


class FakeBook:
    def __init__(self, bids, asks, uid: int = 1):
        self.bids, self.asks, self.snapshot_uid, self.last_diff_uid = bids, asks, uid, 0

    def bid_entries(self):
        return (OrderBookRow(price, amount, 1) for price, amount in self.bids)

    def ask_entries(self):
        return (OrderBookRow(price, amount, 1) for price, amount in self.asks)


def open_order(order_id: str, is_buy: bool, price: str, amount: str, filled: str = "0",
               client_order_id: str = "haveli-x") -> OpenOrder:
    return OpenOrder(client_order_id=client_order_id, trading_pair="ETH-INR", price=Decimal(price),
                     amount=Decimal(amount), executed_amount=Decimal(filled), status="open",
                     order_type=OrderType.LIMIT, is_buy=is_buy, time=1795675620118, exchange_order_id=order_id)


def trade(trade_id: str, at_ms: float, side: str = "buy", price: Any = 175000.0, quantity: Any = 0.0007,
          symbol: str = "ETHINR") -> Dict[str, Any]:
    return {"id": trade_id, "order_id": f"o{trade_id}", "side": side, "fee_amount": "0.06", "quantity": quantity,
            "price": price, "symbol": symbol, "timestamp": at_ms}


class FakeConnector:
    name = "coindcx"

    def __init__(self):
        self.balances = {"INR": Decimal("125.5"), "ETH": Decimal("0.0007"), "BTC": Decimal("0")}
        self.available = {"INR": Decimal("100"), "ETH": Decimal("0.0007"), "BTC": Decimal("0")}
        self.balance_error: Optional[Exception] = None
        self.orders = [open_order("11", True, "173000", "0.0007"), open_order("12", False, "180000", "0.0007", "0.0002")]
        self.orders_error: Optional[Exception] = None
        self.trade_pages: List[List[Dict[str, Any]]] = []
        self.trade_starts: List[int] = []
        self.books = {"ETH-INR": FakeBook([(100.0, 1.0), (99.5, 2.0), (98.0, 5.0)],
                                          [(101.0, 1.0), (101.5, 1.0), (103.0, 4.0)])}

    async def _update_balances(self):
        if self.balance_error:
            raise self.balance_error

    def get_all_balances(self):
        return dict(self.balances)

    @property
    def available_balances(self):
        return dict(self.available)

    async def get_open_orders(self):
        if self.orders_error:
            raise self.orders_error
        return list(self.orders)

    async def get_all_account_trades(self, start_time=None, end_time=None, limit=100, trading_pairs=None):
        self.trade_starts.append(start_time)
        return self.trade_pages.pop(0) if self.trade_pages else []

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        return {"ETHINR": "ETH-INR"}[symbol]

    def get_order_book(self, pair: str):
        return self.books[pair]


class AccountMonitorTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.connector, self.writer = FakeConnector(), FakeWriter()
        self.monitor = self.make_monitor()

    def make_monitor(self, page_size: int = 100) -> AccountMonitor:
        monitor = AccountMonitor(self.connector, "main", self.writer, ["ETH-INR"], trades_page_size=page_size)
        monitor._fills_since_ms = T0  # read fills from here on
        return monitor

    async def test_update_writes_balances_open_orders_fills_and_a_summary(self):
        self.connector.trade_pages = [[trade("2", T0 + 1000), trade("1", T0 + 500, side="sell")]]

        result = await self.monitor.update()

        account = {"account": "main", "exchange": "coindcx"}
        [(update_tags, update, update_time)] = self.writer.rows("account_update")
        self.assertEqual(account, update_tags)
        self.assertEqual({"balances_ok": True, "orders_ok": True, "fills_ok": True, "assets": 2, "open_orders": 2,
                          "new_fills": 2, "error": ""}, update)
        self.assertEqual(update, result)

        balances = {tags["asset"]: fields for tags, fields, _ in self.writer.rows("account_balance")}
        self.assertEqual({"ETH": {"total": 0.0007, "available": 0.0007},
                          "INR": {"total": 125.5, "available": 100.0}}, balances)  # BTC has none: left out

        orders = self.writer.rows("account_open_order")
        self.assertEqual([update_time, update_time + 1], [stamp for *_, stamp in orders])  # the update's snapshot
        self.assertEqual([{**account, "pair": "ETH-INR", "side": "BUY"}, {**account, "pair": "ETH-INR", "side": "SELL"}],
                         [tags for tags, _, _ in orders])
        self.assertEqual({"order_id": "12", "client_order_id": "haveli-x", "price": 180000.0, "amount": 0.0007,
                          "filled": 0.0002, "status": "open", "created_ms": 1795675620118}, orders[1][1])

        fills = self.writer.rows("account_fill")
        self.assertEqual([("1", "SELL", (T0 + 500) * 1_000_000), ("2", "BUY", (T0 + 1000) * 1_000_000)],
                         [(tags["trade_id"], tags["side"], stamp) for tags, _, stamp in fills])  # oldest first
        self.assertEqual({**account, "pair": "ETH-INR", "side": "BUY", "trade_id": "2"}, fills[1][0])
        self.assertEqual({"order_id": "o2", "price": 175000.0, "amount": 0.0007, "quote_amount": 175000.0 * 0.0007,
                          "fee": 0.06}, fills[1][1])

    async def test_an_asset_that_drops_to_zero_gets_one_zero_row(self):
        await self.monitor.update()
        self.connector.balances["ETH"] = self.connector.available["ETH"] = Decimal("0")
        self.writer.clear()

        await self.monitor.update()
        self.assertEqual({"INR": 125.5, "ETH": 0.0}, {tags["asset"]: fields["total"]
                                                      for tags, fields, _ in self.writer.rows("account_balance")})
        self.writer.clear()

        await self.monitor.update()
        self.assertEqual(["INR"], [tags["asset"] for tags, _, _ in self.writer.rows("account_balance")])

    async def test_a_failed_read_is_reported_and_the_others_are_still_written(self):
        self.connector.orders_error = IOError("HTTP status is 401")

        result = await self.monitor.update()

        self.assertFalse(result["orders_ok"])
        self.assertIsNone(result["open_orders"])
        self.assertEqual("open orders: HTTP status is 401", result["error"])
        self.assertEqual([], self.writer.rows("account_open_order"))  # the last good snapshot stays the latest
        self.assertEqual(2, len(self.writer.rows("account_balance")))
        [(_, update, _)] = self.writer.rows("account_update")
        self.assertNotIn("open_orders", update)

        self.connector.balance_error = IOError("timeout")
        result = await self.monitor.update()
        self.assertFalse(result["balances_ok"])
        self.assertIn("balances: timeout", result["error"])

    async def test_a_connector_that_cannot_list_orders_or_trades_says_so_once(self):
        connector = MagicMock(spec=["name", "_update_balances", "get_all_balances", "available_balances"])
        connector.name = "binance"
        connector._update_balances = AsyncMock()
        connector.get_all_balances.return_value = {}
        connector.available_balances = {}
        monitor = AccountMonitor(connector, "main", self.writer, ["ETH-USDT"])

        with self.assertLogs("hummingbot.remote_iface.account_monitor", "WARNING") as logs:
            await monitor.update()
            result = await monitor.update()

        self.assertEqual(2, len(logs.output))  # once each for orders and fills, not once per update
        self.assertFalse(result["orders_ok"])
        self.assertFalse(result["fills_ok"])
        self.assertTrue(result["balances_ok"])

    async def test_fills_are_read_a_page_at_a_time_and_never_written_twice(self):
        monitor = self.make_monitor(page_size=2)
        self.connector.trade_pages = [
            [trade("1", T0 + 100), trade("2", T0 + 200)],  # a full page: there may be more
            [trade("2", T0 + 200), trade("3", T0 + 300)],  # starts at the last time read, so "2" comes again
            [trade("3", T0 + 300)],
        ]
        result = await monitor.update()
        self.assertEqual(3, result["new_fills"])
        self.assertEqual([T0, T0 + 200, T0 + 300], self.connector.trade_starts)
        self.assertEqual(["1", "2", "3"], [tags["trade_id"] for tags, _, _ in self.writer.rows("account_fill")])

        self.writer.clear()
        self.connector.trade_pages = [[trade("3", T0 + 300)], [trade("4", T0 + 300)]]
        result = await monitor.update()
        self.assertEqual(0, result["new_fills"])  # "3" was written before
        result = await monitor.update()
        self.assertEqual(1, result["new_fills"])  # "4" shares 3's millisecond but is new
        self.assertEqual(["4"], [tags["trade_id"] for tags, _, _ in self.writer.rows("account_fill")])

    async def test_a_newest_first_page_loses_nothing(self):
        self.connector.trade_pages = [[trade("3", T0 + 300), trade("2", T0 + 200), trade("1", T0 + 100)]]
        result = await self.monitor.update()
        self.assertEqual(3, result["new_fills"])

    async def test_fill_fields_in_other_shapes(self):
        self.connector.trade_pages = [[
            trade("1", (T0 + 1000) / 1000, price="175000.5", quantity="0.001"),  # seconds and numeric strings
            trade("2", T0 + 2000, symbol="XYZINR"),  # a market the connector doesn't know
            {"id": "3", "price": 1.0},  # no time or amount: skipped
        ]]
        self.monitor._fills_since_ms = 0
        await self.monitor.update()
        fills = self.writer.rows("account_fill")
        self.assertEqual([("1", "ETH-INR", (T0 + 1000) * 1_000_000), ("2", "XYZINR", (T0 + 2000) * 1_000_000)],
                         [(tags["trade_id"], tags["pair"], stamp) for tags, _, stamp in fills])
        self.assertEqual(175000.5, fills[0][1]["price"])

    def test_market_sample_from_the_live_order_book(self):
        self.assertEqual(1, self.monitor.sample_market(now=1000.0))
        [(tags, fields, stamp)] = self.writer.rows("market")
        self.assertEqual({"exchange": "coindcx", "pair": "ETH-INR", "source": "account_monitor"}, tags)
        self.assertEqual(1000 * 10**9, stamp)
        self.assertEqual((100.0, 101.0, 100.5), (fields["best_bid"], fields["best_ask"], fields["mid_price"]))
        self.assertAlmostEqual(1 / 100.5 * 100, fields["spread_pct"])
        self.assertEqual(100.0 + 99.5 * 2, fields["bid_depth_quote_1_0"])  # bids at or above 99.495
        self.assertEqual(101.0 + 101.5, fields["ask_depth_quote_1_0"])  # asks at or below 101.505
        self.assertEqual(0.0, fields["book_age_s"])

        self.writer.clear()
        self.monitor.sample_market(now=1003.0)
        self.assertEqual(3.0, self.writer.rows("market")[0][1]["book_age_s"])  # nothing changed for 3 s

        self.writer.clear()
        self.connector.books["ETH-INR"].snapshot_uid = 2  # a new snapshot arrived
        self.monitor.sample_market(now=1004.0)
        self.assertEqual(0.0, self.writer.rows("market")[0][1]["book_age_s"])

    def test_no_market_row_without_a_book_or_with_an_empty_side(self):
        monitor = AccountMonitor(self.connector, "main", self.writer, ["ETH-INR", "BTC-INR"])
        self.connector.books["ETH-INR"].asks = []
        self.assertEqual(0, monitor.sample_market())
        self.assertEqual([], self.writer.lines)


class AccountMonitorScriptTests(IsolatedAsyncioWrapperTestCase):

    def test_config_checks_the_account_name_and_the_pairs(self):
        from scripts.account_monitor import AccountMonitorConfig, AccountMonitorScript
        config = AccountMonitorConfig(account_name="coindcx-main", trading_pairs="ETH-INR, BTC-INR")
        self.assertEqual(["ETH-INR", "BTC-INR"], config.pairs())
        self.assertEqual(5.0, config.update_interval)
        AccountMonitorScript.init_markets(config)
        self.assertEqual({"coindcx": {"ETH-INR", "BTC-INR"}}, AccountMonitorScript.markets)
        for bad in ({"account_name": "my account"}, {"account_name": "x' OR 1=1"}, {"trading_pairs": " , "},
                    {"update_interval": 0.5}):
            with self.assertRaises(ValueError):
                AccountMonitorConfig(**bad)

    async def test_it_reads_once_per_interval_and_never_overlaps_reads(self):
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
        from scripts.account_monitor import AccountMonitorConfig, AccountMonitorScript
        config = AccountMonitorConfig(update_interval=5, market_data_interval=1)
        exchange = CoindcxExchange(coindcx_api_key="k", coindcx_api_secret="s", trading_pairs=["ETH-INR"],
                                   trading_required=False)
        with patch.dict("os.environ", {"INFLUXDB_URL": "http://127.0.0.1:9", "INFLUXDB_TOKEN": "t"}):
            script = AccountMonitorScript({"coindcx": exchange}, config)
        release = asyncio.Event()

        async def slow_update():
            await release.wait()

        script._writer = MagicMock()
        script._writer.close = AsyncMock()
        script._monitor = MagicMock()
        script._monitor.update = AsyncMock(side_effect=slow_update)
        clock = {"now": 1000.0}
        with patch.object(AccountMonitorScript, "current_timestamp", property(lambda self: clock["now"])):
            for second in range(12):
                clock["now"] = 1000.0 + second
                script.on_tick()
                await asyncio.sleep(0)
                if second == 7:
                    release.set()  # the first read takes 7 s; the one due at 1005 waits for it
        self.assertEqual(12, script._monitor.sample_market.call_count)  # every second
        self.assertEqual(2, script._monitor.update.call_count)  # at 1000, then at 1008 once the first had finished
        await script.on_stop()
        script._writer.close.assert_awaited_once()

    def test_without_influxdb_settings_it_writes_nothing_and_says_why(self):
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
        from scripts.account_monitor import AccountMonitorConfig, AccountMonitorScript
        exchange = CoindcxExchange(coindcx_api_key="k", coindcx_api_secret="s", trading_pairs=["ETH-INR"],
                                   trading_required=False)
        with patch.dict("os.environ", {"INFLUXDB_URL": "", "INFLUXDB_TOKEN": ""}):
            script = AccountMonitorScript({"coindcx": exchange}, AccountMonitorConfig())
        self.assertIsNone(script._monitor)
        script.on_tick()  # does nothing
        self.assertIn("INFLUXDB_URL and INFLUXDB_TOKEN aren't set", script.format_status())
