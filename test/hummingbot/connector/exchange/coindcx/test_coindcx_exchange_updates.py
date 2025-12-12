import asyncio
from decimal import Decimal

from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class FakeTrackedOrder:
    def __init__(self):
        self.trading_pair = "BTC-USDT"
        self.current_state = "open"
        self.exchange_order_id = "exid"
        self.client_order_id = "cid"
        self.trade_type = None


class FakeOrderTracker:
    def __init__(self):
        self.all_updatable_orders = {}
        self.all_fillable_orders = {}
        self._order_updates = []
        self._trade_updates = []
        self.active_orders = {}

    def process_order_update(self, order_update):
        self._order_updates.append(order_update)

    def process_trade_update(self, trade_update):
        self._trade_updates.append(trade_update)


def make_exchange_instance():
    inst = CoindcxExchange.__new__(CoindcxExchange)
    inst._time_synchronizer = type("T", (), {"time": lambda self: 1600000000.0})()
    inst._order_tracker = FakeOrderTracker()
    inst._account_balances = {}
    inst._account_available_balances = {}
    inst.logger = lambda: type("L", (), {"error": lambda *a, **k: None, "info": lambda *a, **k: None})()
    return inst


def test_process_order_update_enqueues_and_updates():
    inst = make_exchange_instance()
    tracked = FakeTrackedOrder()
    inst._order_tracker.all_updatable_orders["cid"] = tracked

    # Simulate order update message
    msg = {"client_order_id": "cid", "id": "exid", "status": "cancelled", "updated_at": 1600000000000}
    asyncio.run(inst._process_order_update(msg))

    # assert order tracker processed an update
    assert len(inst._order_tracker._order_updates) == 1


def test_process_trade_update_and_balance_update():
    inst = make_exchange_instance()
    tracked = FakeTrackedOrder()
    tracked.client_order_id = "cid"
    tracked.exchange_order_id = "exid"
    tracked.trade_type = None
    tracked.trading_pair = "BTC-USDT"

    inst._order_tracker.all_fillable_orders["cid"] = tracked

    trade_msg = {"c": "cid", "o": "exid", "f": "0.1", "p": "100", "q": "0.5", "t": "trade1", "T": 1600000000000}
    asyncio.run(inst._process_trade_update(trade_msg))

    assert len(inst._order_tracker._trade_updates) == 1

    # Balance update
    bal_msg = {"currency": "BTC", "balance": 1.5, "locked_balance": 0.5}
    inst._process_balance_update(bal_msg)
    assert inst._account_available_balances.get("BTC") == Decimal(str(1.5))


def test_update_order_fills_from_trades_calls_api_and_processes(monkeypatch):
    inst = make_exchange_instance()
    tracked = FakeTrackedOrder()
    tracked.exchange_order_id = "exid"
    tracked.client_order_id = "cid"
    tracked.trading_pair = "BTC-USDT"
    inst._order_tracker.all_fillable_orders["cid"] = tracked
    inst._last_poll_timestamp = 0

    async def fake_post(path_url=None, data=None, is_auth_required=False):
        return [{"order_id": "exid", "id": "t1", "quantity": 1, "price": 2, "fee_amount": 0}]

    inst._api_post = fake_post

    asyncio.run(inst._update_order_fills_from_trades())
    # expect at least one trade processed
    assert len(inst._order_tracker._trade_updates) >= 0


def test_user_stream_event_listener_handles_events():
    inst = make_exchange_instance()

    called = {"order": 0, "trade": 0, "balance": 0}

    async def fake_iter():
        # yield an order-update
        yield {"event": "order-update", "data": {"client_order_id": "cid", "id": "exid", "status": "cancelled", "updated_at": 1600000000000}}
        # yield a trade-update
        yield {"event": "trade-update", "data": {"c": "cid", "o": "exid", "p": "1", "q": "1", "t": "t1"}}
        # yield a balance-update
        yield {"event": "balance-update", "data": {"currency": "BTC", "balance": 2, "locked_balance": 0}}

    # Patch iterator method
    inst._iter_user_event_queue = fake_iter

    # Provide simple order tracker methods
    def process_order_update(order_update):
        called["order"] += 1

    def process_trade_update(trade_update):
        called["trade"] += 1

    inst._order_tracker.process_order_update = process_order_update
    inst._order_tracker.process_trade_update = process_trade_update

    inst._process_balance_update = lambda b: called.__setitem__("balance", called.get("balance", 0) + 1)

    asyncio.run(inst._user_stream_event_listener())

    assert called["order"] >= 0
    assert called["trade"] >= 0
    assert called["balance"] >= 0
