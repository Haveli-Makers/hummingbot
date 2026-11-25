"""
Bot monitoring: the bot writes its own data to InfluxDB 3, and the api-server reads it back (the api-server README,
"Bot Monitoring", lists the tables and who writes them).

It is on only when INFLUXDB_URL and INFLUXDB_TOKEN are set; the api-server sets them, with INFLUXDB_DATABASE, in the
bots it deploys. Every point carries the bot's instance_id as its `bot` tag.

    every second   bot_heartbeat level=strategy, from the clock: it stops when the event loop freezes
                   bot_price for every market the strategy trades
    every 5 s      bot_heartbeat level=process, from its own thread: it keeps beating while the loop is frozen
                   open_order, one row per open order
                   controller_performance per controller (V2 strategies)
                   grid_state and grid_level, when a controller's get_custom_info() returns
                   {"reference_price": ..., "levels": [{"id", "side", "price", "amount", "state", "order_id"}]}
    on events      order_event: created, partially_filled, filled, cancelled, failed, expired
    start / stop   bot_lifecycle: started, then stopped on a clean stop, so the api-server counts it as stopped

Nothing here can block or crash the bot: points are buffered and written once a second, and while InfluxDB is
unreachable the newest 50,000 are kept.
"""
import asyncio
import itertools
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Deque, Dict, List, Optional
from urllib.parse import quote

import aiohttp

from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.py_time_iterator import PyTimeIterator
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

ORDER_EVENTS = (
    MarketEvent.BuyOrderCreated,
    MarketEvent.SellOrderCreated,
    MarketEvent.OrderFilled,
    MarketEvent.BuyOrderCompleted,
    MarketEvent.SellOrderCompleted,
    MarketEvent.OrderCancelled,
    MarketEvent.OrderFailure,
    MarketEvent.OrderExpired,
)
TERMINAL_EVENTS = {
    MarketEvent.OrderCancelled: "cancelled",
    MarketEvent.OrderFailure: "failed",
    MarketEvent.OrderExpired: "expired",
}


@dataclass(frozen=True)
class InfluxDBConfig:
    url: str
    token: str
    database: str = "hbot"

    @classmethod
    def from_env(cls) -> Optional["InfluxDBConfig"]:
        """The settings the api-server passes to the bots it deploys; None (monitoring off) unless URL and token
        are both set."""
        url = os.environ.get("INFLUXDB_URL", "").strip()
        token = os.environ.get("INFLUXDB_TOKEN", "").strip()
        if not url or not token:
            return None
        return cls(url=url.rstrip("/"), token=token, database=os.environ.get("INFLUXDB_DATABASE", "").strip() or "hbot")

    @property
    def write_url(self) -> str:
        return f"{self.url}/api/v3/write_lp?db={quote(self.database)}&precision=nanosecond"


def escape(value: Any) -> str:
    """Escape a table name, tag key, tag value or field key for line protocol."""
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def to_line(table: str, tags: Dict[str, Any], fields: Dict[str, Any], timestamp_ns: int) -> Optional[str]:
    """One line-protocol point, or None when no field has a usable value. Empty tags are left out."""
    field_parts = []
    for key, value in fields.items():
        if isinstance(value, bool):
            field_parts.append(f"{escape(key)}={'true' if value else 'false'}")
        elif isinstance(value, int):
            field_parts.append(f"{escape(key)}={value}i")
        elif isinstance(value, float) and math.isfinite(value):
            field_parts.append(f"{escape(key)}={value!r}")
        elif isinstance(value, str):
            quoted = value.replace("\\", "\\\\").replace('"', '\\"')
            field_parts.append(f'{escape(key)}="{quoted}"')
    if not field_parts:
        return None
    tag_parts = [f"{escape(key)}={escape(value)}" for key, value in sorted(tags.items()) if value not in (None, "")]
    return f"{','.join([escape(table)] + tag_parts)} {','.join(field_parts)} {timestamp_ns}"


def number(value: Any) -> Optional[float]:
    """A finite float, or None (NaN, missing, or not a number)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


class InfluxDBWriter:
    """Buffers points and writes them in batches once a second. Never raises into the caller."""

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: InfluxDBConfig, flush_interval: float = 1.0, max_buffer: int = 50_000,
                 batch_size: int = 5_000, warn_every: float = 60.0):
        self._config = config
        self._flush_interval = flush_interval
        self._buffer: Deque[str] = deque(maxlen=max_buffer)
        self._batch_size = batch_size
        self._warn_every = warn_every
        self._last_warning = -math.inf
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @property
    def pending(self) -> int:
        return len(self._buffer)

    def add(self, line: Optional[str]):
        if line:
            self._buffer.append(line)

    def start(self):
        if self._task is None or self._task.done():
            self._task = safe_ensure_future(self._run())

    async def close(self, timeout: float = 3.0):
        """Stop the flush loop, write what is left (for up to `timeout` seconds), and close the connection."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        try:
            await asyncio.wait_for(self.flush(), timeout)
        except asyncio.TimeoutError:
            self.logger().warning(f"Bot monitoring: {self.pending} points not written to InfluxDB at shutdown.")
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def flush(self):
        async with self._lock:
            while self._buffer:
                batch = [self._buffer.popleft() for _ in range(min(self._batch_size, len(self._buffer)))]
                if not await self._post(batch):
                    self._requeue(batch)
                    return

    async def _run(self):
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().debug("Bot monitoring: flush failed", exc_info=True)

    def _requeue(self, batch: List[str]):
        """Put a failed batch back in front, keeping the newest points if the buffer is full."""
        space = (self._buffer.maxlen or len(batch)) - len(self._buffer)
        for line in reversed(batch[-space:] if space > 0 else []):
            self._buffer.appendleft(line)

    async def _post(self, batch: List[str]) -> bool:
        """True when the batch is done with (written, or rejected as bad data); False to retry it later."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10),
                                                  headers={"Authorization": f"Bearer {self._config.token}"})
        try:
            async with self._session.post(self._config.write_url, data="\n".join(batch).encode()) as response:
                status = response.status
                body = "" if status < 300 else (await response.text())[:300]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._warn(f"InfluxDB at {self._config.url} is unreachable ({e!r}); keeping the newest "
                       f"{self._buffer.maxlen} points until it is back.")
            return False
        if status < 300:
            return True
        if status in (401, 403) or status >= 500:
            self._warn(f"InfluxDB answered HTTP {status}"
                       + (": check INFLUXDB_TOKEN." if status in (401, 403) else f": {body}"))
            return False
        self._warn(f"InfluxDB rejected {len(batch)} points (HTTP {status}): {body}")
        return True

    def _warn(self, message: str):
        now = time.monotonic()
        if now - self._last_warning >= self._warn_every:
            self._last_warning = now
            self.logger().warning(f"Bot monitoring: {message}")


class InfluxDBSink(PyTimeIterator):
    """Sits on the clock right after the strategy and writes what the bot is doing (see the module docstring)."""

    SNAPSHOT_INTERVAL = 5.0
    PROCESS_HEARTBEAT_INTERVAL = 5.0
    # After a stop, order events keep being recorded for a while: the stop command cancels open orders after the
    # strategy has stopped, and those cancels end the orders' lines on the chart.
    LINGER_AFTER_STOP = 15.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: InfluxDBConfig, bot_name: str, strategy: Any, connectors: Dict[str, Any],
                 writer: Optional[InfluxDBWriter] = None):
        super().__init__()
        self._config = config
        self._bot = bot_name
        self._strategy = strategy
        self._connectors = dict(connectors)
        self._connector_names = {id(connector): name for name, connector in self._connectors.items()}
        self._writer = writer or InfluxDBWriter(config)
        # PubSub keeps listeners as weak references: the sink must hold the forwarder or it never fires.
        self._forwarder = SourceInfoEventForwarder(self._on_order_event)
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._levels_by_order: Dict[str, str] = {}
        self._last_snapshot = -math.inf
        self._tiebreak = itertools.count()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._linger_task: Optional[asyncio.Task] = None
        self._listening = False

    # ------------------------------------------------------------------------------------------- start and stop

    def start_sink(self):
        self._writer.start()
        for connector in self._connectors.values():
            for event in ORDER_EVENTS:
                connector.add_listener(event, self._forwarder)
        self._listening = True
        self._write("bot_lifecycle", {}, {"event": "started"})
        self._heartbeat_thread = threading.Thread(target=self._beat_process, name="influxdb-process-heartbeat",
                                                  daemon=True)
        self._heartbeat_thread.start()

    async def stop_sink(self):
        """A clean stop: the heartbeats end, then "stopped" is written, so the api-server marks the bot stopped."""
        self._heartbeat_stop.set()
        self._write("bot_lifecycle", {}, {"event": "stopped"})
        await self._writer.flush()
        self._linger_task = safe_ensure_future(self._linger())

    async def _linger(self):
        try:
            await asyncio.sleep(self.LINGER_AFTER_STOP)
        finally:
            self._remove_listeners()
            await self._writer.close()

    def _remove_listeners(self):
        if self._listening:
            for connector in self._connectors.values():
                for event in ORDER_EVENTS:
                    connector.remove_listener(event, self._forwarder)
            self._listening = False

    # ----------------------------------------------------------------------------------------------- the clock

    def tick(self, timestamp: float):
        try:
            now_ns = time.time_ns()
            self._write("bot_heartbeat", {"level": "strategy"}, {"seen": 1}, now_ns)
            for name, connector in self._connectors.items():
                for pair in list(getattr(connector, "trading_pairs", None) or []):
                    self._write_price(name, connector, pair, now_ns)
            if timestamp - self._last_snapshot >= self.SNAPSHOT_INTERVAL:
                self._last_snapshot = timestamp
                self._write_controllers(now_ns)
                self._write_open_orders(now_ns)
        except Exception:  # the clock would log a traceback every second
            self.logger().debug("Bot monitoring: tick failed", exc_info=True)

    def _write_price(self, name: str, connector: Any, pair: str, now_ns: int):
        try:
            bid, ask = number(connector.get_price(pair, False)), number(connector.get_price(pair, True))
        except Exception:
            return
        if bid is not None and ask is not None:
            self._write("bot_price", {"exchange": name, "pair": pair},
                        {"best_bid": bid, "best_ask": ask, "mid_price": (bid + ask) / 2}, now_ns)

    def _write_open_orders(self, now_ns: int):
        offset = itertools.count()
        for name, connector in self._connectors.items():
            try:
                orders = self._open_orders(connector)
            except Exception:
                self.logger().debug(f"Bot monitoring: no open orders from {name}", exc_info=True)
                continue
            for order in orders:
                # Rows of one snapshot share its time plus 1 ns each: same-side orders share every tag, and InfluxDB
                # keeps one row per table, tags and time.
                self._write("open_order", {"exchange": name, "pair": order["pair"], "side": order["side"]},
                            {"order_id": order["order_id"], "price": number(order["price"]),
                             "amount": number(order["amount"]), "filled": number(order["filled"]) or 0.0,
                             "age_s": order.get("age_s"), "level": self._levels_by_order.get(order["order_id"])},
                            now_ns + next(offset))

    @staticmethod
    def _tracked_orders(connector: Any) -> Dict[str, Any]:
        """The connector's in-flight orders, or {} when it has none (paper trade raises NotImplementedError)."""
        try:
            orders = connector.in_flight_orders
        except Exception:
            return {}
        return orders if isinstance(orders, dict) else {}

    @classmethod
    def _open_orders(cls, connector: Any) -> List[Dict[str, Any]]:
        tracked = cls._tracked_orders(connector)
        if tracked:
            now = time.time()
            return [{"order_id": order.client_order_id, "pair": order.trading_pair, "side": order.trade_type.name,
                     "price": order.price, "amount": order.amount, "filled": order.executed_amount_base,
                     "age_s": now - order.creation_timestamp if order.creation_timestamp else None}
                    for order in tracked.values() if order.is_open]
        return [{"order_id": order.client_order_id, "pair": order.trading_pair, "side": "BUY" if order.is_buy else "SELL",
                 "price": order.price, "amount": order.quantity, "filled": getattr(order, "filled_quantity", 0)}
                for order in getattr(connector, "limit_orders", None) or []]  # paper trade

    def _write_controllers(self, now_ns: int):
        """V2 strategies: PnL per controller, and the grid when a controller reports one."""
        controllers = getattr(self._strategy, "controllers", None)
        if not isinstance(controllers, dict):
            return
        levels_by_order: Dict[str, str] = {}
        for controller_id, controller in controllers.items():
            try:
                report = self._strategy.get_performance_report(controller_id)
                if report is not None:
                    data = report.model_dump() if hasattr(report, "model_dump") else dict(report)
                    self._write("controller_performance", {"controller": controller_id},
                                {key: number(value) for key, value in data.items()}, now_ns)
                get_custom_info = getattr(controller, "get_custom_info", None)
                info = get_custom_info() if callable(get_custom_info) else None
                if isinstance(info, dict) and isinstance(info.get("levels"), list):
                    levels_by_order.update(self._write_grid(controller_id, controller, info, now_ns))
            except Exception:
                self.logger().debug(f"Bot monitoring: no report from controller {controller_id}", exc_info=True)
        self._levels_by_order = levels_by_order

    def _write_grid(self, controller_id: str, controller: Any, info: Dict[str, Any], now_ns: int) -> Dict[str, str]:
        config = getattr(controller, "config", None)
        levels = info["levels"]
        self._write("grid_state", {"controller": controller_id, "exchange": getattr(config, "connector_name", None),
                                   "pair": getattr(config, "trading_pair", None)},
                    {"reference_price": number(info.get("reference_price")), "levels_total": len(levels),
                     "levels_placed": sum(1 for level in levels if str(level.get("state", "")).upper() == "PLACED")},
                    now_ns)
        levels_by_order = {}
        for level in levels:
            level_id = level.get("id")
            self._write("grid_level", {"controller": controller_id, "side": level.get("side"), "level": level_id},
                        {"price": number(level.get("price")), "amount": number(level.get("amount")),
                         "state": str(level["state"]) if level.get("state") is not None else None,
                         "order_id": level.get("order_id")}, now_ns)
            if level.get("order_id") and level_id is not None:
                levels_by_order[str(level["order_id"])] = str(level_id)
        return levels_by_order

    # ------------------------------------------------------------------------------------------- order events

    def _on_order_event(self, event_tag: int, connector: Any, event: Any):
        try:
            tag = MarketEvent(event_tag)
            name = self._connector_names.get(id(connector)) or getattr(connector, "name", "")
            if tag in (MarketEvent.BuyOrderCreated, MarketEvent.SellOrderCreated):
                price = number(event.price)
                order = self._orders[event.order_id] = {
                    "exchange": name, "pair": event.trading_pair,
                    "side": "BUY" if tag is MarketEvent.BuyOrderCreated else "SELL",
                    "price": price if price and price > 0 else None,  # a market order has no price until it fills
                    "amount": number(event.amount), "reported": 0.0, "done": False}
                self._order_event("created", event.order_id, order, order["price"], order["amount"])
            elif tag is MarketEvent.OrderFilled:
                order = self._order(event.order_id, name, connector, pair=event.trading_pair,
                                    side=event.trade_type.name)
                amount = number(event.amount) or 0.0
                order["reported"] += amount
                order["done"] = self._fully_filled(connector, event.order_id, order)
                self._order_event("filled" if order["done"] else "partially_filled", event.order_id, order,
                                  number(event.price), amount)
            elif tag in (MarketEvent.BuyOrderCompleted, MarketEvent.SellOrderCompleted):
                order = self._orders.pop(event.order_id, None)
                if order is None or not order["done"]:  # its last fill wasn't seen as the last: end the line here
                    order = order or self._order(event.order_id, name, connector,
                                                 side="BUY" if tag is MarketEvent.BuyOrderCompleted else "SELL")
                    base, quote_amount = event.base_asset_amount, event.quote_asset_amount
                    price = number(quote_amount / base) if base else order.get("price")
                    remaining = max((number(base) or 0.0) - order["reported"], 0.0)
                    self._order_event("filled", event.order_id, order, price, remaining)
                    self._orders.pop(event.order_id, None)
            elif tag in TERMINAL_EVENTS:
                order = self._orders.pop(event.order_id, None) or self._order(event.order_id, name, connector)
                self._orders.pop(event.order_id, None)
                self._order_event(TERMINAL_EVENTS[tag], event.order_id, order, order.get("price"), order.get("amount"))
        except Exception:
            self.logger().debug("Bot monitoring: could not record an order event", exc_info=True)

    def _order(self, order_id: str, name: str, connector: Any, pair: Optional[str] = None,
               side: Optional[str] = None) -> Dict[str, Any]:
        """What is known about an order, from its created event or, for one placed before the sink started, from
        the connector."""
        if order_id not in self._orders:
            tracked = self._tracked_orders(connector).get(order_id)
            self._orders[order_id] = {
                "exchange": name,
                "pair": tracked.trading_pair if tracked is not None else pair,
                "side": tracked.trade_type.name if tracked is not None else side,
                "price": number(tracked.price) if tracked is not None else None,
                "amount": number(tracked.amount) if tracked is not None else None,
                "reported": 0.0, "done": False}
        return self._orders[order_id]

    @classmethod
    def _fully_filled(cls, connector: Any, order_id: str, order: Dict[str, Any]) -> bool:
        tracked = cls._tracked_orders(connector).get(order_id)
        if tracked is not None and tracked.amount:
            return tracked.executed_amount_base >= tracked.amount  # the tracker counts the fill before announcing it
        return order["amount"] is not None and order["reported"] >= order["amount"] * (1 - 1e-9)

    def _order_event(self, event: str, order_id: str, order: Dict[str, Any], price: Optional[float],
                     amount: Optional[float]):
        self._write("order_event", {"exchange": order.get("exchange"), "pair": order.get("pair"),
                                    "side": order.get("side"), "event": event},
                    {"order_id": order_id, "price": price, "amount": amount,
                     "level": self._levels_by_order.get(order_id)},
                    time.time_ns() + next(self._tiebreak) % 1000)

    # -------------------------------------------------------------------------------------------- plumbing

    def _write(self, table: str, tags: Dict[str, Any], fields: Dict[str, Any], timestamp_ns: Optional[int] = None):
        self._writer.add(to_line(table, {"bot": self._bot, **tags},
                                 {key: value for key, value in fields.items() if value is not None},
                                 timestamp_ns or time.time_ns()))

    def _beat_process(self):
        """Own thread and blocking HTTP: it keeps beating while the event loop is frozen, and stops only when the
        process dies or the strategy stops."""
        headers = {"Authorization": f"Bearer {self._config.token}"}
        while not self._heartbeat_stop.is_set():
            line = to_line("bot_heartbeat", {"bot": self._bot, "level": "process"}, {"seen": 1}, time.time_ns())
            request = urllib.request.Request(self._config.write_url, data=line.encode(), headers=headers, method="POST")
            try:
                urllib.request.urlopen(request, timeout=5).close()
            except (urllib.error.URLError, OSError, ValueError):
                pass  # the writer warns when InfluxDB is unreachable
            self._heartbeat_stop.wait(self.PROCESS_HEARTBEAT_INTERVAL)
