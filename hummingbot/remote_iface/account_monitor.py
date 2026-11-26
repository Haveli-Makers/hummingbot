"""
Account monitoring: one exchange account's balances, open orders, fills and market data, written to InfluxDB 3 for
the dashboard's Account Monitoring page, which reads them back through the api-server's /metrics endpoints.

It runs inside a bot that never trades (scripts/account_monitor.py) and holds the account's API keys, so it sees the
account whatever acts on it: other bots on the same account, or people. Every account row carries an `account` tag
(a name from the script's config) and an `exchange` tag.

    every update interval (default 5 s)
        account_balance     per asset with a balance: total and available. An asset that drops to zero gets one
                            zero row, so charts fall to 0 instead of stopping at the last value.
        account_open_order  every open order on the account, read from the exchange. All rows of one update share
                            the update's time (plus i ns), so the latest update's rows are the orders open now.
        account_fill        new trades from the exchange's trade history, at the trade's own time. Writing a trade
                            again overwrites it, so restarts can't double count.
        account_update      one row per update: which reads worked (balances_ok, orders_ok, fills_ok), counts, and
                            any errors. The api-server finds the latest open-order snapshot through it.
    every market data interval (default 1 s)
        market              per pair: best bid and ask, mid, spread %, quote depth within 1% of mid, and book_age_s
                            (seconds since the order book last changed), tagged source=account_monitor.
"""
import itertools
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from hummingbot.remote_iface.influxdb import number, to_line

logger = logging.getLogger(__name__)

TRADE_PAGES_PER_UPDATE = 10
TIME_KEYS = ("timestamp", "time", "created_at")  # a trade's time, in epoch ms or s, by exchange


def as_float(value: Any) -> Optional[float]:
    """A finite float from a number or a numeric string, or None."""
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return None
    return number(value)


def first(raw: Dict[str, Any], *keys: str) -> Any:
    """The first of `keys` that `raw` has a value for: exchanges name the same trade fields differently."""
    for key in keys:
        if raw.get(key) not in (None, ""):
            return raw[key]
    return None


class AccountMonitor:
    """Reads one account through a connector and writes what it finds to an InfluxDBWriter. Never raises."""

    def __init__(self, connector: Any, account: str, writer: Any, trading_pairs: List[str],
                 fills_lookback_s: float = 24 * 3600, trades_page_size: int = 100):
        self._connector = connector
        self._writer = writer
        self._pairs = list(trading_pairs)
        self._tags = {"account": account, "exchange": connector.name}
        self._page_size = trades_page_size
        self._fills_since_ms = int((time.time() - fills_lookback_s) * 1000)
        self._fills_at_since: Set[str] = set()  # trade ids at exactly _fills_since_ms, already written
        self._assets: Set[str] = set()
        self._book_changed: Dict[str, Tuple[Any, float]] = {}
        self._warned: Set[str] = set()
        self.last_update: Dict[str, Any] = {}

    # ------------------------------------------------------------------------------------------ account

    async def update(self) -> Dict[str, Any]:
        """Reads balances, open orders and new fills, and writes them with one account_update row."""
        now_ns = time.time_ns()
        errors: List[str] = []
        balances_ok, assets = await self._write_balances(now_ns, errors)
        orders_ok, open_orders = await self._write_open_orders(now_ns, errors)
        fills_ok, new_fills = await self._write_fills(errors)
        result = {"balances_ok": balances_ok, "orders_ok": orders_ok, "fills_ok": fills_ok, "assets": assets,
                  "open_orders": open_orders, "new_fills": new_fills, "error": "; ".join(errors)}
        self._writer.add(to_line("account_update", self._tags, result, now_ns))
        self.last_update = {**result, "time": now_ns / 1e9}
        return result

    async def _write_balances(self, now_ns: int, errors: List[str]) -> Tuple[bool, Optional[int]]:
        try:
            # Fresh from the exchange: with its user stream up, the connector itself refreshes only every 2 minutes.
            await self._connector._update_balances()
        except Exception as e:
            errors.append(f"balances: {e}")
            return False, None
        totals = self._connector.get_all_balances()
        available = self._connector.available_balances
        held = {asset for asset, total in totals.items() if number(total) or number(available.get(asset))}
        offset = itertools.count()
        for asset in sorted(held | self._assets):
            gone = asset not in held
            self._writer.add(to_line("account_balance", {**self._tags, "asset": asset},
                                     {"total": 0.0 if gone else number(totals.get(asset)) or 0.0,
                                      "available": 0.0 if gone else number(available.get(asset)) or 0.0},
                                     now_ns + next(offset)))
        self._assets = held
        return True, len(held)

    async def _write_open_orders(self, now_ns: int, errors: List[str]) -> Tuple[bool, Optional[int]]:
        get_open_orders = getattr(self._connector, "get_open_orders", None)
        if get_open_orders is None:
            self._warn_once("orders", f"{self._connector.name} can't list the account's open orders yet, so the "
                                      "Open orders tab stays empty for this account.")
            return False, None
        try:
            orders = await get_open_orders()
        except Exception as e:
            errors.append(f"open orders: {e}")
            return False, None
        offset = itertools.count()
        for order in orders:
            self._writer.add(to_line(
                "account_open_order",
                {**self._tags, "pair": order.trading_pair, "side": "BUY" if order.is_buy else "SELL"},
                {"order_id": str(order.exchange_order_id or ""), "client_order_id": str(order.client_order_id or ""),
                 "price": number(order.price), "amount": number(order.amount),
                 "filled": number(order.executed_amount), "status": str(order.status or ""),
                 "created_ms": int(order.time or 0)},
                now_ns + next(offset)))
        return True, len(orders)

    async def _write_fills(self, errors: List[str]) -> Tuple[bool, Optional[int]]:
        get_trades = getattr(self._connector, "get_all_account_trades", None)
        if get_trades is None:
            self._warn_once("fills", f"{self._connector.name} can't read the account's trade history yet, so the "
                                     "Fills tab stays empty for this account.")
            return False, None
        written = 0
        try:
            # Oldest first from _fills_since_ms, a page at a time. A full page means there may be more.
            for _ in range(TRADE_PAGES_PER_UPDATE):
                trades = await get_trades(start_time=self._fills_since_ms, limit=self._page_size) or []
                since_before = self._fills_since_ms
                # Oldest first within the page too, whatever order the exchange sends: the cursor only moves forward.
                for raw in sorted(trades, key=lambda trade: as_float(first(trade, *TIME_KEYS)) or 0):
                    written += await self._write_fill(raw)
                if len(trades) < self._page_size or self._fills_since_ms == since_before:
                    break
        except Exception as e:
            errors.append(f"fills: {e}")
            return False, written
        return True, written

    async def _write_fill(self, raw: Dict[str, Any]) -> int:
        """Writes one trade unless it was written already; returns 1 when written."""
        trade_id = first(raw, "id", "trade_id", "tradeId")
        at_ms = as_float(first(raw, *TIME_KEYS))
        price, amount = as_float(first(raw, "price")), as_float(first(raw, "quantity", "qty", "amount"))
        if trade_id is None or at_ms is None or price is None or amount is None:
            return 0
        at_ms = at_ms if at_ms > 1e11 else at_ms * 1000
        trade_id, ms = str(trade_id), int(at_ms)
        if ms < self._fills_since_ms or (ms == self._fills_since_ms and trade_id in self._fills_at_since):
            return 0
        if ms > self._fills_since_ms:
            self._fills_since_ms, self._fills_at_since = ms, set()
        self._fills_at_since.add(trade_id)
        side = str(first(raw, "side") or "").upper()
        self._writer.add(to_line(
            "account_fill",
            {**self._tags, "pair": await self._trading_pair(first(raw, "symbol", "market", "pair")),
             "side": side, "trade_id": trade_id},
            {"order_id": str(first(raw, "order_id", "orderId") or ""), "price": price, "amount": amount,
             "quote_amount": price * amount, "fee": as_float(first(raw, "fee_amount", "fee", "commission"))},
            int(at_ms * 1_000_000)))
        return 1

    async def _trading_pair(self, symbol: Any) -> str:
        if not symbol:
            return ""
        try:
            return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=str(symbol))
        except Exception:  # a market the connector doesn't know: keep the exchange's name for it
            return str(symbol)

    # ------------------------------------------------------------------------------------------- market

    def sample_market(self, now: Optional[float] = None) -> int:
        """Writes a `market` row per pair from the connector's live order books; returns how many it wrote."""
        now = time.time() if now is None else now
        written = 0
        for pair in self._pairs:
            try:
                book = self._connector.get_order_book(pair)
                bids, asks = list(book.bid_entries()), list(book.ask_entries())
            except Exception:  # no book yet for this pair
                continue
            if not bids or not asks:
                continue
            best_bid, best_ask = max(row.price for row in bids), min(row.price for row in asks)
            mid = (best_bid + best_ask) / 2
            if mid <= 0:
                continue
            signature = (book.snapshot_uid, book.last_diff_uid, best_bid, best_ask)
            if self._book_changed.get(pair, (None,))[0] != signature:
                self._book_changed[pair] = (signature, now)
            self._writer.add(to_line(
                "market", {"exchange": self._connector.name, "pair": pair, "source": "account_monitor"},
                {"best_bid": float(best_bid), "best_ask": float(best_ask), "mid_price": float(mid),
                 "spread_pct": float((best_ask - best_bid) / mid * 100),
                 "bid_depth_quote_1_0": float(sum(row.price * row.amount for row in bids if row.price >= mid * 0.99)),
                 "ask_depth_quote_1_0": float(sum(row.price * row.amount for row in asks if row.price <= mid * 1.01)),
                 "book_age_s": float(now - self._book_changed[pair][1])},
                int(now * 1e9)))
            written += 1
        return written

    def _warn_once(self, key: str, message: str):
        if key not in self._warned:
            self._warned.add(key)
            logger.warning(f"Account monitoring: {message}")
