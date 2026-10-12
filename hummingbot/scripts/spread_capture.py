import argparse
import asyncio
import logging
import os
import random
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

from pydantic import Field
from sqlalchemy import create_engine, delete, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hummingbot import set_data_path
from hummingbot.client.config.client_config_map import ClientConfigMap, DBOtherMode
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.model.market_data import MarketData
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType

SUPPORTED_CONNECTORS = list(RATE_ORACLE_SOURCES.keys())
DB_TARGETS = ("local", "production")

DB_LOCK_TIMEOUT_MS = 15_000       
DB_STATEMENT_TIMEOUT_MS = 45_000  
DB_CONNECT_TIMEOUT_S = 10
DB_WRITE_ATTEMPTS = 5             
DB_WRITE_BUDGET_S = 60           
INSERT_CHUNK_SIZE = 500
RETENTION_INTERVAL_S = 3600       
RETENTION_PAIRS_PER_BATCH = 100   
RETENTION_DELISTED_LOOKBACK_S = 86400 
RETENTION_BUDGET_S = 20          
MAX_STORABLE_SPREAD = 1000        

T = TypeVar("T")


class SpreadCaptureConfig(BaseClientModel):
    """
    Configuration for the Spread Capture script.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the connector name ({', '.join(SUPPORTED_CONNECTORS)}): ",
            "prompt_on_new": True,
            "input_type": "select",
            "options": SUPPORTED_CONNECTORS,
        },
    )
    quote_token: str = Field(
        default="USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter the quote token to filter pairs (e.g., USDT, USDC): ",
            "prompt_on_new": True,
        },
    )
    excluding_pairs: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Enter trading pairs to exclude (comma-separated, e.g., BTC-USDT,ETH-USDT), leave empty to include all: ",
            "prompt_on_new": True,
        },
    )
    data_retention_days: int = Field(
        default=30,
        ge=0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the number of days to retain market data (rows older than this will be deleted, enter 0 to keep data): ",
            "prompt_on_new": True,
        },
    )
    db_target: str = Field(
        default="local",
        json_schema_extra={
            "prompt": lambda mi: f"Database target to store fetched data in ({', '.join(DB_TARGETS)}): ",
            "prompt_on_new": True,
            "input_type": "select",
            "options": list(DB_TARGETS),
            "show_on_dashboard": False,
        },
    )


def get_rate_source(connector_name: str) -> RateSourceBase:
    source_cls = RATE_ORACLE_SOURCES.get(connector_name.lower())
    if source_cls is None:
        raise ValueError(
            f"Unsupported connector: {connector_name}. Supported connectors: {', '.join(SUPPORTED_CONNECTORS)}"
        )
    return source_cls()


def _is_transient_db_error(exc: BaseException) -> bool:
    """Lock waits, deadlocks, serialization failures and dropped connections are worth a retry."""
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    message = str(getattr(exc, "orig", exc)).lower()
    return any(marker in message for marker in (
        "database is locked",            
        "deadlock detected",              
        "could not serialize",            
        "lock timeout",                  
        "server closed the connection",
        "connection reset",
        "connection refused",
        "could not connect",
        "timeout expired",
        "ssl syscall error",
        "terminating connection",
    ))


class SpreadCapture:
    _logger: Optional[logging.Logger] = None

    _engines: Dict[str, Engine] = {}
    _engine_creation_lock: threading.Lock = threading.Lock()
    _sqlite_write_locks: Dict[str, threading.Lock] = {}
    _last_retention_at: Dict[tuple, float] = {}
    _retention_lock: threading.Lock = threading.Lock()

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: Optional[SpreadCaptureConfig] = None):
        if config is None:
            config = SpreadCaptureConfig()

        self.connector_name = config.connector_name
        self.quote_token = config.quote_token
        self.excluding_pairs: Set[str] = self._parse_excluding_pairs(config.excluding_pairs)
        self.data_retention_days: int = config.data_retention_days
        self.db_target: str = (config.db_target or "local").strip().lower()
        if self.db_target not in DB_TARGETS:
            self.logger().warning(f"Unknown db_target '{self.db_target}', falling back to 'local'")
            self.db_target = "local"

        self._rate_source: Optional[RateSourceBase] = None
        self._initialized: bool = False
        self.timings: Dict[str, Any] = {}
        self._initialize_rate_source()

    # ------------------------------------------------------------------ timing

    @contextmanager
    def _timed(self, phase: str):
        """
        Record how long `phase` took as timings['<phase>_s']. While it runs, timings['current_phase']
        names it; if it raises or is cancelled (runner timeout), timings['stopped_in_phase'] keeps
        the innermost phase that was interrupted.
        """
        previous_phase = self.timings.get("current_phase")
        self.timings["current_phase"] = phase
        start = time.perf_counter()
        try:
            yield
        except BaseException:
            self.timings.setdefault("stopped_in_phase", phase)
            raise
        finally:
            self.timings[f"{phase}_s"] = round(time.perf_counter() - start, 3)
            if previous_phase is None:
                self.timings.pop("current_phase", None)
            else:
                self.timings["current_phase"] = previous_phase

    def _add_timing(self, key: str, seconds: float):
        self.timings[key] = round(self.timings.get(key, 0) + seconds, 3)

    def _timing_summary(self) -> str:
        t = self.timings
        parts = [f"fetch {t.get('fetch_s', '-')}s ({t.get('pairs_fetched', '?')} pairs)"]
        store = f"store {t.get('store_s', '-')}s ({t.get('rows_stored', 0)} rows, {t.get('store_attempts', 0)} attempt(s)"
        if t.get("store_lock_wait_s"):
            store += f", lock wait {t['store_lock_wait_s']}s"
        if t.get("store_retry_sleep_s"):
            store += f", retry backoff {t['store_retry_sleep_s']}s"
        parts.append(store + ")")
        parts.append(
            f"retention {t.get('retention_s', '-')}s ({t.get('retention_status', '-')}, "
            f"{t.get('retention_deleted', 0)} deleted)"
        )
        if "engine_init_s" in t and t["engine_init_s"] >= 0.01:
            parts.insert(0, f"db init {t['engine_init_s']}s")
        return (
            f"[{self.connector_name} {self.quote_token} -> {self.db_target}] "
            + " | ".join(parts)
            + f" | total {t.get('total_s', '-')}s"
        )

    # ------------------------------------------------------------------ DB engine

    @classmethod
    def get_engine(cls, db_target: str = "local") -> Engine:
        """
        Return the tuned engine for db_target, creating tables/migrations once per process.
        Safe to call from a worker thread (no main-thread requirement, unlike MarketsRecorder).
        """
        engine = cls._engines.get(db_target)
        if engine is not None:
            return engine

        with cls._engine_creation_lock:
            engine = cls._engines.get(db_target)
            if engine is not None:
                return engine

            data_dir = os.path.abspath(os.path.join(os.getcwd(), "bots", "data"))
            os.makedirs(data_dir, exist_ok=True)
            set_data_path(data_dir)

            client_config = ClientConfigAdapter(ClientConfigMap())
            db_name = "haveli"

            if db_target == "production":
                db_name = os.environ.get("PROD_DB_NAME", "haveli")
                client_config.db_mode = DBOtherMode(
                    db_engine=os.environ.get("PROD_DB_ENGINE", "postgresql"),
                    db_host=os.environ["PROD_DB_HOST"],
                    db_port=int(os.environ.get("PROD_DB_PORT", "5432")),
                    db_username=os.environ["PROD_DB_USERNAME"],
                    db_password=os.environ["PROD_DB_PASSWORD"],
                    db_name=db_name,
                )

            sql_manager = SQLConnectionManager(client_config, SQLConnectionType.TRADE_FILLS, db_name=db_name)
            url = sql_manager.engine.url
            sql_manager.engine.dispose()

            engine = cls._build_engine(url)
            cls._engines[db_target] = engine
            cls._sqlite_write_locks.setdefault(db_target, threading.Lock())
            return engine

    @staticmethod
    def _build_engine(url) -> Engine:
        if url.get_backend_name() == "sqlite":
            engine = create_engine(
                url,
                connect_args={"timeout": DB_LOCK_TIMEOUT_MS / 1000, "check_same_thread": False},
            )

            @event.listens_for(engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _):
                cursor = dbapi_conn.cursor()
                # WAL lets the dashboard read while a schedule writes (and vice versa).
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute(f"PRAGMA busy_timeout={DB_LOCK_TIMEOUT_MS}")
                cursor.close()

            return engine

        connect_args = {}
        if url.get_backend_name() == "postgresql":
            connect_args = {
                "connect_timeout": DB_CONNECT_TIMEOUT_S,
                "options": f"-c lock_timeout={DB_LOCK_TIMEOUT_MS} -c statement_timeout={DB_STATEMENT_TIMEOUT_MS}",
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3,
            }
        return create_engine(url, poolclass=NullPool, connect_args=connect_args)

    @classmethod
    def _sqlite_write_lock(cls, db_target: str, engine: Engine) -> Optional[threading.Lock]:
        if engine.dialect.name != "sqlite":
            return None
        with cls._engine_creation_lock:
            return cls._sqlite_write_locks.setdefault(db_target, threading.Lock())

    def _run_db_write(self, description: str, operation: Callable[[Engine], T], metric: str) -> T:
        engine = self.get_engine(self.db_target)
        write_lock = self._sqlite_write_lock(self.db_target, engine)
        deadline = time.monotonic() + DB_WRITE_BUDGET_S
        attempt = 0
        while True:
            attempt += 1
            self.timings[f"{metric}_attempts"] = self.timings.get(f"{metric}_attempts", 0) + 1
            try:
                if write_lock is None:
                    return operation(engine)
                wait_start = time.perf_counter()
                acquired = write_lock.acquire(timeout=max(1.0, deadline - time.monotonic()))
                self._add_timing(f"{metric}_lock_wait_s", time.perf_counter() - wait_start)
                if not acquired:
                    raise TimeoutError(f"Timed out waiting for the '{self.db_target}' write lock")
                try:
                    return operation(engine)
                finally:
                    write_lock.release()
            except Exception as e:
                retryable = isinstance(e, TimeoutError) or _is_transient_db_error(e)
                backoff = min(8.0, 0.5 * 2 ** (attempt - 1)) * random.uniform(0.5, 1.5)
                if not retryable or attempt >= DB_WRITE_ATTEMPTS or time.monotonic() + backoff > deadline:
                    raise
                self.logger().warning(
                    f"{description} on '{self.db_target}' hit a transient DB error "
                    f"(attempt {attempt}/{DB_WRITE_ATTEMPTS}), retrying in {backoff:.1f}s: {e}"
                )
                time.sleep(backoff)
                self._add_timing(f"{metric}_retry_sleep_s", backoff)

    # ------------------------------------------------------------------ rate source

    def _initialize_rate_source(self):
        """Initialize the rate source based on the configured connector."""
        try:
            if self.connector_name.lower() not in [c.lower() for c in SUPPORTED_CONNECTORS]:
                self.logger().error(
                    f"Unsupported connector: {self.connector_name}. " f"Supported: {', '.join(SUPPORTED_CONNECTORS)}"
                )
                return

            self._rate_source = get_rate_source(self.connector_name)

            if self.excluding_pairs:
                self.logger().info(f"✓ Excluding pairs: {', '.join(self.excluding_pairs)}")

            self._initialized = True
        except Exception as e:
            self.logger().error(f"Failed to initialize rate source: {e}")
            self._initialized = False

    async def fetch_and_store_spread(self):
        run_start = time.perf_counter()
        try:
            with self._timed("engine_init"):
                await asyncio.to_thread(self.get_engine, self.db_target)
            with self._timed("fetch"):
                bid_ask_prices = await self._rate_source.get_bid_ask_prices(quote_token=self.quote_token)
            self.timings["pairs_fetched"] = len(bid_ask_prices or {})

            if not bid_ask_prices:
                self.timings["total_s"] = round(time.perf_counter() - run_start, 3)
                self.logger().warning(
                    f"No bid/ask prices received from {self.connector_name}. {self._timing_summary()}"
                )
                return []

            market_data_batch: List[dict] = []
            excluded_count = 0

            for trading_pair, price_data in bid_ask_prices.items():
                # Skip excluded pairs
                if trading_pair in self.excluding_pairs:
                    excluded_count += 1
                    continue

                bid = float(price_data["bid"])
                ask = float(price_data["ask"])
                mid_price = float(price_data["mid"])
                spread = float(price_data["spread"])

                self.logger().debug(
                    f"{trading_pair} → BID: {bid}, ASK: {ask}, SPREAD: {spread:.4f}%"
                )

                market_data_batch.append(
                    {
                        "exchange": self.connector_name,
                        "trading_pair": trading_pair,
                        "best_bid": bid,
                        "best_ask": ask,
                        "mid_price": mid_price,
                        "spread": spread,
                    }
                )

            with self._timed("store"):
                await asyncio.to_thread(self.store_spread_data, market_data_batch)
            with self._timed("retention"):
                await asyncio.to_thread(self._remove_old_market_data, list(bid_ask_prices.keys()))

            self.timings["total_s"] = round(time.perf_counter() - run_start, 3)
            self.logger().info(
                f"Processed {len(market_data_batch)} trading pairs from {self.connector_name}"
                + (f" (excluded {excluded_count} pairs)" if excluded_count > 0 else "")
                + f". {self._timing_summary()}"
            )
            return market_data_batch

        except asyncio.CancelledError:
            self.timings["total_s"] = round(time.perf_counter() - run_start, 3)
            self.logger().warning(
                f"Spread capture for {self.connector_name} was cancelled (runner timeout) "
                f"during '{self.timings.get('stopped_in_phase', 'unknown')}'. {self._timing_summary()}"
            )
            raise
        except Exception as e:
            self.timings["total_s"] = round(time.perf_counter() - run_start, 3)
            self.logger().error(
                f"Error capturing spreads from {self.connector_name} "
                f"during '{self.timings.get('stopped_in_phase', 'unknown')}': {e}. {self._timing_summary()}"
            )
            raise

    # ------------------------------------------------------------------ writes

    def store_spread_data(self, market_data_list: List[dict]):
        """
        Bulk-insert this run's rows in short chunked transactions. Raises on failure so the run
        is reported as failed rather than silently losing data.
        """
        if not market_data_list:
            return

        timestamp = int(time.time())
        include_order_book = any(data.get("order_book") is not None for data in market_data_list)
        rows = []
        skipped = []
        for data in market_data_list:
            spread = round(float(data["spread"]), 2)
            if abs(spread) >= MAX_STORABLE_SPREAD:
                skipped.append(data["trading_pair"])
                continue
            row = {
                "timestamp": timestamp,
                "exchange": data["exchange"],
                "trading_pair": data["trading_pair"],
                "mid_price": data["mid_price"],
                "best_bid": data["best_bid"],
                "best_ask": data["best_ask"],
                "spread": spread,
            }
            if include_order_book:
                row["order_book"] = data.get("order_book")
            rows.append(row)
        if skipped:
            self.logger().warning(
                f"Skipped {len(skipped)} pairs with spread >= {MAX_STORABLE_SPREAD}%: {', '.join(skipped)}"
            )
        if not rows:
            return

        def _insert(engine: Engine) -> None:
            with engine.begin() as conn:
                for start in range(0, len(rows), INSERT_CHUNK_SIZE):
                    conn.execute(self._insert_ignoring_duplicates(engine, rows[start:start + INSERT_CHUNK_SIZE]))

        self._run_db_write("Storing market data", _insert, metric="store")
        self.timings["rows_stored"] = len(rows)
        if skipped:
            self.timings["rows_skipped"] = len(skipped)

    @staticmethod
    def _insert_ignoring_duplicates(engine: Engine, rows: List[dict]):
        table = MarketData.__table__
        if engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            return pg_insert(table).values(rows).on_conflict_do_nothing()
        if engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            return sqlite_insert(table).values(rows).on_conflict_do_nothing()
        return table.insert().values(rows)

    def _remove_old_market_data(self, current_pairs: List[str]):
        """
        Delete this exchange's MarketData rows older than data_retention_days.
        """
        if self.data_retention_days == 0:
            self.timings["retention_status"] = "disabled"
            return

        key = (self.db_target, self.connector_name)
        with self._retention_lock:
            if time.time() - self._last_retention_at.get(key, 0) < RETENTION_INTERVAL_S:
                self.timings["retention_status"] = "skipped (ran within the last hour)"
                return
            self._last_retention_at[key] = time.time()

        cutoff = int(time.time() - self.data_retention_days * 24 * 3600)
        table = MarketData.__table__
        started = time.monotonic()
        deleted_total = 0
        self.timings["retention_status"] = "done"
        try:
            engine = self.get_engine(self.db_target)
            find_start = time.perf_counter()
            with engine.connect() as conn:
                recent_pairs = conn.execute(
                    select(table.c.trading_pair).distinct().where(
                        table.c.exchange == self.connector_name,
                        table.c.timestamp >= cutoff - RETENTION_DELISTED_LOOKBACK_S,
                        table.c.timestamp < cutoff,
                    )
                ).scalars().all()
            self._add_timing("retention_find_pairs_s", time.perf_counter() - find_start)
            pairs = sorted(set(current_pairs) | set(recent_pairs))
            self.timings["retention_pairs"] = len(pairs)

            for start in range(0, len(pairs), RETENTION_PAIRS_PER_BATCH):
                if time.monotonic() - started > RETENTION_BUDGET_S:
                    self.timings["retention_status"] = "partial (time budget reached, continues next run)"
                    with self._retention_lock:
                        self._last_retention_at.pop(key, None)
                    break
                batch = pairs[start:start + RETENTION_PAIRS_PER_BATCH]

                def _delete_batch(engine: Engine, batch=batch) -> int:
                    with engine.begin() as conn:
                        return conn.execute(
                            delete(table).where(
                                table.c.exchange == self.connector_name,
                                table.c.trading_pair.in_(batch),
                                table.c.timestamp < cutoff,
                            )
                        ).rowcount or 0

                deleted_total += self._run_db_write("Removing old market data", _delete_batch, metric="retention")
                self.timings["retention_deleted"] = deleted_total
                self.timings["retention_batches"] = self.timings.get("retention_batches", 0) + 1

            if deleted_total > 0:
                self.logger().info(
                    f"Removed {deleted_total} {self.connector_name} market data records older than "
                    f"{self.data_retention_days} days from '{self.db_target}' database"
                )
            elif self.timings["retention_status"] == "done":
                self.timings["retention_status"] = "nothing to delete"
        except Exception as e:
            self.timings["retention_status"] = f"error: {e}"[:200]
            self.logger().error(f"Error removing old market data (will retry in an hour): {e}")

    def _parse_excluding_pairs(self, excluding_pairs_str: str) -> Set[str]:
        """
        Parse the comma-separated excluding pairs string into a set.
        """
        if not excluding_pairs_str or not excluding_pairs_str.strip():
            return set()
        return {pair.strip().upper() for pair in excluding_pairs_str.split(",") if pair.strip()}


def _create_config_from_args(connector_name: str, quote_token: str,
                             excluding_pairs: str, data_retention_days: int,
                             db_target: str = "local") -> SpreadCaptureConfig:
    return SpreadCaptureConfig(
        connector_name=connector_name,
        quote_token=quote_token,
        excluding_pairs=excluding_pairs,
        data_retention_days=data_retention_days,
        db_target=db_target,
    )


def main():
    parser = argparse.ArgumentParser(description="Run spread_capture as a standalone script")
    parser.add_argument("--connector_name", default="binance", help="Connector name, e.g. binance, kucoin")
    parser.add_argument(
        "--quote_tokens",
        default="USDT",
        help="Comma-separated quote tokens to fetch (e.g. INR,USDT)",
    )
    parser.add_argument("--interval_sec", type=int, default=900, help="Fetch interval in seconds")
    parser.add_argument(
        "--excluding_pairs",
        default="",
        help="Comma-separated trading pairs to exclude (e.g. BTC-USDT)",
    )
    parser.add_argument(
        "--data_retention_days",
        type=int,
        default=30,
        help="Days to retain market data (0 to keep all)",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--db_target",
        default="local",
        choices=list(DB_TARGETS),
        help="Database to store fetched data in: 'local' (sqlite) or 'production' (needs PROD_DB_* env vars)",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    try:
        SpreadCapture.get_engine(args.db_target)
        logging.getLogger("spread_capture").info(f"Database engine initialized for '{args.db_target}'")
    except Exception as e:
        logging.getLogger("spread_capture").exception(f"Failed to initialize database engine: {e}")

    quote_tokens = [t.strip() for t in args.quote_tokens.split(",") if t.strip()]

    async def run_loop():
        while True:
            for qt in quote_tokens:
                config = _create_config_from_args(
                    connector_name=args.connector_name,
                    quote_token=qt,
                    excluding_pairs=args.excluding_pairs,
                    data_retention_days=args.data_retention_days,
                    db_target=args.db_target,
                )

                sc = SpreadCapture(config=config)
                if not sc._initialized:
                    logging.getLogger("spread_capture_standalone").error("Rate source not initialized; skipping run")
                    continue
                try:
                    await sc.fetch_and_store_spread()
                except Exception as e:
                    logging.getLogger("spread_capture_standalone").exception(f"Error during fetch for {qt}: {e}")

            if args.once:
                return

            await asyncio.sleep(args.interval_sec)

    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrupted, exiting")


if __name__ == "__main__":
    """Run the spread_capture script standalone using python -m hummingbot.scripts.spread_capture --connector_name mexc --quote_tokens USDT --interval_sec 900 --once"""
    main()
