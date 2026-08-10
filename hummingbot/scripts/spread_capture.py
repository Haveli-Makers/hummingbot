import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Set

from pydantic import Field

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hummingbot import set_data_path
from hummingbot.client.config.client_config_map import ClientConfigMap, DBOtherMode, MarketDataCollectionConfigMap
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType

SUPPORTED_CONNECTORS = list(RATE_ORACLE_SOURCES.keys())
DB_TARGETS = ("local", "production")


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
        },
    )


def get_rate_source(connector_name: str) -> RateSourceBase:
    source_cls = RATE_ORACLE_SOURCES.get(connector_name.lower())
    if source_cls is None:
        raise ValueError(
            f"Unsupported connector: {connector_name}. Supported connectors: {', '.join(SUPPORTED_CONNECTORS)}"
        )
    return source_cls()


class SpreadCapture:
    _logger: Optional[logging.Logger] = None
    # Keyed by db_target ("local"/"production") - MarketsRecorder.__init__ overwrites the
    # process-wide MarketsRecorder._shared_instance, so a single long-lived process (the API
    # server) handling both local and production writes must keep its own recorder per target
    # rather than relying on that global singleton.
    _recorders: Dict[str, MarketsRecorder] = {}

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
        self._initialize_rate_source()

    @classmethod
    def initialize_markets_recorder(cls, db_target: str = "local") -> MarketsRecorder:
        if db_target in cls._recorders:
            return cls._recorders[db_target]

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

        sql_manager = SQLConnectionManager(
            client_config, SQLConnectionType.TRADE_FILLS, db_name=db_name
        )
        try:
            market_data_collection = client_config.hb_config.market_data_collection
        except Exception:
            market_data_collection = MarketDataCollectionConfigMap()

        recorder = MarketsRecorder(
            sql=sql_manager,
            markets=[],
            config_file_path=f"spread_capture_{db_target}",
            strategy_name="spread_capture",
            market_data_collection=market_data_collection,
        )
        cls._recorders[db_target] = recorder
        return recorder

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

        try:
            self._recorder = self.initialize_markets_recorder(self.db_target)
            bid_ask_prices = await self._rate_source.get_bid_ask_prices(quote_token=self.quote_token)

            if not bid_ask_prices:
                self.logger().warning(f"No bid/ask prices received from {self.connector_name}")
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

                self.logger().info(
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

            self.store_spread_data(market_data_batch)
            self._remove_old_market_data()
            self.logger().info(
                f"Processed {len(market_data_batch)} trading pairs from {self.connector_name}"
                + (f" (excluded {excluded_count} pairs)" if excluded_count > 0 else "")
            )
            return market_data_batch

        except Exception as e:
            self.logger().error(f"Error fetching bid/ask prices from {self.connector_name}: {e}")
            raise

    def _remove_old_market_data(self):
        """Delete MarketData rows older than data_retention_days. Skipped if data_retention_days is 0."""
        if self.data_retention_days == 0:
            return
        try:
            markets_recorder = getattr(self, "_recorder", None) or self.initialize_markets_recorder(self.db_target)
            cutoff = time.time() - self.data_retention_days * 24 * 3600
            deleted = markets_recorder.delete_old_market_data(cutoff)
            if deleted > 0:
                self.logger().info(
                    f"Removed {deleted} market data records older than {self.data_retention_days} days "
                    f"from '{self.db_target}' database"
                )
        except Exception as e:
            self.logger().error(f"Error removing old market data: {e}")

    def store_spread_data(self, market_data_list: List[dict]):
        """
        Store spread/market data using the MarketsRecorder for this run's db_target.
        """
        if not market_data_list:
            return

        try:
            markets_recorder = getattr(self, "_recorder", None) or self.initialize_markets_recorder(self.db_target)
            markets_recorder.store_market_data(market_data_list)
            self.logger().info(
                f"Stored {len(market_data_list)} market data records to '{self.db_target}' database"
            )
        except Exception as e:
            self.logger().error(f"Error storing market data: {e}")

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
        SpreadCapture.initialize_markets_recorder(args.db_target)
        logging.getLogger("spread_capture").info(f"MarketsRecorder initialized for '{args.db_target}' database")
    except Exception as e:
        logging.getLogger("spread_capture").exception(f"Failed to initialize MarketsRecorder: {e}")

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
