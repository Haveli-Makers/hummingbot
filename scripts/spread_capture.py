import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Set

from pydantic import Field

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

SUPPORTED_CONNECTORS = [
    "binance",
    "binance_perpetual",
    "binance_us",
    "kucoin",
    "gate_io",
    "mexc",
    "ascend_ex",
    "cube",
    "hyperliquid",
    "dexalot",
    "coindcx",
    "wazirx",
]


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
        },
    )
    quote_token: str = Field(
        default="USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter the quote token to filter pairs (e.g., USDT, USDC): ",
            "prompt_on_new": True,
        },
    )
    interval_sec: int = Field(
        default=900,
        gt=0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the fetch interval in seconds (e.g., 900 for 15 minutes): ",
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


def get_rate_source(connector_name: str) -> RateSourceBase:
    """
    Factory method to get the appropriate rate source based on connector name.

    :param connector_name: Name of the connector (e.g., 'binance', 'kucoin', 'gate_io', etc.)
    :return: The corresponding rate source instance
    :raises ValueError: If the connector is not supported
    """
    connector_name_lower = connector_name.lower()

    if connector_name_lower == "binance":
        from hummingbot.core.rate_oracle.sources.binance_rate_source import BinanceRateSource

        return BinanceRateSource()
    elif connector_name_lower == "binance_us":
        from hummingbot.core.rate_oracle.sources.binance_us_rate_source import BinanceUSRateSource

        return BinanceUSRateSource()
    elif connector_name_lower == "kucoin":
        from hummingbot.core.rate_oracle.sources.kucoin_rate_source import KucoinRateSource

        return KucoinRateSource()
    elif connector_name_lower == "gate_io":
        from hummingbot.core.rate_oracle.sources.gate_io_rate_source import GateIoRateSource

        return GateIoRateSource()
    elif connector_name_lower == "mexc":
        from hummingbot.core.rate_oracle.sources.mexc_rate_source import MexcRateSource

        return MexcRateSource()
    elif connector_name_lower == "ascend_ex":
        from hummingbot.core.rate_oracle.sources.ascend_ex_rate_source import AscendExRateSource

        return AscendExRateSource()
    elif connector_name_lower == "cube":
        from hummingbot.core.rate_oracle.sources.cube_rate_source import CubeRateSource

        return CubeRateSource()
    elif connector_name_lower == "hyperliquid":
        from hummingbot.core.rate_oracle.sources.hyperliquid_rate_source import HyperliquidRateSource

        return HyperliquidRateSource()
    elif connector_name_lower == "dexalot":
        from hummingbot.core.rate_oracle.sources.dexalot_rate_source import DexalotRateSource

        return DexalotRateSource()

    elif connector_name_lower == "wazirx":
        from hummingbot.core.rate_oracle.sources.wazirx_rate_source import WazirxRateSource

        return WazirxRateSource()
    elif connector_name_lower == "coindcx":
        from hummingbot.core.rate_oracle.sources.coindcx_rate_source import CoindcxRateSource

        return CoindcxRateSource()
    elif connector_name_lower == "coinswitch":
        from hummingbot.core.rate_oracle.sources.coinswitch_rate_source import CoinswitchRateSource

        return CoinswitchRateSource()

    else:
        raise ValueError(
            f"Unsupported connector: {connector_name}. Supported connectors: " f"{', '.join(SUPPORTED_CONNECTORS)}"
        )


class SpreadCapture(ScriptStrategyBase):
    """
    A script that fetches and stores spread data from various exchanges.

    Configuration is created via the CLI 'create' command:
        - connector_name: The exchange connector to use (e.g., 'binance', 'kucoin', 'gate_io', 'mexc')
        - quote_token: The quote token to filter pairs (e.g., 'USDT', 'USDC')
        - interval_sec: How often to fetch data (in seconds)
    """

    markets: Dict[str, Set[str]] = {}

    @classmethod
    def init_markets(cls, config: SpreadCaptureConfig):
        """Initialize markets from config. Called by the start command."""
        cls.markets = {}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[SpreadCaptureConfig] = None):
        if config is None:
            config = SpreadCaptureConfig()
        super().__init__(connectors, config)

        self.connector_name = config.connector_name
        self.quote_token = config.quote_token
        self.interval_sec = config.interval_sec
        self.excluding_pairs: Set[str] = self._parse_excluding_pairs(config.excluding_pairs)
        self.data_retention_days: int = config.data_retention_days

        self.last_run: int = 0
        self._rate_source: Optional[RateSourceBase] = None
        self._initialized: bool = False
        self._initialize_rate_source()

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
            bid_ask_prices = await self._rate_source.get_bid_ask_prices(quote_token=self.quote_token)

            if not bid_ask_prices:
                self.logger().warning(f"No bid/ask prices received from {self.connector_name}")
                return

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
            self.logger().info(
                f"Processed {len(market_data_batch)} trading pairs from {self.connector_name}"
                + (f" (excluded {excluded_count} pairs)" if excluded_count > 0 else "")
            )
            self._remove_old_market_data()

        except Exception as e:
            self.logger().error(f"Error fetching bid/ask prices from {self.connector_name}: {e}")

    def _remove_old_market_data(self):
        """Delete MarketData rows older than data_retention_days. Skipped if data_retention_days is 0."""
        if self.data_retention_days == 0:
            return
        try:
            markets_recorder = MarketsRecorder._shared_instance
            if markets_recorder is None:
                return
            cutoff = time.time() - self.data_retention_days * 24 * 3600
            deleted = markets_recorder.delete_old_market_data(cutoff)
            if deleted > 0:
                self.logger().info(
                    f"Removed {deleted} market data records older than {self.data_retention_days} days"
                )
        except Exception as e:
            self.logger().error(f"Error removing old market data: {e}")

    def store_spread_data(self, market_data_list: List[dict]):
        """
        Store spread/market data using the MarketsRecorder.
        """
        if not market_data_list:
            return

        try:
            markets_recorder = MarketsRecorder._shared_instance
            if markets_recorder is not None:
                markets_recorder.store_market_data(market_data_list)
                self.logger().info(f"Stored {len(market_data_list)} market data records to database")
            else:
                self.logger().warning("MarketsRecorder instance not available - data not stored")
        except Exception as e:
            self.logger().error(f"Error storing market data: {e}")

    def _parse_excluding_pairs(self, excluding_pairs_str: str) -> Set[str]:
        """
        Parse the comma-separated excluding pairs string into a set.
        """
        if not excluding_pairs_str or not excluding_pairs_str.strip():
            return set()
        return {pair.strip().upper() for pair in excluding_pairs_str.split(",") if pair.strip()}

    def on_tick(self):
        if not self._initialized:
            return

        now = int(time.time())
        if now - self.last_run < self.interval_sec:
            return

        self.last_run = now
        safe_ensure_future(self.fetch_and_store_spread())


def _create_config_from_args(connector_name: str, quote_token: str, interval_sec: int,
                             excluding_pairs: str, data_retention_days: int) -> SpreadCaptureConfig:
    return SpreadCaptureConfig(
        connector_name=connector_name,
        quote_token=quote_token,
        interval_sec=interval_sec,
        excluding_pairs=excluding_pairs,
        data_retention_days=data_retention_days,
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

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    try:
        client_config = ClientConfigAdapter(ClientConfigMap())
        sql_manager = SQLConnectionManager(client_config, SQLConnectionType.TRADE_FILLS, db_name="spread_capture_db")
        try:
            market_data_collection = client_config.hb_config.market_data_collection
        except Exception:
            market_data_collection = MarketDataCollectionConfigMap()

        try:
            MarketsRecorder(sql=sql_manager, markets=[], config_file_path="spread_capture", strategy_name="spread_capture", market_data_collection=market_data_collection)
            logging.getLogger("spread_capture").info("MarketsRecorder initialized; DB persistence enabled")
        except Exception as e:
            logging.getLogger("spread_capture").exception(f"Failed to initialize MarketsRecorder: {e}")
    except Exception as e:
        logging.getLogger("spread_capture").exception(f"Failed to initialize DB manager: {e}")

    quote_tokens = [t.strip() for t in args.quote_tokens.split(",") if t.strip()]

    async def run_loop():
        while True:
            for qt in quote_tokens:
                config = _create_config_from_args(
                    connector_name=args.connector_name,
                    quote_token=qt,
                    interval_sec=args.interval_sec,
                    excluding_pairs=args.excluding_pairs,
                    data_retention_days=args.data_retention_days,
                )

                sc = SpreadCapture(connectors={}, config=config)
                if not sc._initialized:
                    logging.getLogger("spread_capture").error("Rate source not initialized; skipping run")
                    continue
                try:
                    await sc.fetch_and_store_spread()
                except Exception as e:
                    logging.getLogger("spread_capture").exception(f"Error during fetch for {qt}: {e}")

            if args.once:
                return

            await asyncio.sleep(args.interval_sec)

    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrupted, exiting")


if __name__ == "__main__":
    """Run the spread_capture script standalone using python scripts/spread_capture.py --connector_name abc --quote_tokens INR,USDT --interval_sec 900 --once."""
    main()
