import os
import time
from typing import Dict, List, Optional, Set

from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


SUPPORTED_CONNECTORS = [
    "binance", "binance_perpetual", "binance_us", "kucoin", "gate_io", 
    "mexc", "ascend_ex", "coinbase_advanced_trade", "cube", "hyperliquid", "dexalot"
]


class SpreadCalculationConfig(BaseClientModel):
    """
    Configuration for the Spread Calculation script.
    """
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    connector_name: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: f"Enter the connector name ({', '.join(SUPPORTED_CONNECTORS)}): ",
            "prompt_on_new": True
        }
    )
    quote_token: str = Field(
        default="USDT",
        json_schema_extra={
            "prompt": lambda mi: "Enter the quote token to filter pairs (e.g., USDT, USDC): ",
            "prompt_on_new": True
        }
    )
    interval_sec: int = Field(
        default=900,
        gt=0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the fetch interval in seconds (e.g., 900 for 15 minutes): ",
            "prompt_on_new": True
        }
    )


def get_rate_source(connector_name: str) -> RateSourceBase:
    """
    Factory method to get the appropriate rate source based on connector name.
    
    :param connector_name: Name of the connector (e.g., 'binance', 'kucoin', 'gate_io', etc.)
    :return: The corresponding rate source instance
    :raises ValueError: If the connector is not supported
    """
    connector_name_lower = connector_name.lower()
    
    if connector_name_lower in ("binance"):
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
    else:
        raise ValueError(f"Unsupported connector: {connector_name}. Supported connectors: "
                        f"{', '.join(SUPPORTED_CONNECTORS)}")


class USDTQuoteSpreadViewer(ScriptStrategyBase):
    """
    A script that fetches and stores spread data from various exchanges.
    
    Configuration is created via the CLI 'create' command:
        - connector_name: The exchange connector to use (e.g., 'binance', 'kucoin', 'gate_io', 'mexc')
        - quote_token: The quote token to filter pairs (e.g., 'USDT', 'USDC')
        - interval_sec: How often to fetch data (in seconds)
    """

    markets: Dict[str, Set[str]] = {}

    @classmethod
    def init_markets(cls, config: SpreadCalculationConfig):
        """Initialize markets from config. Called by the start command."""
        cls.markets = {}  
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: SpreadCalculationConfig):
        super().__init__(connectors, config)
        
        # Load configuration from the config object
        self.connector_name = config.connector_name
        self.quote_token = config.quote_token
        self.interval_sec = config.interval_sec
        
        self.last_run: int = 0
        self._rate_source: Optional[RateSourceBase] = None
        self._initialized: bool = False
        self._initialize_rate_source()
    
    def _initialize_rate_source(self):
        """Initialize the rate source based on the configured connector."""
        try:
            if self.connector_name.lower() not in [c.lower() for c in SUPPORTED_CONNECTORS]:
                self.logger().error(
                    f"Unsupported connector: {self.connector_name}. "
                    f"Supported: {', '.join(SUPPORTED_CONNECTORS)}"
                )
                return
                
            self._rate_source = get_rate_source(self.connector_name)
            self.logger().info("=" * 60)
            self.logger().info("SPREAD CALCULATION SCRIPT STARTED")
            self.logger().info("=" * 60)
            self.logger().info(f"✓ Connector: {self.connector_name}")
            self.logger().info(f"✓ Quote token filter: {self.quote_token}")
            self.logger().info(f"✓ Fetch interval: {self.interval_sec} seconds")
            self.logger().info("=" * 60)
            self._initialized = True
        except Exception as e:
            self.logger().error(f"Failed to initialize rate source: {e}")
            self._initialized = False

    async def grid(self):
        # If not initialized, don't run
        if not self._initialized:
            return
            
        now = int(time.time())
        if now - self.last_run < self.interval_sec:
            return

        self.last_run = now
        
        try:
            if not hasattr(self._rate_source, 'get_bid_ask_prices'):
                self.logger().warning(
                    f"Rate source for {self.connector_name} does not support get_bid_ask_prices. "
                    f"Falling back to get_prices method."
                )
                await self._fetch_using_get_prices()
                return
            
            bid_ask_prices = await self._rate_source.get_bid_ask_prices(quote_token=self.quote_token)
            
            if not bid_ask_prices:
                self.logger().warning(f"No bid/ask prices received from {self.connector_name}")
                return
            
            market_data_batch: List[dict] = []
            
            for trading_pair, price_data in bid_ask_prices.items():
                bid = float(price_data['bid'])
                ask = float(price_data['ask'])
                mid_price = float(price_data['mid'])
                spread = float(price_data['spread'])
                spread_pct = float(price_data['spread_pct'])
                
                self.logger().info(
                    f"{trading_pair} → BID: {bid}, ASK: {ask}, "
                    f"SPREAD: {spread:.6f} ({spread_pct:.4f}%)"
                )
                
                market_data_batch.append({
                    'exchange': self.connector_name,
                    'trading_pair': trading_pair,
                    'best_bid': bid,
                    'best_ask': ask,
                    'mid_price': mid_price,
                    'spread': spread,
                    'spread_pct': spread_pct
                })
            
            self.store_spread_data(market_data_batch)
            self.logger().info(f"Processed {len(bid_ask_prices)} trading pairs from {self.connector_name}")
            
        except Exception as e:
            self.logger().error(f"Error fetching bid/ask prices from {self.connector_name}: {e}")
    
    async def _fetch_using_get_prices(self):
        """
        Fallback method for rate sources that don't support get_bid_ask_prices.
        Uses get_prices which only returns mid prices (no spread data).
        """
        try:
            prices = await self._rate_source.get_prices(quote_token=self.quote_token)
            
            if not prices:
                self.logger().warning(f"No prices received from {self.connector_name}")
                return
            
            market_data_batch: List[dict] = []
            
            for trading_pair, mid_price in prices.items():
                self.logger().info(f"{trading_pair} → MID: {float(mid_price)}")
                
                market_data_batch.append({
                    'exchange': self.connector_name,
                    'trading_pair': trading_pair,
                    'best_bid': None,
                    'best_ask': None,
                    'mid_price': float(mid_price),
                    'spread': None,
                    'spread_pct': None
                })
            
            self.store_spread_data(market_data_batch)
            self.logger().info(f"Processed {len(prices)} trading pairs from {self.connector_name} (mid prices only)")
            
        except Exception as e:
            self.logger().error(f"Error fetching prices from {self.connector_name}: {e}")
    
    def store_spread_data(self, market_data_list: List[dict]):
        """
        Store spread/market data using the MarketsRecorder.
        """
        if not market_data_list:
            return
            
        try:
            markets_recorder = MarketsRecorder._shared_instance
            if markets_recorder is not None:
                markets_recorder.store_market_data_batch(market_data_list)
                self.logger().info(f"Stored {len(market_data_list)} market data records to database")
            else:
                self.logger().warning("MarketsRecorder instance not available - data not stored")
        except Exception as e:
            self.logger().error(f"Error storing market data: {e}")
                    
    def on_tick(self):
        if not self._initialized:
            return
        safe_ensure_future(self.grid())
        
