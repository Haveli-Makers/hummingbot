import asyncio
from typing import Dict, Set, List
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
import time
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.rate_oracle.sources.binance_rate_source import BinanceRateSource
from hummingbot.connector.markets_recorder import MarketsRecorder

class USDTQuoteSpreadViewer(ScriptStrategyBase):

    interval_sec = 900  # 15 minutes
    last_run = 0

    # Manually defined markets
    markets: Dict[str, Set[str]] = {
        "binance": {'BTC-USDT'}  # Example pairs
    }

    connector_name = "binance"
    
    def __init__(self, connectors: Dict = None):
        super().__init__(connectors or {})
        self._rate_source = BinanceRateSource()

    async def grid(self):
        now = int(time.time())
        if now - self.last_run < self.interval_sec:
            return

        self.last_run = now
        
        try:
            bid_ask_prices = await self._rate_source.get_bid_ask_prices(quote_token="USDT")
            
            if not bid_ask_prices:
                self.logger().warning("No bid/ask prices received from Binance")
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
            self.logger().info(f"Processed {len(bid_ask_prices)} trading pairs")
            
        except Exception as e:
            self.logger().error(f"Error fetching bid/ask prices: {e}")
    
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
        safe_ensure_future(self.grid())
        
