
from decimal import Decimal
from typing import Dict, Optional

import aiohttp

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache


class WazirxRateSource(RateSourceBase):
    """
    Rate source for WazirX exchange.
    Fetches ticker data from WazirX public API: https://api.wazirx.com/api/v2/tickers
    """

    TICKER_URL = "https://api.wazirx.com/api/v2/tickers"

    # Common quote currencies to parse symbols
    QUOTE_CURRENCIES = ["USDT", "USDC", "INR", "BTC", "ETH", "BUSD"]

    def __init__(self):
        super().__init__()

    @property
    def name(self) -> str:
        return "wazirx"

    def _parse_trading_pair(self, ticker_key: str, ticker_obj: Dict) -> Optional[Dict]:
        """
        Parse a WazirX ticker into base/quote. WazirX returns either keys like 'btcinr'
        and also provides `base_unit` and `quote_unit` inside the ticker object.
        """
        try:
            base = ticker_obj.get("base_unit") or ticker_obj.get("base")
            quote = ticker_obj.get("quote_unit") or ticker_obj.get("quote")

            if base and quote:
                base_u = base.upper()
                quote_u = quote.upper()
                return {"trading_pair": f"{base_u}-{quote_u}", "base": base_u, "quote": quote_u}

            # Fallback: try splitting the key by known quotes
            key_upper = ticker_key.upper()
            for q in self.QUOTE_CURRENCIES:
                if key_upper.endswith(q):
                    base_u = key_upper[:-len(q)]
                    if base_u:
                        return {"trading_pair": f"{base_u}-{q}", "base": base_u, "quote": q}
        except Exception:
            return None

        return None

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.TICKER_URL) as resp:
                    if resp.status != 200:
                        return results
                    data = await resp.json()

                    # data is a mapping of symbol -> ticker info
                    for key, ticker in data.items():
                        market_info = self._parse_trading_pair(key, ticker or {})
                        if not market_info:
                            continue

                        if quote_token and market_info["quote"] != quote_token:
                            continue

                        buy = ticker.get("buy") or ticker.get("bid")
                        sell = ticker.get("sell") or ticker.get("ask")
                        try:
                            if buy is None or sell is None:
                                continue
                            buy_dec = Decimal(str(buy))
                            sell_dec = Decimal(str(sell))
                            if buy_dec > 0 and sell_dec > 0:
                                mid = (buy_dec + sell_dec) / Decimal("2")
                                results[market_info["trading_pair"]] = mid
                        except Exception:
                            continue
        except Exception as e:
            self.logger().error(f"Error fetching WazirX prices: {e}")

        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.TICKER_URL) as resp:
                    if resp.status != 200:
                        return results
                    data = await resp.json()

                    for key, ticker in data.items():
                        market_info = self._parse_trading_pair(key, ticker or {})
                        if not market_info:
                            continue

                        if quote_token and market_info["quote"] != quote_token:
                            continue

                        buy = ticker.get("buy") or ticker.get("bid")
                        sell = ticker.get("sell") or ticker.get("ask")
                        try:
                            if buy is None or sell is None:
                                continue
                            bid_dec = Decimal(str(buy))
                            ask_dec = Decimal(str(sell))
                            if bid_dec > 0 and ask_dec > 0 and bid_dec <= ask_dec:
                                mid = (bid_dec + ask_dec) / Decimal("2")
                                spread = ask_dec - bid_dec
                                spread_pct = (spread / mid) * Decimal("100") if mid > 0 else Decimal("0")
                                results[market_info["trading_pair"]] = {
                                    "bid": bid_dec,
                                    "ask": ask_dec,
                                    "mid": mid,
                                    "spread": spread,
                                    "spread_pct": spread_pct,
                                }
                        except Exception:
                            continue
        except Exception as e:
            self.logger().error(f"Error fetching WazirX bid/ask prices: {e}")

        return results