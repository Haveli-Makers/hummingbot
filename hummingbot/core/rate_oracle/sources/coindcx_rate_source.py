import time
from decimal import Decimal
from typing import Dict, Optional

import aiohttp

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase


class CoindcxRateSource(RateSourceBase):
    """
    Rate source for CoinDCX exchange.
    Fetches ticker data directly from CoinDCX public API.
    """

    TICKER_URL = "https://api.coindcx.com/exchange/ticker"
    MARKETS_DETAILS_URL = "https://api.coindcx.com/exchange/v1/markets_details"
    MARKETS_CACHE_TTL = 300

    # Common quote currencies to parse symbols
    QUOTE_CURRENCIES = ["USDT", "USDC", "INR", "BTC", "ETH", "DAI", "BUSD", "TRX"]

    def __init__(self):
        super().__init__()
        self._markets_cache: Optional[Dict[str, Dict]] = None
        self._markets_cache_time: float = 0

    @property
    def name(self) -> str:
        return "coindcx"

    def _parse_trading_pair(self, symbol: str) -> Optional[Dict]:
        """
        Parse a CoinDCX symbol into base and quote currencies.
        CoinDCX symbols are like "BTCUSDT", "BTCINR", etc.
        """
        symbol_upper = symbol.upper()
        for quote in self.QUOTE_CURRENCIES:
            if symbol_upper.endswith(quote):
                base = symbol_upper[:-len(quote)]
                if base:
                    return {
                        "trading_pair": f"{base}-{quote}",
                        "base": base,
                        "quote": quote
                    }
        return None

    async def _fetch_markets(self) -> Dict[str, Dict]:
        """Fetch market details and cache symbol to trading pair mapping."""
        current_time = time.time()
        if self._markets_cache is not None and (current_time - self._markets_cache_time) < self.MARKETS_CACHE_TTL:
            return self._markets_cache

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.MARKETS_DETAILS_URL) as response:
                    if response.status == 200:
                        markets = await response.json()
                        self._markets_cache = {}
                        for market in markets:
                            # CoinDCX markets_details has reversed naming:
                            # - base_currency_short_name is actually the quote (e.g., USDT for BTCUSDT)
                            # - target_currency_short_name is actually the base (e.g., BTC for BTCUSDT)
                            symbol = market.get("coindcx_name", "")
                            quote = market.get("base_currency_short_name", "")  # This is the quote
                            base = market.get("target_currency_short_name", "")  # This is the base
                            if symbol and base and quote:
                                # Hummingbot format: BASE-QUOTE (e.g., BTC-USDT)
                                trading_pair = f"{base}-{quote}"
                                self._markets_cache[symbol] = {
                                    "trading_pair": trading_pair,
                                    "base": base,
                                    "quote": quote
                                }
                        self._markets_cache_time = current_time
                        return self._markets_cache
        except Exception as e:
            self.logger().warning(f"Error fetching markets details: {e}")

        return {}

    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Fetches mid prices for all trading pairs.

        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to mid prices
        """
        results = {}
        try:
            markets = await self._fetch_markets()

            async with aiohttp.ClientSession() as session:
                async with session.get(self.TICKER_URL) as response:
                    if response.status == 200:
                        data = await response.json()

                        tickers = data if isinstance(data, list) else data.get("data", data)
                        if isinstance(tickers, dict):
                            tickers = list(tickers.values()) if not isinstance(list(tickers.values())[0], str) else []

                        for ticker in tickers:
                            if isinstance(ticker, str):
                                continue

                            symbol = ticker.get("market", "")

                            market_info = markets.get(symbol)
                            if not market_info:
                                market_info = self._parse_trading_pair(symbol)

                            if not market_info:
                                continue

                            trading_pair = market_info["trading_pair"]

                            if quote_token and market_info["quote"] != quote_token:
                                continue

                            bid = ticker.get("bid")
                            ask = ticker.get("ask")

                            if bid is not None and ask is not None:
                                try:
                                    bid_dec = Decimal(str(bid))
                                    ask_dec = Decimal(str(ask))
                                    if bid_dec > 0 and ask_dec > 0:
                                        results[trading_pair] = (bid_dec + ask_dec) / Decimal("2")
                                except Exception:
                                    continue
        except Exception as e:
            self.logger().error(f"Error fetching CoinDCX prices: {e}")

        return results

    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetches best bid and ask prices for all trading pairs.

        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to {"bid": Decimal, "ask": Decimal, "mid": Decimal, "spread": Decimal}
        """
        results = {}
        try:
            markets = await self._fetch_markets()

            async with aiohttp.ClientSession() as session:
                async with session.get(self.TICKER_URL) as response:
                    if response.status == 200:
                        data = await response.json()

                        tickers = data if isinstance(data, list) else data.get("data", data)
                        if isinstance(tickers, dict):
                            tickers = list(tickers.values()) if not isinstance(list(tickers.values())[0], str) else []

                        for ticker in tickers:
                            if isinstance(ticker, str):
                                continue

                            symbol = ticker.get("market", "")

                            market_info = markets.get(symbol)
                            if not market_info:
                                market_info = self._parse_trading_pair(symbol)

                            if not market_info:
                                continue

                            trading_pair = market_info["trading_pair"]

                            if quote_token and market_info["quote"] != quote_token:
                                continue

                            bid = ticker.get("bid")
                            ask = ticker.get("ask")

                            if bid is not None and ask is not None:
                                try:
                                    bid_dec = Decimal(str(bid))
                                    ask_dec = Decimal(str(ask))
                                    if bid_dec > 0 and ask_dec > 0 and bid_dec <= ask_dec:
                                        mid = (bid_dec + ask_dec) / Decimal("2")
                                        spread = ask_dec - bid_dec
                                        spread_pct = (spread / mid) * Decimal("100") if mid > 0 else Decimal("0")
                                        results[trading_pair] = {
                                            "bid": bid_dec,
                                            "ask": ask_dec,
                                            "mid": mid,
                                            "spread": spread,
                                            "spread_pct": spread_pct
                                        }
                                except Exception:
                                    continue
        except Exception as e:
            self.logger().error(f"Error fetching CoinDCX bid/ask prices: {e}")

        return results
