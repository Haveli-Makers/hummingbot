import logging
from decimal import Decimal
from typing import Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

EXCHANGE_CONFIGS: Dict[str, Dict] = {
    "binance": {
        "url": "https://api.binance.com/api/v3/ticker/24hr",
        "pair_format": lambda base, quote: f"{base}{quote}",
        "param_key": "symbol",
        "volume_field": "volume",
        "quote_volume_field": "quoteVolume",
        "symbol_field": "symbol",
        "last_price_field": "lastPrice",
    },
    "coindcx": {
        "url": "https://api.coindcx.com/exchange/ticker",
        "pair_format": lambda base, quote: f"{base}{quote}",
        "param_key": None,
        "volume_field": "volume",
        "quote_volume_field": None,
        "symbol_field": "market",
        "last_price_field": "last_price",
    },
    "okx": {
        "url": "https://www.okx.com/api/v5/market/ticker",
        "pair_format": lambda base, quote: f"{base}-{quote}",
        "param_key": "instId",
        "volume_field": "vol24h",
        "quote_volume_field": "volCcy24h",
        "symbol_field": "instId",
        "last_price_field": "last",
        "response_wrapper": "data",
    },
    "wazirx": {
        "url": "https://api.wazirx.com/sapi/v1/ticker/24hr",
        "pair_format": lambda base, quote: f"{base}{quote}".lower(),
        "param_key": "symbol",
        "volume_field": "volume",
        "quote_volume_field": "quoteVolume",
        "symbol_field": "symbol",
        "last_price_field": "lastPrice",
    },
    "kucoin": {
        "url": "https://api.kucoin.com/api/v1/market/stats",
        "pair_format": lambda base, quote: f"{base}-{quote}",
        "param_key": "symbol",
        "volume_field": "vol",
        "quote_volume_field": "volValue",
        "symbol_field": "symbol",
        "last_price_field": "last",
        "response_wrapper": "data",
    },
    "bybit": {
        "url": "https://api.bybit.com/v5/market/tickers",
        "pair_format": lambda base, quote: f"{base}{quote}",
        "param_key": "symbol",
        "volume_field": "volume24h",
        "quote_volume_field": "turnover24h",
        "symbol_field": "symbol",
        "last_price_field": "lastPrice",
        "response_wrapper": "result",
        "list_field": "list",
        "extra_params": {"category": "spot"},
    },
}


class VolumeFetcher:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session
        self._owns_session = session is None

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owns_session = True

    async def stop(self):
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def get_24h_volume(
        self,
        exchange: str,
        trading_pair: str,
    ) -> Dict[str, Decimal]:
        """
        Fetch 24h volume for a trading pair from a given exchange.

        :param exchange: Exchange name (binance, coindcx, okx, wazirx, kucoin, bybit)
        :param trading_pair: Trading pair in HB format e.g. "BTC-USDT"
        """
        exchange = exchange.lower().strip()
        if exchange not in EXCHANGE_CONFIGS:
            raise ValueError(f"Unsupported exchange: {exchange}. Supported: {list(EXCHANGE_CONFIGS.keys())}")

        if "-" not in trading_pair:
            raise ValueError(f"Trading pair must be in BASE-QUOTE format (e.g. BTC-USDT), got: {trading_pair}")

        if self._session is None:
            await self.start()

        base, quote = trading_pair.upper().split("-", 1)
        config = EXCHANGE_CONFIGS[exchange]
        exchange_symbol = config["pair_format"](base, quote)

        params = {}
        if config.get("extra_params"):
            params.update(config["extra_params"])

        if config["param_key"] is not None:
            params[config["param_key"]] = exchange_symbol

        async with self._session.get(config["url"], params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if "response_wrapper" in config:
            data = data[config["response_wrapper"]]

        if "list_field" in config:
            data = data[config["list_field"]]

        ticker = self._find_ticker(data, config, exchange_symbol)
        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({exchange_symbol}) not found on {exchange}")

        result = {
            "exchange": exchange,
            "trading_pair": trading_pair,
            "symbol": ticker.get(config["symbol_field"], exchange_symbol),
            "base_volume": Decimal(str(ticker[config["volume_field"]])),
            "last_price": Decimal(str(ticker[config["last_price_field"]])),
        }

        if config.get("quote_volume_field") and ticker.get(config["quote_volume_field"]):
            result["quote_volume"] = Decimal(str(ticker[config["quote_volume_field"]]))

        return result

    @staticmethod
    def _find_ticker(data, config: dict, exchange_symbol: str) -> Optional[dict]:
        """Find the matching ticker from exchange response."""
        if isinstance(data, dict):
            symbol_field = config["symbol_field"]
            if symbol_field in data and data[symbol_field].upper() == exchange_symbol.upper():
                return data
            return data if config["param_key"] is not None else None

        if isinstance(data, list):
            symbol_field = config["symbol_field"]
            for item in data:
                if isinstance(item, dict) and item.get(symbol_field, "").upper() == exchange_symbol.upper():
                    return item

        return None


async def get_24h_volume(
    exchange: str,
    trading_pair: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> Dict[str, Decimal]:
    async with VolumeFetcher(session=session) as fetcher:
        return await fetcher.get_24h_volume(exchange, trading_pair)
