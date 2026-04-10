from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bybit.bybit_exchange import BybitExchange


class BybitVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "bybit"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}{quote}"
        self._ensure_exchange()

        resp = await self._exchange.get_24h_volume_ticker(symbol)

        tickers = resp.get("result", {}).get("list", [])
        if not tickers:
            raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")

        ticker = tickers[0]
        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("symbol", symbol),
            "base_volume": Decimal(str(ticker["volume24h"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("turnover24h"):
            result["quote_volume"] = Decimal(str(ticker["turnover24h"]))
        return result

    def _build_exchange(self) -> "BybitExchange":
        from hummingbot.connector.exchange.bybit.bybit_exchange import BybitExchange

        return BybitExchange(
            bybit_api_key="",
            bybit_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
