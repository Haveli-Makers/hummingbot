from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibVolumeSource(VolumeSourceBase):
    """
    24h volume source for Ajaib spot.

    The Ajaib Open API has no 24h ticker endpoint, so the exchange sums the
    figures from ``/v1/klines`` (see ``AjaibExchange.get_all_24h_volume_tickers``).
    Every Ajaib endpoint is signed and IP-allowlisted, so unlike the other
    sources this one needs real credentials; they are read the same way as the
    Ajaib rate source.
    """

    @property
    def name(self) -> str:
        return "ajaib"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("symbol", ""))
            if not raw_symbol:
                continue
            try:
                hb_symbol = await self._exchange.trading_pair_associated_to_exchange_symbol(symbol=raw_symbol)
            except KeyError:
                continue

            result[hb_symbol] = self._normalize_ticker(item, hb_symbol)

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("quoteVolume") is not None:
            result["quote_volume"] = Decimal(str(ticker["quoteVolume"]))
        return result

    def _build_exchange(self) -> "AjaibExchange":
        from hummingbot.core.rate_oracle.sources.ajaib_rate_source import AjaibRateSource

        return AjaibRateSource._build_ajaib_connector()
