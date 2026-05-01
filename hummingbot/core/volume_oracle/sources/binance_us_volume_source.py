from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange


class BinanceUSVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "binance_us"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        return {
            "exchange": self.name,
            "symbol": str(ticker["symbol"]).upper(),
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
            "quote_volume": Decimal(str(ticker["quoteVolume"])),
        }

    def _build_exchange(self) -> "BinanceExchange":
        from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange

        return BinanceExchange(
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain="us",
        )
