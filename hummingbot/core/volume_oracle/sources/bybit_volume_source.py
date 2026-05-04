from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bybit.bybit_exchange import BybitExchange


class BybitVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "bybit"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_24h_volume_tickers(trading_pairs=trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("result", {}).get("list", []):
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("symbol", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(item, hb_symbol)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(ticker["volume24h"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("turnover24h") is not None:
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
