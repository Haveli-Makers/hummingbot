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

            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": str(ticker["symbol"]).upper(),
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
