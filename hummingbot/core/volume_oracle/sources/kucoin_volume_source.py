from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange


class KucoinVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "kucoin"

    def _safe_decimal(self, value):
        try:
            if value in (None, "", "NaN", "N/A", "--"):
                return Decimal("0")
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("data", {}).get("ticker", []):
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("symbol", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        vol = ticker.get("vol")
        last = ticker.get("last")
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": self._safe_decimal(vol),
            "last_price": self._safe_decimal(last),
        }
        vol_value = ticker.get("volValue")
        if vol_value is not None:
            result["quote_volume"] = self._safe_decimal(vol_value)
        return result

    def _build_exchange(self) -> "KucoinExchange":
        from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange

        return KucoinExchange(
            kucoin_api_key="",
            kucoin_passphrase="",
            kucoin_secret_key="",
            trading_pairs=[],
            trading_required=False,
        )
