from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrVolumeSource(VolumeSourceBase):
    """
    24h volume source for VALR spot.

    Reads the public ``/v1/public/marketsummary`` (no credentials). Per pair:
      - ``baseVolume``      -> base volume
      - ``quoteVolume``     -> quote volume
      - ``lastTradedPrice`` -> last traded price
    """

    @property
    def name(self) -> str:
        return "valr"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            raw_symbol = str(item.get("currencyPair", ""))
            if not raw_symbol:
                continue
            try:
                hb_symbol = await self._exchange.trading_pair_associated_to_exchange_symbol(symbol=raw_symbol)
            except Exception:
                continue
            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except (KeyError, ValueError, TypeError):
                continue
        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        normalized: Dict[str, Decimal] = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(ticker["baseVolume"])),
            "last_price": Decimal(str(ticker.get("lastTradedPrice") or "0")),
        }
        quote_volume = ticker.get("quoteVolume")
        if quote_volume is not None:
            normalized["quote_volume"] = Decimal(str(quote_volume))
        return normalized

    def _build_exchange(self) -> "ValrExchange":
        from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange

        return ValrExchange(
            valr_api_key="",
            valr_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
