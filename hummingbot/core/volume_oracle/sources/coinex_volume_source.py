from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexVolumeSource(VolumeSourceBase):
    """
    24h volume source for CoinEx spot.

    Reads CoinEx's public ``/spot/ticker`` (no credentials). For each market:
      - ``volume`` -> base volume
      - ``value``  -> quote volume
      - ``last``   -> last traded price
    """

    @property
    def name(self) -> str:
        return "coinex"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("market", ""))
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
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker.get("last") or "0")),
        }
        quote_volume = ticker.get("value")
        if quote_volume is not None:
            normalized["quote_volume"] = Decimal(str(quote_volume))
        return normalized

    def _build_exchange(self) -> "CoinexExchange":
        from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange

        return CoinexExchange(
            coinex_api_key="",
            coinex_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
