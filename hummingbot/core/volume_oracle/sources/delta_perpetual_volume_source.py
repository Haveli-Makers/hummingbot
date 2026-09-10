from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative


class DeltaPerpetualVolumeSource(VolumeSourceBase):
    """
    24h volume source for Delta Exchange (India) perpetuals.

    Reads Delta's public ``/v2/tickers`` (no credentials). For each perpetual:
      - ``volume``       -> base volume (already in base units, e.g. BTC)
      - ``turnover_usd`` -> quote volume (USD)
      - ``close``        -> last traded price
    """

    @property
    def name(self) -> str:
        return "delta_perpetual"

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
            # Only perpetuals are in the symbol map; everything else raises and is skipped.
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
            "last_price": Decimal(str(ticker.get("close") or ticker.get("mark_price") or "0")),
        }
        # Delta reports settled (USD) turnover; expose it as quote volume when present.
        turnover_usd = ticker.get("turnover_usd")
        if turnover_usd is not None:
            normalized["quote_volume"] = Decimal(str(turnover_usd))
        return normalized

    def _build_exchange(self) -> "DeltaPerpetualDerivative":
        from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative

        return DeltaPerpetualDerivative(
            delta_perpetual_api_key="",
            delta_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
