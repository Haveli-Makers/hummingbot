from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange


class CsxVolumeSource(VolumeSourceBase):
    """Volume oracle source for CoinSwitch Kuber (CSX)."""

    @property
    def name(self) -> str:
        return "csx"

    async def get_all_24h_volumes(
        self, trading_pairs: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in tickers:
            if not isinstance(item, dict):
                continue
            instrument = (item.get("Instrument") or item.get("instrument")
                          or item.get("symbol") or "")
            hb_symbol = instrument.replace("/", "-").upper()
            if not hb_symbol or "-" not in hb_symbol:
                continue
            try:
                result[hb_symbol] = self._normalize_ticker(item, hb_symbol)
            except (KeyError, ValueError):
                continue
        return result

    def _normalize_ticker(
        self, ticker: Dict[str, Any], hb_symbol: str
    ) -> Dict[str, Any]:
        last = (ticker.get("LastTradedPrice") or ticker.get("lastTradedPrice")
                or ticker.get("last") or "0")
        base_vol = (ticker.get("Volume24HBase") or ticker.get("volume24HBase")
                    or ticker.get("baseVolume") or ticker.get("volume") or "0")
        quote_vol = ticker.get("Volume24HQuote") or ticker.get("volume24HQuote") or ticker.get("quoteVolume")

        entry: Dict[str, Any] = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(base_vol)),
            "last_price": Decimal(str(last)),
        }
        if quote_vol is not None:
            entry["quote_volume"] = Decimal(str(quote_vol))
        return entry

    def _build_exchange(self) -> "CsxExchange":
        from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange

        return CsxExchange(
            csx_api_key="",
            csx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
