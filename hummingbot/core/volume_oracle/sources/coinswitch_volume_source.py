from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange


class CoinswitchVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coinswitch"

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

            hb_symbol = raw_symbol.replace("/", "-").upper()
            if not hb_symbol or "-" not in hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Any]:
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(ticker["baseVolume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("quoteVolume") is not None:
            result["quote_volume"] = Decimal(str(ticker["quoteVolume"]))
        return result

    def _build_exchange(self) -> "CoinswitchExchange":
        from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

        return CoinswitchExchange(
            coinswitch_api_key="",
            coinswitch_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )

