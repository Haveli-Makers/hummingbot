from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.derive.derive_exchange import DeriveExchange


class DeriveVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "derive"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            ticker = item.get("result")
            if not isinstance(ticker, dict):
                continue

            raw_symbol = str(ticker.get("instrument_name", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            result[hb_symbol] = self._normalize_ticker(ticker, hb_symbol)

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "last_price": Decimal(str(ticker.get("mark_price", ticker.get("best_bid_price", "0")))),
            "base_volume": Decimal(str(ticker.get("amount_24h", ticker.get("volume_24h", "0")))),
        }
        if ticker.get("quote_volume_24h") is not None:
            result["quote_volume"] = Decimal(str(ticker["quote_volume_24h"]))
        return result

    def _build_exchange(self) -> "DeriveExchange":
        from hummingbot.connector.exchange.derive.derive_exchange import DeriveExchange

        return DeriveExchange(
            derive_api_secret="",
            trading_pairs=[],
            sub_id="",
            derive_api_key="",
            trading_required=False,
        )
