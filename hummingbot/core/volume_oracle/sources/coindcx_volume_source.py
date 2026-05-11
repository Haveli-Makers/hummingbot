from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coindcx import coindcx_utils
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class CoindcxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coindcx"

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
            hb_symbol = coindcx_utils.coindcx_pair_to_hb_pair(raw_symbol)
            if not hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        return {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["last_price"])),
        }

    def _build_exchange(self) -> "CoindcxExchange":
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange

        return CoindcxExchange(
            coindcx_api_key="",
            coindcx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
