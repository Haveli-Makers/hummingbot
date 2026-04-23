from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class OkxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "okx"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("data", []):
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("instId", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        symbol = str(ticker.get("instId", "")).upper()
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "base_volume": Decimal(str(ticker["vol24h"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("volCcy24h") is not None:
            result["quote_volume"] = Decimal(str(ticker["volCcy24h"]))
        return result

    def _build_exchange(self) -> "OkxExchange":
        from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange

        return OkxExchange(
            okx_api_key="",
            okx_secret_key="",
            okx_passphrase="",
            trading_pairs=[],
            trading_required=False,
        )
