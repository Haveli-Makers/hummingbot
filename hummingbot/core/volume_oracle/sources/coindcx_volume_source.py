from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class CoindcxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coindcx"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_pairs_prices()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("market", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        symbol = str(ticker.get("market", "")).upper()
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["last_price"])),
        }
        return result

    def _build_exchange(self) -> "CoindcxExchange":
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange

        return CoindcxExchange(
            coindcx_api_key="",
            coindcx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
