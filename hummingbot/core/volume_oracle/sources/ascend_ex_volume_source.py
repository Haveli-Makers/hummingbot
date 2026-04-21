from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ascend_ex.ascend_ex_exchange import AscendExExchange


class AscendExVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "ascend_ex"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_pairs_prices()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("data", []):
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        symbol = str(ticker.get("symbol", "")).upper()
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["close"])),
        }
        return result

    def _build_exchange(self) -> "AscendExExchange":
        from hummingbot.connector.exchange.ascend_ex.ascend_ex_exchange import AscendExExchange

        return AscendExExchange(
            ascend_ex_api_key="",
            ascend_ex_secret_key="",
            ascend_ex_group_id="",
            trading_pairs=[],
            trading_required=False,
        )
