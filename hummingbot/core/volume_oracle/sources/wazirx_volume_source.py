from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange


class WazirxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "wazirx"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}{quote}".lower()
        self._ensure_exchange()

        data = await self._exchange._api_get(
            path_url=CONSTANTS.TICKERS_PATH_URL,
        )

        ticker = None
        for item in data:
            if isinstance(item, dict) and item.get("symbol", "").lower() == symbol:
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("symbol", symbol),
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("quoteVolume"):
            result["quote_volume"] = Decimal(str(ticker["quoteVolume"]))
        return result

    def _build_exchange(self) -> "WazirxExchange":
        from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange

        return WazirxExchange(
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
