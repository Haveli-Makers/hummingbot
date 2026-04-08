from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.mexc import mexc_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod

if TYPE_CHECKING:
    from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange


class MexcVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "mexc"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}{quote}"
        self._ensure_exchange()

        ticker = await self._exchange._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params={"symbol": symbol},
            headers={"Content-Type": "application/json"},
        )

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker["symbol"],
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("quoteVolume"):
            result["quote_volume"] = Decimal(str(ticker["quoteVolume"]))
        return result

    def _build_exchange(self) -> "MexcExchange":
        from hummingbot.connector.exchange.mexc.mexc_exchange import MexcExchange

        return MexcExchange(
            mexc_api_key="",
            mexc_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
