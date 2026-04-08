from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.coindcx import coindcx_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class CoindcxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coindcx"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        exchange_symbol = f"{base}{quote}"
        self._ensure_exchange()

        data = await self._exchange._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL,
        )

        ticker = None
        for item in data:
            if isinstance(item, dict) and item.get("market", "").upper() == exchange_symbol.upper():
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({exchange_symbol}) not found on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("market", exchange_symbol),
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
