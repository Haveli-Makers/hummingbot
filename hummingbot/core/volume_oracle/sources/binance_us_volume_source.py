from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.binance import binance_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange


class BinanceUSVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "binance_us"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}{quote}"
        self._ensure_exchange()

        ticker = await self._exchange._api_get(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params={"symbol": symbol},
        )

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker["symbol"],
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
            "quote_volume": Decimal(str(ticker["quoteVolume"])),
        }
        return result

    def _build_exchange(self) -> "BinanceExchange":
        from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange

        return BinanceExchange(
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain="us",
        )
