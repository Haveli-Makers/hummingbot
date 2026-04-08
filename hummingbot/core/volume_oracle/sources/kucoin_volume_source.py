from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange


class KucoinVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "kucoin"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}-{quote}"
        self._ensure_exchange()

        resp = await self._exchange.get_all_pairs_prices()

        tickers = resp.get("data", {}).get("ticker", [])
        ticker = None
        for item in tickers:
            if item.get("symbol", "").upper() == symbol.upper():
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("symbol", symbol),
            "base_volume": Decimal(str(ticker["vol"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("volValue"):
            result["quote_volume"] = Decimal(str(ticker["volValue"]))
        return result

    def _build_exchange(self) -> "KucoinExchange":
        from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange

        return KucoinExchange(
            kucoin_api_key="",
            kucoin_passphrase="",
            kucoin_secret_key="",
            trading_pairs=[],
            trading_required=False,
        )
