from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kucoin.kucoin_exchange import KucoinExchange


class KucoinVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "kucoin"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_pairs_prices()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("data", {}).get("ticker", []):
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
            "base_volume": Decimal(str(ticker["vol"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("volValue") is not None:
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
