from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.cube.cube_exchange import CubeExchange


class CubeVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "cube"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_pairs_prices()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in tickers:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("ticker_id", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        symbol = str(ticker.get("ticker_id", "")).upper()
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "last_price": Decimal(str(ticker["last_price"])),
            "base_volume": Decimal(str(ticker.get("base_volume", ticker.get("volume", "0")))),
        }
        if ticker.get("quote_volume") is not None:
            result["quote_volume"] = Decimal(str(ticker["quote_volume"]))
        return result

    def _build_exchange(self) -> "CubeExchange":
        from hummingbot.connector.exchange.cube.cube_exchange import CubeExchange

        return CubeExchange(
            cube_api_key="",
            cube_api_secret="",
            cube_subaccount_id="1",
            trading_pairs=[],
            trading_required=False,
            domain="live",
        )
