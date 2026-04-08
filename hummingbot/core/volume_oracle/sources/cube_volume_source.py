from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.cube.cube_exchange import CubeExchange


class CubeVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "cube"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        ticker_id = f"{base}{quote}".upper()
        self._ensure_exchange()

        tickers = await self._exchange.get_all_pairs_prices()

        ticker = None
        for item in tickers:
            if isinstance(item, dict) and item.get("ticker_id", "").upper() == ticker_id:
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({ticker_id}) not found on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("ticker_id", ticker_id),
            "last_price": Decimal(str(ticker["last_price"])),
            "base_volume": Decimal(str(ticker.get("base_volume", ticker.get("volume", "0")))),
        }
        if ticker.get("quote_volume"):
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
