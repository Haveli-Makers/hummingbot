from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.cube.cube_exchange import CubeExchange


class CubeVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "cube"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_24h_volume_tickers(trading_pairs)
        tickers = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        result: Dict[str, Dict[str, Decimal]] = {}
        for item in tickers:
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("ticker_id", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except (KeyError, ValueError, InvalidOperation):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        last_price = ticker.get("last_price")
        if last_price is None:
            raise ValueError(f"Missing last_price for {hb_symbol}")
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "last_price": Decimal(str(last_price)),
            "base_volume": Decimal(str(ticker.get("base_volume") if ticker.get("base_volume") is not None else ticker.get("volume", 0))),
        }
        if ticker.get("quote_volume") is not None:
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
