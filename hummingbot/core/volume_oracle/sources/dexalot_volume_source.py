from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.dexalot.dexalot_exchange import DexalotExchange


class DexalotVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "dexalot"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("pair", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError, InvalidOperation):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        symbol = str(ticker.get("pair", "")).upper()
        last_price_raw = ticker.get("close") or ticker.get("last") or ticker.get("high")
        if last_price_raw is None:
            raise ValueError(f"Missing last_price for {symbol}")
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "last_price": Decimal(str(last_price_raw)),
            "base_volume": Decimal(str(ticker.get("volume") or "0")),
        }
        if ticker.get("quote_volume") is not None:
            result["quote_volume"] = Decimal(str(ticker["quote_volume"]))
        return result

    def _build_exchange(self) -> "DexalotExchange":
        from hummingbot.connector.exchange.dexalot.dexalot_exchange import DexalotExchange

        return DexalotExchange(
            dexalot_api_key="",
            dexalot_api_secret="13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930",  # noqa: mock
            trading_pairs=[],
            trading_required=False,
        )
