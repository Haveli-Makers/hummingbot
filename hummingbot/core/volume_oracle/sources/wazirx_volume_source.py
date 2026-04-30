from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange


class WazirxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "wazirx"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch 24h volume for all trading pairs in a single request.

        :return: A symbol-keyed mapping with exchange, symbol, base_volume, last_price and optional quote_volume.
        """
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol_raw = str(item.get("symbol", "")).upper()
            if not symbol_raw:
                continue

            symbol = self._exchange.normalize_trading_pair(symbol_raw)

            try:
                ticker_data = self._normalize_ticker(ticker=item, symbol=symbol)
                result[symbol] = ticker_data
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], symbol: str) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": symbol, 
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if ticker.get("quoteVolume") is not None:
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
