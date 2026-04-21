from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange


class WazirxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "wazirx"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}{quote}".lower()
        self._ensure_exchange()

        data = await self._exchange.get_all_pairs_prices()

        ticker = None
        for item in data:
            if isinstance(item, dict) and item.get("symbol", "").lower() == symbol:
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")

        return self._normalize_ticker(ticker=ticker, trading_pair=trading_pair)

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch 24h volume for all trading pairs in a single request.

        :return: A symbol-keyed mapping with exchange, symbol, base_volume, last_price and optional quote_volume.
        """
        self._ensure_exchange()
        data = await self._exchange.get_all_pairs_prices()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "")).lower()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(ticker=item)
            except (KeyError, ValueError):
                # Skip malformed ticker entries but keep the bulk response available.
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], trading_pair: str = "") -> Dict[str, Decimal]:
        symbol = str(ticker.get("symbol", "")).lower()
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["lastPrice"])),
        }
        if trading_pair:
            result["trading_pair"] = trading_pair

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
