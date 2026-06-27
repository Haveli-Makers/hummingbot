from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange


class ZebpayVolumeSource(VolumeSourceBase):
    """Volume oracle source for Zebpay (spot)."""

    @property
    def name(self) -> str:
        return "zebpay"

    async def get_all_24h_volumes(
        self, trading_pairs: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in tickers:
            if not isinstance(item, dict):
                continue
            hb_symbol = await self._resolve_trading_pair(item)
            if not hb_symbol or "-" not in hb_symbol:
                continue
            try:
                result[hb_symbol] = self._normalize_ticker(item, hb_symbol)
            except (KeyError, ValueError):
                continue
        return result

    async def _resolve_trading_pair(self, ticker: Dict[str, Any]) -> Optional[str]:
        """
        Translate a ticker's exchange symbol to an HB trading pair via the connector
        symbol map so the source keeps working if Zebpay ever returns a non-dashed
        symbol. Falls back to baseAsset/quoteAsset, then a raw dashed symbol.
        """
        symbol = str(ticker.get("symbol", ""))
        if symbol:
            try:
                return await self._exchange.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except KeyError:
                pass
        base = str(ticker.get("baseAsset", "")).upper()
        quote = str(ticker.get("quoteAsset", "")).upper()
        if base and quote:
            return f"{base}-{quote}"
        if "-" in symbol:
            return symbol.upper()
        return None

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Any]:
        last = ticker.get("last") or ticker.get("lastPrice") or "0"
        base_vol = ticker.get("baseVolume") or ticker.get("volume") or "0"
        quote_vol = ticker.get("quoteVolume")

        entry: Dict[str, Any] = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": Decimal(str(base_vol)),
            "last_price": Decimal(str(last)),
        }
        if quote_vol is not None:
            entry["quote_volume"] = Decimal(str(quote_vol))
        return entry

    def _build_exchange(self) -> "ZebpayExchange":
        from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange

        return ZebpayExchange(
            zebpay_api_key="",
            zebpay_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
