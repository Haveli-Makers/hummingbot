from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class OkxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "okx"

    def _safe_decimal(self, value) -> Decimal:
        try:
            if value in (None, "", "NaN", "N/A", "--"):
                return Decimal("0")
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        resp = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in resp.get("data", []):
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("instId", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            try:
                result[hb_symbol] = self._normalize_ticker(ticker=item, hb_symbol=hb_symbol)
            except Exception as e:
                self.logger().warning(f"Bad ticker data for {raw_symbol}: {item} | Error: {e}")
                continue

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        vol24h = ticker.get("vol24h")
        last = ticker.get("last")
        if vol24h is None or last is None:
            raise ValueError(f"Missing vol24h or last for {hb_symbol}")
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
            "base_volume": self._safe_decimal(vol24h),
            "last_price": self._safe_decimal(last),
        }
        if ticker.get("volCcy24h") is not None:
            result["quote_volume"] = self._safe_decimal(ticker["volCcy24h"])
        return result

    def _build_exchange(self) -> "OkxExchange":
        from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange

        return OkxExchange(
            okx_api_key="",
            okx_secret_key="",
            okx_passphrase="",
            trading_pairs=[],
            trading_required=False,
        )
