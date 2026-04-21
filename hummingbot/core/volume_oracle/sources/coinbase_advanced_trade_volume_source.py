from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinbase_advanced_trade.coinbase_advanced_trade_exchange import (
        CoinbaseAdvancedTradeExchange,
    )


class CoinbaseAdvancedTradeVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coinbase_advanced_trade"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("product_id", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(item)
            except (KeyError, ValueError, InvalidOperation):
                continue

        return result

    @staticmethod
    def _safe_decimal(value, default: str = "0") -> Decimal:
        v = str(value).strip() if value is not None else ""
        return Decimal(v) if v else Decimal(default)

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": str(ticker["product_id"]).upper(),
            "last_price": self._safe_decimal(ticker.get("price")),
            "base_volume": self._safe_decimal(ticker.get("volume_24h")),
        }
        if ticker.get("quote_volume_24h") is not None:
            result["quote_volume"] = self._safe_decimal(ticker["quote_volume_24h"])
        return result

    def _build_exchange(self) -> "CoinbaseAdvancedTradeExchange":
        from hummingbot.connector.exchange.coinbase_advanced_trade.coinbase_advanced_trade_exchange import (
            CoinbaseAdvancedTradeExchange,
        )

        return CoinbaseAdvancedTradeExchange(
            coinbase_advanced_trade_api_key="",
            coinbase_advanced_trade_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
