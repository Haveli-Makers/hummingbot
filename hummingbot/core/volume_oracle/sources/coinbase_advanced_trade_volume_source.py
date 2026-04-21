from decimal import Decimal
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

            result[symbol] = self._normalize_ticker(item)

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": str(ticker["product_id"]).upper(),
            "last_price": Decimal(str(ticker.get("price", "0"))),
            "base_volume": Decimal(str(ticker.get("volume_24h", "0"))),
        }
        if ticker.get("quote_volume_24h") is not None:
            result["quote_volume"] = Decimal(str(ticker["quote_volume_24h"]))
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
