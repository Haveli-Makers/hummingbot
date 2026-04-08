from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.coinbase_advanced_trade import coinbase_advanced_trade_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinbase_advanced_trade.coinbase_advanced_trade_exchange import (
        CoinbaseAdvancedTradeExchange,
    )


class CoinbaseAdvancedTradeVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "coinbase_advanced_trade"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        product_id = f"{base}-{quote}"
        self._ensure_exchange()

        path_url, limit_id = CONSTANTS.get_ticker_endpoint(use_auth_for_public_endpoints=False)
        resp = await self._exchange._api_get(
            path_url=path_url.format(product_id=product_id),
            params={"limit": 1},
            limit_id=limit_id,
            is_auth_required=False,
        )

        trades = resp.get("trades", [])
        if not trades:
            raise ValueError(f"Trading pair {trading_pair} ({product_id}) not found on {self.name}")

        last_price = Decimal(str(trades[0]["price"]))
        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": product_id,
            "last_price": last_price,
            "base_volume": Decimal(str(resp.get("volume", "0"))),
        }
        if resp.get("quote_volume"):
            result["quote_volume"] = Decimal(str(resp["quote_volume"]))
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
