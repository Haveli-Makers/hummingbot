from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange


class HyperliquidVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "hyperliquid"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        response = await self._exchange.get_all_24h_volume_tickers()

        contexts = response[1]
        result: Dict[str, Dict[str, Decimal]] = {}
        for ctx in contexts:
            if not isinstance(ctx, dict):
                continue

            symbol = str(ctx.get("coin", "")).upper()
            if not symbol:
                continue

            try:
                result[symbol] = self._normalize_ticker(symbol=symbol, context=ctx)
            except (KeyError, ValueError):
                continue

        return result

    def _normalize_ticker(self, symbol: str, context: Dict[str, Any]) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "last_price": Decimal(str(context["midPx"])),
            "base_volume": Decimal(str(context.get("dayNtlVlm", "0"))),
        }
        return result

    def _build_exchange(self) -> "HyperliquidExchange":
        from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange

        return HyperliquidExchange(
            hyperliquid_api_secret="",
            trading_pairs=[],
            use_vault=False,
            hyperliquid_api_key="",
            trading_required=False,
        )
