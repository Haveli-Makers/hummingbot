from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange


class HyperliquidVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "hyperliquid"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        response = await self._exchange.get_all_24h_volume_tickers()

        result: Dict[str, Dict[str, Decimal]] = {}

        perps_data = response.get("perps", [])
        if len(perps_data) == 2:
            perps_universe = perps_data[0].get("universe", [])
            perps_contexts = perps_data[1]
            for i, ctx in enumerate(perps_contexts):
                if not isinstance(ctx, dict):
                    continue
                name = perps_universe[i].get("name", "") if i < len(perps_universe) else ""
                if not name:
                    continue
                symbol = name.upper() + "-USD"
                hb_symbol = await self.normalize_symbol(symbol)
                if not hb_symbol:
                    continue
                try:
                    result[hb_symbol] = self._normalize_ticker(symbol=hb_symbol, context=ctx)
                except (KeyError, ValueError, InvalidOperation):
                    continue

        spot_data = response.get("spot", [])
        if len(spot_data) == 2:
            spot_contexts = spot_data[1]
            for ctx in spot_contexts:
                if not isinstance(ctx, dict):
                    continue
                raw_symbol = str(ctx.get("coin", ""))
                if not raw_symbol or raw_symbol.startswith("@"):
                    continue
                hb_symbol = await self.normalize_symbol(raw_symbol)
                if not hb_symbol:
                    continue
                try:
                    result[hb_symbol] = self._normalize_ticker(symbol=hb_symbol, context=ctx)
                except (KeyError, ValueError, InvalidOperation):
                    continue

        if trading_pairs:
            filter_symbols = {tp.upper() for tp in trading_pairs}
            for tp in filter_symbols:
                if tp not in result:
                    self.logger().warning(f"Skipping {tp}: symbol not found on {self.name}")
            result = {k: v for k, v in result.items() if k in filter_symbols}

        return result

    def _normalize_ticker(self, symbol: str, context: Dict[str, Any]) -> Dict[str, Decimal]:
        mid_px = context.get("midPx")
        if mid_px is None:
            raise ValueError(f"Missing midPx for {symbol}")
        result = {
            "exchange": self.name,
            "symbol": symbol,
            "last_price": Decimal(str(mid_px)),
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
