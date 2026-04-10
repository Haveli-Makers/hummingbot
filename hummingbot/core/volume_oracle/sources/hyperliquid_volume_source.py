from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange


class HyperliquidVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "hyperliquid"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        self._ensure_exchange()

        response = await self._exchange.get_24h_volume_ticker()

        meta = response[0]
        contexts = response[1]
        meta_tokens = meta.get("tokens", [])
        universe = meta.get("universe", [])

        coin_to_context = {}
        for ctx in contexts:
            coin_to_context[ctx.get("coin", "")] = ctx

        target_coin = None
        for entry in universe:
            token_indices = entry.get("tokens", [])
            if len(token_indices) >= 2:
                base_idx, quote_idx = token_indices[0], token_indices[1]
                if base_idx < len(meta_tokens) and quote_idx < len(meta_tokens):
                    base_name = meta_tokens[base_idx].get("name", "").upper()
                    quote_name = meta_tokens[quote_idx].get("name", "").upper()
                    if base_name == base.upper() and quote_name == quote.upper():
                        target_coin = entry.get("name")
                        break

        if target_coin is None:
            raise ValueError(f"Trading pair {trading_pair} not found on {self.name}")

        ctx = coin_to_context.get(target_coin)
        if ctx is None:
            raise ValueError(f"No market data for {trading_pair} ({target_coin}) on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": target_coin,
            "last_price": Decimal(str(ctx["midPx"])),
            "base_volume": Decimal(str(ctx.get("dayNtlVlm", "0"))),
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
