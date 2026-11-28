from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative


class WazirxPerpetualVolumeSource(VolumeSourceBase):
    """
    24h volume source for WazirX perpetual futures.

    Reads the public all-symbols 24h ticker (no credentials). Its ``volume`` is
    the 24h **quote** volume (INR or USDT) — live BTCINR reports ~1.2e12, which
    is only plausible in rupees and matches the stream's quote-volume field —
    so the base volume is derived as ``volume / lastPrice``.
    """

    @property
    def name(self) -> str:
        return "wazirx_perpetual"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_pairs_prices()

        wanted = set(trading_pairs) if trading_pairs else None
        results: Dict[str, Dict[str, Decimal]] = {}

        for ticker in tickers:
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=str(ticker.get("symbol", "")))
            except KeyError:
                continue
            if wanted is not None and trading_pair not in wanted:
                continue
            try:
                quote_volume = Decimal(str(ticker.get("volume", "0")))
                last_price = Decimal(str(ticker.get("lastPrice", "0")))
            except Exception:
                continue

            results[trading_pair] = {
                "exchange": self.name,
                "symbol": trading_pair,
                "base_volume": (quote_volume / last_price) if last_price > 0 else Decimal("0"),
                "last_price": last_price,
                "quote_volume": quote_volume,
            }
        return results

    def _build_exchange(self) -> "WazirxPerpetualDerivative":
        from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import (
            WazirxPerpetualDerivative,
        )

        return WazirxPerpetualDerivative(
            wazirx_perpetual_api_key="",
            wazirx_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
