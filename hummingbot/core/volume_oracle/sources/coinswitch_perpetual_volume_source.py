from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
        CoinswitchPerpetualDerivative,
    )


class CoinswitchPerpetualVolumeSource(VolumeSourceBase):
    """
    24h volume source for CoinSwitch PRO perpetual futures, from the all-pairs
    ticker: ``base_asset_volume_24h`` and ``quote_asset_volume_24h`` (USDT).
    """

    @property
    def name(self) -> str:
        return "coinswitch_perpetual"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_pairs_prices()

        wanted = set(trading_pairs) if trading_pairs else None
        results: Dict[str, Dict[str, Decimal]] = {}
        for ticker in tickers:
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=str(ticker.get("symbol", "")).upper())
            except KeyError:
                continue
            if wanted is not None and trading_pair not in wanted:
                continue
            try:
                base_volume = Decimal(str(ticker.get("base_asset_volume_24h") or "0"))
                quote_volume = Decimal(str(ticker.get("quote_asset_volume_24h") or "0"))
                last_price = Decimal(str(ticker.get("last_price") or "0"))
            except Exception:
                continue
            results[trading_pair] = {
                "exchange": self.name,
                "symbol": trading_pair,
                "base_volume": base_volume,
                "last_price": last_price,
                "quote_volume": quote_volume,
            }
        return results

    def _build_exchange(self) -> "CoinswitchPerpetualDerivative":
        from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
            CoinswitchPerpetualDerivative,
        )

        return CoinswitchPerpetualDerivative(
            coinswitch_perpetual_api_key="",
            coinswitch_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
