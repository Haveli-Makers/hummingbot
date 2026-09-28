from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


class CoinDCXPerpetualVolumeSource(VolumeSourceBase):
    """
    24h volume source for CoinDCX perpetual futures.

    Reads the public ``current_prices/futures/rt`` feed (no credentials), where
    ``ls`` is the last traded price and ``v`` is the 24h **quote** (USDT) volume,
    so the base volume is derived as ``v / last_price``.
    """

    @property
    def name(self) -> str:
        return "coindcx_perpetual"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_utils as utils

        self._ensure_exchange()
        tickers = await self._exchange.get_all_pairs_prices()

        wanted = set(trading_pairs) if trading_pairs else None
        results: Dict[str, Dict[str, Decimal]] = {}

        for ticker in tickers:
            trading_pair = utils.coindcx_pair_to_hb_pair(ticker.get("symbol", ""))
            if "-" not in trading_pair:
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

    def _build_exchange(self) -> "CoindcxPerpetualDerivative":
        from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
            CoindcxPerpetualDerivative,
        )

        return CoindcxPerpetualDerivative(
            coindcx_perpetual_api_key="",
            coindcx_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
