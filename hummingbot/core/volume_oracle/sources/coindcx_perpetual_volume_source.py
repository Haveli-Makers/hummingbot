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

    Reads the public FAPI ``ticker/24hr`` for every symbol in one call (no
    credentials): ``volume`` is the base volume and ``quoteVolume`` the quote
    volume, as on Binance.
    """

    @property
    def name(self) -> str:
        return "coindcx_perpetual"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        wanted = set(trading_pairs) if trading_pairs else None
        results: Dict[str, Dict[str, Decimal]] = {}
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=str(ticker.get("symbol", "")))
            except KeyError:
                continue  # not a tradable perpetual
            if wanted is not None and trading_pair not in wanted:
                continue
            try:
                results[trading_pair] = {
                    "exchange": self.name,
                    "symbol": trading_pair,
                    "base_volume": Decimal(str(ticker["volume"])),
                    "last_price": Decimal(str(ticker["lastPrice"])),
                    "quote_volume": Decimal(str(ticker["quoteVolume"])),
                }
            except (KeyError, ArithmeticError, ValueError):
                continue
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
