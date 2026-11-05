from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import WazirxPerpetualDerivative


class WazirxPerpetualRateSource(RateSourceBase):
    """
    Rate source for WazirX perpetual futures (INR- and USDT-quoted contracts).

    Backed by the public all-symbols 24h ticker (no credentials needed), which
    reports the last price for every contract in a single call.
    """

    def __init__(self):
        super().__init__()
        self._exchange: Optional["WazirxPerpetualDerivative"] = None

    @property
    def name(self) -> str:
        return "wazirx_perpetual"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        for trading_pair, prices in (await self._get_bid_ask_prices(quote_token=quote_token)).items():
            mid = prices.get("mid", Decimal("0"))
            if mid > 0:
                results[trading_pair] = mid
        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        return await self._get_bid_ask_prices(quote_token=quote_token)

    async def _get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}
        try:
            tickers = await self._exchange.get_all_pairs_prices()
        except Exception as exception:
            self.logger().error(
                msg="Unexpected error while retrieving rates from WazirX futures. "
                    "Check the log file for more info.",
                exc_info=exception,
            )
            return results

        for ticker in tickers:
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=str(ticker.get("symbol", "")))
            except KeyError:
                continue
            if quote_token is not None and trading_pair.split("-")[1] != quote_token:
                continue
            try:
                last = Decimal(str(ticker.get("lastPrice", "0")))
            except Exception:
                continue
            if last <= 0:
                continue
            # The ticker carries no book, so last price stands in for both sides
            # and the spread is reported as zero.
            results[trading_pair] = {
                "bid": last,
                "ask": last,
                "mid": last,
                "spread": Decimal("0"),
            }
        return results

    @staticmethod
    def _build_exchange() -> "WazirxPerpetualDerivative":
        from hummingbot.connector.derivative.wazirx_perpetual.wazirx_perpetual_derivative import (
            WazirxPerpetualDerivative,
        )

        return WazirxPerpetualDerivative(
            wazirx_perpetual_api_key="",
            wazirx_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
