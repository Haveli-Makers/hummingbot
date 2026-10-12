from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


class CoinDCXPerpetualRateSource(RateSourceBase):
    """
    Rate source for CoinDCX USDT-margined perpetual futures.

    Backed by the public ``current_prices/futures/rt`` feed (no credentials
    needed), which reports the last price (``ls``) and mark price (``mp``) for
    every instrument in a single call.
    """

    def __init__(self):
        super().__init__()
        self._exchange: Optional["CoindcxPerpetualDerivative"] = None

    @property
    def name(self) -> str:
        return "coindcx_perpetual"

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
        self._ensure_exchange()
        results: Dict[str, Dict[str, Decimal]] = {}
        try:
            tickers = await self._exchange.get_all_pairs_prices()
        except Exception as exception:
            self.logger().error(
                msg="Unexpected error while retrieving rates from CoinDCX futures. "
                    "Check the log file for more info.",
                exc_info=exception,
            )
            return results

        from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_utils as utils

        for ticker in tickers:
            trading_pair = utils.coindcx_pair_to_hb_pair(ticker.get("symbol", ""))
            if "-" not in trading_pair:
                continue
            if quote_token is not None and trading_pair.split("-")[1] != quote_token:
                continue
            try:
                last = Decimal(str(ticker.get("lastPrice", "0")))
            except Exception:
                continue
            if last <= 0:
                continue
            # The feed carries no book, so last price stands in for both sides
            # and the spread is reported as zero.
            results[trading_pair] = {
                "bid": last,
                "ask": last,
                "mid": last,
                "spread": Decimal("0"),
            }
        return results

    def _ensure_exchange(self):
        if self._exchange is None:
            self._exchange = self._build_exchange()

    @staticmethod
    def _build_exchange() -> "CoindcxPerpetualDerivative":
        from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
            CoindcxPerpetualDerivative,
        )

        return CoindcxPerpetualDerivative(
            coindcx_perpetual_api_key="",
            coindcx_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
