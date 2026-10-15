from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coindcx_perpetual.coindcx_perpetual_derivative import (
        CoindcxPerpetualDerivative,
    )


class CoinDCXPerpetualRateSource(RateSourceBase):
    """
    Rate source for CoinDCX perpetual futures.

    Backed by the public FAPI ``ticker/bookTicker`` (no credentials), which gives
    the best bid and ask of every symbol in one call, so the spread is real.
    """

    def __init__(self):
        super().__init__()
        self._exchange: Optional["CoindcxPerpetualDerivative"] = None

    @property
    def name(self) -> str:
        return "coindcx_perpetual"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        return {trading_pair: prices["mid"]
                for trading_pair, prices in (await self._get_bid_ask_prices(quote_token=quote_token)).items()}

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

        for ticker in tickers:
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=ticker.get("symbol", ""))
            except KeyError:
                continue  # not a tradable perpetual
            if quote_token is not None and split_hb_trading_pair(trading_pair)[1] != quote_token:
                continue
            try:
                bid = Decimal(str(ticker["bidPrice"]))
                ask = Decimal(str(ticker["askPrice"]))
            except Exception:
                continue
            # An empty side or a crossed book is not a quotable market.
            if not (0 < bid <= ask):
                continue
            mid = (bid + ask) / Decimal("2")
            results[trading_pair] = {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread": (ask - bid) / mid * Decimal("100"),
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
