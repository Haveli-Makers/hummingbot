from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
        CoinswitchPerpetualDerivative,
    )


class CoinswitchPerpetualRateSource(RateSourceBase):
    """
    Rate source for CoinSwitch PRO perpetual futures (USDT contracts).

    Backed by the all-pairs ticker, one call for every contract. Each ticker
    carries the best bid and ask, so the spread is real; contracts without a
    two-sided book fall back to the last price.
    """

    def __init__(self):
        super().__init__()
        self._exchange: Optional["CoinswitchPerpetualDerivative"] = None

    @property
    def name(self) -> str:
        return "coinswitch_perpetual"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        for trading_pair, prices in (await self._get_bid_ask_prices(quote_token=quote_token)).items():
            if prices["mid"] > 0:
                results[trading_pair] = prices["mid"]
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
                msg="Unexpected error while retrieving rates from CoinSwitch futures. "
                    "Check the log file for more info.",
                exc_info=exception,
            )
            return results

        for ticker in tickers:
            try:
                trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=str(ticker.get("symbol", "")).upper())
            except KeyError:
                continue
            if quote_token is not None and trading_pair.split("-")[1] != quote_token:
                continue
            bid = self._to_decimal(ticker.get("best_bid_price"))
            ask = self._to_decimal(ticker.get("best_ask_price"))
            if 0 < bid <= ask:
                mid = (bid + ask) / Decimal("2")
                results[trading_pair] = {"bid": bid, "ask": ask, "mid": mid, "spread": (ask - bid) / mid * 100}
                continue
            last = self._to_decimal(ticker.get("last_price"))
            if last > 0:
                results[trading_pair] = {"bid": last, "ask": last, "mid": last, "spread": Decimal("0")}
        return results

    @staticmethod
    def _to_decimal(value) -> Decimal:
        try:
            return Decimal(str(value)) if value not in (None, "") else Decimal("0")
        except Exception:
            return Decimal("0")

    @staticmethod
    def _build_exchange() -> "CoinswitchPerpetualDerivative":
        from hummingbot.connector.derivative.coinswitch_perpetual.coinswitch_perpetual_derivative import (
            CoinswitchPerpetualDerivative,
        )

        return CoinswitchPerpetualDerivative(
            coinswitch_perpetual_api_key="",
            coinswitch_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
