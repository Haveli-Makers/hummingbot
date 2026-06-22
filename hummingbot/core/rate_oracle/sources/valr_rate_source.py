from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrRateSource(RateSourceBase):
    """
    Rate source for VALR spot.

    Fetches the public ``/v1/public/marketsummary`` (no credentials), which exposes
    real ``bidPrice``/``askPrice`` and ``lastTradedPrice`` per pair.
    """

    @property
    def name(self) -> str:
        return "valr"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        bid_ask = await self.get_bid_ask_prices(quote_token=quote_token)
        return {trading_pair: data["mid"] for trading_pair, data in bid_ask.items()}

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}
        task_results = await safe_gather(
            self._get_valr_prices(exchange=self._exchange, quote_token=quote_token),
            return_exceptions=True,
        )
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from VALR. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            results.update(task_result)
        return results

    @staticmethod
    async def _get_valr_prices(exchange: "ValrExchange", quote_token: Optional[str] = None
                               ) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results
        try:
            tickers = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for ticker in tickers:
            symbol = ticker.get("currencyPair")
            if not symbol:
                continue
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except Exception:
                continue
            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue
            try:
                bid = Decimal(str(ticker.get("bidPrice") or "0"))
                ask = Decimal(str(ticker.get("askPrice") or "0"))
            except Exception:
                continue
            if bid <= 0 or ask <= 0 or bid > ask:
                last = ticker.get("lastTradedPrice")
                if last is None:
                    continue
                price = Decimal(str(last))
                if price <= 0:
                    continue
                results[trading_pair] = {"bid": price, "ask": price, "mid": price, "spread": Decimal("0")}
                continue
            mid = (bid + ask) / Decimal("2")
            spread_pct = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
            results[trading_pair] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread_pct}
        return results

    def _build_exchange(self) -> "ValrExchange":
        from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange

        return ValrExchange(
            valr_api_key="",
            valr_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
