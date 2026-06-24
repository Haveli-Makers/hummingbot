from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange


class CoinexRateSource(RateSourceBase):
    """
    Rate source for CoinEx spot.

    Fetches ticker data from CoinEx's public ``/spot/ticker`` API (no credentials).
    CoinEx tickers expose ``last`` but no top-of-book, so bid/ask fall back to the
    last price (spread 0); ``get_prices`` (the conversion rate) uses ``last``.
    """

    @property
    def name(self) -> str:
        return "coinex"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """Fetch last prices for all spot trading pairs (optionally filtered by quote)."""
        bid_ask = await self.get_bid_ask_prices(quote_token=quote_token)
        return {trading_pair: data["mid"] for trading_pair, data in bid_ask.items()}

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch a price snapshot for all spot pairs as {bid, ask, mid, spread}.

        CoinEx's ticker has no best-bid/ask, so bid == ask == last (spread 0).
        """
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}

        task_results = await safe_gather(
            self._get_coinex_prices(exchange=self._exchange, quote_token=quote_token),
            return_exceptions=True,
        )
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from CoinEx. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            results.update(task_result)

        return results

    @staticmethod
    async def _get_coinex_prices(exchange: "CoinexExchange", quote_token: Optional[str] = None
                                 ) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        try:
            tickers = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for ticker in tickers:
            symbol = ticker.get("market")
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

            last = ticker.get("last")
            if last is None:
                continue
            try:
                price = Decimal(str(last))
            except Exception:
                continue
            if price <= 0:
                continue
            results[trading_pair] = {"bid": price, "ask": price, "mid": price, "spread": Decimal("0")}

        return results

    def _build_exchange(self) -> "CoinexExchange":
        from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange

        return CoinexExchange(
            coinex_api_key="",
            coinex_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
