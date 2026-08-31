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
        """Fetch last prices (ticker-based) for all spot trading pairs (optionally filtered by quote)."""
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

        return {trading_pair: data["mid"] for trading_pair, data in results.items()}

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch a best bid/ask snapshot for all spot pairs as {bid, ask, mid, spread}.

        CoinEx's ticker has no best-bid/ask, so bid/ask are taken from an order book
        snapshot (``/spot/depth``) for every pair listed by MARKETS_PATH_URL.
        """
        self._ensure_exchanges()
        return await self._get_coinex_order_book_prices(exchange=self._exchange, quote_token=quote_token)

    @staticmethod
    async def _get_coinex_order_book_prices(exchange: "CoinexExchange", quote_token: Optional[str] = None
                                            ) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        # The tradable universe comes from MARKETS_PATH_URL (drives the symbol map).
        try:
            symbol_map = await exchange.trading_pair_symbol_map()
        except Exception:
            return results

        for trading_pair in symbol_map.values():
            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue

            try:
                depth = await exchange.get_order_book_snapshot(trading_pair)
            except Exception:
                continue

            bids = depth.get("bids") or []
            asks = depth.get("asks") or []
            if not bids or not asks:
                continue
            try:
                bid = Decimal(str(bids[0][0]))
                ask = Decimal(str(asks[0][0]))
            except Exception:
                continue
            if bid <= 0 or ask <= 0 or bid > ask:
                continue

            mid = (bid + ask) / Decimal("2")
            spread = ((ask - bid) / mid) * Decimal("100")
            results[trading_pair] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}

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
