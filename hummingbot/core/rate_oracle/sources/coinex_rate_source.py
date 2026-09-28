import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange

# Ceiling on simultaneously open depth requests. The pair universe here is every
# CoinEx market matching the quote token — several hundred for USDT, ~900+ with no
# filter — and the connector throttler paces the REQUEST RATE but not concurrency,
# so an unbounded gather opens that many HTTPS connections at once every cache
# refresh. That burst can trip WAF/abuse detection independently of the per-minute
# quota. The cap keeps the same total throughput but spreads it over the window.
MAX_CONCURRENT_DEPTH_REQUESTS = 20


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
        """
        Fetch last prices (ticker-based) for all spot trading pairs (optionally
        filtered by quote).

        NOTE: this and ``get_bid_ask_prices`` now draw on DIFFERENT sources and so
        cover different pair sets — do not assume a pair in one appears in the other.
        This one is the conversion-rate feed and stays on the ticker (broad coverage,
        one request); the other needs a real top-of-book and is limited to pairs with
        a usable order book. Deliberately not reconciled: making them agree would mean
        either injecting synthetic spread-0 rows into the bid/ask feed (which is
        exactly the fake data the order-book source was added to remove) or dropping
        real conversion rates from this one. Callers needing both must handle a miss.
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

        return {trading_pair: data["mid"] for trading_pair, data in results.items()}

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch a best bid/ask snapshot for all spot pairs as {bid, ask, mid, spread}.

        CoinEx's ticker has no best-bid/ask, so bid/ask are taken from an order book
        snapshot (``/spot/depth``) for every pair listed by MARKETS_PATH_URL.

        Covers a different pair set from ``get_prices`` — see the note there. Pairs
        whose depth call fails or whose book is one-sided are omitted rather than
        filled in with a synthetic zero spread.
        """
        self._ensure_exchanges()
        return await self._get_coinex_order_book_prices(exchange=self._exchange, quote_token=quote_token)

    @classmethod
    async def _get_coinex_order_book_prices(cls, exchange: "CoinexExchange", quote_token: Optional[str] = None
                                            ) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        # The tradable universe comes from MARKETS_PATH_URL (drives the symbol map).
        try:
            symbol_map = await exchange.trading_pair_symbol_map()
        except Exception:
            return results

        # Select the pairs we care about, then fetch the order books CONCURRENTLY but
        # CAPPED. One depth call per pair done sequentially is N x latency (worse over
        # a proxy); an uncapped gather is the opposite failure — hundreds of sockets
        # opened at once. The semaphore keeps at most MAX_CONCURRENT_DEPTH_REQUESTS in
        # flight while the connector throttler still bounds the request rate. Per-pair
        # failures are isolated (return_exceptions).
        trading_pairs = []
        for trading_pair in symbol_map.values():
            if quote_token is not None:
                _, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue
            trading_pairs.append(trading_pair)

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DEPTH_REQUESTS)

        async def _bounded_snapshot(pair: str):
            async with semaphore:
                return await exchange.get_order_book_snapshot(pair)

        depths = await safe_gather(
            *(_bounded_snapshot(trading_pair) for trading_pair in trading_pairs),
            return_exceptions=True,
        )

        failures = 0
        for trading_pair, depth in zip(trading_pairs, depths):
            if isinstance(depth, Exception) or not isinstance(depth, dict):
                failures += 1
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

        # A systemic block (WAF, IP ban, connectivity) fails every pair the same way,
        # and per-pair isolation would otherwise degrade that to a quietly near-empty
        # result. Surface it once, at a level the operator will actually see.
        if failures and trading_pairs:
            message = (
                f"CoinEx depth fetch failed for {failures}/{len(trading_pairs)} pairs; "
                f"bid/ask results are incomplete."
            )
            if failures == len(trading_pairs):
                cls.logger().error(f"{message} Every pair failed — check connectivity / rate limits.")
            else:
                cls.logger().warning(message)

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
