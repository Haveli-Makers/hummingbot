from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange


class CsxRateSource(RateSourceBase):
    """Rate source for CoinSwitch Kuber (CSX)."""

    def __init__(self):
        super().__init__()
        self._csx_exchange: Optional["CsxExchange"] = None

    @property
    def name(self) -> str:
        return "csx"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchange()
        tickers = await self._csx_exchange.get_all_pairs_prices()
        return self._extract_mid_prices(tickers, quote_token)

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Best bid/ask per pair from the REST order book.

        The CSX ticker carries no top-of-book (no bid/ask fields), so real
        bid/ask must come from the depth endpoint. The previous implementation
        read ``bestBid``/``bestAsk`` off the ticker (fields that never exist) and
        fell back to ``last`` for both — yielding ``bid == ask == last`` with a
        constant zero spread for every pair. Here the order book provides the
        actual top-of-book; only when it is empty/unavailable do we fall back to
        the last price (zero spread).
        """
        self._ensure_exchange()
        tickers = await self._csx_exchange.get_all_pairs_prices()

        results: Dict[str, Dict[str, Decimal]] = {}
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            instrument = (ticker.get("Instrument") or ticker.get("instrument")
                          or ticker.get("symbol") or "")
            tp = instrument.replace("/", "-").upper()
            if not tp or "-" not in tp:
                continue
            if quote_token and not tp.endswith(f"-{quote_token}"):
                continue

            bid, ask = await self._best_bid_ask(tp)
            if bid > 0 and ask > 0 and bid <= ask:
                mid = (bid + ask) / Decimal("2")
                spread = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
                results[tp] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}
                continue

            # Order book unavailable/empty for this pair — fall back to the last
            # traded price (zero spread) so the pair still has a usable rate.
            last = self._to_decimal(
                ticker.get("LastTradedPrice") or ticker.get("lastTradedPrice")
                or ticker.get("last") or 0)
            if last > 0:
                results[tp] = {"bid": last, "ask": last, "mid": last, "spread": Decimal("0")}

        return results

    def _ensure_exchange(self):
        if self._csx_exchange is None:
            self._csx_exchange = self._build_csx_connector()

    def _extract_mid_prices(
        self, tickers, quote_token: Optional[str]
    ) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            # CSX returns PascalCase field names
            instrument = (ticker.get("Instrument") or ticker.get("instrument")
                          or ticker.get("symbol") or "")
            tp = instrument.replace("/", "-").upper()
            if not tp or "-" not in tp:
                continue
            if quote_token and not tp.endswith(f"-{quote_token}"):
                continue

            last = self._to_decimal(
                ticker.get("LastTradedPrice") or ticker.get("lastTradedPrice")
                or ticker.get("last") or 0)
            if last > 0:
                results[tp] = last
        return results

    async def _best_bid_ask(self, trading_pair: str) -> Tuple[Decimal, Decimal]:
        """
        Return ``(best_bid, best_ask)`` from the REST order book, or ``(0, 0)``.

        Best bid/ask are computed as max(bids)/min(asks) rather than assuming the
        depth arrays are pre-sorted.
        """
        try:
            snapshot = await self._csx_exchange.get_order_book_snapshot(trading_pair)
        except Exception:
            return Decimal("0"), Decimal("0")

        bids = [p for p in (self._to_decimal(e[0]) for e in snapshot.get("bids", []) if e) if p > 0]
        asks = [p for p in (self._to_decimal(e[0]) for e in snapshot.get("asks", []) if e) if p > 0]
        best_bid = max(bids) if bids else Decimal("0")
        best_ask = min(asks) if asks else Decimal("0")
        return best_bid, best_ask

    @staticmethod
    def _to_decimal(value) -> Decimal:
        try:
            return Decimal(str(value)) if value else Decimal("0")
        except Exception:
            return Decimal("0")

    @staticmethod
    def _build_csx_connector() -> "CsxExchange":
        from hummingbot.connector.exchange.csx.csx_exchange import CsxExchange

        return CsxExchange(
            csx_api_key="",
            csx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
