from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

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
        self._ensure_exchange()
        tickers = await self._csx_exchange.get_all_pairs_prices()
        return self._extract_bid_ask(tickers, quote_token)

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

    def _extract_bid_ask(
        self, tickers, quote_token: Optional[str]
    ) -> Dict[str, Dict[str, Decimal]]:
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

            # CSX ticker has no separate bid/ask fields; use LastTradedPrice for both
            last = self._to_decimal(
                ticker.get("LastTradedPrice") or ticker.get("lastTradedPrice")
                or ticker.get("last") or 0)
            bid = self._to_decimal(ticker.get("bestBid") or ticker.get("bid") or 0) or last
            ask = self._to_decimal(ticker.get("bestAsk") or ticker.get("ask") or 0) or last

            if last > 0:
                if bid > 0 and ask > 0 and bid <= ask:
                    mid = (bid + ask) / Decimal("2")
                    spread = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
                    results[tp] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}
                else:
                    results[tp] = {"bid": last, "ask": last, "mid": last, "spread": Decimal("0")}

        return results

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
