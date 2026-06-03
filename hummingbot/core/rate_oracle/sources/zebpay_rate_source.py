from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange


class ZebpayRateSource(RateSourceBase):
    """Rate source for Zebpay (spot)."""

    def __init__(self):
        super().__init__()
        self._zebpay_exchange: Optional["ZebpayExchange"] = None

    @property
    def name(self) -> str:
        return "zebpay"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchange()
        tickers = await self._zebpay_exchange.get_all_pairs_prices()
        return self._extract_mid_prices(tickers, quote_token)

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._zebpay_exchange.get_all_pairs_prices()
        return self._extract_bid_ask(tickers, quote_token)

    def _ensure_exchange(self):
        if self._zebpay_exchange is None:
            self._zebpay_exchange = self._build_zebpay_connector()

    def _extract_mid_prices(self, tickers, quote_token: Optional[str]) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            tp = str(ticker.get("symbol", "")).upper()
            if not tp or "-" not in tp:
                continue
            if quote_token and not tp.endswith(f"-{quote_token}"):
                continue
            bid = self._to_decimal(ticker.get("bid"))
            ask = self._to_decimal(ticker.get("ask"))
            last = self._to_decimal(ticker.get("last") or ticker.get("lastPrice"))
            if bid > 0 and ask > 0:
                results[tp] = (bid + ask) / Decimal("2")
            elif last > 0:
                results[tp] = last
        return results

    def _extract_bid_ask(self, tickers, quote_token: Optional[str]) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            tp = str(ticker.get("symbol", "")).upper()
            if not tp or "-" not in tp:
                continue
            if quote_token and not tp.endswith(f"-{quote_token}"):
                continue
            last = self._to_decimal(ticker.get("last") or ticker.get("lastPrice"))
            bid = self._to_decimal(ticker.get("bid")) or last
            ask = self._to_decimal(ticker.get("ask")) or last
            if bid > 0 and ask > 0 and bid <= ask:
                mid = (bid + ask) / Decimal("2")
                spread = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
                results[tp] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}
            elif last > 0:
                results[tp] = {"bid": last, "ask": last, "mid": last, "spread": Decimal("0")}
        return results

    @staticmethod
    def _to_decimal(value) -> Decimal:
        try:
            return Decimal(str(value)) if value not in (None, "") else Decimal("0")
        except Exception:
            return Decimal("0")

    @staticmethod
    def _build_zebpay_connector() -> "ZebpayExchange":
        from hummingbot.connector.exchange.zebpay.zebpay_exchange import ZebpayExchange

        return ZebpayExchange(
            zebpay_api_key="",
            zebpay_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
