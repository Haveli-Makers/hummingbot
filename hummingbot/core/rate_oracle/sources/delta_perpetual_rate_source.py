from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative


class DeltaPerpetualRateSource(RateSourceBase):
    """
    Rate source for Delta Exchange (India) perpetuals.

    Fetches ticker data directly from Delta's public ``/v2/tickers`` API (no
    credentials needed). Best bid/ask come from each ticker's ``quotes`` object.
    """

    @property
    def name(self) -> str:
        return "delta_perpetual"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Fetch mid prices for all perpetual trading pairs.

        :param quote_token: if given, only pairs with this quote are returned (e.g. "USD")
        :return: a dictionary of trading pairs to mid prices
        """
        bid_ask = await self.get_bid_ask_prices(quote_token=quote_token)
        return {trading_pair: data["mid"] for trading_pair, data in bid_ask.items()}

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch best bid/ask for all perpetual trading pairs.

        :param quote_token: if given, only pairs with this quote are returned (e.g. "USD")
        :return: a dict of trading pair -> {"bid", "ask", "mid", "spread"} (spread is a percentage)
        """
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}

        task_results = await safe_gather(
            self._get_delta_bid_ask_prices(exchange=self._exchange, quote_token=quote_token),
            return_exceptions=True,
        )
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving bid/ask prices from Delta. "
                        "Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            results.update(task_result)

        return results

    @staticmethod
    def _extract_bid_ask(ticker: dict):
        quotes = ticker.get("quotes") or {}
        bid, ask = quotes.get("best_bid"), quotes.get("best_ask")
        if bid is None or ask is None:
            return None
        try:
            bid_dec, ask_dec = Decimal(str(bid)), Decimal(str(ask))
        except Exception:
            return None
        if bid_dec <= 0 or ask_dec <= 0 or bid_dec > ask_dec:
            return None
        return bid_dec, ask_dec

    @classmethod
    async def _get_delta_bid_ask_prices(
        cls, exchange: "DeltaPerpetualDerivative", quote_token: Optional[str] = None
    ) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        try:
            tickers = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for ticker in tickers:
            symbol = ticker.get("symbol")
            if not symbol:
                continue
            # Only perpetuals are in the symbol map; everything else raises and is skipped.
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except Exception:
                continue

            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue

            bid_ask = cls._extract_bid_ask(ticker)
            if bid_ask is None:
                continue
            bid, ask = bid_ask
            mid = (bid + ask) / Decimal("2")
            spread_pct = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
            results[trading_pair] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread_pct}

        return results

    def _build_exchange(self) -> "DeltaPerpetualDerivative":
        from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_derivative import DeltaPerpetualDerivative

        return DeltaPerpetualDerivative(
            delta_perpetual_api_key="",
            delta_perpetual_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
