from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange


class CoindcxRateSource(RateSourceBase):
    """
    Rate source for CoinDCX exchange.
    Fetches ticker data directly from CoinDCX public API.
    """

    def __init__(self):
        super().__init__()
        self._coindcx_exchange: Optional["CoindcxExchange"] = None

    @property
    def name(self) -> str:
        return "coindcx"

    # Connector is expected to resolve exchange symbols to trading pairs; no local parser kept.
    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        """
        Fetches mid prices for all trading pairs.

        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to mid prices
        """
        self._ensure_exchanges()
        results: Dict[str, Decimal] = {}

        tasks = [
            self._get_coindcx_prices(exchange=self._coindcx_exchange, quote_token=quote_token),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from CoinDCX. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)

        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetches best bid and ask prices for all trading pairs.

        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to {"bid": Decimal, "ask": Decimal, "mid": Decimal, "spread": Decimal, "spread_pct": Decimal}
        """
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}

        tasks = [
            self._get_coindcx_bid_ask_prices(exchange=self._coindcx_exchange, quote_token=quote_token),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving bid/ask prices from CoinDCX. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)

        return results

    def _ensure_exchanges(self):
        if self._coindcx_exchange is None:
            self._coindcx_exchange = self._build_coindcx_connector_without_private_keys()

    @staticmethod
    async def _get_coindcx_prices(exchange: 'CoindcxExchange', quote_token: str = None) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        if exchange is None:
            return results

        # Try to use exchange method to get all pairs prices similar to Binance
        try:
            pairs_prices = await exchange.get_all_pairs_prices()
        except Exception:
            # If connector doesn't provide, return empty dict
            return results

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=pair_price.get("symbol") or pair_price.get("market"))
            except Exception:
                # Could not resolve trading pair via connector; skip this symbol
                continue

            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue

            bid_price = pair_price.get("bid") or pair_price.get("bidPrice")
            ask_price = pair_price.get("ask") or pair_price.get("askPrice")
            if bid_price is not None and ask_price is not None:
                try:
                    bid_dec = Decimal(str(bid_price))
                    ask_dec = Decimal(str(ask_price))
                    if bid_dec > 0 and ask_dec > 0:
                        results[trading_pair] = (bid_dec + ask_dec) / Decimal("2")
                except Exception:
                    continue

        return results

    @staticmethod
    async def _get_coindcx_bid_ask_prices(exchange: 'CoindcxExchange', quote_token: str = None) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        try:
            pairs_prices = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=pair_price.get("symbol") or pair_price.get("market"))
            except Exception:
                # Could not resolve trading pair via connector; skip this symbol
                continue

            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue

            bid_price = pair_price.get("bid") or pair_price.get("bidPrice")
            ask_price = pair_price.get("ask") or pair_price.get("askPrice")
            if bid_price is not None and ask_price is not None:
                try:
                    bid = Decimal(str(bid_price))
                    ask = Decimal(str(ask_price))
                    if bid > 0 and ask > 0 and bid <= ask:
                        mid = (bid + ask) / Decimal("2")
                        spread = ask - bid
                        spread_pct = (spread / mid) * Decimal("100") if mid > 0 else Decimal("0")
                        results[trading_pair] = {
                            "bid": bid,
                            "ask": ask,
                            "mid": mid,
                            "spread": spread_pct,
                        }
                except Exception:
                    continue

        return results

    @staticmethod
    def _build_coindcx_connector_without_private_keys() -> 'CoindcxExchange':
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange

        return CoindcxExchange(
            coindcx_api_key="",
            coindcx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
