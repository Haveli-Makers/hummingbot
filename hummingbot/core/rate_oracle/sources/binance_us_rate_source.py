from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange


class BinanceUSRateSource(RateSourceBase):
    def __init__(self):
        super().__init__()
        self._binance_us_exchange: Optional[BinanceExchange] = None  # delayed because of circular reference

    @property
    def name(self) -> str:
        return "binance_us"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchanges()
        results = {}
        tasks = [
            self._get_binance_prices(exchange=self._binance_us_exchange, quote_token="USD"),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from Binance. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)
        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetches best bid and ask prices for all trading pairs from Binance US.
        
        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to {"bid": Decimal, "ask": Decimal, "mid": Decimal, "spread": Decimal, "spread_pct": Decimal}
        """
        self._ensure_exchanges()
        results = {}
        try:
            pairs_prices = await self._binance_us_exchange.get_all_pairs_prices()
            for pair_price in pairs_prices:
                try:
                    trading_pair = await self._binance_us_exchange.trading_pair_associated_to_exchange_symbol(
                        symbol=pair_price["symbol"]
                    )
                except KeyError:
                    continue

                if quote_token is not None:
                    base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                    if quote != quote_token:
                        continue

                bid_price = pair_price.get("bidPrice")
                ask_price = pair_price.get("askPrice")
                if bid_price is not None and ask_price is not None:
                    bid = Decimal(bid_price)
                    ask = Decimal(ask_price)
                    if bid > 0 and ask > 0 and bid <= ask:
                        results[trading_pair] = {
                            "bid": bid,
                            "ask": ask,
                            "mid": (bid + ask) / Decimal("2"),
                            "spread": ask - bid,
                            "spread_pct": ((ask - bid) / ((bid + ask) / Decimal("2"))) * Decimal("100")
                        }
        except Exception:
            self.logger().exception(
                msg="Unexpected error while retrieving bid/ask prices from Binance US. Check the log file for more info.",
            )
        return results

    def _ensure_exchanges(self):
        if self._binance_us_exchange is None:
            self._binance_us_exchange = self._build_binance_connector_without_private_keys(domain="us")

    @staticmethod
    async def _get_binance_prices(exchange: 'BinanceExchange', quote_token: str = None) -> Dict[str, Decimal]:
        """
        Fetches binance prices

        :param exchange: The exchange instance from which to query prices.
        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included for prices
        :return: A dictionary of trading pairs and prices
        """
        pairs_prices = await exchange.get_all_pairs_prices()
        results = {}
        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=pair_price["symbol"])
            except KeyError:
                continue  # skip pairs that we don't track
            if quote_token is not None:
                base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                if quote != quote_token:
                    continue
            bid_price = pair_price.get("bidPrice")
            ask_price = pair_price.get("askPrice")
            if bid_price is not None and ask_price is not None and 0 < Decimal(bid_price) <= Decimal(ask_price):
                results[trading_pair] = (Decimal(bid_price) + Decimal(ask_price)) / Decimal("2")

        return results

    @staticmethod
    def _build_binance_connector_without_private_keys(domain: str) -> 'BinanceExchange':
        from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange

        return BinanceExchange(
            binance_api_key="",
            binance_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=domain,
        )
