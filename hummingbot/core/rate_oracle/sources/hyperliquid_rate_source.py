from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange


class HyperliquidRateSource(RateSourceBase):
    def __init__(self):
        super().__init__()
        self._exchange: Optional[HyperliquidExchange] = None  # delayed because of circular reference

    @property
    def name(self) -> str:
        return "hyperliquid"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchange()
        results = {}
        try:
            pairs_prices = await self._exchange.get_all_pairs_prices()
            for pair_price in pairs_prices:
                pair = pair_price["symbol"]
                try:
                    trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(symbol=pair)
                except KeyError:
                    continue  # skip pairs that we don't track
                if quote_token is not None:
                    base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                    if quote != quote_token:
                        continue
                price = pair_price["price"]
                if price is not None:
                    results[trading_pair] = Decimal(price)
        except Exception:
            self.logger().exception(
                msg="Unexpected error while retrieving rates from Hyperliquid. Check the log file for more info.",
            )
        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetches best bid and ask prices for all trading pairs.
        Note: Hyperliquid API may not provide separate bid/ask, using price as mid.
        
        :param quote_token: A quote symbol, if specified only pairs with the quote symbol are included
        :return: A dictionary of trading pairs to {"bid": Decimal, "ask": Decimal, "mid": Decimal, "spread": Decimal, "spread_pct": Decimal}
        """
        self._ensure_exchange()
        results = {}
        try:
            pairs_prices = await self._exchange.get_all_pairs_prices()
            for pair_price in pairs_prices:
                pair = pair_price["symbol"]
                try:
                    trading_pair = await self._exchange.trading_pair_associated_to_exchange_symbol(symbol=pair)
                except KeyError:
                    continue
                if quote_token is not None:
                    base, quote = split_hb_trading_pair(trading_pair=trading_pair)
                    if quote != quote_token:
                        continue
                price = pair_price.get("price")
                bid = pair_price.get("bid", price)
                ask = pair_price.get("ask", price)
                if bid is not None and ask is not None:
                    bid = Decimal(str(bid))
                    ask = Decimal(str(ask))
                    if bid > 0 and ask > 0:
                        mid = (bid + ask) / Decimal("2")
                        results[trading_pair] = {
                            "bid": bid,
                            "ask": ask,
                            "mid": mid,
                            "spread": ask - bid,
                            "spread_pct": ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
                        }
        except Exception:
            self.logger().exception(
                msg="Unexpected error while retrieving bid/ask prices from Hyperliquid. Check the log file for more info.",
            )
        return results

    def _ensure_exchange(self):
        if self._exchange is None:
            self._exchange = self._build_hyperliquid_connector_without_private_keys()

    @staticmethod
    def _build_hyperliquid_connector_without_private_keys() -> 'HyperliquidExchange':
        from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange

        return HyperliquidExchange(
            hyperliquid_api_secret="",
            trading_pairs=[],
            use_vault = False,
            hyperliquid_api_key="",
            trading_required=False,
        )
