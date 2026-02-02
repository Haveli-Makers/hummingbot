
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange


class WazirxRateSource(RateSourceBase):
    """
    Rate source for WazirX exchange.
    """

    def __init__(self):
        super().__init__()
        self._wazirx_exchange: Optional["WazirxExchange"] = None

    @property
    def name(self) -> str:
        return "wazirx"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchanges()
        results = await self._get_wazirx_prices(exchange=self._wazirx_exchange, quote_token=quote_token)
        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchanges()
        results = await self._get_wazirx_bid_ask_prices(exchange=self._wazirx_exchange, quote_token=quote_token)
        return results

    def _ensure_exchanges(self):
        if self._wazirx_exchange is None:
            self._wazirx_exchange = self._build_wazirx_connector_without_private_keys()

    @staticmethod
    async def _get_wazirx_prices(exchange: "WazirxExchange", quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        pairs_prices = await exchange.get_all_pairs_prices()
        results = {}

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=pair_price["symbol"])
            except KeyError:
                base = pair_price.get("baseAsset", "").upper()
                quote = pair_price.get("quoteAsset", "").upper()
                if base and quote:
                    trading_pair = f"{base}-{quote}"
                else:
                    continue

            if quote_token is not None:
                _, quote = split_hb_trading_pair(trading_pair=trading_pair)
                if quote != quote_token:
                    continue

            bid_price = pair_price.get("bidPrice")
            ask_price = pair_price.get("askPrice")

            if bid_price is not None and ask_price is not None:
                try:
                    bid = Decimal(str(bid_price))
                    ask = Decimal(str(ask_price))
                    if bid > 0 and ask > 0 and bid <= ask:
                        results[trading_pair] = (bid + ask) / Decimal("2")
                except Exception:
                    continue

        return results

    @staticmethod
    async def _get_wazirx_bid_ask_prices(
        exchange: "WazirxExchange", quote_token: Optional[str] = None
    ) -> Dict[str, Dict[str, Decimal]]:
        pairs_prices = await exchange.get_all_pairs_prices()
        results = {}

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(symbol=pair_price["symbol"])
            except KeyError:
                base = pair_price.get("baseAsset", "").upper()
                quote = pair_price.get("quoteAsset", "").upper()
                if base and quote:
                    trading_pair = f"{base}-{quote}"
                else:
                    continue

            if quote_token is not None:
                _, quote = split_hb_trading_pair(trading_pair=trading_pair)
                if quote != quote_token:
                    continue

            bid_price = pair_price.get("bidPrice")
            ask_price = pair_price.get("askPrice")

            if bid_price is not None and ask_price is not None:
                try:
                    bid = Decimal(str(bid_price))
                    ask = Decimal(str(ask_price))
                    if bid > 0 and ask > 0 and bid <= ask:
                        mid = (bid + ask) / Decimal("2")
                        spread_pct = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
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
    def _build_wazirx_connector_without_private_keys() -> "WazirxExchange":
        from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange

        return WazirxExchange(
            wazirx_api_key="",
            wazirx_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
