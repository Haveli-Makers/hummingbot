from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.connector.exchange.coinswitch import coinswitch_constants as CONSTANTS
from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange


class CoinswitchRateSource(RateSourceBase):
    """Rate source for CoinSwitch using the connector."""

    def __init__(self, exchange: str = CONSTANTS.DEFAULT_EXCHANGE, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._exchange_name = exchange
        self._domain = domain
        self._coinswitch_exchange: Optional[CoinswitchExchange] = None  # delayed because of circular reference

    @property
    def name(self) -> str:
        return "coinswitch"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchange()
        tickers = await self._fetch_all_tickers()
        return self._extract_mid_prices(tickers=tickers, quote_token=quote_token)

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        tickers = await self._fetch_all_tickers()
        return self._extract_bid_ask(tickers=tickers, quote_token=quote_token)

    def _ensure_exchange(self):
        if self._coinswitch_exchange is None:
            self._coinswitch_exchange = self._build_coinswitch_connector_without_private_keys()

    async def _fetch_all_tickers(self) -> Dict[str, Dict[str, Decimal]]:
        """Fetch all tickers from CoinSwitch using the public ticker API."""
        try:
            response = await self._coinswitch_exchange.get_all_pairs_prices()

            if not response or "data" not in response:
                return {}

            data = response.get("data", {})
            if not data:
                return {}

            if isinstance(data, dict) and len(data) == 1 and self._exchange_name.lower() in data:
                data = data.get(self._exchange_name.lower(), {})

            if isinstance(data, dict):
                return data
            elif isinstance(data, list):
                result = {}
                for item in data:
                    if isinstance(item, dict):
                        symbol = item.get("symbol") or item.get("s") or item.get("pair")
                        if symbol:
                            result[symbol] = item
                return result
            else:
                return {}

        except Exception as e:
            self.logger().error(
                msg=f"Unexpected error while retrieving rates from CoinSwitch: {e}",
                exc_info=True,
            )
            return {}

    def _extract_mid_prices(self, tickers: Dict[str, Dict[str, Decimal]], quote_token: Optional[str]) -> Dict[str, Decimal]:
        results: Dict[str, Decimal] = {}
        for symbol, ticker in tickers.items():
            trading_pair = self._to_trading_pair(symbol)
            if trading_pair is None:
                continue
            if quote_token is not None and not trading_pair.endswith(f"-{quote_token}"):
                continue

            bid = self._to_decimal(ticker.get("bidPrice") or ticker.get("bid_price") or ticker.get("bid"))
            ask = self._to_decimal(ticker.get("askPrice") or ticker.get("ask_price") or ticker.get("ask"))
            last_price = self._to_decimal(ticker.get("lastPrice") or ticker.get("last_price") or ticker.get("last"))

            if bid > 0 and ask > 0 and bid <= ask:
                results[trading_pair] = (bid + ask) / Decimal("2")
            elif last_price > 0:
                results[trading_pair] = last_price
        return results

    def _extract_bid_ask(self, tickers: Dict[str, Dict[str, Decimal]], quote_token: Optional[str]) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        for symbol, ticker in tickers.items():
            trading_pair = self._to_trading_pair(symbol)
            if trading_pair is None:
                continue

            if quote_token is not None and not trading_pair.endswith(f"-{quote_token}"):
                continue

            bid = self._to_decimal(
                ticker.get("bidPrice") or ticker.get("bid_price") or ticker.get("bid") or
                ticker.get("b") or ticker.get("bestBid") or ticker.get("best_bid")
            )
            ask = self._to_decimal(
                ticker.get("askPrice") or ticker.get("ask_price") or ticker.get("ask") or
                ticker.get("a") or ticker.get("bestAsk") or ticker.get("best_ask")
            )

            if bid > 0 and ask > 0 and bid <= ask:
                mid = (bid + ask) / Decimal("2")
                spread = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
                results[trading_pair] = {
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread": spread,
                }

        return results

    @staticmethod
    def _to_decimal(value: Optional[Decimal]) -> Decimal:
        try:
            return Decimal(str(value)) if value is not None else Decimal("0")
        except Exception:
            return Decimal("0")

    @staticmethod
    def _to_trading_pair(symbol: str) -> Optional[str]:
        if not symbol:
            return None
        normalized = symbol.replace("/", "-").replace("_", "-").upper()
        parts = normalized.split("-")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return f"{parts[0]}-{parts[1]}"

    @staticmethod
    def _build_coinswitch_connector_without_private_keys() -> 'CoinswitchExchange':
        from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

        return CoinswitchExchange(
            coinswitch_api_key="",
            coinswitch_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
