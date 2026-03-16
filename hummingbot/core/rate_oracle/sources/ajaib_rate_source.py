from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibRateSource(RateSourceBase):
    """
    Rate source for Ajaib exchange.
    Fetches ticker data from the Ajaib API.

    NOTE: Ajaib requires Ed25519 authentication for all endpoints.
    API keys must be configured in conf/connectors/ajaib.yml for this to work.
    """

    def __init__(self):
        super().__init__()
        self._ajaib_exchange: Optional["AjaibExchange"] = None

    @property
    def name(self) -> str:
        return "ajaib"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchanges()
        results: Dict[str, Decimal] = {}

        tasks = [
            self._get_ajaib_prices(exchange=self._ajaib_exchange, quote_token=quote_token),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from Ajaib. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)

        return results

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_bid_ask_prices(self, quote_token: Optional[str] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchanges()
        results: Dict[str, Dict[str, Decimal]] = {}

        tasks = [
            self._get_ajaib_bid_ask_prices(exchange=self._ajaib_exchange, quote_token=quote_token),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving bid/ask prices from Ajaib. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)

        return results

    def _ensure_exchanges(self):
        if self._ajaib_exchange is None:
            self._ajaib_exchange = self._build_ajaib_connector()

    @staticmethod
    async def _get_ajaib_prices(exchange: 'AjaibExchange', quote_token: str = None) -> Dict[str, Decimal]:
        """
        Fetches mid prices from Ajaib bookTicker response.

        Expected response format (Binance-compatible):
        [
            {"symbol": "BTC_USDT", "bidPrice": "50000.00", "bidQty": "1.0", "askPrice": "50001.00", "askQty": "0.5"},
            ...
        ]
        """
        results: Dict[str, Decimal] = {}
        if exchange is None:
            return results

        try:
            pairs_prices = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=pair_price.get("symbol", ""))
            except Exception:
                continue

            if quote_token is not None:
                base, quote = trading_pair.split("-")
                if quote != quote_token:
                    continue

            bid_price = pair_price.get("bidPrice")
            ask_price = pair_price.get("askPrice")
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
    async def _get_ajaib_bid_ask_prices(exchange: 'AjaibExchange', quote_token: str = None) -> Dict[str, Dict[str, Decimal]]:
        results: Dict[str, Dict[str, Decimal]] = {}
        if exchange is None:
            return results

        try:
            pairs_prices = await exchange.get_all_pairs_prices()
        except Exception:
            return results

        for pair_price in pairs_prices:
            try:
                trading_pair = await exchange.trading_pair_associated_to_exchange_symbol(
                    symbol=pair_price.get("symbol", ""))
            except Exception:
                continue

            if quote_token is not None:
                base, quote = trading_pair.split("-")
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
    def _build_ajaib_connector() -> 'AjaibExchange':
        """
        Build an Ajaib exchange connector with API keys from config.
        Ajaib requires Ed25519 authentication for all endpoints, so keys must be configured.
        """
        from pydantic import SecretStr

        from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange

        api_key = ""
        api_secret = ""

        try:
            from hummingbot.client.settings import AllConnectorSettings
            connector_config = AllConnectorSettings.get_connector_config_keys("ajaib")
            if connector_config is not None:
                api_key = getattr(connector_config, "ajaib_api_key", SecretStr("")).get_secret_value()
                api_secret = getattr(connector_config, "ajaib_api_secret", SecretStr("")).get_secret_value()
        except Exception:
            pass

        return AjaibExchange(
            ajaib_api_key=api_key,
            ajaib_api_secret=api_secret,
            trading_pairs=[],
            trading_required=False,
        )
