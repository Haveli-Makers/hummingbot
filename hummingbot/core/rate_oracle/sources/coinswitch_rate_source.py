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
        """Fetch all tickers from CoinSwitch using the authenticated API."""
        try:
            if not self._coinswitch_exchange.api_key or not self._coinswitch_exchange.secret_key:
                self.logger().warning(
                    "CoinSwitch credentials not configured. Please add API credentials in conf/connectors/coinswitch.yml"
                )
                return {}

            self.logger().info(f"Fetching tickers from CoinSwitch for exchange: {self._exchange_name}")

            response = await self._coinswitch_exchange.get_all_pairs_prices()

            self.logger().info(f"CoinSwitch ticker response received: {type(response)}")

            if not response:
                self.logger().warning("CoinSwitch ticker request returned empty response")
                return {}

            if "data" not in response:
                self.logger().warning(f"CoinSwitch ticker response missing 'data' field. Response: {str(response)[:500]}")
                return {}

            data = response.get("data", {})

            if not data:
                self.logger().warning("CoinSwitch ticker data is empty")
                return {}

            self.logger().info(f"CoinSwitch data type: {type(data)}, keys/len: {list(data.keys())[:5] if isinstance(data, dict) else len(data)}")

            if isinstance(data, dict) and len(data) == 1 and self._exchange_name.lower() in data:
                data = data.get(self._exchange_name.lower(), {})

            if isinstance(data, dict):
                self.logger().info(f"CoinSwitch returned {len(data)} ticker entries")
                return data
            elif isinstance(data, list):
                self.logger().info(f"CoinSwitch returned list with {len(data)} entries, converting to dict")
                result = {}
                for item in data:
                    if isinstance(item, dict):
                        symbol = item.get("symbol") or item.get("s") or item.get("pair")
                        if symbol:
                            result[symbol] = item
                return result
            else:
                self.logger().warning(f"CoinSwitch data has unexpected type: {type(data)}")
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
        logged_sample = False
        for symbol, ticker in tickers.items():
            trading_pair = self._to_trading_pair(symbol)
            if trading_pair is None:
                continue

            if not logged_sample:
                self.logger().info(f"Sample ticker data for {symbol}: {ticker}")
                logged_sample = True

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

        self.logger().info(f"Extracted {len(results)} bid/ask pairs (quote_token filter: {quote_token})")
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
        from hummingbot.client.config.config_helpers import (
            get_connector_config_yml_path,
            load_connector_config_map_from_file,
        )
        from hummingbot.client.config.security import Security
        from hummingbot.connector.exchange.coinswitch.coinswitch_exchange import CoinswitchExchange

        api_key = ""
        api_secret = ""

        try:
            connector_config_path = get_connector_config_yml_path("coinswitch")
            if connector_config_path.exists():
                if not Security._decryption_done.is_set():
                    try:
                        pass
                    except RuntimeError:
                        pass

                config_map = load_connector_config_map_from_file(connector_config_path)

                raw_api_key = config_map.coinswitch_api_key
                raw_api_secret = config_map.coinswitch_api_secret

                if hasattr(raw_api_key, 'get_secret_value'):
                    api_key = raw_api_key.get_secret_value() or ""
                else:
                    api_key = str(raw_api_key) if raw_api_key else ""

                if hasattr(raw_api_secret, 'get_secret_value'):
                    api_secret = raw_api_secret.get_secret_value() or ""
                else:
                    api_secret = str(raw_api_secret) if raw_api_secret else ""

                import logging
                logging.getLogger(__name__).info(f"Loaded CoinSwitch credentials, api_key length: {len(api_key)}")
        except FileNotFoundError:
            import logging
            logging.getLogger(__name__).debug("CoinSwitch credentials file not found - using empty credentials")
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Could not load CoinSwitch credentials: {e}")

        return CoinswitchExchange(
            coinswitch_api_key=api_key,
            coinswitch_api_secret=api_secret,
            trading_pairs=[],
            trading_required=False,
        )
