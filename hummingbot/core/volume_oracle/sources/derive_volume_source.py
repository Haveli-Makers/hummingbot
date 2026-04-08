from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.derive import derive_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.derive.derive_exchange import DeriveExchange


class DeriveVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "derive"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        instrument_name = f"{base}-{quote}"
        self._ensure_exchange()

        resp = await self._exchange._api_post(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            data={"instrument_name": instrument_name},
        )

        ticker = resp.get("result")
        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({instrument_name}) not found on {self.name}")

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("instrument_name", instrument_name),
            "last_price": Decimal(str(ticker.get("mark_price", ticker.get("best_bid_price", "0")))),
            "base_volume": Decimal(str(ticker.get("amount_24h", ticker.get("volume_24h", "0")))),
        }
        if ticker.get("quote_volume_24h"):
            result["quote_volume"] = Decimal(str(ticker["quote_volume_24h"]))
        return result

    def _build_exchange(self) -> "DeriveExchange":
        from hummingbot.connector.exchange.derive.derive_exchange import DeriveExchange

        return DeriveExchange(
            derive_api_secret="",
            trading_pairs=[],
            sub_id="",
            derive_api_key="",
            trading_required=False,
        )
