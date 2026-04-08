from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.dexalot.dexalot_exchange import DexalotExchange


class DexalotVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "dexalot"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        pair_symbol = f"{base}/{quote}"
        self._ensure_exchange()

        data = await self._exchange.get_all_pairs_prices()

        ticker = None
        for item in data:
            if isinstance(item, dict) and item.get("pair", "").upper() == pair_symbol.upper():
                ticker = item
                break

        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({pair_symbol}) not found on {self.name}")

        last_price = Decimal(str(ticker.get("last", ticker.get("high", "0"))))
        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("pair", pair_symbol),
            "last_price": last_price,
            "base_volume": Decimal(str(ticker.get("volume", "0"))),
        }
        if ticker.get("quote_volume"):
            result["quote_volume"] = Decimal(str(ticker["quote_volume"]))
        return result

    def _build_exchange(self) -> "DexalotExchange":
        from hummingbot.connector.exchange.dexalot.dexalot_exchange import DexalotExchange

        return DexalotExchange(
            dexalot_api_key="",
            dexalot_api_secret="13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930",  # noqa: mock
            trading_pairs=[],
            trading_required=False,
        )
