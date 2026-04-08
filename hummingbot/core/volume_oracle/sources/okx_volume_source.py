from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange


class OkxVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "okx"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        inst_id = f"{base}-{quote}"
        self._ensure_exchange()

        resp = await self._exchange._api_get(
            path_url=CONSTANTS.OKX_TICKER_PATH,
            params={"instId": inst_id},
        )

        tickers = resp.get("data", [])
        if not tickers:
            raise ValueError(f"Trading pair {trading_pair} ({inst_id}) not found on {self.name}")

        ticker = tickers[0]
        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("instId", inst_id),
            "base_volume": Decimal(str(ticker["vol24h"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("volCcy24h"):
            result["quote_volume"] = Decimal(str(ticker["volCcy24h"]))
        return result

    def _build_exchange(self) -> "OkxExchange":
        from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange

        return OkxExchange(
            okx_api_key="",
            okx_secret_key="",
            okx_passphrase="",
            trading_pairs=[],
            trading_required=False,
        )
