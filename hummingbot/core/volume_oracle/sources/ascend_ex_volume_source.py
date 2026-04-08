from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.connector.exchange.ascend_ex import ascend_ex_constants as CONSTANTS
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ascend_ex.ascend_ex_exchange import AscendExExchange


class AscendExVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "ascend_ex"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        symbol = f"{base}/{quote}"
        self._ensure_exchange()

        resp = await self._exchange._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PATH_URL,
            params={"symbol": symbol},
        )

        ticker = resp.get("data")
        if ticker is None:
            raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")

        if isinstance(ticker, list):
            match = None
            for item in ticker:
                if item.get("symbol", "").upper() == symbol.upper():
                    match = item
                    break
            if match is None:
                raise ValueError(f"Trading pair {trading_pair} ({symbol}) not found on {self.name}")
            ticker = match

        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("symbol", symbol),
            "base_volume": Decimal(str(ticker["volume"])),
            "last_price": Decimal(str(ticker["close"])),
        }
        return result

    def _build_exchange(self) -> "AscendExExchange":
        from hummingbot.connector.exchange.ascend_ex.ascend_ex_exchange import AscendExExchange

        return AscendExExchange(
            ascend_ex_api_key="",
            ascend_ex_secret_key="",
            ascend_ex_group_id="",
            trading_pairs=[],
            trading_required=False,
        )
