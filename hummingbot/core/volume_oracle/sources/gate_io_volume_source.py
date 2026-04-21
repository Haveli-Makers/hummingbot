from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange


class GateIoVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "gate_io"

    async def get_all_24h_volumes(self) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers()

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("currency_pair", "")).upper()
            if not symbol:
                continue

            result[symbol] = self._normalize_ticker(item)

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any]) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": str(ticker["currency_pair"]).upper(),
            "base_volume": Decimal(str(ticker["base_volume"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("quote_volume") is not None:
            result["quote_volume"] = Decimal(str(ticker["quote_volume"]))
        return result

    def _build_exchange(self) -> "GateIoExchange":
        from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange

        return GateIoExchange(
            gate_io_api_key="",
            gate_io_secret_key="",
            trading_pairs=[],
            trading_required=False,
        )
