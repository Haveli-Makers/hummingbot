from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange


class GateIoVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "gate_io"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        self._ensure_exchange()
        data = await self._exchange.get_all_24h_volume_tickers(trading_pairs)

        result: Dict[str, Dict[str, Decimal]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            raw_symbol = str(item.get("currency_pair", ""))
            if not raw_symbol:
                continue
            hb_symbol = await self.normalize_symbol(raw_symbol)
            if not hb_symbol:
                continue

            result[hb_symbol] = self._normalize_ticker(item, hb_symbol)

        return result

    def _normalize_ticker(self, ticker: Dict[str, Any], hb_symbol: str) -> Dict[str, Decimal]:
        result = {
            "exchange": self.name,
            "symbol": hb_symbol,
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
