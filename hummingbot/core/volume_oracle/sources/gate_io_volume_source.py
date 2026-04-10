from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange


class GateIoVolumeSource(VolumeSourceBase):

    @property
    def name(self) -> str:
        return "gate_io"

    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        base, quote = self._parse_trading_pair(trading_pair)
        currency_pair = f"{base}_{quote}"
        self._ensure_exchange()

        resp = await self._exchange.get_24h_volume_ticker(currency_pair)

        if not resp:
            raise ValueError(f"Trading pair {trading_pair} ({currency_pair}) not found on {self.name}")

        ticker = resp[0]
        result = {
            "exchange": self.name,
            "trading_pair": trading_pair,
            "symbol": ticker.get("currency_pair", currency_pair),
            "base_volume": Decimal(str(ticker["base_volume"])),
            "last_price": Decimal(str(ticker["last"])),
        }
        if ticker.get("quote_volume"):
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
