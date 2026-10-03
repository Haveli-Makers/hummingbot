from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange


class AjaibVolumeSource(VolumeSourceBase):
    """
    24h volume source for Ajaib spot.

    The Ajaib Open API exposes no 24h ticker / volume endpoint, so this source
    always returns an empty mapping. It is registered for interface parity with
    the other exchanges and to keep the volume oracle from raising on "ajaib".
    """

    @property
    def name(self) -> str:
        return "ajaib"

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        return {}

    def _build_exchange(self) -> "AjaibExchange":
        from hummingbot.connector.exchange.ajaib.ajaib_exchange import AjaibExchange

        return AjaibExchange(
            ajaib_api_key="",
            ajaib_api_secret="",
            trading_pairs=[],
            trading_required=False,
        )
