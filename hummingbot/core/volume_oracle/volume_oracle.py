import logging
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.core.volume_oracle.sources.ascend_ex_volume_source import AscendExVolumeSource
from hummingbot.core.volume_oracle.sources.binance_us_volume_source import BinanceUSVolumeSource
from hummingbot.core.volume_oracle.sources.binance_volume_source import BinanceVolumeSource
from hummingbot.core.volume_oracle.sources.bybit_volume_source import BybitVolumeSource
from hummingbot.core.volume_oracle.sources.coinbase_advanced_trade_volume_source import (
    CoinbaseAdvancedTradeVolumeSource,
)
from hummingbot.core.volume_oracle.sources.coindcx_volume_source import CoindcxVolumeSource
from hummingbot.core.volume_oracle.sources.coinswitch_volume_source import CoinswitchVolumeSource
from hummingbot.core.volume_oracle.sources.cube_volume_source import CubeVolumeSource
from hummingbot.core.volume_oracle.sources.derive_volume_source import DeriveVolumeSource
from hummingbot.core.volume_oracle.sources.dexalot_volume_source import DexalotVolumeSource
from hummingbot.core.volume_oracle.sources.gate_io_volume_source import GateIoVolumeSource
from hummingbot.core.volume_oracle.sources.hyperliquid_volume_source import HyperliquidVolumeSource
from hummingbot.core.volume_oracle.sources.kucoin_volume_source import KucoinVolumeSource
from hummingbot.core.volume_oracle.sources.mexc_volume_source import MexcVolumeSource
from hummingbot.core.volume_oracle.sources.okx_volume_source import OkxVolumeSource
from hummingbot.core.volume_oracle.sources.volume_source_base import VolumeSourceBase
from hummingbot.core.volume_oracle.sources.wazirx_volume_source import WazirxVolumeSource
from hummingbot.logger import HummingbotLogger

VOLUME_ORACLE_SOURCES = {
    "ascend_ex": AscendExVolumeSource,
    "binance": BinanceVolumeSource,
    "binance_us": BinanceUSVolumeSource,
    "bybit": BybitVolumeSource,
    "coinbase_advanced_trade": CoinbaseAdvancedTradeVolumeSource,
    "coindcx": CoindcxVolumeSource,
    "coinswitch": CoinswitchVolumeSource,
    "cube": CubeVolumeSource,
    "derive": DeriveVolumeSource,
    "dexalot": DexalotVolumeSource,
    "gate_io": GateIoVolumeSource,
    "hyperliquid": HyperliquidVolumeSource,
    "kucoin": KucoinVolumeSource,
    "mexc": MexcVolumeSource,
    "okx": OkxVolumeSource,
    "wazirx": WazirxVolumeSource,
}


class VolumeOracle:
    _logger: Optional[HummingbotLogger] = None
    _shared_instance: "VolumeOracle" = None

    @classmethod
    def get_instance(cls) -> "VolumeOracle":
        if cls._shared_instance is None:
            cls._shared_instance = VolumeOracle()
        return cls._shared_instance

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, source: Optional[VolumeSourceBase] = None):
        self._source: VolumeSourceBase = source if source is not None else BinanceVolumeSource()

    def __str__(self):
        return f"{self._source.name} volume oracle"

    @property
    def name(self) -> str:
        return "volume_oracle"

    @property
    def source(self) -> VolumeSourceBase:
        return self._source

    @source.setter
    def source(self, new_source: VolumeSourceBase):
        self._source = new_source

    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        return await self._source.get_all_24h_volumes(trading_pairs)

    async def close(self):
        await self._source.close()

    @classmethod
    def source_for_exchange(cls, exchange: str) -> VolumeSourceBase:
        exchange = exchange.lower().strip()
        if exchange not in VOLUME_ORACLE_SOURCES:
            raise ValueError(
                f"Unsupported exchange: {exchange}. Supported: {list(VOLUME_ORACLE_SOURCES.keys())}"
            )
        return VOLUME_ORACLE_SOURCES[exchange]()
