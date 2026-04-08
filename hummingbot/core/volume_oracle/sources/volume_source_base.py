import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, Optional

from hummingbot.logger import HummingbotLogger


class VolumeSourceBase(ABC):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self):
        self._exchange = None

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def _ensure_exchange(self):
        if self._exchange is None:
            self._exchange = self._build_exchange()

    def _build_exchange(self):
        raise NotImplementedError

    @abstractmethod
    async def get_24h_volume(self, trading_pair: str) -> Dict[str, Decimal]:
        """
        Fetch 24h volume for a trading pair.

        :param trading_pair: Trading pair in HB format e.g. "BTC-USDT"
        :return: exchange, trading_pair, symbol, base_volume, last_price, and optionally quote_volume
        """
        ...

    async def close(self):
        pass

    @staticmethod
    def _parse_trading_pair(trading_pair: str):
        if "-" not in trading_pair:
            raise ValueError(f"Trading pair must be in BASE-QUOTE format (e.g. BTC-USDT), got: {trading_pair}")
        return trading_pair.upper().split("-", 1)
