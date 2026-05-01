import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Optional

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
    async def get_all_24h_volumes(self, trading_pairs: Optional[List[str]] = None) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch 24h volumes for trading pairs on the source exchange.

        :param trading_pairs: Optional list of trading pairs in HB format (e.g. ["BTC-USDT"]).
                              If None or empty, fetches volumes for all available trading pairs.
        :return: a symbol-keyed mapping containing exchange, symbol, base_volume, last_price,
                 and optionally quote_volume for each trading pair.
        """
        ...

    async def normalize_symbol(self, raw_symbol: str) -> Optional[str]:
        """
        Convert an exchange-specific symbol to Hummingbot BASE-QUOTE format.

        :param raw_symbol: Symbol in the exchange's native format.
        :return: Normalised HB trading pair string (e.g. "BTC-USDT"), or None.
        """
        return await self._exchange.normalize_trading_pair(raw_symbol)

    async def close(self):
        pass

    @staticmethod
    def _parse_trading_pair(trading_pair: str):
        if "-" not in trading_pair:
            raise ValueError(f"Trading pair must be in BASE-QUOTE format (e.g. BTC-USDT), got: {trading_pair}")
        return trading_pair.upper().split("-", 1)
