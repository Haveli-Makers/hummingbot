import asyncio
import logging
from typing import Dict, List, Optional

from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.option_info import OptionInfo, OptionInfoUpdate, OptionInstrumentInfo
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger


class OptionsTrading:
    """
    Keeps options trading state: the instrument index, live option info and positions.
    Options positions are net (one position per instrument), so positions are keyed by trading pair.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str]):
        self._trading_pairs = trading_pairs
        self._account_positions: Dict[str, Position] = {}
        self._instruments: Dict[str, OptionInstrumentInfo] = {}
        self._option_info: Dict[str, OptionInfo] = {}
        self._option_info_stream = asyncio.Queue()
        self._option_info_updater_task: Optional[asyncio.Task] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    def account_positions(self) -> Dict[str, Position]:
        return self._account_positions

    @property
    def instruments(self) -> Dict[str, OptionInstrumentInfo]:
        return dict(self._instruments)

    @property
    def option_info_stream(self) -> asyncio.Queue:
        return self._option_info_stream

    def set_position(self, trading_pair: str, position: Position):
        self._account_positions[trading_pair] = position

    def remove_position(self, trading_pair: str) -> Optional[Position]:
        return self._account_positions.pop(trading_pair, None)

    def get_position(self, trading_pair: str) -> Optional[Position]:
        return self._account_positions.get(trading_pair)

    def set_instruments(self, instruments: List[OptionInstrumentInfo]):
        self._instruments = {instrument.trading_pair: instrument for instrument in instruments}

    def get_instrument(self, trading_pair: str) -> Optional[OptionInstrumentInfo]:
        return self._instruments.get(trading_pair)

    def initialize_option_info(self, option_info: OptionInfo):
        self._option_info[option_info.trading_pair] = option_info

    def is_option_info_initialized(self) -> bool:
        return all(trading_pair in self._option_info for trading_pair in self._trading_pairs)

    def get_option_info(self, trading_pair: str) -> OptionInfo:
        return self._option_info[trading_pair]

    def start(self):
        self.stop()
        self._option_info_updater_task = safe_ensure_future(self._option_info_updater())

    def stop(self):
        if self._option_info_updater_task is not None:
            self._option_info_updater_task.cancel()
            self._option_info_updater_task = None
        self._option_info.clear()

    async def _option_info_updater(self):
        while True:
            try:
                update: OptionInfoUpdate = await self._option_info_stream.get()
                option_info = self._option_info.get(update.trading_pair)
                if option_info is not None:
                    option_info.update(update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error updating option info.", exc_info=True)
