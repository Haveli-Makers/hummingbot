import asyncio
import copy
import logging
import warnings
from collections import defaultdict
from typing import Callable, Dict, List, Optional

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import PositionMode, PositionSide
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger


class PerpetualTrading:
    """Keeps perpetual trading state."""

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str]):
        self._account_positions: Dict[str, Position] = {}
        self._position_mode: PositionMode = PositionMode.ONEWAY
        self._leverage: Dict[str, int] = defaultdict(lambda: 1)
        self._trading_pairs = trading_pairs

        self._funding_info: Dict[str, FundingInfo] = {}
        self._funding_payment_span: List[int] = [0, 0]
        self._funding_info_stream = asyncio.Queue()

        self._funding_info_updater_task: Optional[asyncio.Task] = None

        self._position_update_listeners: List[Callable[[str], None]] = []
        self._funding_info_update_listeners: List[Callable[[str], None]] = []

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    def account_positions(self) -> Dict[str, Position]:
        """
        Returns a dictionary of current active open positions
        """
        return self._account_positions

    @property
    def funding_info(self) -> Dict[str, FundingInfo]:
        """
        The funding information per trading pair.
        """
        return copy.deepcopy(self._funding_info)

    @property
    def funding_info_stream(self) -> asyncio.Queue:
        """
        The stream to which to supply funding info updates to be processed by the class.
        """
        return self._funding_info_stream

    def set_position(self, pos_key: str, position: Position):
        self.logger().debug(f"Setting position {pos_key} to {Position}")
        self._account_positions[pos_key] = position
        if self._position_update_listeners:
            self._notify_listeners(self._position_update_listeners, position.trading_pair, "position update")

    def remove_position(self, post_key: str) -> Optional[Position]:
        position = self._account_positions.pop(post_key, None)
        if position is not None and self._position_update_listeners:
            self._notify_listeners(self._position_update_listeners, position.trading_pair, "position update")
        return position

    def add_position_update_listener(self, listener: Callable[[str], None]):
        """
        Call `listener(trading_pair)` whenever a position is set or removed.

        Connectors update positions silently, so this is the hook for consumers that need them as they
        change, such as the MQTT account data publisher. Listeners run inline, so keep them cheap.
        """
        if listener not in self._position_update_listeners:
            self._position_update_listeners.append(listener)

    def remove_position_update_listener(self, listener: Callable[[str], None]):
        if listener in self._position_update_listeners:
            self._position_update_listeners.remove(listener)

    def add_funding_info_update_listener(self, listener: Callable[[str], None]):
        """
        Call `listener(trading_pair)` whenever a pair's funding info is initialised or updated.
        Same contract as add_position_update_listener.
        """
        if listener not in self._funding_info_update_listeners:
            self._funding_info_update_listeners.append(listener)

    def remove_funding_info_update_listener(self, listener: Callable[[str], None]):
        if listener in self._funding_info_update_listeners:
            self._funding_info_update_listeners.remove(listener)

    def _notify_listeners(self, listeners: List[Callable[[str], None]], trading_pair: str, what: str):
        # A failing listener must never break position or funding bookkeeping.
        for listener in list(listeners):
            try:
                listener(trading_pair)
            except Exception:
                self.logger().error(f"Error in {what} listener for {trading_pair}.", exc_info=True)

    def initialize_funding_info(self, funding_info: FundingInfo):
        """
        Initializes a single trading pair funding information.
        """
        self._funding_info[funding_info.trading_pair] = funding_info
        if self._funding_info_update_listeners:
            self._notify_listeners(
                self._funding_info_update_listeners, funding_info.trading_pair, "funding info update")

    def is_funding_info_initialized(self) -> bool:
        """
        Checks if there is funding information for all trading pairs.
        """
        return all(
            trading_pair in self._funding_info
            for trading_pair in self._trading_pairs
        )

    def start(self):
        """
        Starts the async task that updates the funding information from the updates stream queue.
        """
        self.stop()
        self._funding_info_updater_task = safe_ensure_future(
            self._funding_info_updater()
        )

    def stop(self):
        """
        Stops the funding info updating async task.
        """
        if self._funding_info_updater_task is not None:
            self._funding_info_updater_task.cancel()
            self._funding_info_updater_task = None
        self._funding_info.clear()

    def position_key(self, trading_pair: str, side: PositionSide = None, mode: PositionMode = None) -> str:
        """
        Returns a key to a position in account_positions. On OneWay position mode this is the trading pair.
        On Hedge position mode this is a combination of trading pair and position side
        :param trading_pair: The market trading pair
        :param side: The position side (long or short)
        :return: A key to the position in account_positions dictionary
        """
        pos_key = ""
        if mode is not None:
            pos_key = f"{trading_pair}{side.name}" if mode == PositionMode.HEDGE else trading_pair
        else:
            pos_key = f"{trading_pair}{side.name}" if self._position_mode == PositionMode.HEDGE else trading_pair
        return pos_key

    def get_position(self, trading_pair: str, side: PositionSide = None) -> Optional[Position]:
        """
        Returns an active position if exists, otherwise returns None
        :param trading_pair: The market trading pair
        :param side: The position side (long or short)
        :return: A position from account_positions or None
        """
        return self.account_positions.get(self.position_key(trading_pair, side), None)

    @property
    def funding_payment_span(self) -> List[int]:
        """
        Time span(in seconds) before and after funding period when exchanges consider active positions eligible for
        funding payment.
        :return: a list of seconds (before and after)
        """
        return self._funding_payment_span

    @property
    def position_mode(self) -> PositionMode:
        return self._position_mode

    def set_position_mode(self, value: PositionMode):
        """
        Sets position mode for perpetual trading, a child class might need to override this to set position mode on
        the exchange
        :param value: the position mode
        """
        self._position_mode = value

    def get_leverage(self, trading_pair: str) -> int:
        """
        Gets leverage level of a particular market
        :param trading_pair: the market trading pair
        :return: leverage level
        """
        return self._leverage[trading_pair]

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        """
        Sets leverage level, e.g. 2x, 10x, etc..
        A child class may need to override this to set leverage level on the exchange
        :param trading_pair: the market trading pair
        :param leverage: leverage to be used
        """
        self._leverage[trading_pair] = leverage

    def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Returns funding information
        :param trading_pair: the market trading pair
        :return: funding info
        """
        return self._funding_info[trading_pair]

    async def _funding_info_updater(self):
        while True:
            try:
                funding_info_message: FundingInfoUpdate = await self._funding_info_stream.get()
                trading_pair = funding_info_message.trading_pair
                funding_info = self._funding_info[trading_pair]
                funding_info.update(funding_info_message)
                if self._funding_info_update_listeners:
                    self._notify_listeners(self._funding_info_update_listeners, trading_pair, "funding info update")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error updating funding info.", exc_info=True)

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        warnings.warn(
            "This method is replaced by PerpetualDerivativePyBase.get_buy_collateral_token, and will be removed"
            " once all perpetual connectors are updated to the latest standards.",
            DeprecationWarning,
            stacklevel=2,
        )
        _, quote = split_hb_trading_pair(trading_pair)
        return quote

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        warnings.warn(
            "This method is replaced by PerpetualDerivativePyBase.get_sell_collateral_token, and will be removed"
            " once all perpetual connectors are updated to the latest standards.",
            DeprecationWarning,
            stacklevel=2,
        )
        _, quote = split_hb_trading_pair(trading_pair)
        return quote
