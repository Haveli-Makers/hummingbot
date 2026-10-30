import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from hummingbot.core.data_type.option_info import OptionInfo
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class OptionAPIOrderBookDataSource(OrderBookTrackerDataSource, ABC):
    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._option_info_messages_queue_key = "option_info"

    @abstractmethod
    async def get_option_info(self, trading_pair: str) -> OptionInfo:
        """
        Return the live option information (mark price, IV, Greeks) for a single trading pair.
        """
        raise NotImplementedError

    async def listen_for_option_info(self, output: asyncio.Queue):
        """
        Reads the option info events queue and puts the parsed updates in the output queue.
        """
        message_queue = self._message_queue[self._option_info_messages_queue_key]
        while True:
            try:
                option_info_event = await message_queue.get()
                await self._parse_option_info_message(raw_message=option_info_event, message_queue=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public option info updates from exchange")

    @abstractmethod
    async def _parse_option_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError

    def _get_messages_queue_keys(self) -> List[str]:
        return [
            self._snapshot_messages_queue_key,
            self._diff_messages_queue_key,
            self._trade_messages_queue_key,
            self._option_info_messages_queue_key,
        ]
