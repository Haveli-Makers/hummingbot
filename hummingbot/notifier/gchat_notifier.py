import asyncio
import logging
from typing import Optional

import aiohttp

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.notifier.notifier_base import NotifierBase


class GChatNotifier(NotifierBase):
    """
    Notifier that posts messages to a Google Chat space through an incoming webhook.

    Messages are queued through NotifierBase (which drains at most one per second) and
    delivered as simple ``{"text": ...}`` payloads. Delivery failures are logged, never
    raised: a broken notification channel must not disturb trading.
    """
    _logger: Optional[HummingbotLogger] = None

    RETRY_DELAY = 2.0
    REQUEST_TIMEOUT = 10.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, webhook_url: str):
        super().__init__()
        if not webhook_url:
            raise ValueError("GChatNotifier requires a non-empty webhook URL.")
        self._webhook_url = webhook_url
        self._session: Optional[aiohttp.ClientSession] = None

    def stop(self):
        super().stop()
        if self._session is not None and not self._session.closed:
            safe_ensure_future(self._session.close())
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_message(self, message: str):
        """
        POST the message to the webhook. Retries once on connection errors, 429 and 5xx.
        """
        payload = {"text": message}
        for attempt in range(2):
            try:
                session = await self._get_session()
                async with session.post(
                    self._webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status < 300:
                        return
                    retriable = resp.status == 429 or resp.status >= 500
                    if not retriable or attempt > 0:
                        self.logger().error(
                            f"Google Chat webhook responded with status {resp.status}; message dropped."
                        )
                        return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt > 0:
                    self.logger().error(f"Failed to send Google Chat message: {e}")
                    return
            await self._sleep(self.RETRY_DELAY)
