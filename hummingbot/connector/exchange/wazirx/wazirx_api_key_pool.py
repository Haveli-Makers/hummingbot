import asyncio
import time
from collections import deque
from typing import Callable, Collection, Deque, Dict, List, Optional, Tuple

from hummingbot.connector.exchange.wazirx.wazirx_auth import WazirxAuth
from hummingbot.core.api_throttler.data_types import RateLimit


class WazirxApiKeyPool:
    """
    Rotates signed requests across the API keys of one WazirX account, keeping each key
    within its per-endpoint limit. A key rests on an endpoint after a 429; all keys pause after a 418.
    """

    def __init__(self,
                 auths: List[WazirxAuth],
                 rate_limits: List[RateLimit],
                 time_provider: Callable[[], float] = time.monotonic):
        if not auths:
            raise ValueError("WazirxApiKeyPool needs at least one API key.")
        self._auths = auths
        self._rate_limits: Dict[str, RateLimit] = {rate_limit.limit_id: rate_limit for rate_limit in rate_limits}
        self._time = time_provider
        self._active: List[int] = list(range(len(auths)))
        self._sent: Dict[Tuple[int, str], Deque[float]] = {}     # (key, limit id) -> send times in the window
        self._rest_until: Dict[Tuple[int, str], float] = {}      # (key, limit id) -> end of its 429 rest
        self._next_index: Dict[str, int] = {}                    # limit id -> key to try first
        self._paused_until = 0.0

    @property
    def size(self) -> int:
        return len(self._auths)

    @property
    def active_indexes(self) -> List[int]:
        return list(self._active)

    def auth(self, index: int) -> WazirxAuth:
        return self._auths[index]

    def label(self, index: int) -> str:
        return f"key {index + 1} of {self.size}"

    def set_active(self, indexes: Collection[int]):
        if not indexes:
            raise ValueError("At least one WazirX API key must stay in rotation.")
        self._active = sorted(indexes)

    def rest(self, index: int, limit_id: str, seconds: float):
        self._rest_until[(index, limit_id)] = self._time() + seconds

    def pause(self, seconds: float):
        self._paused_until = max(self._paused_until, self._time() + seconds)

    async def acquire(self, limit_id: str, exclude: Collection[int] = (), index: Optional[int] = None) -> int:
        """
        Returns the key to use for the next request on ``limit_id``, waiting while every key is at its limit.
        Raises IOError while paused or when every eligible key is resting.
        """
        while True:
            key_index, wait = self._try_acquire(limit_id, exclude, index)
            if key_index is not None:
                return key_index
            await asyncio.sleep(wait)

    def _try_acquire(self,
                     limit_id: str,
                     exclude: Collection[int],
                     index: Optional[int]) -> Tuple[Optional[int], float]:
        now = self._time()
        if now < self._paused_until:
            raise IOError(
                f"WazirX signed requests are paused for {self._paused_until - now:.0f}s after an IP ban (HTTP 418).")

        candidates = [index] if index is not None else [i for i in self._active if i not in exclude]
        if not candidates:
            raise ValueError("No WazirX API key is left to try.")
        ready = [i for i in candidates if self._rest_until.get((i, limit_id), 0) <= now]
        if not ready:
            rest_left = min(self._rest_until[(i, limit_id)] for i in candidates) - now
            raise IOError(f"Every WazirX API key is rate limited on {limit_id}; retry in {rest_left:.1f}s.")

        rate_limit = self._rate_limits.get(limit_id)
        first = self._next_index.get(limit_id, 0)
        ready.sort(key=lambda i: (i - first) % self.size)
        wait: Optional[float] = None
        for key_index in ready:
            if rate_limit is not None:
                sent = self._sent.setdefault((key_index, limit_id), deque())
                while sent and now - sent[0] >= rate_limit.time_interval:
                    sent.popleft()
                if len(sent) >= rate_limit.limit:
                    key_wait = sent[0] + rate_limit.time_interval - now
                    wait = key_wait if wait is None else min(wait, key_wait)
                    continue
                sent.append(now)
            self._next_index[limit_id] = (key_index + 1) % self.size
            return key_index, 0.0
        return None, wait
