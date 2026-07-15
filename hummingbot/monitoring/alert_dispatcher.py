import logging
import time
from typing import Callable, Dict, List, Optional, Tuple

from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.alert import Alert, AlertStatus, Severity
from hummingbot.notifier.notifier_base import NotifierBase

SEVERITY_EMOJI = {
    Severity.INFO: "🔵",
    Severity.WARNING: "🟠",
    Severity.CRITICAL: "🔴",
}


class AlertDispatcher:
    """
    Routes Alerts to notification channels with noise control:

    - drops firing alerts below ``min_severity``
    - dedupes repeated firing alerts per (source, check), re-notifying only after
      ``renotify_interval`` seconds while the condition stays broken
    - enforces a global token-bucket rate limit across all alerts
    - sends an all-clear message when a previously notified alert resolves

    Dispatching never raises: a failure here must not disturb trading.
    """
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 notifiers: Optional[List[NotifierBase]] = None,
                 min_severity: Severity = Severity.WARNING,
                 rate_limit_per_min: int = 20,
                 renotify_interval: float = 300.0,
                 time_fn: Callable[[], float] = time.time):
        self._notifiers: List[NotifierBase] = list(notifiers or [])
        self._min_severity = min_severity
        self._renotify_interval = renotify_interval
        self._time = time_fn
        # Token bucket for the global rate limit
        self._max_tokens = float(rate_limit_per_min)
        self._tokens = float(rate_limit_per_min)
        self._tokens_updated = self._time()
        # (source, check) -> timestamp of the last delivered firing notification
        self._active_alerts: Dict[Tuple[str, str], float] = {}

    def add_notifier(self, notifier: NotifierBase):
        self._notifiers.append(notifier)

    def dispatch(self, alert: Alert) -> bool:
        """
        Route one alert through the noise-control pipeline.

        :return: True if a notification was actually delivered to the channels.
        """
        try:
            if alert.status is AlertStatus.RESOLVED:
                return self._dispatch_resolved(alert)
            return self._dispatch_firing(alert)
        except Exception as e:
            self.logger().error(f"AlertDispatcher failed to dispatch alert: {e}", exc_info=True)
            return False

    def _dispatch_firing(self, alert: Alert) -> bool:
        if alert.severity.value < self._min_severity.value:
            return False
        key = (alert.source, alert.check)
        now = self._time()
        last_sent = self._active_alerts.get(key)
        if last_sent is not None and now - last_sent < self._renotify_interval:
            return False
        if not self._take_token():
            self.logger().warning(f"Alert rate limit reached; dropping alert {key}.")
            return False
        self._active_alerts[key] = now
        self._deliver(self._format(alert))
        return True

    def _dispatch_resolved(self, alert: Alert) -> bool:
        key = (alert.source, alert.check)
        if key not in self._active_alerts:
            # The firing side was never notified, so an all-clear would only confuse.
            return False
        del self._active_alerts[key]
        if not self._take_token():
            return False
        self._deliver(self._format(alert))
        return True

    def _take_token(self) -> bool:
        now = self._time()
        elapsed = max(0.0, now - self._tokens_updated)
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._max_tokens / 60.0)
        self._tokens_updated = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def _format(self, alert: Alert) -> str:
        prefix = "✅" if alert.status is AlertStatus.RESOLVED else SEVERITY_EMOJI[alert.severity]
        lines = [f"{prefix} [{alert.source}] {alert.title}"]
        if alert.message:
            lines.append(alert.message)
        if alert.metrics:
            lines.append(" | ".join(f"{k}={v}" for k, v in alert.metrics.items()))
        return "\n".join(lines)

    def _deliver(self, text: str):
        for notifier in self._notifiers:
            notifier.add_message_to_queue(text)
