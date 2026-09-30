import asyncio
import logging
import threading
from typing import Optional

from hummingbot.monitoring.alert import Alert, Severity
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher

# Log records from the alerting pipeline itself are never forwarded, so a failing
# notifier or dispatcher can not feed its own errors back into the pipeline.
EXCLUDED_LOGGER_PREFIXES = ("hummingbot.monitoring", "hummingbot.notifier")

TITLE_MAX_LEN = 120
MESSAGE_MAX_LEN = 600


class GChatLogHandler(logging.Handler):
    """
    Layer A of the monitoring framework: forwards serious log records (ERROR and above)
    to Google Chat through the AlertDispatcher, piggybacking on the errors the bot
    already produces (crashes, disconnections, order failures, insufficient balance).

    The dispatcher's dedup and rate limit keep log storms (e.g. a reconnect loop) down
    to one notification per logger per renotify interval.
    """

    def __init__(self,
                 dispatcher: AlertDispatcher,
                 source: str = "hummingbot",
                 level: int = logging.ERROR):
        super().__init__(level=level)
        self.name = self.__class__.__name__
        self._dispatcher = dispatcher
        self._source = source
        try:
            self._ev_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            self._ev_loop = None

    def emit(self, record: logging.LogRecord):
        try:
            if record.name.startswith(EXCLUDED_LOGGER_PREFIXES):
                return
            if threading.current_thread() is not threading.main_thread() and self._ev_loop is not None:
                self._ev_loop.call_soon_threadsafe(self.emit, record)
                return
            self._dispatcher.dispatch(self._to_alert(record))
        except Exception:
            self.handleError(record)

    def _to_alert(self, record: logging.LogRecord) -> Alert:
        text = record.getMessage()
        if record.exc_info is not None and record.exc_info is not False:
            exc_value = record.exc_info[1] if isinstance(record.exc_info, tuple) else None
            if exc_value is not None:
                text = f"{text}\n{type(exc_value).__name__}: {exc_value}"
        first_line = text.split("\n", 1)[0].strip()
        severity = (Severity.CRITICAL
                    if record.levelno >= logging.CRITICAL or record.exc_info
                    else Severity.WARNING)
        return Alert(
            source=f"{self._source}.log",
            check=f"log.{record.name}",
            severity=severity,
            title=first_line[:TITLE_MAX_LEN] or record.levelname,
            message=text[:MESSAGE_MAX_LEN],
            metrics={"logger": record.name, "level": record.levelname},
        )
