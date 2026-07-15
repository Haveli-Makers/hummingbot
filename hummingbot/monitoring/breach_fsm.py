import time
from enum import Enum
from typing import Any, Callable, Dict, Optional

from hummingbot.monitoring.alert import Alert, AlertStatus, Severity
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher


class BreachState(Enum):
    OK = "ok"
    PENDING = "pending"
    FIRING = "firing"


class BreachStateMachine:
    """
    Turns a per-sample breach signal into alerts with a grace period:

        OK --breach--> PENDING --(sustained >= grace_period)--> FIRING --recovery--> OK
                          |
                          +-- recovery (blip shorter than grace) --> OK, no alert

    While FIRING, every update re-dispatches the alert; the AlertDispatcher's dedup
    suppresses duplicates and re-notifies on its renotify interval, so periodic
    reminders while the condition stays broken come for free. Recovery from FIRING
    dispatches a resolved (all-clear) alert.
    """

    def __init__(self,
                 source: str,
                 check: str,
                 severity: Severity,
                 title: str,
                 dispatcher: AlertDispatcher,
                 grace_period_sec: float,
                 time_fn: Callable[[], float] = time.time):
        self._source = source
        self._check = check
        self._severity = severity
        self._title = title
        self._dispatcher = dispatcher
        self._grace_period_sec = grace_period_sec
        self._time = time_fn
        self._state = BreachState.OK
        self._breach_started_at: Optional[float] = None

    @property
    def state(self) -> BreachState:
        return self._state

    def update(self, breached: bool, message: str = "", metrics: Optional[Dict[str, Any]] = None):
        """Feed one sample's result for this check into the state machine."""
        now = self._time()
        if breached:
            if self._state is BreachState.OK:
                self._state = BreachState.PENDING
                self._breach_started_at = now
            if (self._state is BreachState.PENDING
                    and now - self._breach_started_at >= self._grace_period_sec):
                self._state = BreachState.FIRING
            if self._state is BreachState.FIRING:
                broken_for = int(now - self._breach_started_at)
                detail = f"Broken for {broken_for}s."
                self._dispatcher.dispatch(Alert(
                    source=self._source,
                    check=self._check,
                    severity=self._severity,
                    title=self._title,
                    message=f"{message} {detail}" if message else detail,
                    metrics=metrics or {},
                    status=AlertStatus.FIRING,
                ))
        else:
            if self._state is BreachState.FIRING:
                recovered_after = int(now - self._breach_started_at)
                self._dispatcher.dispatch(Alert(
                    source=self._source,
                    check=self._check,
                    severity=self._severity,
                    title=self._title,
                    message=f"Recovered after {recovered_after}s.",
                    metrics=metrics or {},
                    status=AlertStatus.RESOLVED,
                ))
            self._state = BreachState.OK
            self._breach_started_at = None
