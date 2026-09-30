import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


class Severity(Enum):
    INFO = 1
    WARNING = 2
    CRITICAL = 3


class AlertStatus(Enum):
    FIRING = "firing"
    RESOLVED = "resolved"


@dataclass
class Alert:
    """
    A structured monitoring event, routed to notification channels by the AlertDispatcher.

    :param source: the component that raised the alert, e.g. "pmm.wazirx.USDT-INR".
    :param check: the invariant that broke, e.g. "depth_below_min".
    :param severity: how serious the event is; the dispatcher can filter on this.
    :param title: short human-readable headline.
    :param message: longer description with context.
    :param metrics: key figures at the time of the alert (depths, mid price, ...).
    :param status: FIRING when the condition is broken, RESOLVED when it recovered.
    """
    source: str
    check: str
    severity: Severity
    title: str
    message: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    status: AlertStatus = AlertStatus.FIRING
    timestamp: float = field(default_factory=time.time)
