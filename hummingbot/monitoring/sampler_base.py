from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from hummingbot.monitoring.alert import Severity


@dataclass(frozen=True)
class MonitorIdentity:
    """
    Identifies one monitored strategy instance. Drives alert sources, log banners and
    the file names used by the day tracker and recorder.
    """
    monitor_type: str      # short strategy tag, e.g. "pmm"
    connector_name: str
    trading_pair: str

    @property
    def source(self) -> str:
        return f"{self.monitor_type}.{self.connector_name}.{self.trading_pair}"

    @property
    def instance_id(self) -> str:
        return f"{self.monitor_type}_{self.connector_name}_{self.trading_pair.replace('/', '-')}"


@dataclass
class SLASample:
    """
    One evaluation of a strategy's SLA condition, produced every sample interval.

    ``slo_results`` carries named sub-objectives (e.g. depth tiers) when the monitor
    tracks more than one uptime target; each name accrues its own daily uptime figure.
    Single-objective monitors leave it None.

    ``data_available`` is False when the sampler could not actually measure the SLA
    (e.g. the connector is disconnected and its order book is frozen). Such a sample
    still counts as out of spec, but the engine will not read it as recovery for other
    checks — "can't measure" must never resolve an in-progress breach.

    ``held_checks`` lists checks that are still failing but intentionally not surfaced
    this tick because a more important condition masks them (e.g. tier depth checks
    while a whole side is missing). The engine freezes these — they neither fire a new
    alert nor resolve an in-progress one; "masked" must never read as "recovered".
    """
    in_spec: bool
    reasons: List[str] = field(default_factory=list)   # empty when in_spec
    metrics: Dict[str, str] = field(default_factory=dict)
    slo_results: Optional[Dict[str, bool]] = None
    data_available: bool = True
    held_checks: Optional[List[str]] = None


class SLASamplerBase(ABC):
    """
    The strategy-specific half of the SLA monitor.

    A sampler owns *what* is measured; the generic SLAMonitor owns everything else
    (the sampling loop, grace-period alerting, daily accounting, persistence and
    recording). Adding monitoring for a new strategy means implementing this class,
    a config model, and registering both in the monitoring factory.

    Samplers must be read-only: never place, cancel or modify anything.
    """

    def __init__(self, identity: MonitorIdentity):
        self.identity = identity

    @property
    @abstractmethod
    def check_alerts(self) -> Dict[str, Tuple[Severity, str]]:
        """Maps each reason code the sampler can emit to (severity, alert headline)."""
        ...

    @abstractmethod
    def take_sample(self) -> SLASample:
        """Evaluate the strategy's SLA condition right now."""
        ...

    @abstractmethod
    def config_summary(self) -> str:
        """One-line description of the thresholds, for the startup log banner."""
        ...

    def describe(self, sample: SLASample) -> str:
        """Human-readable detail line for logs and alert messages."""
        return ", ".join(f"{key} {value}" for key, value in sample.metrics.items())

    def describe_check(self, check: str, sample: SLASample) -> str:
        """
        Detail line for one specific check's alert. Defaults to the full describe();
        override to show only the figures relevant to that check (e.g. one tier).
        """
        return self.describe(sample)
