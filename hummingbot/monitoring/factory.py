import logging
from typing import TYPE_CHECKING, Callable, Dict, Optional, Type

from hummingbot.logger import HummingbotLogger
from hummingbot.monitoring.alert_dispatcher import AlertDispatcher
from hummingbot.monitoring.config import MonitoringConfigBase, PMMSLAMonitorConfig
from hummingbot.monitoring.pmm_sampler import PMMDepthSampler
from hummingbot.monitoring.sampler_base import SLASamplerBase
from hummingbot.monitoring.sla_day_tracker import SLADayTracker
from hummingbot.monitoring.sla_monitor import SLAMonitor
from hummingbot.monitoring.sla_recorder import SLARecorder

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore

_logger: Optional[HummingbotLogger] = None


def logger() -> HummingbotLogger:
    global _logger
    if _logger is None:
        _logger = logging.getLogger(__name__)
    return _logger


# Config model -> sampler factory. Register new strategy monitors here (together with
# their config section in hummingbot.monitoring.config.MONITOR_CONFIG_SECTIONS).
SAMPLER_FACTORIES: Dict[Type[MonitoringConfigBase], Callable[["TradingCore", MonitoringConfigBase], SLASamplerBase]] = {
    PMMSLAMonitorConfig: PMMDepthSampler,
}


def create_sla_monitor(trading_core: "TradingCore",
                       config: MonitoringConfigBase,
                       dispatcher: Optional[AlertDispatcher] = None) -> Optional[SLAMonitor]:
    """
    Assemble the full monitoring stack for a loaded config: the strategy-specific
    sampler plus the shared engine, day tracker and recorder. Returns None when the
    config's connector is not part of the running strategy.
    """
    sampler_factory = SAMPLER_FACTORIES.get(type(config))
    if sampler_factory is None:
        logger().warning(f"No SLA sampler registered for config type {type(config).__name__}.")
        return None
    sampler = sampler_factory(trading_core, config)
    identity = sampler.identity
    if identity.connector_name not in trading_core.markets:
        logger().warning(
            f"SLA monitor configured for '{identity.connector_name}' but that connector is not "
            f"part of this strategy; monitor not started."
        )
        return None
    day_tracker = SLADayTracker(config, identity)
    recorder = SLARecorder(config, identity, dispatcher=dispatcher)
    return SLAMonitor(sampler, config, dispatcher=dispatcher,
                      day_tracker=day_tracker, recorder=recorder)
