from decimal import Decimal
from pathlib import Path
from typing import Optional, Union

import yaml
from pydantic import BaseModel, Field

from hummingbot.client.settings import CONF_DIR_PATH

DEFAULT_MONITORING_CONFIG_PATH = CONF_DIR_PATH / "monitoring.yml"


class PMMSLAMonitorConfig(BaseModel):
    """
    Configuration for the PMM SLA monitor, loaded from conf/monitoring.yml.
    All thresholds are tunable; nothing is hard-coded in the monitor.
    """
    enabled: bool = True
    connector_name: str
    trading_pair: str
    spread_band_pct: Decimal = Field(default=Decimal("1.5"), gt=0)
    min_depth_quote: Decimal = Field(default=Decimal("20000"), gt=0)
    required_uptime_pct: Decimal = Field(default=Decimal("96"), gt=0, le=100)
    sample_interval_sec: float = Field(default=1.0, gt=0)
    grace_period_sec: float = Field(default=2.0, ge=0)
    alert_warmup_sec: float = Field(default=10.0, ge=0)
    day_reset_timezone: str = "Asia/Kolkata"
    heartbeat_log_interval_sec: float = Field(default=300.0, gt=0)


def load_monitoring_config(
        path: Union[str, Path] = DEFAULT_MONITORING_CONFIG_PATH) -> Optional[PMMSLAMonitorConfig]:
    """
    Load the monitoring config. Returns None (monitoring off) when the file does not
    exist, has no ``pmm_sla_monitor`` section, or the section sets ``enabled: false``.
    Invalid values raise, so a misconfiguration is loud instead of silently ignored.
    """
    config_path = Path(path)
    if not config_path.exists():
        return None
    data = yaml.safe_load(config_path.read_text()) or {}
    section = data.get("pmm_sla_monitor")
    if not section:
        return None
    config = PMMSLAMonitorConfig(**section)
    return config if config.enabled else None
