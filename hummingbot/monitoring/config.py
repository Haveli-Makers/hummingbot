from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Type, Union

import yaml
from pydantic import BaseModel, Field, field_validator

from hummingbot.client.settings import CONF_DIR_PATH

DEFAULT_MONITORING_CONFIG_PATH = CONF_DIR_PATH / "monitoring.yml"


class MonitoringConfigBase(BaseModel):
    """
    Strategy-agnostic monitoring settings, shared by every SLA monitor. Strategy
    configs extend this with their own thresholds (see PMMSLAMonitorConfig).
    """
    enabled: bool = True
    required_uptime_pct: Decimal = Field(default=Decimal("96"), gt=0, le=100)
    sample_interval_sec: float = Field(default=1.0, gt=0)
    grace_period_sec: float = Field(default=2.0, ge=0)
    alert_warmup_sec: float = Field(default=10.0, ge=0)
    day_reset_timezone: str = "Asia/Kolkata"
    heartbeat_log_interval_sec: float = Field(default=300.0, gt=0)

    def slo_targets(self) -> Dict[str, Decimal]:
        """
        Per-sub-objective daily uptime targets (SLO name -> required %). Monitors with a
        single overall target return an empty dict.
        """
        return {}


class PMMSLAMonitorConfig(MonitoringConfigBase):
    """Market-making SLA: standing orders within a spread band with minimum depth."""
    connector_name: str
    trading_pair: str
    spread_band_pct: Decimal = Field(default=Decimal("1.5"), gt=0)
    min_depth_quote: Decimal = Field(default=Decimal("20000"), gt=0)


class SLATierConfig(BaseModel):
    """One tier of a multi-level market-making SLA (cumulative depth within a band)."""
    name: str = Field(min_length=1)
    spread_band_pct: Decimal = Field(gt=0)
    min_depth_quote: Decimal = Field(gt=0)
    required_uptime_pct: Decimal = Field(gt=0, le=100)


class MultiLevelPMMSLAMonitorConfig(MonitoringConfigBase):
    """
    Multi-level market-making SLA: several nested depth tiers, each with its own
    spread band, minimum cumulative depth per side, and daily uptime target.
    """
    connector_name: str
    trading_pair: str
    tiers: List[SLATierConfig] = Field(min_length=1)

    @field_validator("tiers")
    @classmethod
    def validate_tiers(cls, tiers: List[SLATierConfig]) -> List[SLATierConfig]:
        names = [tier.name for tier in tiers]
        if len(names) != len(set(names)):
            raise ValueError("Tier names must be unique.")
        return tiers

    def slo_targets(self) -> Dict[str, Decimal]:
        return {tier.name: tier.required_uptime_pct for tier in self.tiers}


# conf/monitoring.yml section name -> config model. Register new strategy monitors here.
MONITOR_CONFIG_SECTIONS: Dict[str, Type[MonitoringConfigBase]] = {
    "pmm_sla_monitor": PMMSLAMonitorConfig,
    "multilevel_pmm_sla_monitor": MultiLevelPMMSLAMonitorConfig,
}


def load_monitoring_config(
        path: Union[str, Path] = DEFAULT_MONITORING_CONFIG_PATH) -> Optional[MonitoringConfigBase]:
    """
    Load the monitoring config. Returns None (monitoring off) when the file does not
    exist, has no known monitor section, or the section sets ``enabled: false``.
    Invalid values raise, so a misconfiguration is loud instead of silently ignored.
    """
    config_path = Path(path)
    if not config_path.exists():
        return None
    data = yaml.safe_load(config_path.read_text()) or {}
    for section_name, config_cls in MONITOR_CONFIG_SECTIONS.items():
        section = data.get(section_name)
        if section:
            config = config_cls(**section)
            return config if config.enabled else None
    return None
