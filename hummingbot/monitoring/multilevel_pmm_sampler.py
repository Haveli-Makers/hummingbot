from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.alert import Severity
from hummingbot.monitoring.config import MultiLevelPMMSLAMonitorConfig
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample, SLASamplerBase
from hummingbot.monitoring.sla_sampler import ONE_SIDE_MISSING, ORDER_BOOK_STALE, OpenOrder, evaluate_sample

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore


def tier_check(tier_name: str) -> str:
    return f"{tier_name}_depth_below_min"


class MultiLevelPMMSampler(SLASamplerBase):
    """
    Multi-level market-making SLA sampler. Each tier requires a minimum *cumulative*
    quote depth per side within its own spread band; because the bands are nested,
    orders in an inner band count toward every outer tier. Each tier carries its own
    daily uptime target (reported via ``SLASample.slo_results``) and raises its own
    tier-scoped alert.
    """

    def __init__(self, trading_core: "TradingCore", config: MultiLevelPMMSLAMonitorConfig):
        super().__init__(MonitorIdentity(
            monitor_type="pmm_ml",
            connector_name=config.connector_name,
            trading_pair=config.trading_pair,
        ))
        self._trading_core = trading_core
        self._config = config

    @property
    def check_alerts(self) -> Dict[str, Tuple[Severity, str]]:
        alerts = {
            ONE_SIDE_MISSING: (Severity.CRITICAL, "One side has no standing orders"),
            ORDER_BOOK_STALE: (Severity.WARNING, "Order book stale or connector disconnected"),
        }
        for tier in self._config.tiers:
            alerts[tier_check(tier.name)] = (
                Severity.WARNING,
                f"Tier '{tier.name}' depth below the SLA minimum "
                f"({tier.min_depth_quote} within {tier.spread_band_pct}%)",
            )
        return alerts

    def config_summary(self) -> str:
        tiers = ", ".join(
            f"{tier.name}: {tier.min_depth_quote}@{tier.spread_band_pct}%->{tier.required_uptime_pct}%"
            for tier in self._config.tiers
        )
        return f"tiers [{tiers}]"

    def take_sample(self) -> SLASample:
        connector = self._trading_core.markets.get(self._config.connector_name)
        connected = (connector is not None
                     and getattr(connector, "network_status", NetworkStatus.CONNECTED) is NetworkStatus.CONNECTED)
        mid: Optional[Decimal] = None
        orders = []
        if connected:
            try:
                mid = connector.get_price_by_type(self._config.trading_pair, PriceType.MidPrice)
            except Exception:
                mid = None  # evaluated as orderbook_stale
            orders = [
                OpenOrder(
                    is_buy=order.trade_type.name == "BUY",
                    price=order.price,
                    amount_remaining=order.amount - order.executed_amount_base,
                )
                for order in connector.in_flight_orders.values()
                if order.trading_pair == self._config.trading_pair and order.is_open
            ]

        if mid is None or mid.is_nan() or mid <= 0:
            return SLASample(
                in_spec=False,
                reasons=[ORDER_BOOK_STALE],
                metrics={"mid": "n/a"},
                slo_results={tier.name: False for tier in self._config.tiers},
            )

        reasons = []
        metrics = {"mid": str(mid)}
        slo_results: Dict[str, bool] = {}
        side_missing = not any(o.is_buy for o in orders) or not any(not o.is_buy for o in orders)
        if side_missing:
            reasons.append(ONE_SIDE_MISSING)
        for tier in self._config.tiers:
            result = evaluate_sample(
                mid=mid,
                orders=orders,
                band_pct=tier.spread_band_pct,
                min_depth_quote=tier.min_depth_quote,
            )
            slo_results[tier.name] = result.in_spec
            metrics[f"{tier.name}_bid"] = f"{result.bid_depth:.0f}"
            metrics[f"{tier.name}_ask"] = f"{result.ask_depth:.0f}"
            # A missing side already raises the critical alert; tier alerts cover the
            # cases where orders exist but a tier's depth requirement is not met.
            if not result.in_spec and not side_missing:
                reasons.append(tier_check(tier.name))

        return SLASample(
            in_spec=len(reasons) == 0,
            reasons=reasons,
            metrics=metrics,
            slo_results=slo_results,
        )

    def describe(self, sample: SLASample) -> str:
        parts = []
        for tier in self._config.tiers:
            bid = sample.metrics.get(f"{tier.name}_bid", "n/a")
            ask = sample.metrics.get(f"{tier.name}_ask", "n/a")
            parts.append(f"{tier.name} bid/ask {bid}/{ask} (need {tier.min_depth_quote})")
        return ", ".join(parts) + f", mid {sample.metrics.get('mid', 'n/a')}"

    def describe_check(self, check: str, sample: SLASample) -> str:
        for tier in self._config.tiers:
            if check == tier_check(tier.name):
                bid = sample.metrics.get(f"{tier.name}_bid", "n/a")
                ask = sample.metrics.get(f"{tier.name}_ask", "n/a")
                return (f"{tier.name}: bid {bid} / ask {ask}, need {tier.min_depth_quote} "
                        f"within {tier.spread_band_pct}% of mid {sample.metrics.get('mid', 'n/a')}")
        return self.describe(sample)
