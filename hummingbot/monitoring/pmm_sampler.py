from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from hummingbot.core.data_type.common import PriceType
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.monitoring.alert import Severity
from hummingbot.monitoring.config import PMMSLAMonitorConfig
from hummingbot.monitoring.sampler_base import MonitorIdentity, SLASample, SLASamplerBase
from hummingbot.monitoring.sla_sampler import (
    DEPTH_BELOW_MIN,
    ONE_SIDE_MISSING,
    ORDER_BOOK_STALE,
    SPREAD_TOO_WIDE,
    OpenOrder,
    evaluate_sample,
)

if TYPE_CHECKING:
    from hummingbot.core.trading_core import TradingCore

CHECK_ALERTS = {
    ONE_SIDE_MISSING: (Severity.CRITICAL, "One side has no standing orders"),
    SPREAD_TOO_WIDE: (Severity.WARNING, "Standing orders are outside the spread band"),
    DEPTH_BELOW_MIN: (Severity.WARNING, "Depth below the SLA minimum"),
    ORDER_BOOK_STALE: (Severity.WARNING, "Order book stale or connector disconnected"),
}


class PMMDepthSampler(SLASamplerBase):
    """
    Market-making SLA sampler: both sides must hold open orders within
    ``spread_band_pct`` of the live mid with at least ``min_depth_quote`` of
    cumulative value each.
    """

    def __init__(self, trading_core: "TradingCore", config: PMMSLAMonitorConfig):
        super().__init__(MonitorIdentity(
            monitor_type="pmm",
            connector_name=config.connector_name,
            trading_pair=config.trading_pair,
        ))
        self._trading_core = trading_core
        self._config = config

    @property
    def check_alerts(self) -> Dict[str, Tuple[Severity, str]]:
        return CHECK_ALERTS

    def config_summary(self) -> str:
        return (f"band {self._config.spread_band_pct}%, "
                f"min depth {self._config.min_depth_quote} quote")

    def take_sample(self) -> SLASample:
        connector = self._trading_core.markets.get(self._config.connector_name)
        mid: Optional[Decimal] = None
        orders = []
        # A disconnected connector serves a frozen local order book: the last-known mid
        # looks valid but proves nothing. Score those seconds as orderbook_stale.
        connected = (connector is not None
                     and getattr(connector, "network_status", NetworkStatus.CONNECTED) is NetworkStatus.CONNECTED)
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
        result = evaluate_sample(
            mid=mid,
            orders=orders,
            band_pct=self._config.spread_band_pct,
            min_depth_quote=self._config.min_depth_quote,
        )
        return SLASample(
            in_spec=result.in_spec,
            reasons=result.reasons,
            metrics={
                "bid_depth": f"{result.bid_depth:.0f}",
                "ask_depth": f"{result.ask_depth:.0f}",
                "mid": str(result.mid_price),
            },
            data_available=ORDER_BOOK_STALE not in result.reasons,
        )

    def describe(self, sample: SLASample) -> str:
        return (f"bid depth {sample.metrics['bid_depth']}, ask depth {sample.metrics['ask_depth']}, "
                f"need {self._config.min_depth_quote} per side (mid {sample.metrics['mid']})")
