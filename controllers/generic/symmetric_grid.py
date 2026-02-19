from decimal import Decimal
from typing import List, Optional

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.pmm_executor.data_types import PMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class SymmetricGridConfig(ControllerConfigBase):
    """
    Configuration for the Symmetric Grid controller.

    Places symmetric buy and sell orders at specified percentage distances from the fair price.
    When any order fills, the executor automatically re-places it at the same level.

    Example with USDT-INR on CoinDCX:
        fair_price = mid_price (or set reference_price)
        spreads = [0.01, 0.02, 0.03, 0.05]  (1%, 2%, 3%, 5%)
        amounts_quote = [200, 800, 200, 800]

        If mid_price = 85.0:
          Sell orders at: 85.85, 86.70, 87.55, 89.25
          Buy orders at: 84.15, 83.30, 82.45, 80.75
    """
    controller_type: str = "generic"
    controller_name: str = "symmetric_grid"
    candles_config: List[CandlesConfig] = []

    # Account configuration
    leverage: int = 1

    # Connector & pair
    connector_name: str = Field(
        default="coindcx",
        json_schema_extra={"prompt_on_new": True, "prompt": "Enter the connector name (e.g., coindcx):"}
    )
    trading_pair: str = Field(
        default="USDT-INR",
        json_schema_extra={"prompt_on_new": True, "prompt": "Enter the trading pair (e.g., USDT-INR):"}
    )

    # Total budget (informational, actual per-level amounts are in amounts_quote)
    total_amount_quote: Decimal = Field(
        default=Decimal("240"),
        json_schema_extra={"prompt_on_new": True, "is_updatable": True}
    )

    # Optional fixed reference price; None = use current mid price
    reference_price: Optional[Decimal] = Field(
        default=None,
        json_schema_extra={"prompt_on_new": True, "is_updatable": True,
                           "prompt": "Enter a fixed reference price, or leave blank for mid price:"}
    )

    # Spread percentages from fair price for each level
    spreads: List[Decimal] = Field(
        default=[Decimal("0.01"), Decimal("0.02"), Decimal("0.03"), Decimal("0.05")],
        json_schema_extra={"prompt_on_new": True, "is_updatable": True,
                           "prompt": "Enter comma-separated spread percentages (e.g., 0.01,0.02,0.03,0.05):"}
    )

    # Quote amounts per level  (must match length of spreads)
    amounts_quote: List[Decimal] = Field(
        default=[Decimal("200"), Decimal("800"), Decimal("200"), Decimal("800")],
        json_schema_extra={"prompt_on_new": True, "is_updatable": True,
                           "prompt": "Enter comma-separated amounts in quote per level (e.g., 200,800,200,800):"}
    )

    # Order type settings
    open_order_type: OrderType = Field(default=OrderType.LIMIT_MAKER)
    take_profit_order_type: OrderType = Field(default=OrderType.LIMIT)

    # When true, each level's take-profit spread equals its own spread percentage
    take_profit_matches_spread: bool = Field(default=True, json_schema_extra={"is_updatable": True})

    # Fallback take profit if take_profit_matches_spread is False
    global_take_profit: Optional[Decimal] = Field(default=Decimal("0.01"), json_schema_extra={"is_updatable": True})

    # Risk management
    stop_loss: Optional[Decimal] = Field(default=None, json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, json_schema_extra={"is_updatable": True})

    @field_validator('spreads', mode='before')
    @classmethod
    def parse_spreads(cls, v):
        if isinstance(v, str):
            return [Decimal(x.strip()) for x in v.split(',')]
        return [Decimal(str(x)) for x in v]

    @field_validator('amounts_quote', mode='before')
    @classmethod
    def parse_amounts(cls, v):
        if isinstance(v, str):
            return [Decimal(x.strip()) for x in v.split(',')]
        return [Decimal(str(x)) for x in v]

    @field_validator('open_order_type', 'take_profit_order_type', mode="before")
    @classmethod
    def validate_order_type(cls, v) -> OrderType:
        if isinstance(v, OrderType):
            return v
        if isinstance(v, int):
            return OrderType(v)
        if isinstance(v, str):
            if v.upper() in OrderType.__members__:
                return OrderType[v.upper()]
        raise ValueError(f"Invalid order type: {v}")

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class SymmetricGrid(ControllerBase):
    """
    Symmetric Grid controller that uses PMMExecutor to place symmetric buy and sell
    orders around a fair price. Orders that fill are automatically re-placed.
    """

    def __init__(self, config: SymmetricGridConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.initialize_rate_sources()

    def initialize_rate_sources(self):
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=self.config.connector_name,
                          trading_pair=self.config.trading_pair)
        ])

    def active_executors(self) -> List[ExecutorInfo]:
        return [e for e in self.executors_info if e.is_active]

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        If no PMM executor is currently active, create one.
        The PMM executor handles order placement, fill detection, and re-placement internally.
        """
        if len(self.active_executors()) > 0:
            return []

        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)

        fair_price = self.config.reference_price if self.config.reference_price else mid_price

        return [CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=PMMExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                fair_price=fair_price,
                spread_percentages=list(self.config.spreads),
                order_amounts_quote=list(self.config.amounts_quote),
                order_type=self.config.open_order_type,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
                leverage=self.config.leverage,
            )
        )]

    async def update_processed_data(self):
        pass

    def to_format_status(self) -> List[str]:
        status = []
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        fair_price = self.config.reference_price if self.config.reference_price else mid_price

        box_width = 114
        status.append("┌" + "─" * box_width + "┐")
        header = f"│ Symmetric Grid - {self.config.connector_name} {self.config.trading_pair}"
        header += " " * (box_width - len(header) + 1) + "│"
        status.append(header)

        info_line = (f"│ Mid Price: {mid_price:.4f} │ Fair Price: {fair_price:.4f} │ "
                     f"Levels: {len(self.config.spreads)} │ "
                     f"Budget: {self.config.total_amount_quote:.2f}")
        info_line += " " * (box_width - len(info_line) + 1) + "│"
        status.append(info_line)

        # Show spread levels with buy/sell prices
        for i, (spread, amount) in enumerate(zip(self.config.spreads, self.config.amounts_quote)):
            buy_price = fair_price * (1 - spread)
            sell_price = fair_price * (1 + spread)
            level_line = (f"│  L{i}: spread={float(spread)*100:.2f}% │ "
                          f"Buy: {buy_price:.4f} │ Sell: {sell_price:.4f} │ Amount: {amount:.2f}")
            level_line += " " * (box_width - len(level_line) + 1) + "│"
            status.append(level_line)

        status.append("├" + "─" * box_width + "┤")

        # Active executor info
        for executor in self.active_executors():
            ci = executor.custom_info
            perf_line = (f"│ PnL: R={ci.get('realized_pnl_quote', 0):.4f} U={ci.get('unrealized_pnl_quote', 0):.4f} │ "
                         f"Fees: {ci.get('total_fees_quote', 0):.4f} │ "
                         f"Inventory: {ci.get('net_inventory_base', 0):.6f} │ "
                         f"Fills: {len(ci.get('filled_orders', []))}")
            perf_line += " " * (box_width - len(perf_line) + 1) + "│"
            status.append(perf_line)

            # Show per-level states
            for level_info in ci.get("levels", []):
                lev_line = (f"│  {level_info['id']}: spread={level_info['spread_pct']*100:.2f}% │ "
                            f"Buy@{level_info['buy_price']:.4f} [{level_info['buy_state']}] │ "
                            f"Sell@{level_info['sell_price']:.4f} [{level_info['sell_state']}] │ "
                            f"Next: {level_info.get('pending_side', '?')}")
                lev_line += " " * (box_width - len(lev_line) + 1) + "│"
                status.append(lev_line)

        if len(self.active_executors()) == 0:
            no_exec = "│ No active executor — will create on next tick"
            no_exec += " " * (box_width - len(no_exec) + 1) + "│"
            status.append(no_exec)

        status.append("└" + "─" * box_width + "┘")
        return status
