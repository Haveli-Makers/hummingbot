from decimal import Decimal
from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.best_price_executor.data_types import BestPriceExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.symmetric_grid_executor.data_types import SymmetricGridExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class GridV2ExecutorConfig(ControllerConfigBase):
    """
    Configuration required to run the GridV2Executor controller.
    """
    controller_type: str = "generic"
    controller_name: str = "grid_v2"
    candles_config: List[CandlesConfig] = []

    connector_name: str = Field(
        default="binance_perpetual",
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the connector name (e.g., binance_perpetual): ",
        }
    )
    trading_pair: str = Field(
        default="WLD-USDT",
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the trading pair (e.g., WLD-USDT): ",
        }
    )
    leverage: int = Field(
        default=20,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the leverage to use (set 1 for spot): ",
        }
    )
    position_mode: PositionMode = Field(
        default=PositionMode.HEDGE,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the position mode (HEDGE/ONEWAY): ",
        }
    )

    spreads: List[Decimal] = Field(
        default="0.01,0.02",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of grid spreads as decimals (e.g., 0.01,0.02): ",
            "is_updatable": True,
        }
    )
    amounts_quote: List[Decimal] = Field(
        default="10,20",
        validate_default=True,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter a comma-separated list of grid quote amounts (e.g., 10,20): ",
            "is_updatable": True,
        }
    )

    total_balance: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the target base balance to maintain (e.g., 0): ",
            "is_updatable": True,
        }
    )
    max_exposure: Decimal = Field(
        default=Decimal("100"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the maximum net base exposure before forcing an exit (e.g., 100): ",
            "is_updatable": True,
        }
    )
    target_profit: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the target profit as a decimal (e.g., 0.01 for 1%): ",
            "is_updatable": True,
        }
    )
    price_refresh_tolerance: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the price refresh tolerance as a decimal (e.g., 0.01 for 1%): ",
            "is_updatable": True,
        }
    )
    max_open_orders: int = Field(
        default=8,
        gt=0,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the default max orders per grid creation batch: ",
            "is_updatable": True,
        }
    )
    order_frequency: int = Field(
        default=3,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the order frequency in seconds: ",
            "is_updatable": True,
        }
    )
    price_diff: Decimal = Field(
        default=Decimal("0"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the best-price executor price diff as a decimal (e.g., 0): ",
            "is_updatable": True,
        }
    )
    safe_extra_spread: Decimal = Field(
        default=Decimal("0.0001"),
        ge=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the extra spread used to avoid crossing the book (e.g., 0.0001): ",
            "is_updatable": True,
        }
    )
    keep_position: bool = Field(
        default=False,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Keep position after stopping executors? (true/false): ",
            "is_updatable": True,
        }
    )
    open_order_type: OrderType = Field(
        default=OrderType.LIMIT_MAKER,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the open order type (LIMIT/LIMIT_MAKER): ",
            "is_updatable": True,
        }
    )
    max_orders_per_batch: Optional[int] = Field(
        default=None,
        gt=0,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter max orders per batch, or leave blank to use default max orders per batch: ",
            "is_updatable": True,
        }
    )
    min_order_amount_quote: Decimal = Field(
        default=Decimal("5"),
        gt=Decimal("0"),
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter the minimum order amount in quote: ",
            "is_updatable": True,
        }
    )
    stop_loss: Optional[Decimal] = Field(
        default=None,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter stop loss as a decimal, or leave blank to disable: ",
            "is_updatable": True,
        }
    )
    time_limit: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "prompt_on_new": True,
            "prompt": "Enter time limit in seconds, or leave blank to disable: ",
            "is_updatable": True,
        }
    )

    @field_validator("spreads", mode="before")
    def parse_spread_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            if value.strip() == "":
                return []
            return [Decimal(x.strip()) for x in value.split(",") if x.strip()]
        return [Decimal(str(x)) for x in value]

    @field_validator("amounts_quote", mode="before")
    def parse_amount_list(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            if value.strip() == "":
                return []
            return [Decimal(x.strip()) for x in value.split(",") if x.strip()]
        return [Decimal(str(x)) for x in value]

    @field_validator("open_order_type", mode="before")
    @classmethod
    def validate_order_type(cls, value) -> OrderType:
        if isinstance(value, OrderType):
            return value
        if value is None or value == "":
            return OrderType.LIMIT_MAKER
        if isinstance(value, int):
            return OrderType(value)
        if isinstance(value, str):
            cleaned = value.replace("OrderType.", "").upper()
            if cleaned in OrderType.__members__:
                return OrderType[cleaned]
        raise ValueError(f"Invalid order type: {value}")

    @field_validator("max_orders_per_batch", "time_limit", mode="before")
    @classmethod
    def parse_optional_int(cls, value):
        if value is None or value == "":
            return None
        return int(value)

    @field_validator("stop_loss", mode="before")
    @classmethod
    def parse_optional_decimal(cls, value):
        if value is None or value == "":
            return None
        return Decimal(str(value))

    @model_validator(mode="after")
    def validate_levels(self):
        spreads = self.parse_spread_list(self.spreads)
        amounts_quote = self.parse_amount_list(self.amounts_quote)
        if len(spreads) != len(amounts_quote):
            raise ValueError("spreads and amounts_quote must have the same number of entries")
        if not spreads:
            raise ValueError("At least one grid level must be configured")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class GridV2Executor(ControllerBase):
    def __init__(self, config: GridV2ExecutorConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.initialize_rate_sources()

    def initialize_rate_sources(self):
        self.market_data_provider.initialize_rate_sources([
            ConnectorPair(connector_name=self.config.connector_name,
                          trading_pair=self.config.trading_pair)
        ])

    def active_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.executors_info if executor.is_active]

    def active_grid_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "symmetric_grid_executor"]

    def active_target_profit_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "order_executor"
                and executor.config.level_id
                and executor.config.level_id.startswith("grid_v2_tp_")]

    def active_exposure_exit_executors(self) -> List[ExecutorInfo]:
        return [executor for executor in self.active_executors()
                if executor.config.type == "best_price_executor"
                and executor.config.level_id
                and executor.config.level_id.startswith("grid_v2_exposure_exit_")]

    async def update_processed_data(self):
        fair_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        self.processed_data["fair_price"] = fair_price
        self.processed_data["mid_price"] = fair_price

    def determine_executor_actions(self) -> List[ExecutorAction]:
        active_grid_executors = self.active_grid_executors()
        if active_grid_executors:
            return self.create_risk_management_actions(active_grid_executors[0])

        return [CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=SymmetricGridExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                spread_percentages=list(self.config.spreads),
                order_amounts_quote=list(self.config.amounts_quote),
                fair_price=None,
                order_type=self.config.open_order_type,
                order_frequency=self.config.order_frequency,
                max_orders_per_batch=self.config.max_orders_per_batch or self.config.max_open_orders,
                safe_extra_spread=self.config.safe_extra_spread,
                min_order_amount_quote=self.config.min_order_amount_quote,
                price_refresh_tolerance=self.config.price_refresh_tolerance,
                stop_loss=self.config.stop_loss,
                time_limit=self.config.time_limit,
                leverage=self.config.leverage,
                level_id="grid_v2_symmetric",
            )
        )]

    @staticmethod
    def _to_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def create_risk_management_actions(self, grid_executor: ExecutorInfo) -> List[ExecutorAction]:
        exposure = self.calculate_exposure(grid_executor)
        if abs(exposure) > self.config.max_exposure:
            action = self.create_exposure_exit_action(exposure)
            return [action] if action is not None else []

        action = self.create_target_profit_action(grid_executor, exposure)
        return [action] if action is not None else []

    def calculate_exposure(self, grid_executor: ExecutorInfo) -> Decimal:
        base_asset, _ = split_hb_trading_pair(self.config.trading_pair)
        balance = self._to_decimal(self.market_data_provider.get_balance(self.config.connector_name, base_asset))
        return balance - self.config.total_balance

    def get_average_entry_price_for_exposure(self, grid_executor: ExecutorInfo, exposure: Decimal) -> Decimal:
        custom_info = grid_executor.custom_info or {}
        filled_orders = custom_info.get("filled_orders", [])
        side_name = TradeType.BUY.name if exposure > Decimal("0") else TradeType.SELL.name
        total_base = Decimal("0")
        total_quote = Decimal("0")
        for order in filled_orders:
            if order.get("trade_type") != side_name:
                continue
            total_base += self._to_decimal(order.get("executed_amount_base"))
            total_quote += self._to_decimal(order.get("executed_amount_quote"))
        if total_base > Decimal("0"):
            return total_quote / total_base
        return Decimal("0")

    def create_target_profit_action(self, grid_executor: ExecutorInfo, exposure: Decimal) -> Optional[CreateExecutorAction]:
        if exposure == Decimal("0") or self.config.target_profit == Decimal("0"):
            return None
        if self.active_target_profit_executors():
            return None

        avg_price = self.get_average_entry_price_for_exposure(grid_executor, exposure)
        if avg_price <= Decimal("0"):
            return None

        side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
        price = avg_price * (Decimal("1") + self.config.target_profit) if side == TradeType.SELL else avg_price * (Decimal("1") - self.config.target_profit)
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=abs(exposure),
                price=price,
                position_action=PositionAction.CLOSE,
                execution_strategy=ExecutionStrategy.LIMIT_MAKER,
                leverage=self.config.leverage,
                level_id=f"grid_v2_tp_{side.name.lower()}",
            )
        )

    def create_exposure_exit_action(self, exposure: Decimal) -> Optional[CreateExecutorAction]:
        side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
        level_id = f"grid_v2_exposure_exit_{side.name.lower()}"
        if any(executor.config.level_id == level_id for executor in self.active_exposure_exit_executors()):
            return None
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=BestPriceExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                amount=abs(exposure),
                position_action=PositionAction.CLOSE,
                price_diff=self.config.price_diff,
                leverage=self.config.leverage,
                level_id=level_id,
            )
        )

    @staticmethod
    def _format_decimal(value, precision: int = 4) -> str:
        decimal_value = GridV2Executor._to_decimal(value)
        return f"{decimal_value:.{precision}f}"

    @staticmethod
    def _format_pct(value, precision: int = 2) -> str:
        decimal_value = GridV2Executor._to_decimal(value)
        return f"{decimal_value * Decimal('100'):.{precision}f}%"

    @staticmethod
    def _box_line(left: str = "", right: str = "", width: int = 114) -> str:
        if right:
            content = f"{left:<{max(0, width - len(right) - 1)}} {right}"
        else:
            content = left
        return f"│ {content[:width]:<{width}} │"

    @staticmethod
    def _table_row(columns: List[str], widths: List[int], width: int = 114) -> str:
        cells = [f"{column[:width]:<{width}}" for column, width in zip(columns, widths)]
        content = " │ ".join(cells)
        return f"│ {content[:width]:<{width}} │"

    def _get_mid_price_for_status(self) -> Decimal:
        mid_price = self.processed_data.get("mid_price")
        if mid_price is not None:
            return self._to_decimal(mid_price)
        try:
            return self._to_decimal(self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice))
        except Exception:
            return Decimal("0")

    def _get_base_balance_for_status(self, base_asset: str) -> Decimal:
        try:
            return self._to_decimal(self.market_data_provider.get_balance(self.config.connector_name, base_asset))
        except Exception:
            return Decimal("0")

    def _get_last_trades_from_sqlite(self, limit: int = 10) -> List[dict]:
        import glob
        import os
        import sqlite3
        from datetime import datetime

        trades = []
        sqlite_paths = sorted(glob.glob(os.path.join("data", "*.sqlite")), key=os.path.getmtime, reverse=True)
        for sqlite_path in sqlite_paths:
            if len(trades) >= limit:
                break
            try:
                with sqlite3.connect(sqlite_path) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        """
                        SELECT timestamp, market, symbol, trade_type, order_type, price, amount, trade_fee_in_quote
                        FROM TradeFill
                        WHERE market = ? AND symbol = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                        """,
                        (self.config.connector_name, self.config.trading_pair, limit - len(trades)),
                    ).fetchall()
            except Exception:
                continue
            for row in rows:
                timestamp = int(row["timestamp"] or 0)
                trades.append({
                    "time": datetime.fromtimestamp(timestamp / 1e3).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "",
                    "side": row["trade_type"],
                    "order_type": row["order_type"],
                    "price": row["price"],
                    "amount": row["amount"],
                    "fee": row["trade_fee_in_quote"],
                })
        return trades[:limit]

    def to_format_status(self) -> List[str]:
        status = []
        width = 114
        table_widths = [8, 10, 12, 12, 12, 12, 12, 12]

        mid_price = self._get_mid_price_for_status()
        active_grid_executors = self.active_grid_executors()
        grid_executor = active_grid_executors[0] if active_grid_executors else None
        custom_info = grid_executor.custom_info if grid_executor and grid_executor.custom_info else {}
        exposure = self.calculate_exposure(grid_executor) if grid_executor else Decimal("0")
        avg_price = self.get_average_entry_price_for_exposure(grid_executor, exposure) if grid_executor else Decimal("0")
        base_asset, _ = split_hb_trading_pair(self.config.trading_pair)
        base_balance = self._get_base_balance_for_status(base_asset)

        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line(
            f"Grid V2: {self.config.connector_name}:{self.config.trading_pair}",
            f"Mid: {self._format_decimal(mid_price)}",
            width,
        ))
        status.append(self._box_line(
            f"Levels: {len(self.config.spreads)} | Batch Orders: {self.config.max_orders_per_batch or self.config.max_open_orders} | "
            f"Order Type: {self.config.open_order_type.name}",
            f"Leverage: {self.config.leverage}x",
            width,
        ))
        status.append(self._box_line(
            f"Spreads: {', '.join(self._format_pct(spread) for spread in self.config.spreads)}",
            f"Quote Amounts: {', '.join(self._format_decimal(amount, 2) for amount in self.config.amounts_quote)}",
            width,
        ))
        status.append("└" + "─" * width + "┘")

        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line("Balance & Exposure", width=width))
        status.append(self._box_line(
            f"Target Base: {self._format_decimal(self.config.total_balance)} | "
            f"{base_asset} Balance: {self._format_decimal(base_balance)} | "
            f"Exposure: {self._format_decimal(exposure)}",
            f"Max Exposure: {self._format_decimal(self.config.max_exposure)}",
            width,
        ))
        status.append(self._box_line(
            f"Net Quote: {self._format_decimal(custom_info.get('net_inventory_quote'), 2)} | "
            f"Avg Entry: {self._format_decimal(avg_price)}",
            f"TP: {self._format_pct(self.config.target_profit)}",
            width,
        ))
        status.append(self._box_line(
            f"Realized PnL: {self._format_decimal(custom_info.get('realized_pnl_quote'), 2)} | "
            f"Unrealized PnL: {self._format_decimal(custom_info.get('unrealized_pnl_quote'), 2)} | "
            f"Fees: {self._format_decimal(custom_info.get('total_fees_quote'), 2)}",
            width=width,
        ))
        status.append("└" + "─" * width + "┘")

        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line("Current Orders", width=width))
        levels = custom_info.get("levels", [])
        if levels:
            status.append(self._table_row(
                ["Level", "Spread", "Amount", "Buy Price", "Buy State", "Sell Price", "Sell State", "Pending"],
                table_widths,
            ))
            status.append("├" + "─" * width + "┤")
            for level in levels:
                status.append(self._table_row([
                    str(level.get("id", "")),
                    self._format_pct(level.get("spread_pct")),
                    self._format_decimal(level.get("amount_quote"), 2),
                    self._format_decimal(level.get("buy_price")),
                    str(level.get("buy_state", "")),
                    self._format_decimal(level.get("sell_price")),
                    str(level.get("sell_state", "")),
                    str(level.get("pending_side", "")),
                ], table_widths))
        else:
            status.append(self._box_line("No active symmetric grid executor.", width=width))
        status.append("└" + "─" * width + "┘")

        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line("Profit Target / Exposure Exit", width=width))
        target_profit_executors = self.active_target_profit_executors()
        exposure_exit_executors = self.active_exposure_exit_executors()
        if exposure_exit_executors:
            executor_config = exposure_exit_executors[0].config
            status.append(self._box_line(
                f"ACTIVE EXIT | Side: {executor_config.side.name} | Qty: {self._format_decimal(executor_config.amount)}",
                f"Price Diff: {self._format_decimal(executor_config.price_diff)}",
                width,
            ))
        elif target_profit_executors:
            executor_config = target_profit_executors[0].config
            status.append(self._box_line(
                f"ACTIVE TP | Side: {executor_config.side.name} | Qty: {self._format_decimal(executor_config.amount)}",
                f"Price: {self._format_decimal(executor_config.price)}",
                width,
            ))
        elif abs(exposure) > self.config.max_exposure:
            side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
            status.append(self._box_line(
                f"PLANNED EXIT | Side: {side.name} | Qty: {self._format_decimal(abs(exposure))}",
                "Reason: exposure > max",
                width,
            ))
        elif exposure != Decimal("0") and avg_price > Decimal("0") and self.config.target_profit > Decimal("0"):
            side = TradeType.SELL if exposure > Decimal("0") else TradeType.BUY
            target_price = avg_price * (Decimal("1") + self.config.target_profit) if side == TradeType.SELL else avg_price * (Decimal("1") - self.config.target_profit)
            status.append(self._box_line(
                f"PLANNED TP | Side: {side.name} | Qty: {self._format_decimal(abs(exposure))}",
                f"Price: {self._format_decimal(target_price)}",
                width,
            ))
        else:
            status.append(self._box_line("No target-profit or exposure-exit order required.", width=width))
        status.append("└" + "─" * width + "┘")

        filled_orders = custom_info.get("filled_orders", [])[-10:]
        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line("Filled Orders From Current Grid", width=width))
        if filled_orders:
            filled_widths = [8, 14, 14, 14, 14, 14]
            status.append(self._table_row(["Side", "Base", "Quote", "Fee", "Order Type", "Client Order"], filled_widths))
            status.append("├" + "─" * width + "┤")
            for order in filled_orders:
                status.append(self._table_row([
                    str(order.get("trade_type", "")),
                    self._format_decimal(order.get("executed_amount_base")),
                    self._format_decimal(order.get("executed_amount_quote"), 2),
                    self._format_decimal(order.get("cumulative_fee_paid_quote"), 4),
                    str(order.get("order_type", "")),
                    str(order.get("client_order_id", "")),
                ], filled_widths))
        else:
            status.append(self._box_line("No filled orders recorded by the active grid executor.", width=width))
        status.append("└" + "─" * width + "┘")

        status.append("┌" + "─" * width + "┐")
        status.append(self._box_line("Past Trades From SQLite (Last 10)", width=width))
        trades = self._get_last_trades_from_sqlite(limit=10)
        if trades:
            trade_widths = [19, 8, 12, 14, 14, 12]
            status.append(self._table_row(["Time", "Side", "Type", "Price", "Amount", "Fee"], trade_widths))
            status.append("├" + "─" * width + "┤")
            for trade in trades:
                status.append(self._table_row([
                    trade["time"],
                    str(trade["side"]),
                    str(trade["order_type"]),
                    self._format_decimal(trade["price"]),
                    self._format_decimal(trade["amount"]),
                    self._format_decimal(trade["fee"], 4),
                ], trade_widths))
        else:
            status.append(self._box_line("No matching persisted trades found.", width=width))
        status.append("└" + "─" * width + "┘")

        return status
