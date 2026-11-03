"""
Orderbook backtesting engine for strategy_v2 controllers.

Replays recorded orderbook + public trade data and runs any strategy_v2 controller
against it, using the event-driven backtest engine for realistic fill simulation
(queue position, latency, maker/taker fees).

No per-strategy backtest file needed — any controller config works.
"""
import importlib
import inspect
import math
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.backtesting.backtesting_data_provider import BacktestingDataProvider
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.controllers.market_making_controller_base import MarketMakingControllerConfigBase
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.strategy_v2.models.base import RunnableStatus


class _TrackedExecutor:
    """Tracks a single executor spawned by the controller, mapped to backtest engine orders."""

    def __init__(self, config: Union[PositionExecutorConfig, DCAExecutorConfig],
                 create_ts: float, entry_price: float, side: TradeType):
        self.config = config
        self.create_ts = create_ts
        self.entry_price = entry_price
        self.side = side
        self.is_active = True
        self.filled_amount_base = 0.0
        self.filled_amount_quote = 0.0
        self.cum_fees = 0.0
        self.close_type: Optional[CloseType] = None
        self.close_ts: Optional[float] = None
        self.order_ids: List[int] = []
        self.exit_fills: List[Tuple[float, float]] = []

    @property
    def net_pnl_quote(self) -> float:
        if self.filled_amount_base == 0:
            return 0.0
        sign = 1 if self.side == TradeType.BUY else -1
        exit_notional = sum(p * q for p, q in self.exit_fills) if self.exit_fills else 0.0
        entry_notional = self.filled_amount_quote
        return sign * (exit_notional - entry_notional) - self.cum_fees

    @property
    def avg_entry_price(self) -> Optional[float]:
        if self.filled_amount_base == 0:
            return None
        return self.filled_amount_quote / self.filled_amount_base

    def to_executor_info(self, current_price: float, current_ts: float) -> ExecutorInfo:
        filled_quote = Decimal(str(self.filled_amount_quote))
        net_pnl = Decimal(str(self.net_pnl_quote))
        pnl_pct = net_pnl / filled_quote if filled_quote > 0 else Decimal(0)
        return ExecutorInfo(
            id=self.config.id,
            timestamp=self.create_ts,
            type=self.config.type,
            close_timestamp=self.close_ts,
            close_type=self.close_type,
            status=RunnableStatus.RUNNING if self.is_active else RunnableStatus.TERMINATED,
            config=self.config,
            net_pnl_pct=pnl_pct,
            net_pnl_quote=net_pnl,
            cum_fees_quote=Decimal(str(self.cum_fees)),
            filled_amount_quote=filled_quote,
            is_active=self.is_active,
            is_trading=self.filled_amount_base > 0 and self.is_active,
            custom_info={
                "close_price": current_price,
                "level_id": getattr(self.config, "level_id", ""),
                "side": self.side,
                "current_position_average_price": self.avg_entry_price,
            },
        )


class OrderbookBacktestingEngine:
    """
    Runs any strategy_v2 controller against recorded orderbook + trade data.

    Uses the event-driven Backtest engine from backtest_engine.py for realistic
    order matching (latency, queue position, maker/taker distinction).
    """

    def __init__(self):
        self.backtesting_data_provider = BacktestingDataProvider(connectors={})
        self._controller_class_cache = {}

    @staticmethod
    def _get_controller_class(config: ControllerConfigBase) -> Type[ControllerBase]:
        return config.get_controller_class()

    async def run_backtesting(
        self,
        controller_config: ControllerConfigBase,
        connector: str,
        pair: str,
        start: str,
        end: Optional[str] = None,
        maker_fee: float = 0.001,
        taker_fee: float = 0.001,
        latency_ms: float = 100,
        fill_at_touch: bool = False,
        update_interval: float = 1.0,
    ) -> Dict:
        from backtest_engine import Backtest, Strategy, Order
        from backtest_data import replay

        controller_class = self._get_controller_class(controller_config)

        bt = Backtest(connector, pair, start, end, maker_fee, taker_fee, latency_ms, fill_at_touch)

        tracked: Dict[str, _TrackedExecutor] = {}
        order_to_executor: Dict[int, str] = {}
        all_executor_infos: List[ExecutorInfo] = []

        start_ts = None
        end_ts = None

        data_provider = self.backtesting_data_provider
        controller = controller_class(
            config=controller_config,
            market_data_provider=data_provider,
            actions_queue=None,
        )

        class _ControllerBridge(Strategy):
            """Bridges backtest engine events to the strategy_v2 controller."""

            def on_start(self_, bt_instance):
                nonlocal start_ts
                start_ts = bt_instance.now

            def on_book(self_, bt_instance, book):
                nonlocal end_ts
                end_ts = bt_instance.now
                mid = bt_instance.mid
                if mid is None:
                    return
                self._update_controller_state(
                    controller, data_provider, controller_config,
                    bt_instance.now, mid, tracked, all_executor_infos,
                )
                self._process_controller_actions(
                    controller, bt_instance, tracked, order_to_executor,
                    controller_config, mid,
                )
                self._check_barriers(
                    bt_instance, tracked, order_to_executor, controller_config, mid,
                )

            def on_trade(self_, bt_instance, trade):
                mid = bt_instance.mid
                if mid is not None:
                    self._update_price(data_provider, controller_config, bt_instance.now, mid)

            def on_fill(self_, bt_instance, order: Order, price: float, qty: float, maker: bool):
                executor_id = order_to_executor.get(order.id)
                if executor_id is None:
                    return
                tex = tracked.get(executor_id)
                if tex is None:
                    return
                fee = price * qty * (bt_instance.maker_fee if maker else bt_instance.taker_fee)
                is_entry = order.id in (tex.order_ids[:1] if tex.order_ids else [])
                if is_entry or tex.filled_amount_base == 0:
                    tex.filled_amount_base += qty
                    tex.filled_amount_quote += price * qty
                    tex.cum_fees += fee
                else:
                    tex.exit_fills.append((price, qty))
                    tex.cum_fees += fee

            def on_cancel(self_, bt_instance, order: Order):
                pass

            def on_reject(self_, bt_instance, order: Order):
                pass

        bridge = _ControllerBridge()
        bt.run(bridge)

        for tex in tracked.values():
            if tex.is_active:
                tex.is_active = False
                tex.close_type = CloseType.TIME_LIMIT
                tex.close_ts = end_ts or 0
            current_price = bt.mid or 0
            all_executor_infos.append(tex.to_executor_info(current_price, end_ts or 0))

        results = self._summarize_results(all_executor_infos, float(controller_config.total_amount_quote))

        return {
            "executors": all_executor_infos,
            "results": results,
            "fills": bt._fills,
            "equity": bt._equity,
            "fees": bt.fees,
            "final_position": bt.position,
        }

    def _update_price(self, data_provider, config, ts, mid):
        key = f"{config.connector_name}_{config.trading_pair}"
        data_provider.prices = {key: Decimal(str(mid))}
        data_provider._time = ts

    def _update_controller_state(self, controller, data_provider, config, ts, mid, tracked, completed_infos):
        self._update_price(data_provider, config, ts, mid)

        infos = []
        for tex in list(tracked.values()):
            info = tex.to_executor_info(mid, ts)
            if not tex.is_active:
                completed_infos.append(info)
                del tracked[tex.config.id]
            else:
                infos.append(info)
        controller.executors_info = infos

    def _process_controller_actions(self, controller, bt, tracked, order_to_executor, config, mid):
        try:
            controller.processed_data = {
                "reference_price": Decimal(str(mid)),
                "spread_multiplier": Decimal("1"),
            }
            actions = controller.determine_executor_actions()
        except Exception:
            return

        for action in actions:
            if isinstance(action, CreateExecutorAction):
                executor_config = action.executor_config
                if isinstance(executor_config, (PositionExecutorConfig, DCAExecutorConfig)):
                    side = executor_config.side
                    entry_price = float(executor_config.entry_price) if hasattr(executor_config, 'entry_price') else mid

                    tex = _TrackedExecutor(executor_config, bt.now, entry_price, side)

                    place_fn = bt.buy if side == TradeType.BUY else bt.sell
                    amount = float(executor_config.amount) if hasattr(executor_config, 'amount') else 0.001

                    is_limit = (hasattr(executor_config, 'triple_barrier_config') and
                                executor_config.triple_barrier_config.open_order_type.is_limit_type())

                    order_id = place_fn(entry_price, amount, post_only=is_limit)
                    tex.order_ids.append(order_id)
                    order_to_executor[order_id] = executor_config.id
                    tracked[executor_config.id] = tex

            elif isinstance(action, StopExecutorAction):
                tex = tracked.get(action.executor_id)
                if tex is not None and tex.is_active:
                    tex.is_active = False
                    tex.close_type = CloseType.EARLY_STOP
                    tex.close_ts = bt.now
                    for oid in tex.order_ids:
                        if oid in bt.orders:
                            bt.cancel(oid)

    def _check_barriers(self, bt, tracked, order_to_executor, config, mid):
        for executor_id, tex in list(tracked.items()):
            if not tex.is_active or tex.filled_amount_base == 0:
                continue

            ec = tex.config
            if not isinstance(ec, PositionExecutorConfig):
                continue

            tbc = ec.triple_barrier_config
            side_mult = 1 if tex.side == TradeType.BUY else -1
            pnl_pct = side_mult * (mid - tex.avg_entry_price) / tex.avg_entry_price if tex.avg_entry_price else 0

            if tbc.take_profit and pnl_pct >= float(tbc.take_profit):
                self._close_executor(bt, tex, order_to_executor, CloseType.TAKE_PROFIT, mid)
            elif tbc.stop_loss and pnl_pct <= -float(tbc.stop_loss):
                self._close_executor(bt, tex, order_to_executor, CloseType.STOP_LOSS, mid)
            elif tbc.time_limit and bt.now - tex.create_ts >= tbc.time_limit:
                self._close_executor(bt, tex, order_to_executor, CloseType.TIME_LIMIT, mid)

    def _close_executor(self, bt, tex, order_to_executor, close_type, mid):
        tex.is_active = False
        tex.close_type = close_type
        tex.close_ts = bt.now

        for oid in tex.order_ids:
            if oid in bt.orders:
                bt.cancel(oid)

        if tex.filled_amount_base > 0:
            exit_fn = bt.sell if tex.side == TradeType.BUY else bt.buy
            exit_price = mid
            oid = exit_fn(exit_price, tex.filled_amount_base)
            tex.order_ids.append(oid)
            order_to_executor[oid] = tex.config.id

    @staticmethod
    def _summarize_results(executors_info: List[ExecutorInfo], total_amount_quote: float = 1000) -> Dict:
        if not executors_info:
            return {
                "net_pnl": 0, "net_pnl_quote": 0, "total_executors": 0,
                "total_executors_with_position": 0, "total_volume": 0,
                "total_long": 0, "total_short": 0, "close_types": {},
                "accuracy_long": 0, "accuracy_short": 0, "total_positions": 0,
                "accuracy": 0, "max_drawdown_usd": 0, "max_drawdown_pct": 0,
                "sharpe_ratio": 0, "profit_factor": 0, "win_signals": 0, "loss_signals": 0,
            }

        import numpy as np

        executors_df = pd.DataFrame([ei.to_dict() for ei in executors_info])
        net_pnl_quote = executors_df["net_pnl_quote"].sum()
        total_executors = len(executors_df)
        executors_with_pos = executors_df[executors_df["net_pnl_quote"] != 0]
        total_with_pos = len(executors_with_pos)
        total_volume = executors_with_pos["filled_amount_quote"].sum() if total_with_pos else 0

        total_long = (executors_with_pos["side"] == TradeType.BUY).sum() if total_with_pos else 0
        total_short = (executors_with_pos["side"] == TradeType.SELL).sum() if total_with_pos else 0
        correct_long = ((executors_with_pos["side"] == TradeType.BUY) & (executors_with_pos["net_pnl_quote"] > 0)).sum() if total_with_pos else 0
        correct_short = ((executors_with_pos["side"] == TradeType.SELL) & (executors_with_pos["net_pnl_quote"] > 0)).sum() if total_with_pos else 0

        accuracy_long = correct_long / total_long if total_long > 0 else 0
        accuracy_short = correct_short / total_short if total_short > 0 else 0

        close_types = {}
        if "close_type" in executors_df.columns:
            executors_df["close_type_name"] = executors_df["close_type"].apply(
                lambda x: x.name if hasattr(x, "name") else str(x))
            close_types = executors_df.groupby("close_type_name")["timestamp"].count().to_dict()

        win_signals = executors_with_pos[executors_with_pos["net_pnl_quote"] > 0] if total_with_pos else pd.DataFrame()
        loss_signals = executors_with_pos[executors_with_pos["net_pnl_quote"] < 0] if total_with_pos else pd.DataFrame()
        accuracy = len(win_signals) / total_with_pos if total_with_pos else 0

        cumulative_returns = executors_with_pos["net_pnl_quote"].cumsum() if total_with_pos else pd.Series([0])
        peak = np.maximum.accumulate(cumulative_returns) if total_with_pos else np.array([0])
        drawdown = cumulative_returns - peak
        max_drawdown = float(np.min(drawdown)) if len(drawdown) else 0
        max_drawdown_pct = max_drawdown / total_amount_quote if total_amount_quote else 0

        total_won = float(win_signals["net_pnl_quote"].sum()) if len(win_signals) else 0
        total_loss = float(-loss_signals["net_pnl_quote"].sum()) if len(loss_signals) else 0
        profit_factor = total_won / total_loss if total_loss > 0 else 1

        return {
            "net_pnl": float(net_pnl_quote / total_amount_quote) if total_amount_quote else 0,
            "net_pnl_quote": float(net_pnl_quote),
            "total_executors": total_executors,
            "total_executors_with_position": total_with_pos,
            "total_volume": float(total_volume),
            "total_long": int(total_long),
            "total_short": int(total_short),
            "close_types": close_types,
            "accuracy_long": float(accuracy_long),
            "accuracy_short": float(accuracy_short),
            "total_positions": total_with_pos,
            "accuracy": float(accuracy),
            "max_drawdown_usd": max_drawdown,
            "max_drawdown_pct": float(max_drawdown_pct),
            "sharpe_ratio": 0,
            "profit_factor": float(profit_factor),
            "win_signals": len(win_signals),
            "loss_signals": len(loss_signals),
        }
