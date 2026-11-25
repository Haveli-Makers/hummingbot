from __future__ import annotations

import itertools
import logging
import math
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.pubsub import PubSub
from hummingbot.strategy_v2.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

_LOG = logging.getLogger(__name__)

_DUST = 1e-12
_RECENT_TERMINATED = 500
_CLOSE_SLIPPAGE_TICKS = 20

EXECUTOR_MAPPING = {
    "position_executor": PositionExecutor,
    "dca_executor": DCAExecutor,
    "grid_executor": GridExecutor,
    "order_executor": OrderExecutor,
}


async def _async_noop(*args, **kwargs):
    """Stands in for an executor awaitable that would otherwise hit the network or sleep."""
    return None


def _drive(coro):
    """Run an executor control coroutine to completion synchronously.
    """
    if coro is None:
        return None
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    coro.close()
    return None


class _OrderTracker:
    """Keeps the backtest's InFlightOrders by client_order_id and exchange_order_id for the connector. """

    def __init__(self):
        self._orders: Dict[str, InFlightOrder] = {}

    def add(self, order: InFlightOrder) -> None:
        self._orders[order.client_order_id] = order

    def fetch_order(self, client_order_id: Optional[str] = None,
                    exchange_order_id: Optional[str] = None) -> Optional[InFlightOrder]:
        if client_order_id is not None:
            return self._orders.get(client_order_id)
        for order in self._orders.values():
            if order.exchange_order_id == exchange_order_id:
                return order
        return None

    @property
    def all_orders(self) -> Dict[str, InFlightOrder]:
        return self._orders


class _BudgetCheckerStub:
    """Backtest balance is unbounded, so candidates pass through unchanged."""

    def adjust_candidates(self, order_candidates, all_or_none: bool = True):
        return order_candidates

    def adjust_candidate(self, order_candidate, all_or_none: bool = True):
        return order_candidate


class BacktestConnector(PubSub):
    """A connector that drives the real executors against the backtest engine, and emits events
    to the controller as if they were coming from a live exchange. The backtest engine is synchronous, 
    so the connector is too: no asyncio, no sleeps, no network. The backtest engine is the source 
    of truth for fills, cancels, and rejects; the connector just tracks the InFlightOrders and emits 
    the events the controller expects to see.
    """

    def __init__(self, name: str, trading_pair: str, market, bt, maker_fee: float, taker_fee: float):
        super().__init__()
        self._name = name
        self._pair = trading_pair
        self._market = market
        self._bt = bt
        self._maker_fee = maker_fee
        self._taker_fee = taker_fee
        self._now = 0.0

        self._order_tracker = _OrderTracker()
        self.budget_checker = _BudgetCheckerStub()
        self._cid_by_int: Dict[int, str] = {}
        self._int_by_cid: Dict[str, int] = {}
        self._seq = itertools.count(1)
        self._trade_seq = itertools.count(1)
        self._pending_created: List[Tuple[MarketEvent, object]] = []

        tick = market.tick if market.tick and market.tick > 0 else 1e-8
        granular = Decimal("1e-8")
        self.trading_rules: Dict[str, TradingRule] = {
            trading_pair: TradingRule(
                trading_pair=trading_pair,
                min_order_size=granular,
                min_price_increment=Decimal(str(tick)),
                min_base_amount_increment=granular,
                min_quote_amount_increment=granular,
                min_notional_size=Decimal("0"),
            )
        }

    @property
    def name(self) -> str:
        return self._name

    @property
    def current_timestamp(self) -> float:
        return self._now

    def set_now(self, now: float) -> None:
        self._now = now

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Optional[Decimal]:
        price = self._market.price(price_type)
        return Decimal(str(price)) if price is not None else None

    def get_price(self, trading_pair: str, is_buy: bool) -> Optional[Decimal]:
        return self.get_price_by_type(trading_pair, PriceType.BestAsk if is_buy else PriceType.BestBid)

    def get_mid_price(self, trading_pair: str) -> Optional[Decimal]:
        return self.get_price_by_type(trading_pair, PriceType.MidPrice)

    def quantize_order_amount(self, trading_pair: str, amount: Decimal) -> Decimal:
        step = self.trading_rules[trading_pair].min_base_amount_increment
        if step <= 0:
            return Decimal(str(amount))
        return (Decimal(str(amount)) // step) * step

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        return Decimal(str(price))

    def get_balance(self, asset: str) -> Decimal:
        return Decimal("1e18")

    def get_available_balance(self, asset: str) -> Decimal:
        return Decimal("1e18")

    def get_order_book(self, trading_pair: str):
        raise NotImplementedError("The backtest connector serves prices, not a full order book object.")

    async def _update_orders_with_error_handler(self, orders, error_handler):
        """No-op: our InFlightOrders are already current (updated synchronously on each engine fill)."""
        return None

    def _handle_update_error_for_lost_order(self, *args, **kwargs):
        return None

    def _handle_update_error_for_active_order(self, *args, **kwargs):
        return None

    # ---- order placement ------------------------------------------------------------------------

    def _crossing_price(self, side: TradeType) -> float:
        """A price priced through the book so the engine fills it like a market order."""
        ref = self._market.best_ask if side == TradeType.BUY else self._market.best_bid
        if ref is None:
            ref = self._market.mid
        if ref is None:
            return 0.0
        slip = self._market.tick * _CLOSE_SLIPPAGE_TICKS
        if slip <= 0:
            slip = ref * 0.002
        price = ref + slip if side == TradeType.BUY else ref - slip
        return self._market.quantize(price if price > 0 else ref)

    def _translate(self, side: TradeType, order_type: OrderType, price) -> Tuple[Decimal, bool]:
        is_market = order_type == OrderType.MARKET or price is None or \
            (isinstance(price, Decimal) and price.is_nan())
        if is_market:
            return Decimal(str(self._crossing_price(side))), False
        if order_type == OrderType.LIMIT_MAKER:
            return Decimal(str(price)), True
        return Decimal(str(price)), False

    def place_order(self, trading_pair: str, side: TradeType, amount, order_type: OrderType,
                    price, position_action: PositionAction) -> str:
        amount_d = Decimal(str(amount))
        px, post_only = self._translate(side, order_type, price)
        place = self._bt.buy if side == TradeType.BUY else self._bt.sell
        int_id = place(float(px), float(amount_d), post_only=post_only)

        cid = f"{self._name}-{next(self._seq)}"
        self._cid_by_int[int_id] = cid
        self._int_by_cid[cid] = int_id

        order = InFlightOrder(
            client_order_id=cid,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=side,
            amount=amount_d,
            creation_timestamp=self._now,
            price=px,
            exchange_order_id=cid,
            initial_state=OrderState.OPEN,
            position=position_action,
        )
        self._order_tracker.add(order)

        if side == TradeType.BUY:
            self._pending_created.append((MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
                timestamp=self._now, type=order_type, trading_pair=trading_pair, amount=amount_d,
                price=px, order_id=cid, creation_timestamp=self._now, exchange_order_id=cid)))
        else:
            self._pending_created.append((MarketEvent.SellOrderCreated, SellOrderCreatedEvent(
                timestamp=self._now, type=order_type, trading_pair=trading_pair, amount=amount_d,
                price=px, order_id=cid, creation_timestamp=self._now, exchange_order_id=cid)))
        return cid

    def flush_created(self) -> None:
        """Deliver queued order-created acks after the placing control step has returned."""
        while self._pending_created:
            tag, event = self._pending_created.pop(0)
            self.trigger_event(tag, event)

    def cancel_order(self, order_id: str) -> None:
        int_id = self._int_by_cid.get(order_id)
        if int_id is not None and int_id in self._bt.orders:
            self._bt.cancel(int_id)

    def active_limit_orders(self):
        return [o.to_limit_order() for o in self._order_tracker.all_orders.values() if o.is_open]

    # ---- engine callbacks -> market events ------------------------------------------------------

    def on_engine_fill(self, int_id: int, price: float, qty: float, maker: bool) -> Optional[Tuple[float, float]]:
        """Apply a fill to the owning InFlightOrder and emit OrderFilled (+ completion). Returns
        (fee_quote, signed_base_delta) for the reconciliation accumulator, or None if unowned."""
        cid = self._cid_by_int.get(int_id)
        if cid is None:
            _LOG.error("fill on engine order %s with no owning client order", int_id)
            return None
        order = self._order_tracker.fetch_order(client_order_id=cid)
        if order is None:
            _LOG.error("fill on order %s whose InFlightOrder is missing", cid)
            return None

        fee_quote = price * qty * (self._maker_fee if maker else self._taker_fee)
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(order.quote_asset, Decimal(str(fee_quote)))])
        trade_id = f"{cid}-f{next(self._trade_seq)}"
        order.update_with_trade_update(TradeUpdate(
            trade_id=trade_id,
            client_order_id=cid,
            exchange_order_id=cid,
            trading_pair=order.trading_pair,
            fill_timestamp=self._now,
            fill_price=Decimal(str(price)),
            fill_base_amount=Decimal(str(qty)),
            fill_quote_amount=Decimal(str(price * qty)),
            fee=fee,
            is_taker=not maker,
        ))

        self.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
            timestamp=self._now, order_id=cid, trading_pair=order.trading_pair,
            trade_type=order.trade_type, order_type=order.order_type,
            price=Decimal(str(price)), amount=Decimal(str(qty)), trade_fee=fee,
            exchange_trade_id=trade_id, exchange_order_id=cid))

        if order.is_filled and order.current_state != OrderState.FILLED:
            order.current_state = OrderState.FILLED
            completed_args = dict(
                timestamp=self._now, order_id=cid, base_asset=order.base_asset,
                quote_asset=order.quote_asset, base_asset_amount=order.executed_amount_base,
                quote_asset_amount=order.executed_amount_quote, order_type=order.order_type,
                exchange_order_id=cid)
            if order.trade_type == TradeType.BUY:
                self.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(**completed_args))
            else:
                self.trigger_event(MarketEvent.SellOrderCompleted, SellOrderCompletedEvent(**completed_args))

        sign = 1.0 if order.trade_type == TradeType.BUY else -1.0
        return fee_quote, sign * qty

    def on_engine_cancel(self, int_id: int) -> None:
        cid = self._cid_by_int.get(int_id)
        if cid is None:
            return
        order = self._order_tracker.fetch_order(client_order_id=cid)
        if order is not None and order.is_open:
            order.current_state = OrderState.CANCELED
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(timestamp=self._now, order_id=cid, exchange_order_id=cid))

    def on_engine_reject(self, int_id: int) -> None:
        cid = self._cid_by_int.get(int_id)
        if cid is None:
            return
        order = self._order_tracker.fetch_order(client_order_id=cid)
        if order is not None:
            order.current_state = OrderState.FAILED
            self.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
                timestamp=self._now, order_id=cid, order_type=order.order_type))


class BacktestStrategyHost:
    """The ``self._strategy`` the executors reach through: owns the connector, time, and order verbs."""

    def __init__(self, connector: BacktestConnector, connector_name: str, market_data_provider):
        self.connectors = {connector_name: connector}
        self._connector_name = connector_name
        self.current_timestamp = 0.0
        self.market_data_provider = market_data_provider

    def buy(self, connector_name, trading_pair, amount, order_type, price, position_action=PositionAction.NIL):
        return self.connectors[connector_name].place_order(
            trading_pair, TradeType.BUY, amount, order_type, price, position_action)

    def sell(self, connector_name, trading_pair, amount, order_type, price, position_action=PositionAction.NIL):
        return self.connectors[connector_name].place_order(
            trading_pair, TradeType.SELL, amount, order_type, price, position_action)

    def cancel(self, connector_name, trading_pair, order_id):
        self.connectors[connector_name].cancel_order(order_id)

    def get_active_orders(self, connector_name):
        return self.connectors[connector_name].active_limit_orders()


class _RealExecutorOrchestrator:
    """Creates, synchronously steps, and retires the real executors. No asyncio, no DB singleton."""

    def __init__(self, host: BacktestStrategyHost, connector: BacktestConnector, update_interval: float,
                 max_retries: int, strict_errors: bool):
        self._host = host
        self._connector = connector
        self._update_interval = update_interval
        self._max_retries = max_retries
        self._strict = strict_errors
        self.active: Dict[str, object] = {}
        self.retired: List[object] = []
        self._newly_finished: List[object] = []

    def create(self, config) -> None:
        from hummingbot.strategy_v2.backtesting.orderbook_backtesting_engine import UnsupportedExecutorError

        cls = EXECUTOR_MAPPING.get(config.type)
        if cls is None:
            message = (f"executor type {config.type!r} ({type(config).__name__}) has no real executor "
                       f"registered in the orderbook backtester. Supported: {sorted(EXECUTOR_MAPPING)}.")
            if self._strict:
                raise UnsupportedExecutorError(message)
            _LOG.error("%s; ignored", message)
            return
        if config.id in self.active:
            _LOG.error("executor id %s is already live; the controller created it twice", config.id)
            return

        executor = cls(strategy=self._host, config=config,
                       update_interval=self._update_interval, max_retries=self._max_retries)
        # Drive the executor synchronously: no asyncio loop, no real sleeps or network refreshes.
        executor._sleep = _async_noop
        executor.validate_sufficient_balance = _async_noop
        executor._status = RunnableStatus.RUNNING
        executor.register_events()
        _drive(executor.on_start())
        self._connector.flush_created()
        if executor.status == RunnableStatus.TERMINATED:
            self._retire(executor)
            return
        self.active[config.id] = executor
        _drive(executor.control_task())
        self._connector.flush_created()
        if executor.status == RunnableStatus.TERMINATED:
            del self.active[config.id]
            self._retire(executor)

    def step_all(self) -> None:
        for executor_id, executor in list(self.active.items()):
            _drive(executor.control_task())
            self._connector.flush_created()
            if executor.status == RunnableStatus.TERMINATED:
                del self.active[executor_id]
                self._retire(executor)

    def stop(self, executor_id: str, keep_position: bool) -> None:
        executor = self.active.get(executor_id)
        if executor is not None:
            executor.early_stop(keep_position)

    def _retire(self, executor) -> None:
        self.retired.append(executor)
        self._newly_finished.append(executor)

    def take_newly_finished(self) -> List[object]:
        out, self._newly_finished = self._newly_finished, []
        return out

    def active_infos(self):
        return [executor.executor_info for executor in self.active.values()]

    def all_reported_infos(self):
        return [executor.executor_info for executor in self.retired + list(self.active.values())]


def run_real_executor_backtest(engine, bt, market, controller, controller_config, resolved_tick: float,
                               *, maker_fee: float, taker_fee: float, update_interval: float,
                               strict_errors: bool, max_retries: int = 10) -> Dict:
    """Replay ``bt`` driving the controller's REAL executors, and return the same result dict shape
    as ``OrderbookBacktestingEngine.run_backtesting`` produces for the simulator path."""
    from collections import deque

    from backtest_data import NoDataError
    from backtest_engine import Order, Strategy

    from hummingbot.strategy_v2.backtesting.orderbook_backtesting_engine import (
        CandlesUnavailableError,
        UnsupportedExecutorError,
        _PositionBook,
    )
    from hummingbot.core.data_type.common import PositionMode
    from hummingbot.core.rate_oracle.rate_oracle import RateOracle

    connector_name = controller_config.connector_name
    trading_pair = controller_config.trading_pair
    _base, _, _quote = trading_pair.partition("-")
    _rate_oracle = RateOracle.get_instance()
    connector = BacktestConnector(connector_name, trading_pair, market, bt, maker_fee, taker_fee)
    host = BacktestStrategyHost(connector, connector_name, engine.backtesting_data_provider)
    orchestrator = _RealExecutorOrchestrator(host, connector, update_interval, max_retries, strict_errors)

    positions = _PositionBook(getattr(controller_config, "position_mode", PositionMode.HEDGE))
    state = {"books": 0, "ticks": 0, "last_tick": -math.inf, "activity": 0,
             "recent": deque(maxlen=_RECENT_TERMINATED),
             "exec_cash": 0.0, "exec_base": 0.0}

    def controller_tick():
        just_finished = orchestrator.take_newly_finished()
        just_finished_infos = [executor.executor_info for executor in just_finished]
        for info in just_finished_infos:
            positions.absorb(info)
        controller.executors_info = list(state["recent"]) + just_finished_infos + orchestrator.active_infos()
        controller.positions_held = positions.summaries(market.mid)
        try:
            _drive(controller.update_processed_data())
        except (CandlesUnavailableError, UnsupportedExecutorError):
            raise
        except Exception as exc:
            if strict_errors:
                raise RuntimeError(
                    f"controller {type(controller).__name__}.update_processed_data() raised at "
                    f"t={market.now} ({type(exc).__name__}: {exc})") from exc
            _LOG.exception("update_processed_data failed")
            return
        try:
            actions = controller.determine_executor_actions()
        except Exception as exc:
            if strict_errors:
                raise RuntimeError(
                    f"controller {type(controller).__name__}.determine_executor_actions() raised at "
                    f"t={market.now} ({type(exc).__name__}: {exc})") from exc
            _LOG.exception("determine_executor_actions failed")
            actions = []

        state["recent"].extend(just_finished_infos)
        for action in actions or []:
            if isinstance(action, CreateExecutorAction):
                state["activity"] += 1
                orchestrator.create(action.executor_config)
            elif isinstance(action, StopExecutorAction):
                state["activity"] += 1
                orchestrator.stop(action.executor_id, bool(getattr(action, "keep_position", False)))

    class _Bridge(Strategy):
        def on_start(self_, bt_instance):
            _LOG.info("real-executor backtest started at %s (tick size %s)", bt_instance.now, resolved_tick)

        def on_book(self_, bt_instance, book):
            state["books"] += 1
            market.update(bt_instance)
            if market.mid is None:
                return
            now = bt_instance.now
            host.current_timestamp = now
            connector.set_now(now)
            if now - state["last_tick"] >= update_interval:
                state["last_tick"] = now
                state["ticks"] += 1
                mid = Decimal(str(market.mid))
                _rate_oracle.set_price(f"{_base}-{_quote}", mid)
                _rate_oracle.set_price(f"{_quote}-{_base}", Decimal("1") / mid)
                orchestrator.step_all()
                controller_tick()
                orchestrator.step_all()

        def on_trade(self_, bt_instance, trade):
            market.update(bt_instance)
            market.last_trade = trade.get("px") or market.last_trade
            if market.mid is None:
                return
            host.current_timestamp = bt_instance.now
            connector.set_now(bt_instance.now)

        def on_fill(self_, bt_instance, order: Order, price: float, qty: float, maker: bool):
            result = connector.on_engine_fill(order.id, price, qty, maker)
            if result is not None:
                fee_quote, base_delta = result  
                state["exec_cash"] += -price * base_delta - fee_quote
                state["exec_base"] += base_delta
                state["activity"] += 1

        def on_cancel(self_, bt_instance, order: Order):
            connector.on_engine_cancel(order.id)

        def on_reject(self_, bt_instance, order: Order):
            connector.on_engine_reject(order.id)

    bt.run(_Bridge())

    for info in [executor.executor_info for executor in orchestrator.take_newly_finished()]:
        positions.absorb(info)
    reported = orchestrator.all_reported_infos()

    if state["books"] == 0:
        raise NoDataError("no book updates were replayed; the files may be empty or outside the range.")

    strategy_view = engine._capture_strategy_view(controller, reported, positions.summaries(market.mid))

    reported.sort(key=lambda info: (info.close_timestamp or info.timestamp, info.timestamp))
    results = engine._summarize_results(
        reported, float(controller_config.total_amount_quote), bt._equity)

    reconciliation = _reconcile_fills(state, bt, market)
    results["reconciliation_residual"] = reconciliation["residual"]
    results["realized_pnl_quote"] = results["net_pnl_quote"]
    results["total_pnl_quote"] = reconciliation["engine_pnl_quote"]
    results["unrealized_pnl_quote"] = reconciliation["engine_pnl_quote"] - results["net_pnl_quote"]

    _LOG.info("real-executor backtest finished: %s book updates, %s ticks, %s executors",
              state["books"], state["ticks"], len(reported))

    return {
        "executors": reported,
        "results": results,
        "strategy": strategy_view,
        "candles": engine.backtesting_data_provider.candles_summary(),
        "fills": bt._fills,
        "equity": bt._equity,
        "fees": bt.fees,
        "final_position": bt.position,
        "reconciliation": reconciliation,
        "tick_size": resolved_tick,
    }


def _reconcile_fills(state, bt, market) -> Dict:
    """Check the executors' trading against the engine's own books.

    Unlike the simulator path (which compared each sim's self-marked PnL), this reconciles the
    cash and inventory implied by the executors' order fills against the engine. Every engine order
    is placed by an executor, so ``sum(fill cash) + sum(signed base)*mid`` must equal
    ``bt.cash + bt.position*mid``. A non-zero residual means an orphan fill or a fee mismatch.
    """
    mid = market.mid if market.mid is not None else 0.0
    executor_pnl = state["exec_cash"] + state["exec_base"] * mid
    engine_pnl = bt.cash + bt.position * mid
    residual = executor_pnl - engine_pnl
    scale = max(abs(engine_pnl), 1.0)
    ok = abs(residual) <= 1e-6 * scale
    if not ok:
        _LOG.error("PnL reconciliation failed: executors %.8f vs engine %.8f (residual %.8f)",
                   executor_pnl, engine_pnl, residual)
    return {
        "executor_pnl_quote": executor_pnl,
        "engine_pnl_quote": engine_pnl,
        "residual": residual,
        "ok": bool(ok),
    }
