from __future__ import annotations

import bisect
import logging
import math
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type

import pandas as pd
from pydantic import ValidationError

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import PositionAction, PositionMode, PriceType, TradeType
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.strategy_v2.backtesting.backtesting_data_provider import BacktestingDataProvider
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

_LOG = logging.getLogger(__name__)


class CandlesUnavailableError(NotImplementedError):
    """A controller needs OHLCV bars that could not be obtained from the venue."""


class UnsupportedExecutorError(NotImplementedError):
    """A controller created an executor type this engine cannot simulate."""


class _Market:
    """
    The prices a live executor would read, served from the replayed book and trade tape.
    """

    def __init__(self, tick_size: float):
        self.tick = tick_size
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.last_trade: Optional[float] = None
        self.now: float = 0.0

    def update(self, bt) -> None:
        self.now = bt.now
        self.best_bid = bt.best_bid
        self.best_ask = bt.best_ask

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def price(self, price_type: PriceType, fallback: bool = True) -> Optional[float]:
        """
        A price we can compare against, or None.
        """
        direct = {
            PriceType.MidPrice: self.mid,
            PriceType.BestBid: self.best_bid,
            PriceType.BestAsk: self.best_ask,
            PriceType.LastTrade: self.last_trade,
        }.get(price_type)
        if direct is not None and direct > 0:
            return direct
        if fallback and self.mid is not None and self.mid > 0:
            return self.mid
        return None

    def quantize(self, price: float) -> float:
        if self.tick <= 0 or price <= 0:
            return price
        return math.floor(price / self.tick + 1e-9) * self.tick


class _OrderbookDataProvider(BacktestingDataProvider):
    def __init__(self):
        super().__init__(connectors={})
        self._market: Optional[_Market] = None
        self._key: Optional[str] = None
        self.connectors = {}
        self._closes: Dict[str, List[float]] = {}

    def initialize_rate_sources(self, connector_pairs) -> None:
        """Nothing to do, and the base class would start a task polling live venues for rates."""

    def bind(self, market: _Market, connector_name: str, trading_pair: str) -> None:
        self._market = market
        self._key = f"{connector_name}_{trading_pair}"
        tick = Decimal(str(market.tick)) if market.tick and market.tick > 0 else None
        granular = Decimal("1e-8")
        rule = TradingRule(
            trading_pair=trading_pair,
            min_price_increment=tick if tick else granular,
            min_base_amount_increment=granular,
            min_quote_amount_increment=granular,
        )
        if not isinstance(getattr(self, "trading_rules", None), dict):
            self.trading_rules = {}
        self.trading_rules.setdefault(connector_name, {})[trading_pair] = rule

    def get_price_by_type(self, connector_name: str, trading_pair: str, price_type: PriceType):
        key = f"{connector_name}_{trading_pair}"
        if self._market is None or key != self._key:
            return self.prices.get(key)
        price = self._market.price(price_type)
        return Decimal(str(price)) if price is not None else None

    async def load_candles(self, candles_configs, start_ts: int, end_ts: int) -> None:
        for config in candles_configs:
            key = self._generate_candle_feed_key(config)
            if key in self._closes:
                continue
            what = f"{config.interval} candles for {config.connector} {config.trading_pair}"
            seconds = CandlesBase.interval_to_seconds[config.interval]
            self.update_backtesting_time(start_ts - 5 * seconds, end_ts)
            try:
                frame = await super().get_candles_feed(config)
            except Exception as exc:
                raise CandlesUnavailableError(
                    f"could not fetch {what}: {exc}. This controller decides from those bars and "
                    f"they cannot be substituted, so it was not run. Check that the venue offers "
                    f"this pair and interval (candles_connector / interval in the config).") from exc
            if frame is None or frame.empty:
                raise CandlesUnavailableError(
                    f"the venue returned no {what} between {start_ts} and {end_ts}. This controller "
                    f"decides from those bars and they cannot be substituted, so it was not run.")
            frame = frame.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            self.candles_feeds[key] = frame
            self._closes[key] = [float(stamp) + seconds for stamp in frame["timestamp"]]

    def _closed_count(self, key: str) -> int:
        return bisect.bisect_right(self._closes[key], self._market.now if self._market else math.inf)

    def get_candles_df(self, connector_name: str, trading_pair: str, interval: str,
                       max_records: int = 500):
        key = f"{connector_name}_{trading_pair}_{interval}"
        frame = self.candles_feeds.get(key)
        if frame is None:
            raise CandlesUnavailableError(
                f"controller asked for {interval} candles on {connector_name} {trading_pair}, which "
                f"it did not declare in candles_config, so they were not fetched before the replay. "
                f"Fetched feeds: {sorted(self.candles_feeds) or 'none'}.")
        count = self._closed_count(key)
        if count == 0:
            raise CandlesUnavailableError(
                f"no {interval} candle for {connector_name} {trading_pair} had closed by "
                f"t={self._market.now}: the venue's bars start at {frame['timestamp'].iloc[0]}. "
                f"The indicators have no history to start from.")
        return frame.iloc[max(0, count - max_records):count].copy()

    def candles_summary(self) -> List[Dict]:
        """Which bars a result was computed from, so that it can be judged."""
        summary = []
        for key, frame in self.candles_feeds.items():
            connector_pair, interval = key.rsplit("_", 1)
            summary.append({"feed": connector_pair, "interval": interval, "rows": int(len(frame)),
                            "first": float(frame["timestamp"].iloc[0]),
                            "last": float(frame["timestamp"].iloc[-1])})
        return summary

    def time(self):
        return self._market.now if self._market is not None else self._time


class _PositionBook:
    def __init__(self, position_mode):
        from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
        self._hold_class = PositionHold
        self._position_mode = position_mode
        self._positions: List = []

    def _position_side(self, info: ExecutorInfo) -> Optional[TradeType]:
        if "_perpetual" not in info.connector_name:
            return None
        action = getattr(info.config, "position_action", None)
        if action is not None and self._position_mode == PositionMode.HEDGE:
            opposite = TradeType.BUY if info.config.side == TradeType.SELL else TradeType.SELL
            return opposite if action == PositionAction.CLOSE else info.config.side
        return info.config.side

    def absorb(self, info: ExecutorInfo) -> None:
        if info.close_type != CloseType.POSITION_HOLD:
            return
        side = self._position_side(info)
        for position in self._positions:
            if side is None or position.side == side:
                position.add_orders_from_executor(info)
                return
        position = self._hold_class(info.connector_name, info.trading_pair, side or info.config.side)
        position.add_orders_from_executor(info)
        self._positions.append(position)

    def summaries(self, mid: Optional[float]) -> List:
        if mid is None:
            return []
        price = Decimal(str(mid))
        return [position.get_position_summary(price) for position in self._positions]


class OrderbookBacktestingEngine:
    """
    Runs any strategy_v2 controller against recorded orderbook + trade data.
    """

    def __init__(self):
        self.backtesting_data_provider = _OrderbookDataProvider()

    @staticmethod
    def _get_controller_class(config: ControllerConfigBase) -> Type[ControllerBase]:
        return config.get_controller_class()

    @staticmethod
    def _with_validated_defaults(config: ControllerConfigBase) -> ControllerConfigBase:
        values = config.model_dump()
        try:
            return type(config).model_validate(values)
        except ValidationError as exc:
            rejected = {error["loc"][0] for error in exc.errors() if error["loc"]}
        return type(config).model_validate({name: value for name, value in values.items()
                                            if name not in rejected})

    @staticmethod
    def _require_replayable(config: ControllerConfigBase, pair: str) -> None:
        """Refuse what a single recorded market cannot answer, before building the controller."""
        if config.controller_name == "ai_livestream":
            raise UnsupportedExecutorError(
                "ai_livestream takes its signals from a live MQTT stream, which is not recorded, "
                "so there is nothing to replay it against.")
        missing = [name for name in ("connector_name", "trading_pair")
                   if getattr(config, name, None) is None]
        if missing:
            raise UnsupportedExecutorError(
                f"{type(config).__name__} does not define {', '.join(missing)}. "
                f"Controllers that trade several markets at once (xemm_multiple_levels, "
                f"arbitrage_controller, stat_arb, quantum_grid_allocator) need a replay of all of "
                f"them, which this engine does not provide: it replays a single connector and pair.")
        if config.trading_pair != pair:
            raise ValueError(
                f"the controller trades {config.trading_pair} but the recording "
                f"requested is {pair}. They must match.")

    async def load_candles(self, controller_config: ControllerConfigBase, connector: str, pair: str,
                           start: str, end: Optional[str] = None, base_dir: Optional[str] = None,
                           start_ts: Optional[float] = None, end_ts: Optional[float] = None) -> None:
        self._require_replayable(controller_config, pair)
        config = self._with_validated_defaults(controller_config)
        controller = self._get_controller_class(config)(
            config=config, market_data_provider=self.backtesting_data_provider, actions_queue=None)
        candles_configs = list(getattr(controller.config, "candles_config", None) or [])
        if not candles_configs:
            return
        span = _recording_span(connector, pair, start, end, base_dir)
        if span is not None:
            first, last = span
            if start_ts is not None:
                first = max(first, int(start_ts))
            if end_ts is not None:
                last = min(last, int(math.ceil(end_ts)))
            span = (first, last) if first < last else None
        if span is not None:
            await self.backtesting_data_provider.load_candles(candles_configs, *span)

    # ------------------------------------------------------------------ run

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
        tick_size: Optional[float] = None,
        strict_errors: bool = True,
        base_dir: Optional[str] = None,
        start_ts: Optional[float] = None,
        end_ts: Optional[float] = None,
    ) -> Dict:
        """
        Replay ``connector``/``pair`` over ``[start, end]`` and run the controller against it.

        ``start`` / ``end`` are UTC dates that select the recorded day files. ``start_ts`` / ``end_ts``
        (unix seconds, end exclusive) optionally trim the replay to an exact window inside them.
        """
        from backtest_data import DEFAULT_DIR, require_data
        from backtest_engine import Backtest

        base_dir = base_dir or DEFAULT_DIR
        self._require_replayable(controller_config, pair)
        require_data(connector, pair, start, end, base_dir)

        controller_config = self._with_validated_defaults(controller_config)
        controller_class = self._get_controller_class(controller_config)

        if not controller_config.id:
            import uuid
            controller_config.id = str(uuid.uuid4())[:8]

        await self.load_candles(controller_config, connector, pair, start, end, base_dir,
                                start_ts=start_ts, end_ts=end_ts)

        bt = Backtest(connector, pair, start, end, maker_fee, taker_fee, latency_ms, fill_at_touch, base_dir,
                      start_ts=start_ts, end_ts=end_ts)

        resolved_tick = tick_size if tick_size and tick_size > 0 else _infer_tick_size(
            connector, pair, start, end, base_dir=base_dir)
        market = _Market(resolved_tick)

        data_provider = self.backtesting_data_provider
        data_provider.bind(market, controller_config.connector_name, controller_config.trading_pair)
        controller = controller_class(
            config=controller_config,
            market_data_provider=data_provider,
            actions_queue=None,
        )

        from hummingbot.strategy_v2.backtesting.orderbook_executor_runtime import (
            run_real_executor_backtest,
        )
        return run_real_executor_backtest(
            self, bt, market, controller, controller_config, resolved_tick,
            maker_fee=maker_fee, taker_fee=taker_fee, update_interval=update_interval,
            strict_errors=strict_errors)

    @staticmethod
    def _capture_strategy_view(controller, executors: List[ExecutorInfo], positions: List) -> Dict:
        """
        The controller's own numbers, reported the way the strategy itself reports them.
        """
        view: Dict = {"status": None, "processed_data": None, "errors": [],
                      "positions": [_jsonable(position.model_dump()) for position in positions]}
        controller.executors_info = executors
        controller.positions_held = positions
        try:
            lines = controller.to_format_status()
            view["status"] = [str(line) for line in (lines or [])]
        except Exception as exc:
            view["errors"].append(f"to_format_status() raised: {exc}")
            _LOG.exception("controller.to_format_status() failed")
        try:
            view["processed_data"] = _jsonable(getattr(controller, "processed_data", None))
        except Exception as exc:
            view["errors"].append(f"processed_data could not be serialised: {exc}")
            _LOG.exception("controller.processed_data could not be serialised")
        return view

    # ------------------------------------------------------------------ summary

    @staticmethod
    def _summarize_results(executors_info: List[ExecutorInfo], total_amount_quote: float = 1000,
                           equity: Optional[List[Tuple]] = None) -> Dict:
        empty = {
            "net_pnl": 0.0, "net_pnl_quote": 0.0, "total_executors": 0,
            "total_executors_with_position": 0, "total_volume": 0.0,
            "total_long": 0, "total_short": 0, "close_types": {},
            "accuracy_long": 0.0, "accuracy_short": 0.0, "total_positions": 0,
            "accuracy": 0.0, "max_drawdown_usd": 0.0, "max_drawdown_pct": 0.0,
            "profit_factor": None, "win_signals": 0, "loss_signals": 0,
        }
        if not executors_info:
            return empty

        df = pd.DataFrame([info.to_dict() for info in executors_info])
        df["net_pnl_quote"] = df["net_pnl_quote"].astype(float)             
        df["filled_amount_quote"] = df["filled_amount_quote"].astype(float)

        net_pnl_quote = float(df["net_pnl_quote"].sum()) 
        total_executors = len(df)

        traded = df[df["filled_amount_quote"] > 0]       
        total_with_pos = len(traded)
        total_volume = float(traded["filled_amount_quote"].sum()) if total_with_pos else 0.0

        longs = traded[traded["side"] == TradeType.BUY] if total_with_pos else df.iloc[:0]     # what is this do
        shorts = traded[traded["side"] == TradeType.SELL] if total_with_pos else df.iloc[:0]
        accuracy_long = float((longs["net_pnl_quote"] > 0).mean()) if len(longs) else 0.0   # how is this
        accuracy_short = float((shorts["net_pnl_quote"] > 0).mean()) if len(shorts) else 0.0

        close_types = {}
        if "close_type" in df.columns:
            names = df["close_type"].apply(lambda x: x.name if hasattr(x, "name") else str(x))
            close_types = names.value_counts().to_dict()
            close_types = {str(k): int(v) for k, v in close_types.items()}

        wins = traded[traded["net_pnl_quote"] > 0]
        losses = traded[traded["net_pnl_quote"] < 0]
        accuracy = float(len(wins) / total_with_pos) if total_with_pos else 0.0

        total_won = float(wins["net_pnl_quote"].sum()) if len(wins) else 0.0
        total_loss = float(-losses["net_pnl_quote"].sum()) if len(losses) else 0.0
        profit_factor = (total_won / total_loss) if total_loss > 0 else None

        max_drawdown = 0.0
        if equity:
            pnl = pd.Series([point[1] for point in equity], dtype=float)
            max_drawdown = float((pnl - pnl.cummax()).min())
        max_drawdown_pct = max_drawdown / total_amount_quote if total_amount_quote else 0.0

        return {
            "net_pnl": net_pnl_quote / total_amount_quote if total_amount_quote else 0.0,
            "net_pnl_quote": net_pnl_quote,
            "total_executors": total_executors,
            "total_executors_with_position": total_with_pos,
            "total_volume": total_volume,
            "total_long": int(len(longs)),
            "total_short": int(len(shorts)),
            "close_types": close_types,
            "accuracy_long": accuracy_long,
            "accuracy_short": accuracy_short,
            "total_positions": total_with_pos,
            "accuracy": accuracy,
            "max_drawdown_usd": max_drawdown,
            "max_drawdown_pct": float(max_drawdown_pct),
            "profit_factor": profit_factor,
            "win_signals": int(len(wins)),
            "loss_signals": int(len(losses)),
        }

# ---------------------------------------------------------------------------- helpers


def _jsonable(value, _depth: int = 0):
    """Convert a value to something JSON-serializable, or a string if it cannot be represented."""
    if _depth > 4:
        return str(value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        converted = float(value)
        return converted if math.isfinite(converted) else None
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, pd.DataFrame):
        latest = value.iloc[-1].to_dict() if len(value) else {}
        return {"_type": "DataFrame", "rows": int(len(value)),
                "columns": [str(column) for column in value.columns],
                "latest": {str(column): _jsonable(item, _depth + 1) for column, item in latest.items()}}
    if isinstance(value, pd.Series):
        return {"_type": "Series", "rows": int(len(value))}
    if isinstance(value, dict):
        return {str(key): _jsonable(item, _depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item, _depth + 1) for item in value]
    return str(value)


def _recording_span(connector: str, pair: str, start: str, end: Optional[str],
                    base_dir: Optional[str] = None) -> Optional[Tuple[int, int]]:
    from backtest_data import DEFAULT_DIR, iter_rows

    base_dir = base_dir or DEFAULT_DIR
    first = next(iter_rows(connector, pair, "book", start, end, base_dir), None)
    if first is None:
        return None
    last_day = date.fromisoformat(end or start)
    day = last_day
    while day >= date.fromisoformat(start):
        last = None
        for last in iter_rows(connector, pair, "book", day.isoformat(), day.isoformat(), base_dir):
            pass
        if last is not None:
            return int(first["ts"]), int(last["ts"]) + 1
        day -= timedelta(days=1)
    return None


def _infer_tick_size(connector: str, pair: str, start: str, end: Optional[str],
                     books_to_scan: int = 500, base_dir: Optional[str] = None) -> float:
    from backtest_data import DEFAULT_DIR, replay

    decimals = 0
    smallest_gap = math.inf
    scanned = 0
    for kind, _ts, event in replay(connector, pair, start, end, base_dir or DEFAULT_DIR):
        if kind != "book":
            continue
        scanned += 1
        for side in ("b", "a"):
            prices = [level[0] for level in event.get(side) or []]
            for price in prices:
                text = repr(float(price))
                if "e" in text or "E" in text:
                    continue
                if "." in text:
                    places = len(text.split(".")[1].rstrip("0"))
                    decimals = max(decimals, places)
            for first, second in zip(prices, prices[1:]):
                gap = abs(first - second)
                if 0 < gap < smallest_gap:
                    smallest_gap = gap
        if scanned >= books_to_scan:
            break

    if decimals == 0 and not math.isfinite(smallest_gap):
        _LOG.warning("could not infer a tick size for %s %s; pass tick_size explicitly",
                     connector, pair)
        return 0.0
    tick = round(10.0 ** -decimals, 12)
    if math.isfinite(smallest_gap) and smallest_gap < tick * 0.999:
        _LOG.warning("inferred tick %s for %s %s is coarser than the smallest observed gap "
                     "%s; using the gap instead", tick, connector, pair, smallest_gap)
        tick = smallest_gap
    _LOG.info("inferred tick size %s for %s %s (%s decimals, smallest gap %s)",
              tick, connector, pair, decimals, smallest_gap)
    return tick


