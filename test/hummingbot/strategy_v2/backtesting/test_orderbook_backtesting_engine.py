"""
The orderbook engine against small scripted recordings.

Each test scripts a market and the executors a controller asks for, then checks that the simulated
executor did what the live one does. Every run also has to pass the engine's reconciliation: the
executors' PnL must equal the engine's own cash-and-position books.
"""
import asyncio
import gzip
import json
import os
import shutil
import sys
import tempfile
import unittest
from decimal import Decimal
from typing import Callable, List, Optional

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from hummingbot.core.data_type.common import OrderType, TradeType  # noqa: E402
from hummingbot.strategy_v2.backtesting.orderbook_backtesting_engine import (  # noqa: E402
    CandlesUnavailableError,
    OrderbookBacktestingEngine,
    UnsupportedExecutorError,
    _Market,
    _OrderbookDataProvider,
)
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase  # noqa: E402
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig, DCAMode  # noqa: E402
from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig  # noqa: E402
from hummingbot.strategy_v2.executors.order_executor.data_types import (  # noqa: E402
    ExecutionStrategy,
    OrderExecutorConfig,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (  # noqa: E402
    PositionExecutorConfig,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction  # noqa: E402
from hummingbot.strategy_v2.models.executors import CloseType  # noqa: E402

CONNECTOR, PAIR, DAY = "coindcx", "BTC-USDT", "2026-01-01"
T0 = 1_767_225_600.0


class ScriptedController(ControllerBase):
    script: Optional[Callable] = None
    instances: List["ScriptedController"] = []

    def __init__(self, config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.positions_seen = []
        ScriptedController.instances.append(self)

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self):
        self.positions_seen.append([(p.side, float(p.amount)) for p in self.positions_held])
        return type(self).script(self) or []


class ScriptedConfig(ControllerConfigBase):
    controller_name: str = "scripted"
    connector_name: str = CONNECTOR
    trading_pair: str = PAIR
    total_amount_quote: Decimal = Decimal("1000")

    def get_controller_class(self):
        return ScriptedController


def once(*executor_configs):
    """A script that creates these executors on the first tick and then does nothing."""
    state = {"sent": False}

    def script(controller):
        if state["sent"]:
            return []
        state["sent"] = True
        return [CreateExecutorAction(controller_id="c", executor_config=config) for config in executor_configs]
    return script


def position(side=TradeType.BUY, entry=99.0, amount=1.0, **barriers):
    return PositionExecutorConfig(timestamp=T0, connector_name=CONNECTOR, trading_pair=PAIR, side=side,
                                  entry_price=Decimal(str(entry)), amount=Decimal(str(amount)),
                                  triple_barrier_config=TripleBarrierConfig(**barriers))


def dca(prices, stop_loss=None, take_profit=None, **kwargs):
    return DCAExecutorConfig(timestamp=T0, connector_name=CONNECTOR, trading_pair=PAIR, side=TradeType.BUY,
                             amounts_quote=[Decimal(str(p)) for p in prices],
                             prices=[Decimal(str(p)) for p in prices], mode=DCAMode.MAKER,
                             stop_loss=Decimal(str(stop_loss)) if stop_loss else None,
                             take_profit=Decimal(str(take_profit)) if take_profit else None, **kwargs)


class EngineTestCase(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.base_dir, ignore_errors=True)
        ScriptedController.instances = []

    def record(self, books, trades=()):
        """books: (seconds, bid, ask); trades: (seconds, price, qty, taker side)."""
        directory = os.path.join(self.base_dir, CONNECTOR, PAIR)
        os.makedirs(directory, exist_ok=True)
        with gzip.open(os.path.join(directory, f"{DAY}_book.jsonl.gz"), "wt") as f:
            for uid, (seconds, bid, ask) in enumerate(books):
                f.write(json.dumps({"ts": T0 + seconds, "uid": uid, "b": [[bid, 100.0]], "a": [[ask, 100.0]]}) + "\n")
        with gzip.open(os.path.join(directory, f"{DAY}_trades.jsonl.gz"), "wt") as f:
            for seconds, price, qty, side in trades:
                f.write(json.dumps({"ts": T0 + seconds, "ets": T0 + seconds, "id": 1, "px": price, "qty": qty,
                                    "side": side, "bb": 0, "ba": 0}) + "\n")

    def run_engine(self, script, config=None, maker_fee=0.0, taker_fee=0.0):
        ScriptedController.script = staticmethod(script)
        result = asyncio.run(OrderbookBacktestingEngine().run_backtesting(
            config or ScriptedConfig(id="c"), CONNECTOR, PAIR, DAY, maker_fee=maker_fee, taker_fee=taker_fee,
            tick_size=0.01, base_dir=self.base_dir))
        self.assertTrue(result["reconciliation"]["ok"], result["reconciliation"])
        return result


FLAT = [(s, 99.95, 100.05) for s in range(0, 30)]


class PositionExecutorTest(EngineTestCase):
    def test_entry_rests_when_the_market_runs_away(self):
        # SimpleGrid would cross here, one stop distance the wrong way. A position executor waits.
        books = [(s, 99.95 + s * 0.2, 100.05 + s * 0.2) for s in range(30)]
        self.record(books)
        result = self.run_engine(once(position(stop_loss=Decimal("0.01"), take_profit=Decimal("0.01"))))
        self.assertEqual([], result["fills"])

    def test_unfilled_leg_still_honours_its_time_limit(self):
        self.record(FLAT)
        result = self.run_engine(once(position(time_limit=5)))
        info = result["executors"][0]
        self.assertEqual(CloseType.TIME_LIMIT, info.close_type)
        self.assertLess(info.close_timestamp - T0, 8)
        self.assertEqual([], result["fills"])

    def test_limit_take_profit_needs_a_trade_through_it(self):
        books = FLAT + [(s, 100.5, 100.7) for s in range(30, 40)]
        self.record(books, [(5, 98.9, 1.0, "sell"), (35, 100.6, 1.0, "buy")])
        tbc = {"take_profit": Decimal("0.01"), "take_profit_order_type": OrderType.LIMIT}
        result = self.run_engine(once(position(**tbc)))
        # The market reached 99 * 1.01 = 99.99 and went through it, but only that trade fills it.
        info = result["executors"][0]
        self.assertEqual(CloseType.TAKE_PROFIT, info.close_type)
        self.assertEqual(2, len(result["fills"]))
        self.assertAlmostEqual(0.99, float(info.net_pnl_quote), places=6)

    def test_market_take_profit_is_judged_net_of_fees(self):
        # Bought at 99, bid 100: 1.01% gross clears the 0.95% target, but the entry fee takes
        # 0.1% of that and leaves 0.91%, so the leg stays open.
        books = FLAT[:10] + [(s, 100.0, 100.1) for s in range(10, 20)]
        self.record(books, [(5, 98.9, 1.0, "sell")])
        result = self.run_engine(once(position(take_profit=Decimal("0.0095"))), maker_fee=0.001, taker_fee=0.001)
        self.assertEqual(1, len(result["fills"]))

    def test_stop_loss_exits_by_crossing_at_the_bid(self):
        books = FLAT[:10] + [(s, 97.0, 97.1) for s in range(10, 20)]
        self.record(books, [(5, 98.9, 1.0, "sell")])
        result = self.run_engine(once(position(stop_loss=Decimal("0.01"))))
        info = result["executors"][0]
        self.assertEqual(CloseType.STOP_LOSS, info.close_type)
        self.assertEqual(97.0, result["fills"][-1]["price"])
        self.assertFalse(result["fills"][-1]["maker"])

    def test_trailing_stop_ratchets_then_fires(self):
        from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop
        path = [(s, 99.95, 100.05) for s in range(10)] + [(s, 103.0, 103.1) for s in range(10, 15)] + \
               [(s, 105.0, 105.1) for s in range(15, 20)] + [(s, 103.5, 103.6) for s in range(20, 25)]
        self.record(path, [(5, 98.9, 1.0, "sell")])
        trailing = TrailingStop(activation_price=Decimal("0.02"), trailing_delta=Decimal("0.01"))
        result = self.run_engine(once(position(trailing_stop=trailing)))
        self.assertEqual(CloseType.TRAILING_STOP, result["executors"][0].close_type)
        self.assertEqual(103.5, result["fills"][-1]["price"])

    def test_activation_bounds_keep_a_far_order_out_of_the_book(self):
        self.record(FLAT)
        config = position(entry=90.0)
        config.activation_bounds = [Decimal("0.01"), Decimal("0.02")]
        observed = []

        def script(controller):
            observed.extend(info.custom_info["order_ids"] for info in controller.executors_info)
            return once_script(controller)
        once_script = once(config)
        self.run_engine(script)
        self.assertTrue(observed)
        self.assertTrue(all(ids == [] for ids in observed))


class DCAExecutorTest(EngineTestCase):
    def test_every_level_fills_and_the_average_moves(self):
        # The first fill must not cancel the next level: the ladder is the strategy.
        self.record(FLAT, [(5, 98.9, 1.0, "sell"), (10, 97.9, 1.0, "sell")])
        result = self.run_engine(once(dca([99.0, 98.0])))
        self.assertEqual(2, len(result["fills"]))
        info = result["executors"][0]
        self.assertAlmostEqual(98.5, info.custom_info["current_position_average_price"])

    def test_maker_stop_loss_waits_for_every_level(self):
        books = FLAT[:10] + [(s, 96.0, 96.1) for s in range(10, 20)]
        self.record(books, [(5, 98.9, 1.0, "sell")])
        # Level two at 90 never fills, so the 1% stop stays disarmed though the loss is past it.
        result = self.run_engine(once(dca([99.0, 90.0], stop_loss=0.01)))
        self.assertEqual(1, len(result["fills"]))
        self.assertNotEqual(CloseType.STOP_LOSS, result["executors"][0].close_type)

    def test_take_profit_closes_at_market_and_pulls_the_other_levels(self):
        books = FLAT[:10] + [(s, 101.0, 101.1) for s in range(10, 20)]
        self.record(books, [(5, 98.9, 1.0, "sell")])
        result = self.run_engine(once(dca([99.0, 90.0], take_profit=0.01)))
        info = result["executors"][0]
        self.assertEqual(CloseType.TAKE_PROFIT, info.close_type)
        self.assertEqual(0.0, result["final_position"])
        self.assertEqual(101.0, result["fills"][-1]["price"])


class HeldPositionsTest(EngineTestCase):
    def test_order_executor_hands_the_controller_a_position(self):
        self.record(FLAT)
        order = OrderExecutorConfig(timestamp=T0, connector_name=CONNECTOR, trading_pair=PAIR, side=TradeType.BUY,
                                    amount=Decimal("0.5"), execution_strategy=ExecutionStrategy.MARKET)
        result = self.run_engine(once(order))
        self.assertEqual(CloseType.POSITION_HOLD, result["executors"][0].close_type)
        self.assertEqual(0.5, result["final_position"])
        seen = ScriptedController.instances[-1].positions_seen
        self.assertEqual([], seen[0])
        self.assertEqual([(TradeType.BUY, 0.5)], seen[-1])
        self.assertEqual(1, len(result["strategy"]["positions"]))

    def test_opposite_orders_net_out(self):
        self.record(FLAT)

        def order(side):
            return OrderExecutorConfig(timestamp=T0, connector_name=CONNECTOR, trading_pair=PAIR, side=side,
                                       amount=Decimal("0.5"), execution_strategy=ExecutionStrategy.MARKET)
        result = self.run_engine(once(order(TradeType.BUY), order(TradeType.SELL)))
        self.assertEqual(0.0, result["final_position"])
        self.assertEqual(0.0, float(result["strategy"]["positions"][0]["amount"]))

    def test_early_stop_keeping_the_position_holds_it(self):
        self.record(FLAT, [(5, 98.9, 1.0, "sell")])
        config = position()
        created = once(config)
        state = {"stopped": False}

        def script(controller):
            actions = created(controller)
            filled = [info for info in controller.executors_info if info.is_trading]
            if filled and not state["stopped"]:
                state["stopped"] = True
                actions = [StopExecutorAction(controller_id="c", executor_id=filled[0].id, keep_position=True)]
            return actions
        result = self.run_engine(script)
        self.assertEqual(CloseType.POSITION_HOLD, result["executors"][0].close_type)
        self.assertEqual(1.0, result["final_position"])
        self.assertEqual([(TradeType.BUY, 1.0)], ScriptedController.instances[-1].positions_seen[-1])

    def test_early_stop_with_nothing_filled_reports_early_stop(self):
        self.record(FLAT)
        config = position()
        created = once(config)
        state = {"ticks": 0}

        def script(controller):
            state["ticks"] += 1
            if state["ticks"] == 3:
                return [StopExecutorAction(controller_id="c", executor_id=config.id, keep_position=True)]
            return created(controller)
        result = self.run_engine(script)
        self.assertEqual(CloseType.EARLY_STOP, result["executors"][0].close_type)


class GridExecutorTest(EngineTestCase):
    def grid(self, **kwargs):
        defaults = dict(timestamp=T0, connector_name=CONNECTOR, trading_pair=PAIR, side=TradeType.BUY,
                        start_price=Decimal("99"), end_price=Decimal("101"), limit_price=Decimal("95"),
                        total_amount_quote=Decimal("100"), min_spread_between_orders=Decimal("0.005"),
                        min_order_amount_quote=Decimal("5"), max_open_orders=5, max_orders_per_batch=5,
                        order_frequency=0, triple_barrier_config=TripleBarrierConfig(take_profit=Decimal("0.001")))
        return GridExecutorConfig(**{**defaults, **kwargs})

    def test_leaving_the_top_of_the_range_is_a_take_profit(self):
        books = FLAT[:10] + [(s, 102.0, 102.1) for s in range(10, 15)]
        self.record(books)
        result = self.run_engine(once(self.grid()))
        self.assertEqual(CloseType.TAKE_PROFIT, result["executors"][0].close_type)

    def test_limit_price_with_keep_position_holds_inventory(self):
        books = FLAT[:10] + [(s, 94.0, 94.1) for s in range(10, 15)]
        self.record(books, [(5, 98.0, 100.0, "sell")])
        result = self.run_engine(once(self.grid(keep_position=True)))
        info = result["executors"][0]
        self.assertEqual(CloseType.POSITION_HOLD, info.close_type)
        self.assertGreater(result["final_position"], 0)


class RefusalTest(EngineTestCase):
    def test_a_config_for_another_pair_is_refused(self):
        self.record(FLAT)
        with self.assertRaisesRegex(ValueError, "must match"):
            self.run_engine(once(), config=ScriptedConfig(id="c", trading_pair="WLD-USDT"))

    def test_a_config_without_a_single_market_is_refused(self):
        self.record(FLAT)

        class TwoMarkets(ControllerConfigBase):
            controller_name: str = "two_markets"

            def get_controller_class(self):
                return ScriptedController
        with self.assertRaises(UnsupportedExecutorError):
            self.run_engine(once(), config=TwoMarkets(id="c"))


    def test_a_controller_driven_by_a_live_stream_is_refused(self):
        self.record(FLAT)
        with self.assertRaisesRegex(UnsupportedExecutorError, "MQTT"):
            self.run_engine(once(), config=ScriptedConfig(id="c", controller_name="ai_livestream"))


class CandlesTest(unittest.TestCase):
    def provider(self, now):
        provider = _OrderbookDataProvider()
        market = _Market(0.01)
        market.now = now
        provider.bind(market, CONNECTOR, PAIR)
        stamps = [T0 + 60 * i for i in range(10)]
        provider.candles_feeds[f"{CONNECTOR}_{PAIR}_1m"] = pd.DataFrame({"timestamp": stamps, "close": range(10)})
        provider._closes[f"{CONNECTOR}_{PAIR}_1m"] = [stamp + 60 for stamp in stamps]
        return provider, market

    def test_a_bar_is_invisible_until_it_has_closed(self):
        provider, market = self.provider(T0 + 60 * 3 - 0.001)
        self.assertEqual(1, provider.get_candles_df(CONNECTOR, PAIR, "1m").iloc[-1]["close"])
        market.now = T0 + 60 * 3
        self.assertEqual(2, provider.get_candles_df(CONNECTOR, PAIR, "1m").iloc[-1]["close"])

    def test_max_records_counts_back_from_the_clock(self):
        provider, _ = self.provider(T0 + 60 * 6)
        frame = provider.get_candles_df(CONNECTOR, PAIR, "1m", max_records=2)
        self.assertEqual([4, 5], list(frame["close"]))

    def test_a_controller_adding_columns_does_not_touch_the_feed(self):
        provider, _ = self.provider(T0 + 60 * 6)
        frame = provider.get_candles_df(CONNECTOR, PAIR, "1m")
        frame["signal"] = 1
        self.assertNotIn("signal", provider.candles_feeds[f"{CONNECTOR}_{PAIR}_1m"].columns)

    def test_an_undeclared_feed_and_an_empty_history_are_refused(self):
        provider, _ = self.provider(T0)
        with self.assertRaisesRegex(CandlesUnavailableError, "did not declare"):
            provider.get_candles_df(CONNECTOR, PAIR, "5m")
        with self.assertRaisesRegex(CandlesUnavailableError, "had closed"):
            provider.get_candles_df(CONNECTOR, PAIR, "1m")


class ConfigDefaultsTest(unittest.TestCase):
    def test_defaults_go_through_the_validators(self):
        from controllers.generic.pmm import PMMConfig
        config = OrderbookBacktestingEngine._with_validated_defaults(PMMConfig(id="c"))
        self.assertEqual(OrderType.LIMIT_MAKER, config.take_profit_order_type)
        self.assertEqual(len(config.buy_spreads), len(config.buy_amounts_pct))

    def test_a_default_its_own_type_rejects_is_left_alone(self):
        from controllers.directional_trading.dman_v3 import DManV3ControllerConfig
        config = OrderbookBacktestingEngine._with_validated_defaults(DManV3ControllerConfig(id="c"))
        self.assertIsNone(config.dynamic_target)


if __name__ == "__main__":
    unittest.main()
