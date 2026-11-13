import importlib.util
import os
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.core.data_type.option_info import OptionInstrumentInfo, OptionType
from hummingbot.core.data_type.trade_fee import TokenAmount

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts", "options_strangle_example.py")
_spec = importlib.util.spec_from_file_location("options_strangle_example", SCRIPT)
example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(example)

EXPIRY = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()


class CrossingPriceTests(unittest.TestCase):
    def test_a_buy_crosses_up_onto_the_tick_grid(self):
        self.assertEqual(Decimal("102.5"), example.crossing_price(Decimal("100.3"), True, Decimal("0.02"), Decimal("0.5")))

    def test_a_buy_stays_inside_the_band(self):
        price = example.crossing_price(Decimal("100.3"), True, Decimal("0.02"), Decimal("0.5"),
                                       band_upper=Decimal("101.2"))

        self.assertEqual(Decimal("101.0"), price)

    def test_a_sell_crosses_down_and_stays_inside_the_band(self):
        self.assertEqual(Decimal("98.0"), example.crossing_price(Decimal("100"), False, Decimal("0.02"), Decimal("0.5")))
        self.assertEqual(Decimal("98.5"), example.crossing_price(Decimal("100"), False, Decimal("0.02"), Decimal("0.5"),
                                                                 band_lower=Decimal("98.3")))


class HedgeOrderTests(unittest.TestCase):
    CONTRACT = Decimal("0.001")
    THRESHOLD = Decimal("0.0006")

    def order(self, net, hedge):
        return example.hedge_order(Decimal(net), Decimal(hedge), self.THRESHOLD, self.CONTRACT)

    def test_nothing_below_the_threshold(self):
        self.assertIsNone(self.order("0.0005", "0"))
        self.assertIsNone(self.order("-0.0005", "0"))

    def test_a_long_delta_is_hedged_by_selling_whole_contracts(self):
        self.assertEqual((TradeType.SELL, Decimal("0.001"), PositionAction.OPEN), self.order("0.0006", "0"))
        self.assertEqual((TradeType.BUY, Decimal("0.002"), PositionAction.OPEN), self.order("-0.0016", "0"))

    def test_an_order_that_reduces_the_hedge_closes_and_never_flips_it(self):
        self.assertEqual((TradeType.BUY, Decimal("0.001"), PositionAction.CLOSE), self.order("-0.0026", "-0.001"))
        self.assertEqual((TradeType.SELL, Decimal("0.001"), PositionAction.CLOSE), self.order("0.0012", "0.001"))

    def test_hedges_do_not_flip_flop(self):
        side, amount, _ = self.order("0.0006", "0")
        left = Decimal("0.0006") - amount

        self.assertIsNone(self.order(str(left), str(-amount)))


class InitMarketsTests(unittest.TestCase):
    def config(self, **fields):
        return example.OptionsStrangleConfig(**fields)

    def test_preset_options_and_the_hedge_market(self):
        config = self.config(call_pair="C_BTC_102000_251226-USD", put_pair="P_BTC_98000_251226-USD")

        example.OptionsStrangleExample.init_markets(config)

        self.assertEqual({"delta_options": {"C_BTC_102000_251226-USD", "P_BTC_98000_251226-USD"},
                          "delta_perpetual": {"BTC-USD"}}, example.OptionsStrangleExample.markets)

    def test_without_hedge_only_the_options(self):
        config = self.config(call_pair="C_BTC_102000_251226-USD", put_pair="P_BTC_98000_251226-USD", hedge=False)

        example.OptionsStrangleExample.init_markets(config)

        self.assertEqual(["delta_options"], list(example.OptionsStrangleExample.markets))

    def test_options_are_picked_from_delta_when_not_set(self):
        chain = [OptionInstrumentInfo(f"{kind}_BTC_{strike}_251226-USD", "BTC",
                                      OptionType.CALL if kind == "C" else OptionType.PUT, Decimal(strike), EXPIRY)
                 for kind in ("C", "P") for strike in (98000, 100000, 102000)]
        config = self.config()

        with patch.object(example, "fetch_option_chain", return_value=(chain, Decimal("100100"))), \
                patch("time.time", return_value=EXPIRY - 5 * 3600):
            example.OptionsStrangleExample.init_markets(config)

        self.assertEqual(("C_BTC_102000_251226-USD", "P_BTC_98000_251226-USD"), (config.call_pair, config.put_pair))

    def test_no_options_listed_stops_the_start(self):
        with patch.object(example, "fetch_option_chain", return_value=([], None)):
            with self.assertRaises(ValueError):
                example.OptionsStrangleExample.init_markets(self.config())


CALL, PUT, PERP = "C_BTC_102000_251226-USD", "P_BTC_98000_251226-USD", "BTC-USD"
NOW = EXPIRY - 3600


class HedgingTests(unittest.IsolatedAsyncioTestCase):
    """The strategy's decisions, against stand-in connectors (the cycle itself is rehearsed offline)."""

    def setUp(self):
        self.timestamp = patch.object(example.OptionsStrangleExample, "current_timestamp",
                                      new=property(lambda strategy: NOW))
        self.timestamp.start()
        self.addCleanup(self.timestamp.stop)
        self.options, self.perp = MagicMock(), MagicMock()
        self.options.get_instrument.return_value = SimpleNamespace(expiry_timestamp=EXPIRY)
        self.held = {CALL: Decimal("0.001"), PUT: Decimal("0.001")}
        self.options.get_position.side_effect = lambda pair: (
            SimpleNamespace(amount=self.held[pair]) if self.held.get(pair) else None)
        self.deltas = {CALL: Decimal("0.70"), PUT: Decimal("-0.05")}
        self.options.get_option_info.side_effect = lambda pair: SimpleNamespace(delta=self.deltas[pair])
        self.options.flatten = AsyncMock(return_value=[])
        self.hedge = Decimal("0")
        type(self.perp).account_positions = PropertyMock(side_effect=lambda: (
            {PERP: SimpleNamespace(trading_pair=PERP, amount=self.hedge)} if self.hedge else {}))
        self.perp.trading_rules = {PERP: SimpleNamespace(min_order_size=Decimal("0.001"),
                                                         min_price_increment=Decimal("0.5"))}
        book = SimpleNamespace(ask_entries=lambda: [SimpleNamespace(price=100010.0)],
                               bid_entries=lambda: [SimpleNamespace(price=100000.0)])
        self.perp.get_order_book.return_value = book
        self.strategy = self._strategy()

    def _strategy(self, **settings):
        config = example.OptionsStrangleConfig(call_pair=CALL, put_pair=PUT, **settings)
        strategy = example.OptionsStrangleExample.__new__(example.OptionsStrangleExample)
        strategy.connectors = {"delta_options": self.options, "delta_perpetual": self.perp}
        strategy.config, strategy.legs, strategy.state = config, [CALL, PUT], "holding"
        strategy.settlements, strategy.fills, strategy._entry_orders = {}, [], {}
        strategy._close_order, strategy._next_hedge_check, strategy._said = None, 0.0, set()
        strategy.ready_to_trade = True
        strategy.active = []
        strategy.get_active_orders = MagicMock(side_effect=lambda name: strategy.active)
        strategy.buy, strategy.sell = MagicMock(return_value="buy-1"), MagicMock(return_value="sell-1")
        return strategy

    def test_a_net_delta_past_the_threshold_is_hedged_with_the_perpetual(self):
        # 0.70 x 0.001 - 0.05 x 0.001 = 0.00065 BTC
        self.strategy.on_tick()

        self.strategy.sell.assert_called_once_with("delta_perpetual", PERP, Decimal("0.001"), example.OrderType.LIMIT,
                                                   Decimal("98000.0"), position_action=PositionAction.OPEN)

    def test_one_hedge_order_at_a_time_and_only_every_interval(self):
        self.strategy.active = [SimpleNamespace(client_order_id="hedge-1")]
        self.strategy.on_tick()
        self.strategy.sell.assert_not_called()

        self.strategy.active = []
        self.strategy.on_tick()  # within the 10 s interval of the last check
        self.strategy.sell.assert_not_called()

    def test_a_threshold_under_half_a_contract_is_raised(self):
        strategy = self._strategy(hedge_threshold=Decimal("0.0003"))
        self.deltas[CALL] = Decimal("0.55")  # net 0.0005 BTC: over 0.0003, under the raised 0.0006

        strategy.on_tick()

        self.assertEqual(Decimal("0.0006"), strategy.hedge_threshold())
        strategy.sell.assert_not_called()

    def test_expiry_then_settlement_then_the_hedge_is_closed(self):
        self.hedge = Decimal("-0.001")
        self.strategy.fills = [SimpleNamespace(trading_pair=CALL, trade_type=TradeType.BUY),
                               SimpleNamespace(trading_pair=PUT, trade_type=TradeType.BUY)]
        with patch.object(example.OptionsStrangleExample, "current_timestamp", new=property(lambda s: EXPIRY + 5)):
            self.strategy.on_tick()
            self.assertEqual("settling", self.strategy.state)
            self.strategy.settlements = {CALL: object(), PUT: object()}
            self.strategy.on_tick()
            self.assertEqual("closing hedge", self.strategy.state)
            self.strategy.on_tick()

        self.strategy.buy.assert_called_once_with("delta_perpetual", PERP, Decimal("0.001"), example.OrderType.LIMIT,
                                                  Decimal("102010.5"), position_action=PositionAction.CLOSE)

    async def test_a_stop_before_expiry_closes_the_legs_and_the_hedge(self):
        self.hedge = Decimal("0.001")

        with patch.object(example, "STOP_TIMEOUT", 0), patch("time.time", return_value=NOW):
            await self.strategy.on_stop()

        self.options.flatten.assert_awaited_once()
        self.strategy.sell.assert_called_once_with("delta_perpetual", PERP, Decimal("0.001"), example.OrderType.LIMIT,
                                                   Decimal("98000.0"), position_action=PositionAction.CLOSE)
        self.assertEqual("stopped", self.strategy.state)


class ResultTests(unittest.TestCase):
    @staticmethod
    def event(trade_type, amount, price, fee):
        return SimpleNamespace(trade_type=trade_type, amount=Decimal(amount), price=Decimal(price),
                               trade_fee=SimpleNamespace(flat_fees=[TokenAmount(token="USD", amount=Decimal(fee))]))

    def test_cash_from_fills_and_settlements_after_fees(self):
        strategy = example.OptionsStrangleExample.__new__(example.OptionsStrangleExample)
        strategy.fills = [self.event(TradeType.BUY, "0.001", "120", "0.001"),     # call bought
                          self.event(TradeType.BUY, "0.001", "80", "0.001"),      # put bought
                          self.event(TradeType.SELL, "0.001", "100000", "0.05"),  # hedge sold
                          self.event(TradeType.BUY, "0.001", "99900", "0.05")]    # hedge bought back
        strategy.settlements = {"call": self.event(TradeType.SELL, "0.001", "2000", "0.0088"),
                                "put": self.event(TradeType.SELL, "0.001", "0", "0")}

        # Options: -0.12 - 0.08 + 2.0 - 0.0108; hedge: +100 - 99.9 - 0.1
        self.assertEqual("Done: the strangle and its hedge made +1.789200 USD after fees.", strategy.result())
