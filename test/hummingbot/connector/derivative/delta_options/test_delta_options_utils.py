import unittest
from datetime import datetime, timezone
from decimal import Decimal

from hummingbot.connector.derivative.delta_options import delta_options_utils as utils
from hummingbot.core.data_type.option_info import OptionType


def option_product(symbol="C-BTC-100000-251226", contract_type="call_options", state="live", **overrides):
    product = {
        "id": 101,
        "symbol": symbol,
        "contract_type": contract_type,
        "state": state,
        "strike_price": symbol.split("-")[2],
        "settlement_time": "2026-12-25T12:00:00Z",
        "contract_value": "0.001",
        "tick_size": "0.1",
        "underlying_asset": {"symbol": "BTC"},
        "quoting_asset": {"symbol": "USD"},
        "settling_asset": {"symbol": "USD"},
        "barrier_price": None,
    }
    product.update(overrides)
    return product


class DeltaOptionsUtilsTests(unittest.TestCase):
    def test_trading_pair_has_a_single_dash(self):
        self.assertEqual("C_BTC_100000_251226-USD", utils.trading_pair_from_symbol("C-BTC-100000-251226", "usd"))

    def test_only_live_vanilla_options_are_accepted(self):
        self.assertTrue(utils.is_live_vanilla_option(option_product()))
        self.assertTrue(utils.is_live_vanilla_option(option_product("P-ETH-3000-251226", "put_options")))
        self.assertFalse(utils.is_live_vanilla_option(option_product(contract_type="perpetual_futures")))
        self.assertFalse(utils.is_live_vanilla_option(option_product(state="expired")))
        self.assertFalse(utils.is_live_vanilla_option(option_product(barrier_price="95000")))
        self.assertFalse(utils.is_live_vanilla_option("not a product"))

    def test_expiry_comes_from_settlement_time(self):
        expected = datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp()

        self.assertEqual(expected, utils.expiry_timestamp(option_product()))

    def test_expiry_falls_back_to_the_symbol_date_at_noon_utc(self):
        product = option_product("C-BTC-100000-130626", settlement_time=None)

        self.assertEqual(datetime(2026, 6, 13, 12, tzinfo=timezone.utc).timestamp(), utils.expiry_timestamp(product))

    def test_instrument_from_product(self):
        instrument = utils.instrument_from_product(option_product("P-ETH-3000-251226", "put_options",
                                                                  underlying_asset={"symbol": "eth"}),
                                                   "P_ETH_3000_251226-USD")

        self.assertEqual("P_ETH_3000_251226-USD", instrument.trading_pair)
        self.assertEqual("ETH", instrument.underlying)
        self.assertEqual(OptionType.PUT, instrument.option_type)
        self.assertEqual(Decimal("3000"), instrument.strike)
