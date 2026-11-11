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

    def test_instrument_from_the_trading_pair_name(self):
        instrument = utils.instrument_from_trading_pair("P_ETH_3000_251226-USD")

        self.assertEqual(("ETH", OptionType.PUT, Decimal("3000")),
                         (instrument.underlying, instrument.option_type, instrument.strike))
        self.assertEqual(datetime(2026, 12, 25, 12, tzinfo=timezone.utc).timestamp(), instrument.expiry_timestamp)
        with self.assertRaises(ValueError):
            utils.instrument_from_trading_pair("BTC-USD")

    def test_symbol_from_the_trading_pair_name(self):
        self.assertEqual("C-BTC-63800-130626", utils.symbol_from_trading_pair("C_BTC_63800_130626-USD"))

    def test_yes_no_answers(self):
        self.assertEqual([True, True, False, False, True, False],
                         [utils.as_bool(v) for v in ("Yes", "true", "no", "N", True, False)])

    def test_config_defaults_keep_selling_off_and_the_switch_on(self):
        config = utils.DeltaOptionsConfigMap(delta_options_api_key="k", delta_options_api_secret="s")

        self.assertEqual((False, 1, True, 30), (config.delta_options_allow_selling,
                                                config.delta_options_max_short_contracts,
                                                config.delta_options_dead_man_switch,
                                                config.delta_options_stop_before_expiry_minutes))
        with self.assertRaises(ValueError):
            utils.DeltaOptionsConfigMap(delta_options_api_key="k", delta_options_api_secret="s",
                                        delta_options_stop_before_expiry_minutes=-1)

    def test_config_rejects_unclear_answers(self):
        with self.assertRaises(ValueError):
            utils.DeltaOptionsConfigMap(delta_options_api_key="k", delta_options_api_secret="s",
                                        delta_options_allow_selling="maybe")

    def test_trading_settings_reach_the_connector(self):
        fields = utils.DeltaOptionsConfigMap.model_fields
        for name in ("delta_options_allow_selling", "delta_options_max_short_contracts", "delta_options_dead_man_switch",
                     "delta_options_stop_before_expiry_minutes"):
            self.assertTrue(fields[name].json_schema_extra["is_connect_key"])
            self.assertFalse(fields[name].json_schema_extra["is_secure"])

    def test_testnet_urls(self):
        from hummingbot.connector.derivative.delta_options import (
            delta_options_constants as CONSTANTS,
            delta_options_web_utils as web_utils,
        )

        self.assertEqual("https://cdn-ind.testnet.deltaex.org/v2/orders",
                         web_utils.private_rest_url(CONSTANTS.ORDERS_PATH_URL, domain=CONSTANTS.TESTNET_DOMAIN))
        self.assertEqual("wss://socket-ind-pub.testnet.deltaex.org", web_utils.public_wss_url(CONSTANTS.TESTNET_DOMAIN))
        self.assertEqual("wss://socket-ind.testnet.deltaex.org", web_utils.private_wss_url(CONSTANTS.TESTNET_DOMAIN))
