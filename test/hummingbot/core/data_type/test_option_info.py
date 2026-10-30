import unittest
from decimal import Decimal

from hummingbot.core.data_type.option_info import OptionInfo, OptionInfoUpdate, OptionInstrumentInfo, OptionType


class OptionInfoTests(unittest.TestCase):
    def test_update_overwrites_only_the_fields_that_are_set(self):
        info = OptionInfo(trading_pair="C_BTC_100000_251226-USD", mark_price=Decimal("500"),
                          underlying_price=Decimal("98000"), delta=Decimal("0.4"), mark_iv=Decimal("0.55"))

        info.update(OptionInfoUpdate(trading_pair="ignored", mark_price=Decimal("510"), delta=Decimal("0.42")))

        self.assertEqual("C_BTC_100000_251226-USD", info.trading_pair)
        self.assertEqual(Decimal("510"), info.mark_price)
        self.assertEqual(Decimal("0.42"), info.delta)
        self.assertEqual(Decimal("98000"), info.underlying_price)
        self.assertEqual(Decimal("0.55"), info.mark_iv)

    def test_instrument_is_expired_from_its_expiry_time(self):
        instrument = OptionInstrumentInfo(trading_pair="P_ETH_3000_251226-USD", underlying="ETH",
                                          option_type=OptionType.PUT, strike=Decimal("3000"), expiry_timestamp=1000.0)

        self.assertFalse(instrument.is_expired(999.9))
        self.assertTrue(instrument.is_expired(1000.0))
