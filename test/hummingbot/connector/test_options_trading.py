import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.options_trading import OptionsTrading
from hummingbot.core.data_type.common import PositionSide
from hummingbot.core.data_type.option_info import OptionInfo, OptionInfoUpdate, OptionInstrumentInfo, OptionType


class OptionsTradingTests(IsolatedAsyncioWrapperTestCase):
    trading_pair = "C_BTC_100000_251226-USD"

    def setUp(self):
        super().setUp()
        self.trading = OptionsTrading([self.trading_pair])

    def tearDown(self):
        self.trading.stop()
        super().tearDown()

    def test_positions_are_keyed_by_trading_pair(self):
        position = Position(self.trading_pair, PositionSide.SHORT, Decimal("0"), Decimal("500"), Decimal("-0.01"),
                            Decimal("1"))

        self.trading.set_position(self.trading_pair, position)

        self.assertIs(position, self.trading.get_position(self.trading_pair))
        self.assertIs(position, self.trading.remove_position(self.trading_pair))
        self.assertIsNone(self.trading.get_position(self.trading_pair))

    def test_set_instruments_replaces_the_index(self):
        old = OptionInstrumentInfo("P_BTC_90000_251226-USD", "BTC", OptionType.PUT, Decimal("90000"), 1.0)
        new = OptionInstrumentInfo(self.trading_pair, "BTC", OptionType.CALL, Decimal("100000"), 2.0)
        self.trading.set_instruments([old])

        self.trading.set_instruments([new])

        self.assertEqual({self.trading_pair: new}, self.trading.instruments)
        self.assertIsNone(self.trading.get_instrument(old.trading_pair))

    async def test_updates_apply_to_initialized_option_info(self):
        self.assertFalse(self.trading.is_option_info_initialized())
        self.trading.start()
        self.trading.initialize_option_info(
            OptionInfo(trading_pair=self.trading_pair, mark_price=Decimal("500"), underlying_price=Decimal("98000")))

        self.trading.option_info_stream.put_nowait(OptionInfoUpdate(trading_pair="unknown", mark_price=Decimal("1")))
        self.trading.option_info_stream.put_nowait(
            OptionInfoUpdate(trading_pair=self.trading_pair, mark_price=Decimal("520"), delta=Decimal("0.45")))
        await asyncio.sleep(0.01)

        self.assertTrue(self.trading.is_option_info_initialized())
        info = self.trading.get_option_info(self.trading_pair)
        self.assertEqual(Decimal("520"), info.mark_price)
        self.assertEqual(Decimal("0.45"), info.delta)
        self.assertEqual(Decimal("98000"), info.underlying_price)
