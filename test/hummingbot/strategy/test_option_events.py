import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import MarketEvent, OptionExpiredEvent, OptionSettledEvent
from hummingbot.core.pubsub import PubSub
from hummingbot.strategy.option_events import listen_to_option_events


class ListenToOptionEventsTests(unittest.TestCase):
    def test_option_events_reach_the_strategy(self):
        connector = PubSub()
        strategy = MagicMock(spec=["did_expire_option", "did_settle_option"])
        expired = OptionExpiredEvent(timestamp=1.0, market="delta_options", trading_pair="C_BTC_1_010127-USD",
                                     expiry_timestamp=1.0)
        settled = OptionSettledEvent(timestamp=2.0, market="delta_options", trading_pair="C_BTC_1_010127-USD",
                                     trade_type=TradeType.SELL, amount=Decimal("0.001"), price=Decimal("5"),
                                     trade_fee=AddedToCostTradeFee(), exchange_trade_id="f1")

        listeners = listen_to_option_events(strategy, [connector])
        connector.trigger_event(MarketEvent.OptionExpired, expired)
        connector.trigger_event(MarketEvent.OptionSettled, settled)

        strategy.did_expire_option.assert_called_once_with(expired)
        strategy.did_settle_option.assert_called_once_with(settled)
        self.assertEqual(2, len(listeners))

    def test_only_the_handlers_a_strategy_defines_are_wired(self):
        strategy = MagicMock(spec=["did_settle_option"])

        listeners = listen_to_option_events(strategy, [PubSub()])

        self.assertEqual(1, len(listeners))
