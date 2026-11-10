from typing import Any, Iterable, List

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent

OPTION_EVENT_HANDLERS = (
    (MarketEvent.OptionExpired, "did_expire_option"),
    (MarketEvent.OptionSettled, "did_settle_option"),
)


def listen_to_option_events(strategy: Any, connectors: Iterable[ConnectorBase]) -> List[EventForwarder]:
    """
    Calls `strategy.did_expire_option(event)` and `strategy.did_settle_option(event)`, where the strategy defines
    them, for the OptionExpired and OptionSettled events of the given connectors. Strategies receive a fixed set of
    connector events, which doesn't include these.

    Keep the returned list on the strategy, e.g. `self._option_listeners = listen_to_option_events(self,
    self.connectors.values())`: connectors hold their listeners weakly, so a dropped one stops being called.
    """
    forwarders = []
    for event, handler_name in OPTION_EVENT_HANDLERS:
        handler = getattr(strategy, handler_name, None)
        if handler is None:
            continue
        forwarder = EventForwarder(handler)
        for connector in connectors:
            connector.add_listener(event, forwarder)
        forwarders.append(forwarder)
    return forwarders
