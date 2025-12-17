
__all__ = ["CoindcxExchange"]


def __getattr__(name):
    if name == "CoindcxExchange":
        from hummingbot.connector.exchange.coindcx.coindcx_exchange import CoindcxExchange
        return CoindcxExchange
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
