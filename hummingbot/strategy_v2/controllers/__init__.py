from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase

__all__ = [
    "ControllerBase",
    "ControllerConfigBase",
    "DirectionalTradingControllerBase",
    "DirectionalTradingControllerConfigBase",
    "MarketMakingControllerBase",
    "MarketMakingControllerConfigBase",
]


def __getattr__(name):
    if name in {"DirectionalTradingControllerBase", "DirectionalTradingControllerConfigBase"}:
        from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
            DirectionalTradingControllerBase,
            DirectionalTradingControllerConfigBase,
        )

        return {
            "DirectionalTradingControllerBase": DirectionalTradingControllerBase,
            "DirectionalTradingControllerConfigBase": DirectionalTradingControllerConfigBase,
        }[name]

    if name in {"MarketMakingControllerBase", "MarketMakingControllerConfigBase"}:
        from hummingbot.strategy_v2.controllers.market_making_controller_base import (
            MarketMakingControllerBase,
            MarketMakingControllerConfigBase,
        )

        return {
            "MarketMakingControllerBase": MarketMakingControllerBase,
            "MarketMakingControllerConfigBase": MarketMakingControllerConfigBase,
        }[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
