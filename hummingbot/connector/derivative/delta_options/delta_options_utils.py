from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr, field_validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.derivative.delta_options import delta_options_constants as CONSTANTS
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.option_info import OptionInstrumentInfo, OptionType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "C_BTC_100000_251226-USD"

# Rates on the underlying's value; the connector turns them into a fee estimate with the premium cap.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0001"),
    taker_percent_fee_decimal=Decimal("0.0001"),
)


def as_bool(value: Any) -> bool:
    """
    Config answers can arrive as "yes"/"no" strings, where bool("no") would be True.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("yes", "y", "true")
    return bool(value)


def trading_pair_from_symbol(symbol: str, quote: str) -> str:
    """
    C-BTC-63800-130626 quoted in USD becomes C_BTC_63800_130626-USD, since our pairs allow exactly one dash.
    """
    return combine_to_hb_trading_pair(base=symbol.replace("-", "_"), quote=quote.upper())


def is_live_vanilla_option(product: Any) -> bool:
    return (
        isinstance(product, dict)
        and product.get("contract_type") in (CONSTANTS.CALL_OPTIONS, CONSTANTS.PUT_OPTIONS)
        and product.get("state") == CONSTANTS.LIVE_STATE
        and product.get("barrier_price") is None
        and bool(product.get("symbol"))
    )


def expiry_timestamp(product: Dict[str, Any]) -> float:
    settlement_time = product.get("settlement_time")
    if settlement_time:
        return datetime.fromisoformat(settlement_time.replace("Z", "+00:00")).timestamp()
    expiry_date = datetime.strptime(product["symbol"].split("-")[-1], "%d%m%y")
    return expiry_date.replace(hour=CONSTANTS.SETTLEMENT_HOUR_UTC, tzinfo=timezone.utc).timestamp()


def instrument_from_product(product: Dict[str, Any], trading_pair: str) -> OptionInstrumentInfo:
    _, underlying, strike, _ = product["symbol"].split("-")
    return OptionInstrumentInfo(
        trading_pair=trading_pair,
        underlying=((product.get("underlying_asset") or {}).get("symbol") or underlying).upper(),
        option_type=OptionType.CALL if product["contract_type"] == CONSTANTS.CALL_OPTIONS else OptionType.PUT,
        strike=Decimal(str(product.get("strike_price") or strike)),
        expiry_timestamp=expiry_timestamp(product),
    )


def instrument_from_trading_pair(trading_pair: str) -> OptionInstrumentInfo:
    """
    What the name says about an option Delta no longer lists: C_BTC_63800_130626-USD is a BTC call, strike 63,800,
    expiring 13 Jun 2026 at 12:00 UTC.
    """
    symbol = trading_pair.split("-")[0].replace("_", "-")
    kind = symbol.split("-")[0]
    if kind not in ("C", "P"):
        raise ValueError(f"{trading_pair} is not the name of a call or put option.")
    return instrument_from_product(
        {"symbol": symbol, "contract_type": CONSTANTS.CALL_OPTIONS if kind == "C" else CONSTANTS.PUT_OPTIONS},
        trading_pair)


class DeltaOptionsConfigMap(BaseConnectorConfigMap):
    connector: str = "delta_options"
    delta_options_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Delta Exchange API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Delta Exchange API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_proxy_url: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route Delta traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_allow_selling: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda cm: "Allow selling options to open short positions? (Yes/No)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_max_short_contracts: int = Field(
        default=1,
        gt=0,
        json_schema_extra={
            "prompt": lambda cm: "If selling is allowed, how many contracts of one option may the bot be short at most?",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_dead_man_switch: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda cm: (
                "If the bot stops responding for 30 seconds, have Delta cancel this account's open orders on the "
                "bot's options? (Yes/No)"
            ),
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_options_stop_before_expiry_minutes: int = Field(
        default=30,
        ge=0,
        json_schema_extra={
            "prompt": lambda cm: (
                "How many minutes before an option expires should the bot stop opening positions in it? Delta "
                "settles on the index's average over the last 30 minutes. Closing orders are still accepted; 0 turns "
                "this off."
            ),
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="delta_options")

    @field_validator("delta_options_allow_selling", "delta_options_dead_man_switch", mode="before")
    @classmethod
    def validate_yes_no(cls, value: Any):
        if isinstance(value, str) and value.strip().lower() not in ("yes", "y", "true", "no", "n", "false"):
            raise ValueError("Please answer yes or no.")
        return value


KEYS = DeltaOptionsConfigMap.model_construct()
