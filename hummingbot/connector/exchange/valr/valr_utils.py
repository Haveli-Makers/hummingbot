from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-ZAR"

# VALR spot default fees (maker 0.0%, taker 0.1% on most pairs; overridden where the
# exchange reports per-account fees).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True,
)


class ValrConfigMap(BaseConnectorConfigMap):
    """Configuration map for the VALR (spot) connector."""

    connector: str = "valr"
    valr_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your VALR API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    valr_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your VALR API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="valr")


KEYS = ValrConfigMap.model_construct()
