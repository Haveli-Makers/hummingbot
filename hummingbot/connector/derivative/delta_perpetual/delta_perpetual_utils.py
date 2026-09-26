from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
# Delta (India) perpetuals are USD-quoted/settled (e.g. BTCUSD -> BTC-USD).
EXAMPLE_PAIR = "BTC-USD"

# Delta perpetual default fees (overridden per-product from /v2/products when available).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0005"),
)


class DeltaPerpetualConfigMap(BaseConnectorConfigMap):
    """Configuration map for the Delta Exchange perpetual connector."""

    connector: str = "delta_perpetual"
    delta_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Delta Exchange API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Delta Exchange API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    delta_perpetual_proxy_url: SecretStr = Field(
        default=SecretStr(""),  # empty = no proxy (direct connection)
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route Delta traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,        # stored encrypted in conf/connectors/delta_perpetual.yml
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="delta_perpetual")


KEYS = DeltaPerpetualConfigMap.model_construct()
