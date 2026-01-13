from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
    buy_percent_fee_deducted_from_returns=True
)

def wazirx_pair_to_hb_pair(symbol: str) -> str:
    """Convert WazirX symbol like 'btcinr' or 'btcusdt' to Hummingbot format 'BTC-INR' or 'BTC-USDT'.
    This function makes a best-effort conversion based on common quote currencies.
    """
    s = symbol.upper()
    if "_" in s:
        parts = s.split("_")
        return f"{parts[0]}-{parts[1]}"

    quotes = ["USDT", "USDC", "INR", "BTC", "ETH", "BUSD", "TRX"]
    for q in quotes:
        if s.endswith(q):
            base = s[:-len(q)]
            if base:
                return f"{base}-{q}"
    return s

def hb_pair_to_wazirx_symbol(hb_pair: str) -> str:
    return hb_pair.replace("-", "").upper()

class WazirxConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for WazirX connector.
    """
    connector: str = "wazirx"
    wazirx_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your WazirX API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    wazirx_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your WazirX API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="wazirx")


KEYS = WazirxConfigMap.model_construct()
