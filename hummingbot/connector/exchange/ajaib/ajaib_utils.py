from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-IDR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Validate if exchange market information is valid for trading.

    Ajaib exchange-info symbol format:
    {
        "symbol": "BTC_USDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "isSpotTradingAllowed": true,
        "filters": [...]
    }
    """
    if not exchange_info.get("isSpotTradingAllowed", False):
        return False

    filters = exchange_info.get("filters", [])
    for f in filters:
        if f.get("filterType") == "LOT_SIZE":
            try:
                min_qty = float(f.get("minQty", 0))
                max_qty = float(f.get("maxQty", 0))
                if min_qty < 0 or max_qty <= 0:
                    return False
            except (ValueError, TypeError):
                return False

    return bool(exchange_info.get("symbol"))


def ajaib_symbol_to_hb_pair(symbol: str) -> str:
    """
    Converts Ajaib symbol format to Hummingbot trading pair format.
    Ajaib uses "BTC_USDT", Hummingbot uses "BTC-USDT".
    """
    return symbol.replace("_", "-")


def hb_pair_to_ajaib_symbol(hb_pair: str) -> str:
    """
    Converts Hummingbot trading pair format to Ajaib symbol format.
    Hummingbot uses "BTC-USDT", Ajaib uses "BTC_USDT".
    """
    return hb_pair.replace("-", "_")


class AjaibConfigMap(BaseConnectorConfigMap):
    connector: str = "ajaib"
    ajaib_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Ajaib API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    ajaib_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter the path to your Ajaib Ed25519 private key PEM file",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="ajaib")


KEYS = AjaibConfigMap.model_construct()
