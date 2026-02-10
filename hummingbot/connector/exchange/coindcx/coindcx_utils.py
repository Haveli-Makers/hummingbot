from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Validate if exchange market information is valid and active.

    Args:
        exchange_info: Market information dictionary from CoinDCX

    Returns:
        True if the market is active and has valid parameters, False otherwise
    """
    status = exchange_info.get("status", "")
    if status.lower() != "active":
        return False

    try:
        min_quantity = float(exchange_info.get("min_quantity", 0))
        max_quantity = float(exchange_info.get("max_quantity", 0))

        if min_quantity < 0 or max_quantity <= 0:
            return False

        if min_quantity > max_quantity:
            return False

    except (ValueError, TypeError):
        return False

    return True


def coindcx_pair_to_hb_pair(coindcx_pair: str) -> str:
    """
    Converts CoinDCX trading pair format to Hummingbot format.
    CoinDCX uses formats like "BTCUSDT" or "B-BTC_USDT"
    Hummingbot uses format like "BTC-USDT"

    :param coindcx_pair: Trading pair in CoinDCX format
    :return: Trading pair in Hummingbot format
    """
    if "-" in coindcx_pair and "_" in coindcx_pair:
        pair = coindcx_pair.split("-", 1)[1] if "-" in coindcx_pair else coindcx_pair
        return pair.replace("_", "-")

    quote_currencies = ["INR", "USDT", "USDC", "BTC", "ETH", "BUSD"]
    for quote in quote_currencies:
        if coindcx_pair.endswith(quote):
            base = coindcx_pair[:-len(quote)]
            return f"{base}-{quote}"

    return coindcx_pair


def hb_pair_to_coindcx_symbol(hb_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to CoinDCX symbol format.

    Args:
        hb_pair: Trading pair in Hummingbot format (e.g., "BTC-USDT")

    Returns:
        Symbol in CoinDCX format (e.g., "BTCUSDT")
    """
    return hb_pair.replace("-", "")


def hb_pair_to_coindcx_pair(hb_pair: str, ecode: str = "B") -> str:
    """
    Convert Hummingbot trading pair format to CoinDCX pair format with exchange code.

    Args:
        hb_pair: Trading pair in Hummingbot format (e.g., "BTC-USDT")
        ecode: Exchange code prefix (e.g., "B" for main exchange)

    Returns:
        Pair in CoinDCX format (e.g., "B-BTC_USDT")
    """
    return f"{ecode}-{hb_pair.replace('-', '_')}"


class CoinDCXConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for CoinDCX connector.
    """
    connector: str = "coindcx"
    coindcx_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinDCX API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coindcx_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinDCX API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coindcx")


KEYS = CoinDCXConfigMap.model_construct()
