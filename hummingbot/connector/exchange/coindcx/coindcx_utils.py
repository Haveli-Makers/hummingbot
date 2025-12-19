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
    Verifies if a trading pair is enabled to operate with based on its exchange information.
    CoinDCX uses 'status' field to indicate if a market is active.

    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    status = exchange_info.get("status", "")
    return status.lower() == "active"


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

    quote_currencies = ["USDT", "USDC", "BTC", "ETH", "INR", "BUSD"]
    for quote in quote_currencies:
        if coindcx_pair.endswith(quote):
            base = coindcx_pair[:-len(quote)]
            return f"{base}-{quote}"

    return coindcx_pair


def hb_pair_to_coindcx_symbol(hb_pair: str) -> str:
    """
    Converts Hummingbot trading pair format to CoinDCX symbol format.
    Hummingbot uses format like "BTC-USDT"
    CoinDCX symbol format is like "BTCUSDT"

    :param hb_pair: Trading pair in Hummingbot format
    :return: Trading pair in CoinDCX symbol format
    """
    return hb_pair.replace("-", "")


def hb_pair_to_coindcx_pair(hb_pair: str, ecode: str = "B") -> str:
    """
    Converts Hummingbot trading pair format to CoinDCX pair format (used in sockets).
    Hummingbot uses format like "BTC-USDT"
    CoinDCX pair format is like "B-BTC_USDT"

    :param hb_pair: Trading pair in Hummingbot format
    :param ecode: Exchange code (B for Binance markets, I for CoinDCX INR markets)
    :return: Trading pair in CoinDCX pair format
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
