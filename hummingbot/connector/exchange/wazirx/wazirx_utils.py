from decimal import Decimal
from typing import List, Tuple

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.exchange.wazirx.wazirx_constants import MAX_API_KEYS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
    buy_percent_fee_deducted_from_returns=True
)


def wazirx_pair_to_hb_pair(symbol: str) -> str:
    """
    Convert WazirX symbol format to Hummingbot trading pair format.

    Args:
        symbol: WazirX symbol (e.g., "btcinr", "btcusdt")

    Returns:
        Hummingbot trading pair format (e.g., "BTC-INR", "BTC-USDT")

    Examples:
        >>> wazirx_pair_to_hb_pair("btcinr")
        'BTC-INR'
        >>> wazirx_pair_to_hb_pair("btcusdt")
        'BTC-USDT'
    """
    s = symbol.upper()
    if "_" in s:
        parts = s.split("_")
        return f"{parts[0]}-{parts[1]}"

    quotes = ["USDT", "INR"]
    for q in quotes:
        if s.endswith(q):
            base = s[:-len(q)]
            if base:
                return f"{base}-{q}"
    return s


def hb_pair_to_wazirx_symbol(hb_pair: str) -> str:
    """
    Convert Hummingbot trading pair format to WazirX symbol format.

    Args:
        hb_pair: Hummingbot trading pair (e.g., "BTC-INR")

    Returns:
        WazirX symbol format (e.g., "BTCINR")

    Examples:
        >>> hb_pair_to_wazirx_symbol("BTC-INR")
        'BTCINR'
        >>> hb_pair_to_wazirx_symbol("ETH-USDT")
        'ETHUSDT'
    """
    return hb_pair.replace("-", "").upper()


def parse_api_credentials(api_keys: str, api_secrets: str) -> List[Tuple[str, str]]:
    """Splits comma-separated keys and secrets into (key, secret) pairs, matched by position."""
    keys = [key.strip() for key in api_keys.split(",")]
    secrets = [secret.strip() for secret in api_secrets.split(",")]
    if len(keys) != len(secrets):
        raise ValueError(
            f"WazirX needs one API secret per API key, in the same order: got {len(keys)} keys "
            f"and {len(secrets)} secrets.")
    if len(keys) > MAX_API_KEYS:
        raise ValueError(f"WazirX allows at most {MAX_API_KEYS} API keys per account: got {len(keys)}.")
    if len(keys) > 1 and not all(keys + secrets):
        raise ValueError("WazirX API keys and secrets can't have blank entries between commas.")
    if len(set(keys)) != len(keys):
        raise ValueError("Each WazirX API key can be entered only once.")
    return list(zip(keys, secrets))


class WazirxConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for WazirX connector.

    Defines the required configuration parameters for connecting to the
    WazirX exchange, including API credentials.
    """
    connector: str = "wazirx"
    wazirx_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: (
                f"Enter your WazirX API key (up to {MAX_API_KEYS} keys from the same account, "
                "separated by commas)"
            ),
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    wazirx_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your WazirX API secret (one per key, separated by commas, in the same order)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="wazirx")


KEYS = WazirxConfigMap.model_construct()
