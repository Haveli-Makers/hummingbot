from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True,
)


class CsxConfigMap(BaseConnectorConfigMap):
    """Configuration map for the CoinSwitch Kuber (CSX) connector."""

    connector: str = "csx"
    csx_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch Kuber (CSX) API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    csx_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch Kuber (CSX) API secret (64-char hex Ed25519 key)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    csx_proxy_url: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route CSX traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,
            "is_connect_key": False,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="csx")


KEYS = CsxConfigMap.model_construct()


def str_to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal("0")


def parse_balance_response(response: Dict[str, Any]) -> Dict[str, Dict[str, Decimal]]:
    """
    Parse GET /api/v2/me/balance/ response.

    Expected shape:
      {"Available": {"BTC": "0.5", "INR": "50000"}, "Locked": {"BTC": "0.1"}}
    """
    available = response.get("Available") or {}
    locked = response.get("Locked") or {}
    all_assets = set(available.keys()) | set(locked.keys())
    result: Dict[str, Dict[str, Decimal]] = {}
    for asset in all_assets:
        free = str_to_decimal(available.get(asset, "0"))
        held = str_to_decimal(locked.get(asset, "0"))
        result[asset.upper()] = {"free": free, "locked": held, "total": free + held}
    return result


def instrument_to_hb_pair(instrument: str) -> str:
    """Convert CSX instrument string (e.g. 'BTC/INR') to HB pair ('BTC-INR')."""
    return instrument.replace("/", "-").upper()


def hb_pair_to_instrument(hb_pair: str) -> str:
    """Convert HB pair ('BTC-INR') to CSX instrument string ('BTC/INR')."""
    return hb_pair.replace("-", "/").upper()
