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


class ZebpayConfigMap(BaseConnectorConfigMap):
    """Configuration map for the Zebpay (spot) connector."""

    connector: str = "zebpay"
    zebpay_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Zebpay API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    zebpay_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Zebpay API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="zebpay")


KEYS = ZebpayConfigMap.model_construct()


def str_to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal("0")


def unwrap_data(response: Any) -> Any:
    """
    Zebpay responses may wrap the payload under a "data" key
    (e.g. {"statusCode": 200, "data": {...}}). Return the inner payload,
    or the response itself if there is no wrapper.
    """
    if isinstance(response, dict) and "data" in response:
        return response["data"]
    return response


def parse_balance_response(response: Any) -> Dict[str, Dict[str, Decimal]]:
    """
    Parse GET /api/v2/account/balance into {ASSET: {free, locked, total}}.

    Each balance item looks like:
      {"currency": "BTC", "total": "1.0", "free": "0.8", "used": "0.2", ...}
    """
    data = unwrap_data(response)
    items = data if isinstance(data, list) else data.get("balances", data.get("assets", []))
    result: Dict[str, Dict[str, Decimal]] = {}
    if not isinstance(items, list):
        return result
    for item in items:
        if not isinstance(item, dict):
            continue
        asset = str(item.get("currency", item.get("asset", ""))).upper()
        if not asset:
            continue
        free = str_to_decimal(item.get("free", item.get("available", "0")))
        used = str_to_decimal(item.get("used", item.get("locked", "0")))
        total = item.get("total")
        total = str_to_decimal(total) if total is not None else (free + used)
        result[asset] = {"free": free, "locked": used, "total": total}
    return result
