from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"


def unwrap_data(response: Any, identity_key: Optional[str] = None) -> Any:
    """
    Unwrap CSX's ``{"data": ...}`` envelope and return the inner payload.

    CSX wraps most payloads (e.g. ``{"data": {...}, "message": "..."}``) but not
    all of them, and some endpoints have been seen returning the bare object. Pass
    ``identity_key`` — a field that only ever appears on the UNWRAPPED object (e.g.
    ``"orderId"``) — to short-circuit: if the response already carries that key it
    is returned as-is rather than being unwrapped a second time.

    Kept in one place so a future change to the envelope shape is a single edit;
    inlining this at each call site is how one site gets missed.
    """
    if not isinstance(response, dict):
        return response
    if identity_key is not None and identity_key in response:
        return response
    return response.get("data", response)


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
            # MUST be True: only is_connect_key fields are passed to the connector
            # by Security.api_keys() (api_keys_from_connector_config_map). With
            # False the proxy URL is saved but never reaches CsxExchange, so CSX
            # traffic goes direct and gets blocked (HTTP 403) on whitelisted IPs.
            # The field stays optional because it has a default of "".
            "is_connect_key": True,
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

    CSX wraps the payload under a "data" key:
      {"data": {"Available": {"BTC": "0.5", ...}, "Locked": {"BTC": "0.1"}}, "message": "..."}
    A pre-unwrapped dict ({"Available": ..., "Locked": ...}) is also accepted.
    """
    inner = response.get("data", response) if isinstance(response, dict) else {}
    available = inner.get("Available") or {}
    locked = inner.get("Locked") or {}
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
