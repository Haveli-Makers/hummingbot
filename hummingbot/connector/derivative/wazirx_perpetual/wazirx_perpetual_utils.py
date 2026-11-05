from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"

# WazirX advertises 0.02% maker / 0.04% taker on futures with no volume tiers.
# GST is charged on top as its own income record (GST_ON_COMMISSION), so these
# under-state the all-in cost by 18% of the fee. Real fills carry their own
# commission, which is what Hummingbot books.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0004"),
    buy_percent_fee_deducted_from_returns=True,
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    True for a perpetual that can be traded. exchangeInfo carries no status
    field (every listed symbol is tradable), so the contract type, the base and
    quote assets and a usable quantity filter are what is checked.
    """
    if str(symbol_info.get("contractType", "")).upper() != "PERPETUAL":
        return False
    if not (symbol_info.get("symbol") and symbol_info.get("baseAsset") and symbol_info.get("quoteAsset")):
        return False
    qty_filter = get_filter(symbol_info, "limit_qty_size")
    if qty_filter is None:
        return False
    try:
        min_qty = Decimal(str(qty_filter.get("minQty", "0")))
        max_qty = Decimal(str(qty_filter.get("maxQty", "0")))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return Decimal("0") <= min_qty <= max_qty and max_qty > 0


def get_filter(symbol_info: Dict[str, Any], filter_type: str) -> Optional[Dict[str, Any]]:
    """Return the exchangeInfo filter of the given (lowercase) type, if any."""
    for entry in symbol_info.get("filters") or []:
        if isinstance(entry, dict) and str(entry.get("filterType", "")).lower() == filter_type:
            return entry
    return None


def precision_to_increment(precision: Any) -> Decimal:
    """
    exchangeInfo states precision as a count of decimals ("pricePrecision": "2"),
    not as a tick size; 2 means a 0.01 increment and 0 means whole units.
    """
    try:
        return Decimal("1").scaleb(-int(Decimal(str(precision))))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("1")


def hb_pair_to_ws_symbol(hb_pair: str) -> str:
    """
    "BTC-INR" -> "btcinr". Stream names and the ``s`` field of every frame use
    the lowercase exchange symbol.
    """
    return hb_pair.replace("-", "").lower()


def to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Numbers arrive as strings, sometimes empty; anything unparseable is ``default``."""
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


class WazirxPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "wazirx_perpetual"
    wazirx_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your WazirX API key (Futures Trade permission enabled)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    wazirx_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your WazirX API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    wazirx_perpetual_proxy_url: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route WazirX traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,
            # Must stay True: fields flagged False are prompted and stored but dropped
            # before reaching the connector constructor, so the proxy would be ignored.
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="wazirx_perpetual")


KEYS = WazirxPerpetualConfigMap.model_construct()
