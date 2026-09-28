from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# Fallback only — real per-instrument maker/taker fees come from the instrument
# details endpoint (see ``percent_fee_to_decimal``).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.000236"),
    taker_percent_fee_decimal=Decimal("0.00059"),
    buy_percent_fee_deducted_from_returns=True,
)


def is_exchange_information_valid(instrument: Dict[str, Any]) -> bool:
    """
    Returns True when an instrument is an active, non-inverse perpetual that can be traded.
    """
    if str(instrument.get("status", "")).lower() != "active":
        return False
    if str(instrument.get("kind", "perpetual")).lower() != "perpetual":
        return False
    if instrument.get("is_inverse", False):
        return False
    if instrument.get("exit_only", False):
        return False
    try:
        min_quantity = float(instrument.get("min_quantity", 0) or 0)
        max_quantity = float(instrument.get("max_quantity", 0) or 0)
    except (TypeError, ValueError):
        return False
    if min_quantity < 0 or max_quantity <= 0 or min_quantity > max_quantity:
        return False
    return bool(instrument.get("pair"))


def coindcx_pair_to_hb_pair(coindcx_pair: str) -> str:
    """
    "B-BTC_USDT" -> "BTC-USDT"
    """
    without_ecode = coindcx_pair.split("-", 1)[1] if "-" in coindcx_pair else coindcx_pair
    return without_ecode.replace("_", "-")


def hb_pair_to_coindcx_pair(hb_pair: str, ecode: str = CONSTANTS.ECODE) -> str:
    """
    "BTC-USDT" -> "B-BTC_USDT"
    """
    return f"{ecode}-{hb_pair.replace('-', '_')}"


def coindcx_pair_to_market_symbol(coindcx_pair: str) -> str:
    """
    "B-BTC_USDT" -> "BTCUSDT".

    ``depth-snapshot`` frames carry this market symbol in their ``s`` field,
    where every other stream puts the CoinDCX pair. Confirmed against a live
    multi-pair capture: a depth frame reads
    ``{"s": "ETHUSDT", "bids": ..., "asks": ...}`` and carries no ``mkt`` key,
    while a ``new-trade`` frame on the same socket reads ``{"s": "B-BTC_USDT"}``.
    The same value appears as ``mkt`` on the current-prices REST endpoint.
    Verified unique across all active instruments.
    """
    without_ecode = coindcx_pair.split("-", 1)[1] if "-" in coindcx_pair else coindcx_pair
    return without_ecode.replace("_", "")


def split_coindcx_pair(coindcx_pair: str) -> tuple:
    """
    "B-BTC_USDT" -> ("BTC", "USDT"); ("", "") when the name is not parseable.
    """
    without_ecode = coindcx_pair.split("-", 1)[1] if "-" in coindcx_pair else coindcx_pair
    if "_" not in without_ecode:
        return "", ""
    base, quote = without_ecode.split("_", 1)
    return base, quote


def hb_pair_to_market_symbol(hb_pair: str) -> str:
    """
    "BTC-USDT" -> "BTCUSDT"
    """
    return hb_pair.replace("-", "")


def normalize_margin_currency(margin_currency: Any) -> str:
    """
    Validate and canonicalise the configured margin currency.

    CoinDCX futures accept margin in USDT or INR; anything else is rejected here
    rather than being sent to the API, where it surfaces as an opaque rejection.
    """
    candidate = str(margin_currency or CONSTANTS.DEFAULT_MARGIN_CURRENCY).strip().upper()
    if candidate not in CONSTANTS.SUPPORTED_MARGIN_CURRENCIES:
        raise ValueError(
            f"Unsupported CoinDCX futures margin currency {margin_currency!r}. "
            f"Supported: {', '.join(CONSTANTS.SUPPORTED_MARGIN_CURRENCIES)}."
        )
    return candidate


def percent_fee_to_decimal(percent_fee: Any) -> Decimal:
    """
    CoinDCX reports fees as percentages (``maker_fee: 0.0236`` means 0.0236%),
    while Hummingbot works with decimal fractions.
    """
    try:
        return Decimal(str(percent_fee)) / Decimal("100")
    except (TypeError, ValueError, InvalidOperation):
        return Decimal("0")


class CoinDCXPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "coindcx_perpetual"
    coindcx_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinDCX API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    coindcx_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinDCX API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    coindcx_perpetual_margin_currency: str = Field(
        default=CONSTANTS.DEFAULT_MARGIN_CURRENCY,
        json_schema_extra={
            "prompt": lambda cm: (
                f"Margin currency for CoinDCX futures "
                f"({' or '.join(CONSTANTS.SUPPORTED_MARGIN_CURRENCIES)})"
            ),
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    coindcx_perpetual_proxy_url: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route CoinDCX traffic through a whitelisted IP "
                "(e.g. socks5://user:pass@host:1080), or leave blank to connect directly"
            ),
            "is_secure": True,
            # Must stay True: fields flagged False are prompted and stored but dropped
            # before reaching the connector constructor, so the proxy would be ignored.
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="coindcx_perpetual")


KEYS = CoinDCXPerpetualConfigMap.model_construct()
