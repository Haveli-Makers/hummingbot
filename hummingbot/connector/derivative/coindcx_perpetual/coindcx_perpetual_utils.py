from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.derivative.coindcx_perpetual import coindcx_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# The FAPI publishes no per-instrument fees (exchangeInfo carries none; the account
# has a ``feeTier``), so these estimates stand until fills report the real charge.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.000236"),
    taker_percent_fee_decimal=Decimal("0.00059"),
    buy_percent_fee_deducted_from_returns=True,
)


def is_exchange_information_valid(symbol_info: Dict[str, Any]) -> bool:
    """
    True for an exchangeInfo symbol that is a perpetual currently open for trading.
    """
    return (symbol_info.get("status") == CONSTANTS.SYMBOL_STATUS_TRADING
            and symbol_info.get("contractType") == CONSTANTS.CONTRACT_TYPE_PERPETUAL
            and bool(symbol_info.get("symbol"))
            and bool(symbol_info.get("baseAsset"))
            and bool(symbol_info.get("quoteAsset")))


def symbol_filter(symbol_info: Dict[str, Any], filter_type: str) -> Dict[str, Any]:
    """The exchangeInfo filter of the given type for a symbol, or an empty dict."""
    return next((f for f in symbol_info.get("filters") or []
                 if isinstance(f, dict) and f.get("filterType") == filter_type), {})


def coindcx_pair_to_hb_pair(coindcx_pair: str) -> str:
    """
    Legacy-API pair to Hummingbot pair: "B-BTC_USDT" -> "BTC-USDT".
    Only the legacy private code still reads legacy pairs.
    """
    without_ecode = coindcx_pair.split("-", 1)[1] if "-" in coindcx_pair else coindcx_pair
    return without_ecode.replace("_", "-")


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
