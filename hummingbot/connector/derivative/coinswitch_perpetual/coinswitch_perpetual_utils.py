import json
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.derivative.coinswitch_perpetual import coinswitch_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

# The instrument list's default rates (0.024% maker / 0.065% taker on BTCUSDT).
# Each contract states its own rates in instrument_info, which the connector uses
# for estimates once the trading rules are loaded; fills carry the fee actually
# charged (execution_fee).
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00024"),
    taker_percent_fee_decimal=Decimal("0.00065"),
    buy_percent_fee_deducted_from_returns=True,
)


def to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Futures numbers arrive as strings (sometimes empty) or JSON numbers; anything unparseable is ``default``."""
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def unwrap_data(response: Any) -> Any:
    """Spot and Futures v2 wrap every payload in ``{"data": ...}``."""
    if isinstance(response, dict) and "data" in response:
        return response["data"]
    return response


def parse_socket_payload(payload: Any) -> Any:
    """Socket.IO frames are normally decoded JSON, but a raw JSON string is accepted too."""
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode()
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except ValueError:
            return None
    return payload


def is_exchange_information_valid(symbol: str, instrument: Dict[str, Any]) -> bool:
    """True for a USDT perpetual that is open for trading."""
    if not symbol or not isinstance(instrument, dict):
        return False
    if str(instrument.get("status", "")).upper() != CONSTANTS.INSTRUMENT_STATUS_TRADING:
        return False
    instrument_type = str(instrument.get("type", CONSTANTS.INSTRUMENT_TYPE_PERPETUAL)).upper()
    if instrument_type != CONSTANTS.INSTRUMENT_TYPE_PERPETUAL:
        return False
    return base_and_quote(symbol, instrument) is not None


def base_and_quote(symbol: str, instrument: Dict[str, Any]) -> Optional[tuple]:
    """
    ("BTC", "USDT") for "BTCUSDT". The key is the exchange symbol, so the base is
    taken from it rather than from ``base_asset``: for multiplier contracts such
    as 1000PEPEUSDT the key is the only place the multiplier is guaranteed to be.
    """
    quote = str(instrument.get("quote_asset") or CONSTANTS.COLLATERAL_TOKEN).upper()
    symbol = symbol.upper()
    if not symbol.endswith(quote) or len(symbol) <= len(quote):
        return None
    return symbol[:-len(quote)], quote


def price_increment(instrument: Dict[str, Any]) -> Decimal:
    """
    ``tick_size`` is an integer count of units at ``price_precision`` decimals
    (tick_size 5 with price_precision 2 is a 0.05 step).
    """
    precision = int(to_decimal(instrument.get("price_precision"), Decimal("0")))
    ticks = to_decimal(instrument.get("tick_size"), Decimal("1"))
    if ticks <= 0:
        ticks = Decimal("1")
    return ticks * Decimal("1").scaleb(-precision)


def quantity_increment(instrument: Dict[str, Any]) -> Decimal:
    """
    The quantity step. ``base_quantity_step_size`` is preferred; otherwise
    ``quantity_precision`` decimals, where a negative value means trailing zeros
    (-2 on 1000PEPEUSDT: multiples of 100).
    """
    step = to_decimal(instrument.get("base_quantity_step_size") or instrument.get("lot_size"))
    if step > 0:
        return step
    precision = int(to_decimal(instrument.get("quantity_precision"), Decimal("0")))
    return Decimal("1").scaleb(-precision)


def client_order_uuid(client_order_id: str) -> str:
    """The UUID sent as ``client_order_id`` for a Hummingbot client order id (deterministic)."""
    return str(uuid.uuid5(CONSTANTS.CLIENT_ORDER_ID_NAMESPACE, client_order_id))


def hb_pair_to_symbol(trading_pair: str) -> str:
    """"BTC-USDT" -> "BTCUSDT" (REST and socket symbols are the same, unseparated)."""
    return trading_pair.replace("-", "").upper()


class CoinswitchPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "coinswitch_perpetual"
    coinswitch_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch PRO API key (hex)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    coinswitch_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch PRO API secret (hex)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    coinswitch_perpetual_proxy_url: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": lambda cm: (
                "Enter a proxy URL to route CoinSwitch REST traffic through a whitelisted IP "
                "(e.g. http://user:pass@host:3128), or leave blank to connect directly"
            ),
            "is_secure": True,
            # Must stay True: fields flagged False are prompted and stored but dropped
            # before reaching the connector constructor, so the proxy would be ignored.
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="coinswitch_perpetual")


KEYS = CoinswitchPerpetualConfigMap.model_construct()
