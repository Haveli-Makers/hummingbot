from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

import hummingbot.connector.exchange.coinswitch.coinswitch_constants as CONSTANTS
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-INR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0009"),
    taker_percent_fee_decimal=Decimal("0.0009"),
    buy_percent_fee_deducted_from_returns=True
)


class CoinswitchConfigMap(BaseConnectorConfigMap):
    """Configuration map for CoinSwitch connector."""
    connector: str = "coinswitch"
    coinswitch_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    coinswitch_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your CoinSwitch API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="coinswitch")


KEYS = CoinswitchConfigMap.model_construct()


class CoinswitchUtils:
    """
    Utility functions for CoinSwitch exchange.
    """

    @staticmethod
    def order_type_to_string(order_type) -> str:
        """
        Convert order type to CoinSwitch string representation.
        """
        return CONSTANTS.ORDER_TYPE_LIMIT

    @staticmethod
    def string_to_order_type(order_type_str: str):
        """
        Convert string to order type.
        """
        from hummingbot.core.data_type.common import OrderType
        if order_type_str.lower() == "limit":
            return OrderType.LIMIT
        return OrderType.LIMIT

    @staticmethod
    def str_to_float(s: str) -> float:
        """
        Convert string to float safely.
        """
        try:
            return float(s)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def str_to_decimal(s: str) -> Decimal:
        """
        Convert string to Decimal safely.
        """
        try:
            return Decimal(s)
        except (ValueError):
            return Decimal(0)

    @staticmethod
    def parse_order_response(order_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse an order response from CoinSwitch API.
        """
        return {
            "order_id": order_response.get("order_id"),
            "symbol": order_response.get("symbol", "").upper(),
            "price": CoinswitchUtils.str_to_decimal(str(order_response.get("price", 0))),
            "quantity": CoinswitchUtils.str_to_decimal(str(order_response.get("orig_qty", 0))),
            "executed_qty": CoinswitchUtils.str_to_decimal(str(order_response.get("executed_qty", 0))),
            "status": order_response.get("status"),
            "side": order_response.get("side", "").lower(),
            "exchange": order_response.get("exchange"),
            "created_time": order_response.get("created_time"),
            "updated_time": order_response.get("updated_time"),
            "average_price": CoinswitchUtils.str_to_decimal(str(order_response.get("average_price", 0))),
        }

    @staticmethod
    def parse_ticker_response(ticker_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a ticker response from CoinSwitch API.
        """
        return {
            "symbol": ticker_response.get("symbol", "").upper(),
            "bid_price": CoinswitchUtils.str_to_decimal(str(ticker_response.get("bidPrice", 0))),
            "ask_price": CoinswitchUtils.str_to_decimal(str(ticker_response.get("askPrice", 0))),
            "last_price": CoinswitchUtils.str_to_decimal(str(ticker_response.get("lastPrice", 0))),
            "high_price": CoinswitchUtils.str_to_decimal(str(ticker_response.get("highPrice", 0))),
            "low_price": CoinswitchUtils.str_to_decimal(str(ticker_response.get("lowPrice", 0))),
            "base_volume": CoinswitchUtils.str_to_decimal(str(ticker_response.get("baseVolume", 0))),
            "quote_volume": CoinswitchUtils.str_to_decimal(str(ticker_response.get("quoteVolume", 0))),
            "timestamp": ticker_response.get("at"),
        }

    @staticmethod
    def parse_balance_response(balance_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a balance response from CoinSwitch API.
        """
        balances = {}
        for asset_data in balance_response:
            currency = asset_data.get("currency", "").upper()
            balances[currency] = {
                "total": CoinswitchUtils.str_to_decimal(str(asset_data.get("main_balance", 0))),
                "free": CoinswitchUtils.str_to_decimal(str(asset_data.get("main_balance", 0))),
                "locked": CoinswitchUtils.str_to_decimal(str(asset_data.get("blocked_balance_order", 0))),
            }
        return balances

    @staticmethod
    def parse_depth_response(depth_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a depth (order book) response from CoinSwitch API.
        """
        return {
            "symbol": depth_response.get("symbol", "").upper(),
            "timestamp": depth_response.get("timestamp"),
            "bids": [[CoinswitchUtils.str_to_decimal(bid[0]), CoinswitchUtils.str_to_decimal(bid[1])]
                     for bid in depth_response.get("bids", [])],
            "asks": [[CoinswitchUtils.str_to_decimal(ask[0]), CoinswitchUtils.str_to_decimal(ask[1])]
                     for ask in depth_response.get("asks", [])],
        }

    @staticmethod
    def parse_trade_response(trade_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a trade response from CoinSwitch API.
        """
        return {
            "event_time": trade_response.get("E"),
            "is_buyer_maker": trade_response.get("m", False),
            "price": CoinswitchUtils.str_to_decimal(str(trade_response.get("p", 0))),
            "quantity": CoinswitchUtils.str_to_decimal(str(trade_response.get("q", 0))),
            "symbol": trade_response.get("s", "").upper(),
            "trade_id": trade_response.get("t"),
            "exchange": trade_response.get("e", "").lower(),
        }
