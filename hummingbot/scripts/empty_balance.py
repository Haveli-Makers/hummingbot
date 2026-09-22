import argparse
import asyncio
import logging
import os
import sys
import time
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import Field

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import load_client_config_map_from_file, read_system_configs_from_yml
from hummingbot.client.config.security import Security
from hummingbot.client.settings import AllConnectorSettings, ConnectorType
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.connector_manager import ConnectorManager
from hummingbot.core.data_type.common import OrderType, PriceType
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent

SUPPORTED_CONNECTORS = sorted(
    name for name, setting in AllConnectorSettings.get_connector_settings().items()
    if setting.type == ConnectorType.Exchange
)
ORDER_TYPES = ("market", "limit")


class EmptyBalanceConfig(BaseClientModel):
    """
    Configuration for the Empty Balance script.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    exchange: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: "Enter the exchange/connector name (e.g. binance, coindcx, wazirx): ",
            "prompt_on_new": True,
            "input_type": "select",
            "options": SUPPORTED_CONNECTORS,
        },
    )
    trading_pairs: str = Field(
        default="USDT-INR",
        json_schema_extra={
            "prompt": lambda mi: (
                "Enter trading pairs, comma-separated (each pair's base asset is the e.g. USDT-INR,ETH-INR,BTC-INR): "
            ),
            "prompt_on_new": True,
        },
    )
    order_type: str = Field(
        default="market",
        json_schema_extra={
            "prompt": lambda mi: f"Order type to use ({', '.join(ORDER_TYPES)}): ",
            "prompt_on_new": True,
            "input_type": "select",
            "options": list(ORDER_TYPES),
        },
    )
    limit_order_price_spread: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": lambda mi: "If using a limit order, spread below best bid to help it fill (e.g. 0.001 = 0.1%): ",
            "prompt_on_new": True,
        },
    )
    balance_use_pct: Decimal = Field(
        default=Decimal("0.99"),
        json_schema_extra={
            "prompt": lambda mi: "Fraction of each dust balance to sell, leaving room for fees (e.g. 0.99): ",
            "prompt_on_new": True,
        },
    )
    min_balance_to_act: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={
            "prompt": lambda mi: "Skip a dust asset if its balance is below this amount (0 = act on any balance): ",
            "prompt_on_new": True,
        },
    )


def get_connector_manager() -> ConnectorManager:
    client_config_map = load_client_config_map_from_file()
    return ConnectorManager(client_config_map)


class EmptyBalance:
    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: Optional[EmptyBalanceConfig] = None):
        if config is None:
            config = EmptyBalanceConfig()

        self.config = config
        self.exchange_name = config.exchange
        self.trading_pairs: List[str] = self._parse_pairs(config.trading_pairs)
        self.order_type = OrderType.LIMIT if config.order_type.lower() == "limit" else OrderType.MARKET

        self.connector: Optional[ConnectorBase] = None
        self._active_orders: Dict[str, str] = {}
        self._event_forwarders: List[EventForwarder] = []

    @staticmethod
    def _parse_pairs(pairs_str: str) -> List[str]:
        return [p.strip().upper() for p in pairs_str.split(",") if p.strip()]

    async def initialize_connector(self, timeout_sec: float = 60):
        password = os.environ.get("CONFIG_PASSWORD")
        if not password:
            raise RuntimeError(
                "Set the CONFIG_PASSWORD environment variable to the password used to unlock your "
                "encrypted Hummingbot API keys (the same password you use to log into the Hummingbot client)."
            )

        if not Security.login(ETHKeyFileSecretManger(password)):
            raise RuntimeError("Invalid CONFIG_PASSWORD; could not decrypt API keys.")
        await Security.wait_til_decryption_done()
        await read_system_configs_from_yml()

        connector_manager = get_connector_manager()
        self.connector = connector_manager.create_connector(
            connector_name=self.exchange_name,
            trading_pairs=self.trading_pairs,
            trading_required=True,
        )
        self._register_event_listeners()

        await self.connector.start_network()

        start_time = time.time()
        while not self.connector.ready:
            if time.time() - start_time > timeout_sec:
                not_ready = {k: v for k, v in self.connector.status_dict.items() if not v}
                raise TimeoutError(f"Timed out waiting for '{self.exchange_name}' to become ready: {not_ready}")
            await asyncio.sleep(1)

        self.logger().info(f"Connector '{self.exchange_name}' is ready")

    def _register_event_listeners(self):
        def _on_order_filled(event):
            self.logger().info(
                f"Filled {event.amount} {event.trading_pair} at {event.price} "
                f"on {self.exchange_name} (order_id={event.order_id})"
            )

        def _on_order_terminal(event):
            order_id = getattr(event, "order_id", None)
            for trading_pair, active_order_id in list(self._active_orders.items()):
                if active_order_id == order_id:
                    del self._active_orders[trading_pair]

        for tag, handler in (
            (MarketEvent.OrderFilled, _on_order_filled),
            (MarketEvent.SellOrderCompleted, _on_order_terminal),
            (MarketEvent.OrderCancelled, _on_order_terminal),
            (MarketEvent.OrderFailure, _on_order_terminal),
            (MarketEvent.OrderExpired, _on_order_terminal),
        ):
            forwarder = EventForwarder(handler)
            self._event_forwarders.append(forwarder)
            self.connector.add_listener(tag, forwarder)

    async def check_and_place_order(self):
        """
        For every configured trading pair, if there's no order still in flight for it, check the
        available balance of its base asset (the dust asset) and place a single sell order for that
        pair if the balance clears the exchange's minimum order size / notional requirements. All
        pairs are checked independently each cycle, so dust in several assets (e.g. USDT, ETH, BTC)
        can each have an order open at the same time.
        """
        for trading_pair in self.trading_pairs:
            if trading_pair in self._active_orders:
                self.logger().info(
                    f"Order {self._active_orders[trading_pair]} still open for {trading_pair}, skipping this cycle"
                )
                continue

            self._check_and_place_order_for_pair(trading_pair)

    def _check_and_place_order_for_pair(self, trading_pair: str):
        base, quote = split_hb_trading_pair(trading_pair)
        dust_asset = base.upper()

        available = self.connector.get_available_balance(dust_asset)
        if available <= self.config.min_balance_to_act:
            self.logger().info(f"{available} {dust_asset} available, nothing to do for {trading_pair}")
            return

        sell_amount = available * self.config.balance_use_pct

        order_amount = self._size_order(trading_pair, sell_amount)
        if order_amount is None:
            self.logger().warning(
                f"{available} {dust_asset} available, but {trading_pair} does not meet the "
                f"exchange's minimum order requirements."
            )
            return

        price = self.connector.get_price_by_type(trading_pair, PriceType.BestBid)
        if self.order_type == OrderType.LIMIT:
            order_price = self.connector.quantize_order_price(
                trading_pair, price * (Decimal(1) - self.config.limit_order_price_spread)
            )
        else:
            order_price = price

        order_id = self.connector.sell(
            trading_pair=trading_pair,
            amount=order_amount,
            order_type=self.order_type,
            price=order_price,
        )
        self._active_orders[trading_pair] = order_id
        self.logger().info(
            f"Placed {self.order_type.name} SELL {order_amount} {base} via {trading_pair} "
            f"(available: {available} {dust_asset}), order_id={order_id}"
        )

    def _size_order(self, trading_pair: str, sell_amount: Decimal) -> Optional[Decimal]:
        price = self.connector.get_price_by_type(trading_pair, PriceType.BestBid)
        if price is None or price.is_nan() or price <= 0:
            self.logger().warning(f"No valid bid price for {trading_pair}, skipping")
            return None

        amount = self.connector.quantize_order_amount(trading_pair, sell_amount)

        trading_rule = self.connector.trading_rules.get(trading_pair)
        if trading_rule is not None:
            if amount < trading_rule.min_order_size:
                self.logger().info(
                    f"{trading_pair}: quantized amount {amount} is below min_order_size "
                    f"{trading_rule.min_order_size}"
                )
                return None
            notional = amount * price
            if notional < trading_rule.min_notional_size or notional < trading_rule.min_order_value:
                self.logger().info(
                    f"{trading_pair}: order value {notional} is below the exchange minimum"
                )
                return None

        if amount <= 0:
            return None
        return amount

    async def close(self):
        if self.connector is not None:
            await self.connector.stop_network()


def _create_config_from_args(exchange: str, trading_pairs: str,
                             order_type: str, limit_order_price_spread: Decimal,
                             balance_use_pct: Decimal, min_balance_to_act: Decimal) -> EmptyBalanceConfig:
    return EmptyBalanceConfig(
        exchange=exchange,
        trading_pairs=trading_pairs,
        order_type=order_type,
        limit_order_price_spread=limit_order_price_spread,
        balance_use_pct=balance_use_pct,
        min_balance_to_act=min_balance_to_act,
    )


def main():
    parser = argparse.ArgumentParser(description="Run empty_balance as a standalone script")
    parser.add_argument("--exchange", default="binance", help=f"Connector name, one of: {', '.join(SUPPORTED_CONNECTORS)}")
    parser.add_argument(
        "--trading_pairs",
        default="USDT-INR",
        help=(
            "Comma-separated trading pairs to sell dust into; each pair's base asset is sold for its "
            "quote asset, e.g. USDT-INR,ETH-INR,BTC-INR"
        ),
    )
    parser.add_argument("--order_type", default="market", choices=list(ORDER_TYPES), help="Order type to place")
    parser.add_argument(
        "--limit_order_price_spread",
        type=Decimal,
        default=Decimal("0.001"),
        help="Spread below best bid for limit orders (e.g. 0.001 = 0.1%%)",
    )
    parser.add_argument(
        "--balance_use_pct",
        type=Decimal,
        default=Decimal("0.99"),
        help="Fraction of each dust balance to sell (e.g. 0.99)",
    )
    parser.add_argument(
        "--min_balance_to_act",
        type=Decimal,
        default=Decimal("0"),
        help="Skip a dust asset if its balance is below this amount",
    )
    parser.add_argument("--interval_sec", type=int, default=60, help="Seconds between balance checks")
    parser.add_argument("--once", action="store_true", help="Run once and exit")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = _create_config_from_args(
        exchange=args.exchange,
        trading_pairs=args.trading_pairs,
        order_type=args.order_type,
        limit_order_price_spread=args.limit_order_price_spread,
        balance_use_pct=args.balance_use_pct,
        min_balance_to_act=args.min_balance_to_act,
    )

    async def run_loop():
        eb = EmptyBalance(config=config)
        await eb.initialize_connector()
        try:
            while True:
                try:
                    await eb.check_and_place_order()
                except Exception as e:
                    logging.getLogger("empty_balance_standalone").exception(f"Error during check: {e}")

                if args.once:
                    return

                await asyncio.sleep(args.interval_sec)
        finally:
            await eb.close()

    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrupted, exiting")


if __name__ == "__main__":
    """
    Run the empty_balance script standalone, e.g.:
    CONFIG_PASSWORD=your_password python -m hummingbot.scripts.empty_balance \
        --exchange binance --trading_pairs USDT-INR,ETH-INR,BTC-INR --once

    Requires the exchange's API keys to already be configured via the Hummingbot client
    (`connect <exchange>`), and CONFIG_PASSWORD set to the password used to unlock them.
    """
    main()
