import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import Field

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.config_helpers import load_client_config_map_from_file, read_system_configs_from_yml
from hummingbot.client.settings import AllConnectorSettings, ConnectorType
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.connector_manager import ConnectorManager
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent

SUPPORTED_CONNECTORS = sorted(
    name for name, setting in AllConnectorSettings.get_connector_settings().items()
    if setting.type == ConnectorType.Exchange
)
ORDER_TYPES = ("market", "limit")
MODES = ("dry_run", "live")
RESULT_MARKER = "EMPTY_BALANCE_RESULT "
ORDER_ACK_TIMEOUT_SEC = 30
WORKFLOW_RUN_TIMEOUT_SEC = 200


class EmptyBalanceConfig(BaseClientModel):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    exchange: str = Field(
        default="binance",
        json_schema_extra={
            "prompt": lambda mi: "Exchange",
            "prompt_on_new": True,
            "input_type": "select",
            "options": SUPPORTED_CONNECTORS,
        },
    )
    trading_pairs: str = Field(
        default="USDT-INR",
        json_schema_extra={
            "prompt": lambda mi: "Trading pairs to sell dust into (comma-separated, e.g. USDT-INR,ETH-INR)",
            "prompt_on_new": True,
        },
    )
    order_type: str = Field(
        default="market",
        json_schema_extra={
            "prompt": lambda mi: "Order type",
            "prompt_on_new": True,
            "input_type": "select",
            "options": list(ORDER_TYPES),
        },
    )
    limit_order_price_spread: Decimal = Field(
        default=Decimal("0.001"),
        json_schema_extra={
            "prompt": lambda mi: "Spread below best bid (e.g. 0.001 = 0.1%)",
            "prompt_on_new": True,
            "visible_when": {"order_type": ["limit"]},
        },
    )
    balance_use_pct: Decimal = Field(
        default=Decimal("0.99"),
        json_schema_extra={
            "prompt": lambda mi: "Fraction of each dust balance to sell (e.g. 0.99)",
            "prompt_on_new": True,
        },
    )
    min_balance_to_act: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={
            "prompt": lambda mi: "Skip an asset if its available balance is at or below",
            "prompt_on_new": True,
        },
    )
    mode: str = Field(
        default="dry_run",
        json_schema_extra={
            "prompt": lambda mi: "Mode (dry_run only logs orders, live places them)",
            "prompt_on_new": True,
            "input_type": "select",
            "options": list(MODES),
        },
    )
    api_key: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Exchange API key",
            "prompt_on_new": True,
        },
    )
    secret_key: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Exchange API secret",
            "prompt_on_new": True,
        },
    )
    db_target: str = Field(default="local", json_schema_extra={"show_on_dashboard": False})

    @property
    def dry_run(self) -> bool:
        return self.mode != "live"


class EmptyBalance:
    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: EmptyBalanceConfig):
        self.config = config
        self.exchange_name = config.exchange
        self.trading_pairs: List[str] = [p.strip().upper() for p in config.trading_pairs.split(",") if p.strip()]
        self.order_type = OrderType.LIMIT if config.order_type.lower() == "limit" else OrderType.MARKET
        self.dry_run = config.dry_run

        self.connector: Optional[ConnectorBase] = None
        self._active_orders: Dict[str, str] = {}
        self.results: List[Dict[str, Any]] = []
        self._order_status: Dict[str, str] = {}
        self._event_forwarders: List[EventForwarder] = []

    def _connector_key_params(self) -> Dict[str, str]:
        """
        Each connector's constructor takes its keys under its own parameter names
        (e.g. wazirx_api_key / wazirx_api_secret, gate_io_api_key / gate_io_secret_key),
        so --api_key / --secret_key are passed under those names.
        """
        api_key, secret_key = self.config.api_key, self.config.secret_key
        if not api_key or not secret_key:
            raise ValueError("Both --api_key and --secret_key are required.")

        config_keys = AllConnectorSettings.get_connector_config_keys(self.exchange_name)
        field_names = list(type(config_keys).model_fields) if config_keys is not None else []
        key_field = next((f for f in field_names if f.endswith("_api_key")), None)
        secret_field = next((f for f in field_names if f.endswith(("_api_secret", "_secret_key"))), None)
        if key_field is None or secret_field is None:
            raise ValueError(f"Could not find API key / secret parameters for '{self.exchange_name}' (fields: {field_names}).")
        return {key_field: api_key, secret_field: secret_key}

    async def initialize_connector(self, timeout_sec: float = 60):
        api_keys = self._connector_key_params()
        await read_system_configs_from_yml()

        connector_manager = ConnectorManager(load_client_config_map_from_file())
        self.connector = connector_manager.create_connector(
            connector_name=self.exchange_name,
            trading_pairs=self.trading_pairs,
            trading_required=True,
            api_keys=api_keys,
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
        if self.dry_run:
            self.logger().info("DRY RUN mode: orders will be logged but NOT placed")
        else:
            self.logger().warning("LIVE mode: real orders WILL be placed")

    def _register_event_listeners(self):
        def _on_order_created(event):
            self._order_status.setdefault(event.order_id, "open")

        def _on_order_filled(event):
            self._order_status[event.order_id] = "partially_filled"
            self.logger().info(
                f"Filled {event.amount} {event.trading_pair} at {event.price} "
                f"on {self.exchange_name} (order_id={event.order_id})"
            )

        def _on_order_terminal(status: str):
            def _handler(event):
                order_id = getattr(event, "order_id", None)
                self._order_status[order_id] = status
                for trading_pair, active_order_id in list(self._active_orders.items()):
                    if active_order_id == order_id:
                        del self._active_orders[trading_pair]
            return _handler

        for tag, handler in (
            (MarketEvent.SellOrderCreated, _on_order_created),
            (MarketEvent.OrderFilled, _on_order_filled),
            (MarketEvent.SellOrderCompleted, _on_order_terminal("filled")),
            (MarketEvent.OrderCancelled, _on_order_terminal("cancelled")),
            (MarketEvent.OrderFailure, _on_order_terminal("failed")),
            (MarketEvent.OrderExpired, _on_order_terminal("expired")),
        ):
            forwarder = EventForwarder(handler)
            self._event_forwarders.append(forwarder)
            self.connector.add_listener(tag, forwarder)

    async def check_and_place_order(self):
        """
        For every configured trading pair, if there's no order still in flight for it, check the
        available balance of its base asset (the dust asset) and place a single sell order for that
        pair if the balance clears the exchange's minimum order size / notional requirements.
        """
        await self._refresh_balances()

        for trading_pair in self.trading_pairs:
            if trading_pair in self._active_orders:
                self.logger().info(
                    f"Order {self._active_orders[trading_pair]} still open for {trading_pair}, skipping this cycle"
                )
                self._record(trading_pair, "skipped", "previous order still open",
                             order_id=self._active_orders[trading_pair])
                continue

            try:
                self._check_and_place_order_for_pair(trading_pair)
            except Exception as e:
                self.logger().exception(f"Error checking {trading_pair}: {e}")
                self._record(trading_pair, "error", str(e))

    def _record(self, trading_pair: str, action: str, reason: str = "", **fields):
        row = {
            "exchange": self.exchange_name,
            "trading_pair": trading_pair,
            "mode": "dry_run" if self.dry_run else "live",
            "action": action,
            "reason": reason,
        }
        row.update({k: str(v) if isinstance(v, Decimal) else v for k, v in fields.items()})
        self.results.append(row)

    async def wait_for_order_acks(self, timeout_sec: float = ORDER_ACK_TIMEOUT_SEC):
        """
        Waits for all placed orders to be acknowledged by the exchange, or until the timeout is reached.
        """
        placed = [row for row in self.results if row["action"] == "placed"]
        deadline = time.time() + timeout_sec
        while any(row["order_id"] not in self._order_status for row in placed) and time.time() < deadline:
            await asyncio.sleep(0.5)
        for row in placed:
            row["order_status"] = self._order_status.get(row["order_id"], "unconfirmed")

    async def _refresh_balances(self):
        """
        Refreshes the connector's balances and logs all non-zero balances.
        """
        await self.connector._update_balances()

        non_zero = {asset: bal for asset, bal in self.connector.get_all_balances().items() if bal > 0}
        if non_zero:
            balances_str = ", ".join(
                f"{asset}: {bal} (available {self.connector.get_available_balance(asset)})"
                for asset, bal in sorted(non_zero.items())
            )
        else:
            balances_str = "none"
        self.logger().info(f"Balances on {self.exchange_name}: {balances_str}")

    def _check_and_place_order_for_pair(self, trading_pair: str):
        base, quote = split_hb_trading_pair(trading_pair)

        available = self.connector.get_available_balance(base)
        if available <= self.config.min_balance_to_act:
            total = self.connector.get_balance(base)
            self.logger().info(
                f"{available} {base} available (total: {total}, locked in open orders: {total - available}, "
                f"min_balance_to_act: {self.config.min_balance_to_act}), nothing to do for {trading_pair}"
            )
            self._record(trading_pair, "skipped", f"available balance at or below {self.config.min_balance_to_act}",
                         available=available)
            return

        best_bid = self.connector.get_price_by_type(trading_pair, PriceType.BestBid)
        if best_bid is None or best_bid.is_nan() or best_bid <= 0:
            self.logger().warning(f"No valid bid price for {trading_pair}, skipping")
            self._record(trading_pair, "skipped", "no valid bid price", available=available)
            return

        sell_amount = available * self.config.balance_use_pct
        order_amount = self._size_order(trading_pair, sell_amount, best_bid)
        if order_amount is None:
            self.logger().warning(
                f"{available} {base} available, but {trading_pair} does not meet the "
                f"exchange's minimum order requirements."
            )
            self._record(trading_pair, "skipped", "below exchange minimum order size / value",
                         available=available, best_bid=best_bid)
            return

        if self.order_type == OrderType.LIMIT:
            order_price = self.connector.quantize_order_price(
                trading_pair, best_bid * (Decimal(1) - self.config.limit_order_price_spread)
            )
        else:
            order_price = best_bid

        self._log_order_details(
            trading_pair=trading_pair,
            base=base,
            quote=quote,
            available=available,
            sell_amount=sell_amount,
            order_amount=order_amount,
            best_bid=best_bid,
            order_price=order_price,
        )

        if self.dry_run:
            self.logger().info(
                f"[DRY RUN] Would place {self.order_type.name} SELL {order_amount} {base} via {trading_pair} "
                f"at {order_price} {quote} on {self.exchange_name}. No order was sent."
            )
            self._record(trading_pair, "would_sell", order_type=self.order_type.name, available=available,
                         amount=order_amount, best_bid=best_bid, price=order_price)
            return

        order_id = self.connector.sell(
            trading_pair=trading_pair,
            amount=order_amount,
            order_type=self.order_type,
            price=order_price,
        )
        self._active_orders[trading_pair] = order_id
        self._record(trading_pair, "placed", order_type=self.order_type.name, available=available,
                     amount=order_amount, best_bid=best_bid, price=order_price, order_id=order_id)
        self.logger().info(
            f"Placed {self.order_type.name} SELL {order_amount} {base} via {trading_pair} "
            f"(available: {available} {base}), order_id={order_id}"
        )

    def _log_order_details(self, trading_pair: str, base: str, quote: str, available: Decimal,
                           sell_amount: Decimal, order_amount: Decimal, best_bid: Decimal,
                           order_price: Decimal):
        """
        Logs everything about the order that is (or, in dry run mode, would be) placed: balances,
        market prices, exchange trading rules, sizing and estimated fees / proceeds.
        """
        def _safe(fn):
            try:
                return fn()
            except Exception as e:
                return f"n/a ({type(e).__name__}: {e})"

        best_ask = _safe(lambda: self.connector.get_price_by_type(trading_pair, PriceType.BestAsk))
        mid_price = _safe(lambda: self.connector.get_price_by_type(trading_pair, PriceType.MidPrice))
        last_price = _safe(lambda: self.connector.get_price_by_type(trading_pair, PriceType.LastTrade))
        vwap_price = _safe(
            lambda: self.connector.get_vwap_for_volume(trading_pair, False, order_amount).result_price
        )

        rule = self.connector.trading_rules.get(trading_pair)
        if rule is not None:
            rules_str = (
                f"min_order_size={rule.min_order_size}, max_order_size={rule.max_order_size}, "
                f"min_price_increment={rule.min_price_increment}, "
                f"min_base_amount_increment={rule.min_base_amount_increment}, "
                f"min_notional_size={rule.min_notional_size}, min_order_value={rule.min_order_value}"
            )
        else:
            rules_str = "no trading rule found"

        est_exec_price = order_price if self.order_type == OrderType.LIMIT else (
            vwap_price if isinstance(vwap_price, Decimal) and not vwap_price.is_nan() else order_price
        )
        notional = order_amount * est_exec_price

        fee = _safe(lambda: self.connector.get_fee(
            base_currency=base,
            quote_currency=quote,
            order_type=self.order_type,
            order_side=TradeType.SELL,
            amount=order_amount,
            price=est_exec_price,
            is_maker=False,
        ))
        fee_str, est_proceeds = str(fee), notional
        if not isinstance(fee, str):
            fee_amount_in_quote = notional * fee.percent
            est_proceeds = notional - fee_amount_in_quote
            flat = ", ".join(f"{f.amount} {f.token}" for f in fee.flat_fees) or "none"
            fee_str = f"{fee.percent * 100}% (~{fee_amount_in_quote} {quote}), flat fees: {flat}"

        spread_str = (
            f"{self.config.limit_order_price_spread * 100}% below best bid"
            if self.order_type == OrderType.LIMIT else "n/a (market order)"
        )

        prefix = "[DRY RUN] " if self.dry_run else ""
        self.logger().info(
            f"{prefix}Order details for {trading_pair}:\n"
            f"  Exchange              : {self.exchange_name}\n"
            f"  Trading pair          : {trading_pair} (base={base}, quote={quote})\n"
            f"  Side / type           : SELL / {self.order_type.name}\n"
            f"  --- Balances ---\n"
            f"  {base} total            : {self.connector.get_balance(base)}\n"
            f"  {base} available        : {available}\n"
            f"  {quote} available        : {self.connector.get_available_balance(quote)}\n"
            f"  balance_use_pct       : {self.config.balance_use_pct}\n"
            f"  Raw sell amount       : {sell_amount} {base}\n"
            f"  Quantized order amount: {order_amount} {base}\n"
            f"  Leftover after order  : {available - order_amount} {base}\n"
            f"  --- Market ---\n"
            f"  Best bid              : {best_bid} {quote}\n"
            f"  Best ask              : {best_ask} {quote}\n"
            f"  Mid price             : {mid_price} {quote}\n"
            f"  Last trade price      : {last_price} {quote}\n"
            f"  VWAP for order amount : {vwap_price} {quote}\n"
            f"  --- Order ---\n"
            f"  Limit spread          : {spread_str}\n"
            f"  Order price (sent)    : {order_price} {quote}\n"
            f"  Est. execution price  : {est_exec_price} {quote}\n"
            f"  Est. order value      : {notional} {quote}\n"
            f"  Est. fee              : {fee_str}\n"
            f"  Est. proceeds         : {est_proceeds} {quote}\n"
            f"  --- Exchange rules ---\n"
            f"  {rules_str}"
        )

    def _size_order(self, trading_pair: str, sell_amount: Decimal, best_bid: Decimal) -> Optional[Decimal]:
        amount = self.connector.quantize_order_amount(trading_pair, sell_amount)
        self.logger().info(
            f"{trading_pair}: best bid {best_bid}, sell amount {sell_amount} quantized to {amount}"
        )

        trading_rule = self.connector.trading_rules.get(trading_pair)
        if trading_rule is not None:
            if amount < trading_rule.min_order_size:
                self.logger().info(
                    f"{trading_pair}: quantized amount {amount} is below min_order_size "
                    f"{trading_rule.min_order_size}"
                )
                return None
            notional = amount * best_bid
            if notional < trading_rule.min_notional_size or notional < trading_rule.min_order_value:
                self.logger().info(
                    f"{trading_pair}: order value {notional} is below the exchange minimum "
                    f"(min_notional_size={trading_rule.min_notional_size}, "
                    f"min_order_value={trading_rule.min_order_value})"
                )
                return None

        if amount <= 0:
            return None
        return amount

    async def close(self):
        if self.connector is not None:
            await self.connector.stop_network()


class EmptyBalanceWorkflow:
    """Runs empty_balance.py as a subprocess and returns its results to the dashboard."""

    def __init__(self, config: EmptyBalanceConfig):
        self.config = config

    def _command(self) -> List[str]:
        c = self.config
        return [
            sys.executable, os.path.abspath(__file__),
            "--exchange", c.exchange,
            "--trading_pairs", c.trading_pairs,
            "--order_type", c.order_type,
            "--limit_order_price_spread", str(c.limit_order_price_spread),
            "--balance_use_pct", str(c.balance_use_pct),
            "--min_balance_to_act", str(c.min_balance_to_act),
            "--mode", c.mode,
            "--api_key", c.api_key,
            "--secret_key", c.secret_key,
            "--once",
        ]

    async def run_once(self) -> List[Dict[str, Any]]:
        proc = await asyncio.to_thread(
            subprocess.run,
            self._command(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=WORKFLOW_RUN_TIMEOUT_SEC,
        )
        output = proc.stdout.decode("utf-8", errors="replace")

        for line in reversed(output.splitlines()):
            if line.startswith(RESULT_MARKER):
                return json.loads(line[len(RESULT_MARKER):])

        log_tail = "\n".join(output.strip().splitlines()[-30:])
        raise RuntimeError(f"empty_balance exited with code {proc.returncode} without a result:\n{log_tail}")


def main():
    parser = argparse.ArgumentParser(description="Sell dust balances on an exchange")
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
        help="Only used with --order_type limit: spread below best bid (e.g. 0.001 = 0.1%%)",
    )
    parser.add_argument(
        "--balance_use_pct",
        type=Decimal,
        default=Decimal("0.99"),
        help="Fraction of each dust balance to sell, leaving room for fees (e.g. 0.99)",
    )
    parser.add_argument(
        "--min_balance_to_act",
        type=Decimal,
        default=Decimal("0"),
        help="Skip a dust asset if its balance is below this amount",
    )
    parser.add_argument("--interval_sec", type=int, default=60, help="Seconds between balance checks")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--mode",
        default="dry_run",
        choices=list(MODES),
        help="dry_run (default): only log the orders that would be placed. live: actually place orders.",
    )
    parser.add_argument("--api_key", required=True, help="Exchange API key")
    parser.add_argument("--secret_key", required=True, help="Exchange API secret")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )

    config = EmptyBalanceConfig(
        exchange=args.exchange,
        trading_pairs=args.trading_pairs,
        order_type=args.order_type,
        limit_order_price_spread=args.limit_order_price_spread,
        balance_use_pct=args.balance_use_pct,
        min_balance_to_act=args.min_balance_to_act,
        mode=args.mode,
        api_key=args.api_key,
        secret_key=args.secret_key,
    )

    async def run_loop():
        eb = EmptyBalance(config=config)
        try:
            await eb.initialize_connector()
            while True:
                try:
                    await eb.check_and_place_order()
                except Exception as e:
                    eb.logger().exception(f"Error during check: {e}")

                if args.once:
                    await eb.wait_for_order_acks()
                    print(RESULT_MARKER + json.dumps(eb.results, default=str), flush=True)
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
    Sells the dust balance of each trading pair's base asset (e.g. USDT in USDT-INR) for its quote asset.

    Run standalone:
        python -m hummingbot.scripts.empty_balance --exchange wazirx --trading_pairs USDT-INR --once --mode dry_run --api_key <key> --secret_key <secret>

    --api_key and --secret_key are required.
    --mode dry_run (default) only logs the orders that would be placed; --mode live places them.
    --order_type market (default) or limit; limit orders are priced --limit_order_price_spread below the best bid.

    ex: python -m hummingbot.scripts.empty_balance --exchange wazirx --trading_pairs USDT-INR --once --mode dry_run --order_type limit --limit_order_price_spread 0.001 --api_key <key> --secret_key <secret>
    """
    main()
