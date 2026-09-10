import argparse
import asyncio
import getpass
import json
import logging
import math
import os
import sys
from decimal import Decimal
from typing import Any, Dict, Optional

import yaml
from pydantic import Field

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.config.security import Security
from hummingbot.core.utils.market_price import get_last_price
from hummingbot.user.user_balances import UserBalances

_security_logged_in = False


def _ensure_security_login():
    global _security_logged_in
    if _security_logged_in:
        return
    password = os.environ.get("CONFIG_PASSWORD") or getpass.getpass("Enter Hummingbot config password: ")
    if not Security.login(ETHKeyFileSecretManger(password)):
        raise RuntimeError("Invalid Hummingbot config password")
    _security_logged_in = True


def _find_config_key(connector_name: str, account: str) -> Optional[str]:
    candidates = [
        key for key in Security.all_decrypted_values()
        if key.lower().startswith(connector_name.lower()) and account.lower() in key.lower()
    ]
    if not candidates:
        return None
    exact = [k for k in candidates if k.lower() == f"{connector_name}:{account}".lower()]
    return exact[0] if exact else candidates[0]


class ExchangeAdapter:
    def get_account_id(self, account_name: str) -> str:
        return account_name

    def get_acc_from_exchange(self, exchange_label: str) -> str:
        return exchange_label

    async def transfer_currency(
        self, connector: Any, from_account_id: str, to_account_id: str, currency: str, amount: float
    ) -> Any:
        raise NotImplementedError(f"transfer_currency is not implemented for '{connector.name}'")

    async def get_last_price(self, connector: Any, token: str, quote_token: str) -> Optional[float]:
        """Return None to fall back to the generic hummingbot last-traded-price lookup."""
        return None


class CoinDCXAdapter(ExchangeAdapter):
    TRANSFER_PATH_URL = "/exchange/v1/users/transfer"

    def get_account_id(self, account_name: str) -> str:
        """CoinDCX has no public API to list sub-accounts; ids are configured via env vars."""
        return os.environ.get(f"COINDCX_ACCOUNT_ID_{account_name}", account_name)

    async def transfer_currency(
        self, connector: Any, from_account_id: str, to_account_id: str, currency: str, amount: float
    ) -> Any:
        connector.logger().warning(
            f"transfer_currency() calls an unverified CoinDCX endpoint ({self.TRANSFER_PATH_URL}); "
            "confirm it against your CoinDCX enterprise API docs before trusting it in production."
        )
        return await connector._api_post(
            path_url=self.TRANSFER_PATH_URL,
            data={
                "from_account_id": from_account_id,
                "to_account_id": to_account_id,
                "currency": currency,
                "amount": amount,
            },
            is_auth_required=True,
        )

    async def get_last_price(self, connector: Any, token: str, quote_token: str) -> Optional[float]:
        trading_pair = f"{token.upper()}-{quote_token.upper()}"
        try:
            symbol = await connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        except Exception:
            return None

        tickers = await connector.get_all_pairs_prices()
        if isinstance(tickers, list):
            for ticker in tickers:
                if ticker.get("market", "") == symbol:
                    return float(ticker.get("last_price", 0))
        return None


EXCHANGE_ADAPTERS: Dict[str, ExchangeAdapter] = {
    "coindcx": CoinDCXAdapter(),
}


class ExchangeAccount:
    def __init__(self, logger: logging.Logger, exchange: str, account: str):
        self.logger = logger
        self.exchange = exchange.lower()
        self.account = account
        self.adapter = EXCHANGE_ADAPTERS.get(self.exchange, ExchangeAdapter())
        self.connector = None

    async def connect(self):
        _ensure_security_login()
        config_key = _find_config_key(self.exchange, self.account)
        api_keys = Security.api_keys(config_key) if config_key else {}
        if not api_keys:
            self.logger.warning(
                f"No credentials found in conf/connectors for '{self.exchange}' account '{self.account}'; "
                "authenticated calls will fail. Add it via Hummingbot's `connect` command."
            )
        self.connector = UserBalances.connect_market(
            f"{self.exchange}:{self.account}", ClientConfigMap(), **api_keys
        )
        if self.connector is None:
            raise RuntimeError(f"Could not initialize '{self.exchange}' connector for account '{self.account}'")
        await self.connector._update_balances()
        await self.connector._update_trading_rules()

    def get_account_id(self) -> str:
        return self.adapter.get_account_id(self.account)

    def get_acc_from_exchange(self, exchange_label: str) -> str:
        return self.adapter.get_acc_from_exchange(exchange_label)

    def get_balances(self) -> Dict[str, float]:
        return {token: float(balance) for token, balance in self.connector.get_all_balances().items()}

    async def get_last_price(self, token: str, quote_token: str) -> float:
        if token.upper() == quote_token.upper():
            return 1.0
        adapter_price = await self.adapter.get_last_price(self.connector, token, quote_token)
        if adapter_price is not None:
            return adapter_price
        price = await get_last_price(self.exchange, f"{token.upper()}-{quote_token.upper()}")
        return float(price) if price is not None else 0.0

    def floor_transfer_amount(self, token: str, quote_token: str, amount: float) -> float:
        if token.upper() == quote_token.upper():
            return math.floor(amount * 100) / 100
        trading_pair = f"{token.upper()}-{quote_token.upper()}"
        try:
            return float(self.connector.quantize_order_amount(trading_pair, Decimal(str(amount))))
        except Exception as e:
            self.logger.warning(f"Could not quantize {trading_pair} amount, falling back to raw floor: {e}")
            return math.floor(amount * 1e8) / 1e8

    async def transfer_currency(self, from_account_id: str, to_account_id: str, currency: str, amount: float) -> Any:
        return await self.adapter.transfer_currency(self.connector, from_account_id, to_account_id, currency, amount)


class CapitalBalancerConfig(BaseClientModel):
    """
    Configuration for the Capital Balancer script.
    """

    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    exchange: str = Field(
        default="coindcx",
        json_schema_extra={
            "prompt": lambda mi: "Enter the exchange connector to use (e.g. coindcx, coinswitch, binance): ",
            "prompt_on_new": True,
        },
    )
    master_account: str = Field(
        default="ORG",
        json_schema_extra={
            "prompt": lambda mi: "Enter the master/org account name (e.g. ORG): ",
            "prompt_on_new": True,
        },
    )
    account: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Enter the sub-account name to balance (e.g. SUB1): ",
            "prompt_on_new": True,
        },
    )
    quote_token: str = Field(
        default="INR",
        json_schema_extra={
            "prompt": lambda mi: "Enter the quote token to price/transfer in (e.g. INR, USDT): ",
            "prompt_on_new": True,
        },
    )
    token_config_path: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Enter the path to the token quantities JSON config file: ",
            "prompt_on_new": True,
        },
    )
    dry_run: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Run in dry-run mode (no actual transfers)? (True/False): ",
            "prompt_on_new": True,
        },
    )
    upper_threshold: float = Field(
        default=1.12,
        gt=1.0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the upper ratio threshold to trigger transfer to ORG (e.g. 1.12): ",
            "prompt_on_new": True,
        },
    )
    lower_threshold: float = Field(
        default=1.08,
        gt=1.0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the lower ratio threshold to trigger transfer from ORG (e.g. 1.08): ",
            "prompt_on_new": True,
        },
    )
    balance_ratio: float = Field(
        default=1.10,
        gt=1.0,
        json_schema_extra={
            "prompt": lambda mi: "Enter the target balance ratio to maintain (e.g. 1.10): ",
            "prompt_on_new": True,
        },
    )


def load_config_from_yml(config_path: str) -> CapitalBalancerConfig:
    """Load CapitalBalancerConfig from a hummingbot-style .yml config file."""
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    data = {k: v for k, v in data.items() if v is not None}
    return CapitalBalancerConfig(**data)


class CapitalBalancer:
    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: Optional[CapitalBalancerConfig] = None):
        if config is None:
            config = CapitalBalancerConfig()

        self.quote_token = config.quote_token.upper()
        self.dry_run = config.dry_run
        self.upper_threshold = config.upper_threshold
        self.lower_threshold = config.lower_threshold
        self.balance_ratio = config.balance_ratio
        self.sub_acc = config.account

        self.handler_master = ExchangeAccount(self.logger(), config.exchange, config.master_account)
        self.handler_sub = ExchangeAccount(self.logger(), config.exchange, config.account)

        self.config = self.load_token_config(config.token_config_path)

        self.master_account_id: Optional[str] = None
        self.sub_account_id: Optional[str] = None

    def floor_transfer_amount(self, token: str, amount: float) -> float:
        return self.handler_master.floor_transfer_amount(token, self.quote_token, amount)

    def load_token_config(self, config_path: str) -> dict:
        """Load token quantities config from a JSON file."""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            self.logger().info(f"Loaded token config from {config_path}")
            config = {token: details for token, details in config.items() if details.get("active", True)}
            for token, details in config.items():
                if "exchange" in details:
                    config[token]["account"] = self.handler_master.get_acc_from_exchange(details["exchange"])
            return config
        except Exception as e:
            self.logger().error(f"Failed to load token config: {e}")
            raise

    async def calculate_required_amount(self, sub_acc_balances: Dict[str, float]) -> float:
        """
        Calculate the required quote-token amount for trading.

        For each token in config:
        1. Get the required quantity from config
        2. Get current balance from sub account
        3. Calculate deficit quantity
        4. Multiply by last traded price to get quote-token value
        """
        total_required = 0.0

        for token, token_config in self.config.items():
            if token_config.get("account") != self.sub_acc:
                continue

            qty = token_config.get("quantity", 0)
            required_qty = qty + token_config.get("alloted_currency", 0)
            if required_qty == 0:
                continue

            current_balance = sub_acc_balances.get(token.upper(), 0)
            deficit_qty = max(0, required_qty - current_balance)

            last_price = await self.handler_master.get_last_price(token, self.quote_token)
            value = round(deficit_qty * last_price, 2)
            total_required += value

            self.logger().debug(
                f"{token}: Required={required_qty}, Balance={current_balance}, "
                f"Deficit={deficit_qty}, Price={last_price}, {self.quote_token}={value}"
            )

        return round(total_required, 2)

    async def calculate_portfolio_value(self, balances: Dict[str, float]) -> float:
        """Calculate total portfolio value in the quote token."""
        total_value = balances.get(self.quote_token, 0)

        for token, balance in balances.items():
            if token == self.quote_token or balance <= 0:
                continue

            last_price = await self.handler_master.get_last_price(token, self.quote_token)
            token_value = balance * last_price
            total_value += token_value

            if token_value > 0:
                self.logger().debug(f"{token}: Balance={balance}, Price={last_price}, Value={token_value}")

        return round(total_value, 2)

    async def transfer_coins(self, org_acc_balances: Dict[str, float], sub_acc_balances: Dict[str, float]):
        for token, balance in org_acc_balances.items():
            if token == self.quote_token or balance <= 0:
                continue
            if token not in self.config or self.config[token].get("account") != self.sub_acc:
                continue

            if self.dry_run:
                self.logger().info(f"[DRY RUN] Would transfer {balance} {token} from ORG/MASTER to {self.sub_acc} account")
                continue

            transfer_amount = self.floor_transfer_amount(token, balance)
            if transfer_amount <= 0:
                self.logger().info(f"Skipping transfer for {token}; floored amount is 0")
                continue

            await self.handler_master.transfer_currency(
                self.master_account_id, self.sub_account_id, token, transfer_amount
            )

        for token, balance in sub_acc_balances.items():
            if token == self.quote_token or balance <= 0:
                continue
            if token in self.config and self.config[token].get("account") == self.sub_acc:
                continue

            if self.dry_run:
                self.logger().info(f"[DRY RUN] Would transfer {balance} {token} from {self.sub_acc} account to ORG/MASTER")
                continue

            transfer_amount = self.floor_transfer_amount(token, balance)
            if transfer_amount <= 0:
                self.logger().info(f"Skipping transfer for {token}; floored amount is 0")
                continue

            await self.handler_master.transfer_currency(
                self.sub_account_id, self.master_account_id, token, transfer_amount
            )

    async def run(self):
        """Main execution logic."""
        self.logger().info("=" * 60)
        self.logger().info("Starting Capital Balancer")
        self.logger().info("=" * 60)

        self.logger().info("Initializing handlers...")
        await self.handler_master.connect()
        await self.handler_sub.connect()

        self.master_account_id = self.handler_master.get_account_id()
        self.sub_account_id = self.handler_sub.get_account_id()

        trading_pairs = [
            f"{token.upper()}-{self.quote_token}"
            for token, token_config in self.config.items()
            if token_config.get("account") == self.sub_acc
        ]

        self.logger().info("Fetching SUB portfolio balances...")
        sub_acc_balances = self.handler_sub.get_balances()
        self.logger().info(f"SUB ({self.sub_acc}) trading pairs: {', '.join(trading_pairs) or 'none configured'}")
        self.logger().info(f"SUB ({self.sub_acc}) balances: {sub_acc_balances}")

        self.logger().info("Fetching ORG/MASTER portfolio balances...")
        org_acc_balances = self.handler_master.get_balances()
        self.logger().info(f"ORG/MASTER trading pairs: {', '.join(trading_pairs) or 'none configured'}")
        self.logger().info(f"ORG/MASTER balances: {org_acc_balances}")

        self.logger().info("Calculating required amount for trading...")
        required_amount = await self.calculate_required_amount(sub_acc_balances)
        self.logger().info(f"Current Required {self.quote_token} for Trading: {required_amount:,.2f}")

        sub_acc_quote_balance = sub_acc_balances.get(self.quote_token, 0)
        self.logger().info(f"Current {self.quote_token} Balance in SUB account: {sub_acc_quote_balance:,.2f}")

        portfolio_value = await self.calculate_portfolio_value(sub_acc_balances)
        self.logger().info(f"Total Portfolio Value in SUB account: {portfolio_value:,.2f}")

        req_amount = (portfolio_value - sub_acc_quote_balance) + required_amount
        self.logger().info(f"Total Required {self.quote_token} for Trading: {req_amount:,.2f}")

        ratio = portfolio_value / req_amount if req_amount > 0 else float("inf")
        self.logger().info(f"Portfolio to Required Ratio: {ratio:.2%}")

        self.logger().info("\n--- Decision ---")

        await self.transfer_coins(org_acc_balances, sub_acc_balances)

        if ratio > self.upper_threshold:
            excess_amount = portfolio_value - (req_amount * self.balance_ratio)
            transfer_amount = min(excess_amount, sub_acc_quote_balance)

            if transfer_amount > 0:
                self.logger().info(
                    f"Portfolio ({ratio:.2%}) > {self.upper_threshold:.0%} threshold. "
                    f"Transferring {transfer_amount:,.2f} {self.quote_token} from {self.sub_acc} account to ORG/MASTER"
                )
                if self.dry_run:
                    self.logger().info(
                        f"[DRY RUN] Would transfer {transfer_amount} {self.quote_token} from "
                        f"{self.sub_acc} account to ORG/MASTER"
                    )
                    return True
                await self.handler_master.transfer_currency(
                    self.sub_account_id,
                    self.master_account_id,
                    self.quote_token,
                    self.floor_transfer_amount(self.quote_token, transfer_amount),
                )
            else:
                self.logger().info(f"No {self.quote_token} available to transfer from {self.sub_acc} account")

        elif ratio < self.lower_threshold:
            transfer_amount = (req_amount * self.balance_ratio) - portfolio_value
            org_quote_balance = org_acc_balances.get(self.quote_token, 0)
            transfer_amount = min(transfer_amount, org_quote_balance)

            if transfer_amount > 0:
                self.logger().info(
                    f"Portfolio ({ratio:.2%}) < {self.lower_threshold:.0%} threshold. "
                    f"Transferring {transfer_amount:,.2f} {self.quote_token} from ORG/MASTER to {self.sub_acc} account"
                )
                if self.dry_run:
                    self.logger().info(
                        f"[DRY RUN] Would transfer {transfer_amount} {self.quote_token} from "
                        f"ORG/MASTER to {self.sub_acc} account"
                    )
                    return True
                await self.handler_master.transfer_currency(
                    self.master_account_id,
                    self.sub_account_id,
                    self.quote_token,
                    self.floor_transfer_amount(self.quote_token, transfer_amount),
                )
            else:
                self.logger().info("No quote token available to transfer from ORG/MASTER account")
        else:
            self.logger().info(
                f"Portfolio ({ratio:.2%}) is within acceptable range "
                f"({self.lower_threshold:.0%} - {self.upper_threshold:.0%}). No action needed."
            )

        self.logger().info("=" * 60)
        self.logger().info("Capital Balancer completed")
        self.logger().info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Run capital_balancer as a standalone script")
    parser.add_argument(
        "--conf", "-c",
        type=str,
        required=True,
        help="Path to a capital_balancer YAML config file (see conf/scripts/conf_capital_balancer_org6.yml "
             "for an example; it points to the token quantities JSON via token_config_path)",
    )
    parser.add_argument("--dry-run", "-d", action="store_true", help="Force dry-run mode, overriding the config file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose (debug) logging")
    parser.add_argument("--interval_sec", type=int, default=900, help="Balance check interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run once and exit")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    config = load_config_from_yml(args.conf)
    if args.dry_run:
        config.dry_run = True

    async def run_loop():
        while True:
            try:
                balancer = CapitalBalancer(config=config)
                await balancer.run()
            except Exception as e:
                logging.getLogger("capital_balancer_standalone").exception(f"Error during balancer run: {e}")

            if args.once:
                return

            await asyncio.sleep(args.interval_sec)

    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        print("Interrupted, exiting")


if __name__ == "__main__":
    """Run standalone using python -m hummingbot.scripts.capital_balancer_standalone -c conf/scripts/conf_capital_balancer_sub1.yml --once"""
    main()
