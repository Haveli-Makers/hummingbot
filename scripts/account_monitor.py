"""
Account monitor: a bot that never places or cancels an order. It watches one exchange account and writes its
balances, open orders, fills and market data to InfluxDB, for the dashboard's Account Monitoring page. It sees the
whole account, whatever acts on it: other bots on the same account, or people.

Run it like any bot, with:
- the account's API keys (`connect <exchange>`; an IP-bound CoinDCX key also needs the CoinDCX proxy support
  from branch vinayak/cdx-updates);
- the bot-monitoring settings in the environment: INFLUXDB_URL and INFLUXDB_TOKEN, optionally INFLUXDB_DATABASE.

    start --script account_monitor.py --conf conf_account_monitor_coindcx.yml

What it writes, and when, is described in hummingbot/remote_iface/account_monitor.py.
"""
import os
import re
from typing import Dict, List, Optional

from pydantic import Field, field_validator

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.remote_iface.account_monitor import AccountMonitor
from hummingbot.remote_iface.influxdb import InfluxDBConfig, InfluxDBWriter
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

# The api-server only accepts names like this (they go into SQL).
SAFE_NAME = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")


class AccountMonitorConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    account_name: str = Field("coindcx-main", description="The account's name on the Account Monitoring page")
    exchange: str = Field("coindcx")
    trading_pairs: str = Field("ETH-INR", description="Comma-separated: markets for open orders and market data")
    update_interval: float = Field(5.0, ge=1.0, description="Seconds between balance, open order and fill reads")
    market_data_interval: float = Field(1.0, ge=1.0, description="Seconds between order book samples")
    fills_lookback_hours: float = Field(24.0, ge=0.0, description="How far back to read fills when it starts")

    @field_validator("account_name")
    @classmethod
    def check_account_name(cls, value: str) -> str:
        if not SAFE_NAME.match(value):
            raise ValueError("use 1-64 letters, digits, '.', '_' or '-'")
        return value

    @field_validator("trading_pairs")
    @classmethod
    def check_trading_pairs(cls, value: str) -> str:
        if not [pair for pair in value.split(",") if pair.strip()]:
            raise ValueError("give at least one trading pair, e.g. ETH-INR")
        return value

    def pairs(self) -> List[str]:
        return [pair.strip() for pair in self.trading_pairs.split(",") if pair.strip()]


class AccountMonitorScript(ScriptStrategyBase):
    """Reads the account every update_interval seconds and samples the order books every market_data_interval."""

    @classmethod
    def init_markets(cls, config: AccountMonitorConfig):
        cls.markets = {config.exchange: set(config.pairs())}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: AccountMonitorConfig):
        super().__init__(connectors)
        self.config = config
        influxdb = InfluxDBConfig.from_env()
        self._writer: Optional[InfluxDBWriter] = InfluxDBWriter(influxdb) if influxdb else None
        self._monitor: Optional[AccountMonitor] = None
        if self._writer is not None:
            self._monitor = AccountMonitor(connectors[config.exchange], config.account_name, self._writer,
                                           config.pairs(), fills_lookback_s=config.fills_lookback_hours * 3600)
        else:
            self.logger().error("Account monitor: INFLUXDB_URL and INFLUXDB_TOKEN aren't set, so nothing is written. "
                                "Set them (the api-server's `make monitoring` prints the token) and start again.")
        self._next_update = 0.0
        self._next_sample = 0.0
        self._update_task = None

    def on_tick(self):
        if self._monitor is None:
            return
        self._writer.start()  # no-op once it runs
        now = self.current_timestamp
        if now >= self._next_sample:
            self._next_sample = now + self.config.market_data_interval
            self._monitor.sample_market()
        if now >= self._next_update and (self._update_task is None or self._update_task.done()):
            self._next_update = now + self.config.update_interval
            self._update_task = safe_ensure_future(self._monitor.update())

    async def on_stop(self):
        if self._update_task is not None and not self._update_task.done():
            self._update_task.cancel()
        if self._writer is not None:
            await self._writer.close()

    def format_status(self) -> str:
        lines = [f"Account monitor: {self.config.account_name} on {self.config.exchange}, "
                 f"pairs {', '.join(self.config.pairs())}. It never places or cancels orders."]
        if self._monitor is None:
            return "\n".join(lines + ["Not writing: INFLUXDB_URL and INFLUXDB_TOKEN aren't set."])
        last = self._monitor.last_update
        if not last:
            return "\n".join(lines + ["No update yet."])
        lines.append(f"Last update: {last.get('assets')} assets with a balance, {last.get('open_orders')} open orders, "
                     f"{last.get('new_fills')} new fills; {self._writer.pending} rows waiting to be written.")
        if last.get("error"):
            lines.append(f"Errors: {last['error']}")
        return "\n".join(lines)
