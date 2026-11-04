import importlib.util
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from pydantic import ValidationError

from hummingbot.connector.exchange.wazirx import wazirx_constants as CONSTANTS
from hummingbot.connector.exchange.wazirx.wazirx_exchange import WazirxExchange
from hummingbot.core.event.events import MarketEvent

_SCRIPT_PATH = Path(__file__).resolve().parents[4] / "scripts" / "wallet_transfer_example.py"
_spec = importlib.util.spec_from_file_location("wallet_transfer_example", _SCRIPT_PATH)
wallet_transfer_example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wallet_transfer_example)

WalletTransferConfig = wallet_transfer_example.WalletTransferConfig
WalletTransferExample = wallet_transfer_example.WalletTransferExample


class WalletTransferExampleConfigTest(unittest.TestCase):
    def test_default_direction_keeps_funds_on_the_exchange(self):
        self.assertEqual("sub_to_master", WalletTransferConfig().direction)

    def test_amount_must_be_positive(self):
        for amount in ("0", "-1"):
            with self.subTest(amount=amount):
                with self.assertRaises(ValidationError):
                    WalletTransferConfig(amount=Decimal(amount))
        self.assertEqual(Decimal("1.5"), WalletTransferConfig(amount=Decimal("1.5")).amount)

    def test_accepting_the_amount_default_is_refused(self):
        config = WalletTransferConfig()
        with self.assertRaises(ValidationError):
            config.amount = config.amount  # what accepting the pre-filled "0" at the prompt does


class WalletTransferExampleScriptTest(unittest.TestCase):
    def setUp(self):
        # ScriptStrategyBase registers its markets in Cython, which needs a real ConnectorBase.
        self.connector = WazirxExchange(
            wazirx_api_key="k",
            wazirx_api_secret="s",
            wazirx_master_api_key="mk",
            wazirx_master_api_secret="ms",
            wazirx_master_email="master@example.com",
            trading_pairs=["BTC-INR"],
            trading_required=False,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        self.connector.transfer_to_master = MagicMock(return_value="xfer-1")
        self.connector.withdraw_to_address = MagicMock()
        ready_patch = patch.object(WazirxExchange, "ready", new_callable=PropertyMock, return_value=False)
        self.ready = ready_patch.start()
        self.addCleanup(ready_patch.stop)
        config = WalletTransferConfig(connector="wazirx", amount=Decimal("1"), sub_account="sub@example.com")
        self.script = WalletTransferExample(connectors={"wazirx": self.connector}, config=config)

    def test_transfer_waits_for_the_connector_to_be_ready_and_fires_once(self):
        self.script.on_tick()
        self.connector.transfer_to_master.assert_not_called()

        self.ready.return_value = True
        self.script.on_tick()
        self.script.on_tick()

        self.connector.transfer_to_master.assert_called_once_with(
            asset="USDT", amount=Decimal("1"), from_account="sub@example.com", to_account=None,
        )
        self.connector.withdraw_to_address.assert_not_called()

    def test_listens_for_unknown_outcomes(self):
        self.assertEqual(1, len(self.connector.get_listeners(MarketEvent.WalletTransferUnknown)))
