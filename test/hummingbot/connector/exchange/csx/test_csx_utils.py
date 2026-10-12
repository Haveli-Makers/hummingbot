import unittest
from decimal import Decimal

from hummingbot.connector.exchange.csx.csx_utils import (
    CsxConfigMap,
    hb_pair_to_instrument,
    instrument_to_hb_pair,
    parse_balance_response,
    str_to_decimal,
    unwrap_data,
)


class CsxUtilsTests(unittest.TestCase):

    def test_str_to_decimal_valid(self):
        self.assertEqual(Decimal("1.5"), str_to_decimal("1.5"))
        self.assertEqual(Decimal("0"), str_to_decimal("bad"))
        self.assertEqual(Decimal("0"), str_to_decimal(None))

    def test_instrument_to_hb_pair(self):
        self.assertEqual("BTC-INR", instrument_to_hb_pair("BTC/INR"))
        self.assertEqual("ETH-USDT", instrument_to_hb_pair("eth/usdt"))

    def test_hb_pair_to_instrument(self):
        self.assertEqual("BTC/INR", hb_pair_to_instrument("BTC-INR"))
        self.assertEqual("ETH/USDT", hb_pair_to_instrument("eth-usdt"))

    def test_parse_balance_response_basic(self):
        raw = {
            "Available": {"BTC": "0.5", "INR": "50000"},
            "Locked": {"BTC": "0.1"},
        }
        result = parse_balance_response(raw)
        self.assertIn("BTC", result)
        self.assertIn("INR", result)
        self.assertEqual(Decimal("0.5"), result["BTC"]["free"])
        self.assertEqual(Decimal("0.1"), result["BTC"]["locked"])
        self.assertEqual(Decimal("0.6"), result["BTC"]["total"])
        self.assertEqual(Decimal("50000"), result["INR"]["free"])

    def test_parse_balance_response_empty(self):
        result = parse_balance_response({})
        self.assertEqual({}, result)

    def test_parse_balance_response_asset_uppercased(self):
        raw = {"Available": {"btc": "1"}, "Locked": {}}
        result = parse_balance_response(raw)
        self.assertIn("BTC", result)


class CsxConfigMapTests(unittest.TestCase):

    def test_connector_name(self):
        cfg = CsxConfigMap.model_construct()
        self.assertEqual("csx", cfg.connector)

    def test_config_map_has_key_fields(self):
        fields = CsxConfigMap.model_fields
        self.assertIn("csx_api_key", fields)
        self.assertIn("csx_api_secret", fields)


class CsxUnwrapDataTests(unittest.TestCase):
    """
    One shared unwrap for CSX's {"data": ...} envelope. This logic used to be
    spelled out at three call sites with slightly different phrasing, so a change
    to the envelope shape could be fixed in one and silently missed in the others.
    """

    def test_unwraps_envelope(self):
        self.assertEqual({"orderId": "1"}, unwrap_data({"data": {"orderId": "1"}}))

    def test_returns_unwrapped_payload_unchanged(self):
        self.assertEqual({"cancelled": True}, unwrap_data({"cancelled": True}))

    def test_identity_key_short_circuits_double_unwrap(self):
        # Already the bare order object — must not be unwrapped again.
        bare = {"orderId": "1", "data": "something else"}
        self.assertEqual(bare, unwrap_data(bare, identity_key="orderId"))

    def test_identity_key_absent_still_unwraps(self):
        self.assertEqual({"orderId": "1"}, unwrap_data({"data": {"orderId": "1"}}, identity_key="orderId"))

    def test_non_dict_passthrough(self):
        self.assertEqual([1, 2], unwrap_data([1, 2]))
        self.assertIsNone(unwrap_data(None))

    def test_null_data_envelope_returns_none(self):
        self.assertIsNone(unwrap_data({"data": None}))


if __name__ == "__main__":
    unittest.main()
