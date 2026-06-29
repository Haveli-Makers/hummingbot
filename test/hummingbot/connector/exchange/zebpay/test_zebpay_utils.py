import unittest
from decimal import Decimal

from hummingbot.connector.exchange.zebpay.zebpay_utils import (
    ZebpayConfigMap,
    parse_balance_response,
    raise_for_status,
    str_to_decimal,
    unwrap_data,
)


class ZebpayUtilsTests(unittest.TestCase):

    def test_str_to_decimal(self):
        self.assertEqual(Decimal("1.5"), str_to_decimal("1.5"))
        self.assertEqual(Decimal("0"), str_to_decimal("bad"))
        self.assertEqual(Decimal("0"), str_to_decimal(None))

    def test_unwrap_data(self):
        self.assertEqual({"a": 1}, unwrap_data({"data": {"a": 1}, "statusCode": 200}))
        self.assertEqual({"a": 1}, unwrap_data({"a": 1}))  # no wrapper
        self.assertEqual([1, 2], unwrap_data({"data": [1, 2]}))

    def test_parse_balance_response_list(self):
        resp = {"data": [
            {"currency": "BTC", "total": "1.2", "free": "1.0", "used": "0.2"},
            {"currency": "INR", "total": "5000", "free": "5000", "used": "0"},
        ]}
        parsed = parse_balance_response(resp)
        self.assertEqual(Decimal("1.0"), parsed["BTC"]["free"])
        self.assertEqual(Decimal("0.2"), parsed["BTC"]["locked"])
        self.assertEqual(Decimal("1.2"), parsed["BTC"]["total"])
        self.assertEqual(Decimal("5000"), parsed["INR"]["free"])

    def test_parse_balance_response_total_derived(self):
        # When "total" is absent it is derived from free + used.
        resp = [{"currency": "eth", "free": "0.5", "used": "0.1"}]
        parsed = parse_balance_response(resp)
        self.assertEqual(Decimal("0.6"), parsed["ETH"]["total"])

    def test_parse_balance_response_empty(self):
        self.assertEqual({}, parse_balance_response({"data": []}))

    def test_parse_balance_response_null_data(self):
        # {"data": null} must not raise AttributeError — return an empty mapping.
        self.assertEqual({}, parse_balance_response({"data": None}))
        self.assertEqual({}, parse_balance_response({"data": "unexpected-string"}))

    def test_raise_for_status_ok(self):
        ok = {"data": {"orderId": "1"}, "statusCode": 200}
        self.assertIs(ok, raise_for_status(ok))  # no raise, returns input
        self.assertEqual([1], raise_for_status([1]))  # non-dict passes through

    def test_raise_for_status_error(self):
        rejected = {"data": None, "statusCode": 77,
                    "statusDescription": "Rate should be in the range of 5694116 - 7703804"}
        with self.assertRaises(IOError):
            raise_for_status(rejected)


class ZebpayConfigMapTests(unittest.TestCase):

    def test_connector_name(self):
        self.assertEqual("zebpay", ZebpayConfigMap.model_construct().connector)

    def test_fields(self):
        fields = ZebpayConfigMap.model_fields
        self.assertIn("zebpay_api_key", fields)
        self.assertIn("zebpay_api_secret", fields)


if __name__ == "__main__":
    unittest.main()
