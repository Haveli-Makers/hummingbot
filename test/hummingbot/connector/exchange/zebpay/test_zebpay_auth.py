import hashlib
import hmac
import json
import unittest
from unittest.mock import MagicMock

from hummingbot.connector.exchange.zebpay.zebpay_auth import ZebpayAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


def _make_auth(key="api_key", secret="secret_key"):
    tp = MagicMock()
    tp.time.return_value = 1_700_000_000.0  # seconds → 1_700_000_000_000 ms
    return ZebpayAuth(api_key=key, secret_key=secret, time_provider=tp)


class ZebpayAuthTests(unittest.IsolatedAsyncioTestCase):

    async def test_get_signs_query_string_with_timestamp(self):
        auth = _make_auth()
        req = RESTRequest(method=RESTMethod.GET,
                          url="https://sapi.zebpay.com/api/v2/ex/order",
                          params={"orderId": "42"})
        result = await auth.rest_authenticate(req)

        self.assertEqual("api_key", result.headers["x-auth-apikey"])
        self.assertEqual(1_700_000_000_000, result.params["timestamp"])
        expected_q = "orderId=42&timestamp=1700000000000"
        expected_sig = hmac.new(b"secret_key", expected_q.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(expected_sig, result.headers["x-auth-signature"])

    async def test_post_signs_json_body_with_timestamp(self):
        auth = _make_auth()
        req = RESTRequest(method=RESTMethod.POST,
                          url="https://sapi.zebpay.com/api/v2/ex/orders",
                          data={"symbol": "BTC-INR", "side": "BUY"})
        result = await auth.rest_authenticate(req)

        body = json.loads(result.data)
        self.assertEqual(1_700_000_000_000, body["timestamp"])
        self.assertEqual("BTC-INR", body["symbol"])
        expected_sig = hmac.new(b"secret_key", result.data.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(expected_sig, result.headers["x-auth-signature"])

    async def test_empty_credentials_bypass(self):
        auth = ZebpayAuth(api_key="", secret_key="", time_provider=MagicMock())
        req = RESTRequest(method=RESTMethod.GET, url="https://sapi.zebpay.com/api/v2/account/balance")
        result = await auth.rest_authenticate(req)
        self.assertIsNone(result.headers)

    async def test_signature_is_hex(self):
        auth = _make_auth()
        req = RESTRequest(method=RESTMethod.GET, url="https://sapi.zebpay.com/api/v2/account/balance")
        result = await auth.rest_authenticate(req)
        sig = result.headers["x-auth-signature"]
        int(sig, 16)  # raises if not hex
        self.assertEqual(64, len(sig))  # sha256 hex digest


if __name__ == "__main__":
    unittest.main()
