import unittest
from unittest.mock import MagicMock

from hummingbot.connector.exchange.csx.csx_auth import CsxAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

_VALID_SECRET = "aa" * 32  # 64 hex chars = 32 bytes


def _make_auth(key="test_key", secret=_VALID_SECRET):
    time_provider = MagicMock()
    time_provider.time.return_value = 1_725_010_288.0  # epoch seconds
    return CsxAuth(api_key=key, secret_key=secret, time_provider=time_provider)


class CsxAuthInitTests(unittest.TestCase):

    def test_valid_init(self):
        auth = _make_auth()
        self.assertEqual("test_key", auth.api_key)

    def test_invalid_secret_not_hex(self):
        with self.assertRaises(ValueError, msg="Should reject non-hex secret"):
            _make_auth(secret="zz" * 32)

    def test_invalid_secret_wrong_length(self):
        with self.assertRaises(ValueError, msg="Should reject wrong-length secret"):
            _make_auth(secret="aa" * 16)

    def test_empty_credentials_skips_private_key(self):
        auth = CsxAuth(api_key="", secret_key="", time_provider=MagicMock())
        self.assertIsNone(auth._private_key)


class CsxAuthSignatureTests(unittest.IsolatedAsyncioTestCase):

    async def test_rest_authenticate_get_adds_headers(self):
        auth = _make_auth()
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://exchange.coinswitch.co/api/v2/me/balance/",
        )
        result = await auth.rest_authenticate(request)
        self.assertIn("CSX-ACCESS-KEY", result.headers)
        self.assertIn("CSX-SIGNATURE", result.headers)
        self.assertIn("CSX-ACCESS-TIMESTAMP", result.headers)
        # Timestamp is back-dated by REQUEST_TIME_BUFFER_S to avoid "future time" rejections.
        from hummingbot.connector.exchange.csx.csx_auth import REQUEST_TIME_BUFFER_S
        self.assertEqual(str(1725010288 - REQUEST_TIME_BUFFER_S),
                         result.headers["CSX-ACCESS-TIMESTAMP"])

    async def test_rest_authenticate_post_sorts_body(self):
        import json
        auth = _make_auth()
        body = {"type": "LIMIT", "side": "BUY", "instrument": "BTC/INR"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://exchange.coinswitch.co/api/v2/orders/",
            data=body,
        )
        result = await auth.rest_authenticate(request)
        parsed = json.loads(result.data)
        keys = list(parsed.keys())
        self.assertEqual(sorted(keys), keys, "Body keys must be alphabetically sorted")

    async def test_rest_authenticate_empty_creds_returns_unmodified(self):
        auth = CsxAuth(api_key="", secret_key="", time_provider=MagicMock())
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://exchange.coinswitch.co/api/v2/me/balance/",
        )
        result = await auth.rest_authenticate(request)
        self.assertIsNone(result.headers)

    async def test_signature_is_hex_string(self):
        auth = _make_auth()
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://exchange.coinswitch.co/api/v2/me/balance/",
        )
        result = await auth.rest_authenticate(request)
        sig = result.headers["CSX-SIGNATURE"]
        # Must be a valid hex string (128 hex chars for 64-byte Ed25519 sig)
        int(sig, 16)
        self.assertEqual(128, len(sig))


if __name__ == "__main__":
    unittest.main()
