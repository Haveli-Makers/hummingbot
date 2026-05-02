import asyncio
import unittest
from unittest.mock import MagicMock

# Ed25519 private key: exactly 32 bytes expressed as 64 hex chars.
_VALID_HEX_KEY = "aa" * 32
# Fixed epoch in seconds; auth will multiply by 1000 to get ms.
_FIXED_TS_SECONDS = 1_000_000.0
_FIXED_EPOCH_MS = 1_000_000_000  # 1_000_000 * 1000


def _make_time_provider(ts_seconds: float = _FIXED_TS_SECONDS) -> MagicMock:
    """Return a mock whose .time() method returns a deterministic float."""
    mock = MagicMock()
    mock.time.return_value = ts_seconds
    return mock


class CoinswitchAuthTests(unittest.TestCase):
    """Test cases for CoinSwitch authentication."""

    # ------------------------------------------------------------------ helpers

    def _import_auth(self):
        try:
            from hummingbot.connector.exchange.coinswitch.coinswitch_auth import CoinswitchAuth
            return CoinswitchAuth
        except (ImportError, ModuleNotFoundError) as e:
            self.skipTest(f"Skipping due to missing dependency: {e}")

    def _make_auth(self, api_key="test_api_key", secret_key=_VALID_HEX_KEY, ts=_FIXED_TS_SECONDS):
        CoinswitchAuth = self._import_auth()
        return CoinswitchAuth(api_key=api_key, secret_key=secret_key, time_provider=_make_time_provider(ts))

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    # --------------------------------------------------------------- init tests

    def test_auth_module_imports(self):
        CoinswitchAuth = self._import_auth()
        self.assertIsNotNone(CoinswitchAuth)

    def test_auth_initialization_empty_credentials(self):
        """Empty credentials are accepted (used for public-only connectors)."""
        CoinswitchAuth = self._import_auth()
        auth = CoinswitchAuth(api_key="", secret_key="", time_provider=_make_time_provider())
        self.assertIsNotNone(auth)

    def test_auth_initialization_valid_key(self):
        """Valid 64-char hex key stores api_key on the instance."""
        auth = self._make_auth()
        self.assertEqual(auth.api_key, "test_api_key")

    def test_auth_invalid_key_length_raises(self):
        """A key shorter than 32 bytes must raise ValueError."""
        CoinswitchAuth = self._import_auth()
        with self.assertRaises(ValueError):
            CoinswitchAuth(
                api_key="k",
                secret_key="aa" * 16,  # 16 bytes — too short
                time_provider=_make_time_provider(),
            )

    # ------------------------------------------------------- rest_authenticate

    def test_rest_authenticate_get_sets_required_headers(self):
        """GET rest_authenticate must produce X-AUTH-APIKEY, X-AUTH-SIGNATURE, X-AUTH-EPOCH."""
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

        auth = self._make_auth()
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://coinswitch.co/trade/api/v2/24hr/all-pairs/ticker",
            params={"exchange": "coinswitchx"},
        )
        result = self._run(auth.rest_authenticate(request))

        self.assertIn("X-AUTH-APIKEY", result.headers)
        self.assertIn("X-AUTH-SIGNATURE", result.headers)
        self.assertIn("X-AUTH-EPOCH", result.headers)
        self.assertEqual(result.headers["X-AUTH-APIKEY"], "test_api_key")

    def test_rest_authenticate_get_epoch_matches_time_provider(self):
        """The X-AUTH-EPOCH header value must equal time_provider.time() * 1000."""
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

        auth = self._make_auth(ts=_FIXED_TS_SECONDS)
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://coinswitch.co/trade/api/v2/24hr/all-pairs/ticker",
        )
        result = self._run(auth.rest_authenticate(request))
        self.assertEqual(int(result.headers["X-AUTH-EPOCH"]), _FIXED_EPOCH_MS)

    def test_rest_authenticate_get_signature_is_valid_hex(self):
        """Ed25519 signature is 64 bytes → 128 hex chars."""
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

        auth = self._make_auth()
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://coinswitch.co/trade/api/v2/24hr/all-pairs/ticker",
            params={"exchange": "coinswitchx"},
        )
        result = self._run(auth.rest_authenticate(request))
        sig = result.headers["X-AUTH-SIGNATURE"]
        self.assertEqual(len(sig), 128)
        self.assertTrue(all(c in "0123456789abcdefABCDEF" for c in sig))

    def test_rest_authenticate_post_excludes_epoch_header(self):
        """POST requests must NOT include X-AUTH-EPOCH (per CoinSwitch spec)."""
        import json
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

        auth = self._make_auth()
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://coinswitch.co/trade/api/v2/order",
            data=json.dumps({"symbol": "BTC/INR", "side": "buy"}),
        )
        result = self._run(auth.rest_authenticate(request))
        self.assertNotIn("X-AUTH-EPOCH", result.headers)
        self.assertIn("X-AUTH-SIGNATURE", result.headers)
        self.assertIn("X-AUTH-APIKEY", result.headers)

    def test_rest_authenticate_signature_is_deterministic(self):
        """Same inputs produce the same signature (Ed25519 is deterministic)."""
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

        auth = self._make_auth()
        make_req = lambda: RESTRequest(
            method=RESTMethod.GET,
            url="https://coinswitch.co/trade/api/v2/24hr/all-pairs/ticker",
            params={"exchange": "coinswitchx"},
        )
        sig1 = self._run(auth.rest_authenticate(make_req())).headers["X-AUTH-SIGNATURE"]
        # Reset nonce so the second call gets the same timestamp.
        auth._last_timestamp = 0
        sig2 = self._run(auth.rest_authenticate(make_req())).headers["X-AUTH-SIGNATURE"]
        self.assertEqual(sig1, sig2)

    # -------------------------------------------------------- ws_authenticate

    def test_ws_authenticate_sets_required_headers(self):
        """ws_authenticate must set X-AUTH-APIKEY, X-AUTH-SIGNATURE, X-AUTH-EPOCH."""
        from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest

        auth = self._make_auth()
        ws_request = WSJSONRequest(payload={}, is_auth_required=True)
        result = self._run(auth.ws_authenticate(ws_request))

        self.assertIn("X-AUTH-APIKEY", result.headers)
        self.assertIn("X-AUTH-SIGNATURE", result.headers)
        self.assertIn("X-AUTH-EPOCH", result.headers)
        self.assertEqual(result.headers["X-AUTH-APIKEY"], "test_api_key")

    def test_ws_authenticate_signature_is_valid_hex(self):
        """WS signature is 64 bytes → 128 hex chars."""
        from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest

        auth = self._make_auth()
        ws_request = WSJSONRequest(payload={}, is_auth_required=True)
        result = self._run(auth.ws_authenticate(ws_request))
        sig = result.headers["X-AUTH-SIGNATURE"]
        self.assertEqual(len(sig), 128)
        self.assertTrue(all(c in "0123456789abcdefABCDEF" for c in sig))


if __name__ == "__main__":
    unittest.main()
