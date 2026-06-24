import asyncio
import base64
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock
from urllib.parse import urlencode

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.exchange.ajaib import ajaib_constants as CONSTANTS
from hummingbot.connector.exchange.ajaib.ajaib_auth import AjaibAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class AjaibAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.seconds = 1700000000.0
        self.ms = 1700000000000

        self._private_key = Ed25519PrivateKey.generate()
        pem = self._private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8")

        time_provider = MagicMock()
        time_provider.time.return_value = self.seconds
        self.auth = AjaibAuth(api_key=self.api_key, secret_key=pem, time_provider=time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _verify(self, signature_b64: str, params: dict):
        payload = urlencode(sorted(params.items())).encode("ascii")
        # Raises InvalidSignature if it does not match.
        self._private_key.public_key().verify(base64.b64decode(signature_b64), payload)

    def test_get_request_signs_sorted_params_in_query(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.kripto.ajaib.co.id/v1/order",
            params={"symbol": "BTC_IDR", "origClientOrderId": "abc"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(self.api_key, request.headers["X-MBX-APIKEY"])
        self.assertEqual(self.ms, request.params["timestamp"])
        self.assertEqual(CONSTANTS.RECV_WINDOW, request.params["recvWindow"])
        self.assertIn("signature", request.params)

        signature = request.params.pop("signature")
        self._verify(signature, request.params)

    def test_post_request_signs_body_and_sets_content_type(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.kripto.ajaib.co.id/v1/order",
            data={"symbol": "BTC_IDR", "side": "BUY", "type": "LIMIT"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual("application/x-www-form-urlencoded", request.headers["Content-Type"])
        self.assertEqual(self.ms, request.data["timestamp"])
        self.assertIn("signature", request.data)

        signature = request.data.pop("signature")
        self._verify(signature, request.data)

    def test_signature_is_deterministic_regardless_of_param_insertion_order(self):
        sig_a = self.auth._sign({"b": "2", "a": "1", "timestamp": self.ms})
        sig_b = self.auth._sign({"timestamp": self.ms, "a": "1", "b": "2"})
        self.assertEqual(sig_a, sig_b)

    def test_header_for_authentication(self):
        self.assertEqual({"X-MBX-APIKEY": self.api_key}, self.auth.header_for_authentication())

    def test_missing_key_returns_empty_signature(self):
        auth = AjaibAuth(api_key="", secret_key="", time_provider=MagicMock())
        self.assertEqual("", auth._sign({"a": "1"}))
