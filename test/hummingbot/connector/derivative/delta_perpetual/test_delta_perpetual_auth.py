import asyncio
import hashlib
import hmac
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

import hummingbot.connector.derivative.delta_perpetual.delta_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.delta_perpetual.delta_perpetual_auth import DeltaPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class DeltaPerpetualAuthTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"
        self.timestamp = 1700000000
        time_provider = MagicMock()
        time_provider.time.return_value = self.timestamp  # seconds
        self.auth = DeltaPerpetualAuth(
            api_key=self.api_key, secret_key=self.secret_key, time_provider=time_provider
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _sign(self, message: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    def test_rest_authenticate_get_with_params(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.india.delta.exchange/v2/orders",
            params={"product_id": "27", "state": "open"},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        query = "?product_id=27&state=open"
        expected = self._sign(f"GET{self.timestamp}/v2/orders{query}")
        self.assertEqual(self.api_key, request.headers["api-key"])
        self.assertEqual(str(self.timestamp), request.headers["timestamp"])
        self.assertEqual(expected, request.headers["signature"])
        self.assertEqual(CONSTANTS.BROKER_ID, request.headers["User-Agent"])

    def test_rest_authenticate_post_with_body(self):
        body = '{"product_id":27,"size":1}'
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.india.delta.exchange/v2/orders",
            data=body,
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected = self._sign(f"POST{self.timestamp}/v2/orders{body}")
        self.assertEqual(expected, request.headers["signature"])
        # No query string contributes to the prehash when there are no params.
        self.assertEqual("application/json", request.headers["Content-Type"])

    def test_rest_authenticate_dict_body_is_serialized(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.india.delta.exchange/v2/orders",
            data={"product_id": 27, "size": 1},
            is_auth_required=True,
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))
        # Dict body must be written back as a compact JSON string for the body to
        # match what is signed.
        self.assertEqual('{"product_id":27,"size":1}', request.data)
        expected = self._sign(f"POST{self.timestamp}/v2/orders{request.data}")
        self.assertEqual(expected, request.headers["signature"])

    def test_ws_auth_payload(self):
        payload = self.auth.ws_auth_payload()
        expected_sig = self._sign(f"GET{self.timestamp}{CONSTANTS.WS_AUTH_PREHASH_PATH}")
        self.assertEqual("auth", payload["type"])
        self.assertEqual(self.api_key, payload["payload"]["api-key"])
        self.assertEqual(str(self.timestamp), payload["payload"]["timestamp"])
        self.assertEqual(expected_sig, payload["payload"]["signature"])

    def test_no_credentials_returns_request_unmodified(self):
        auth = DeltaPerpetualAuth(api_key="", secret_key="", time_provider=MagicMock())
        request = RESTRequest(method=RESTMethod.GET, url="https://api.india.delta.exchange/v2/products")
        result = self.async_run_with_timeout(auth.rest_authenticate(request))
        self.assertNotIn("signature", result.headers or {})
